"""
Human-in-the-loop handoff for WhatsApp (transform_llm_output hook).

When the WhatsApp AI is about to reply to a customer, it self-reports a confidence
score via a trailing marker in its text (injected by a system-prompt instruction —
see after-install.md / config platform_hints):

    <hermes-confidence>0.42</hermes-confidence>

This hook, fired in the agent process right after the reply is generated and
BEFORE it is sent (agent/turn_finalizer.py), does the following ONLY for WhatsApp:

  1. Parse + strip the confidence marker from the reply.
  2. Decide whether to escalate:
       - confidence < HANDOFF_CONFIDENCE_THRESHOLD, OR
       - the customer explicitly asked for a human, OR
       - (best-effort) the KB has no good match for the question.
  3. If escalating: create a ticket on the client-local API, post a handoff card
     to the admin group (with the suggested reply), attach the handoff message id
     for quote-reply correlation, and RETURN a holding message so the low-
     confidence answer never auto-sends.
  4. If not escalating: return the reply with the marker stripped.

Correlation with the admin's later quote-reply is by message id (handoff_msg_id),
resolved by the pre_gateway_dispatch hook in plugin.py — NOT by parsing text.

All failures are swallowed: a handoff-path error must never break a normal reply.
The hook returns the marker-stripped text on any error so the customer still gets
an answer.
"""

import os
import re
import json
import sqlite3
import urllib.request
import urllib.error

# --- config ----------------------------------------------------------------
CLIENT_API_URL = (os.environ.get("CLIENT_API_URL") or "http://127.0.0.1:8787").rstrip("/")
CLIENT_API_SECRET = os.environ.get("HUB_CLIENT_SECRET", "") or ""
STATE_DB_PATH = os.environ.get("STATE_DB_PATH") or os.path.expanduser("~/.hermes/state.db")
BRIDGE_URL = (os.environ.get("BRIDGE_URL") or "http://127.0.0.1:3000").rstrip("/")

CONFIDENCE_THRESHOLD = float(os.environ.get("HANDOFF_CONFIDENCE_THRESHOLD", "0.6"))
KB_SCORE_FLOOR = float(os.environ.get("HANDOFF_KB_SCORE_FLOOR", "0.0"))  # 0 disables KB gate
HOLDING_MESSAGE = os.environ.get(
    "HANDOFF_HOLDING_MESSAGE",
    "Thanks for your message! Let me check on that and a team member will get back to you shortly. 🙏",
)

_CONF_RE = re.compile(r"<hermes-confidence>\s*([0-9]*\.?[0-9]+)\s*</hermes-confidence>", re.I)
# Explicit human-handoff intent from the customer's own message.
_HUMAN_RE = re.compile(
    r"\b(speak|talk|chat)\s+(to|with)\s+(a\s+)?(human|person|agent|someone|staff|manager)"
    r"|\breal person\b|\bhuman (agent|being|please)\b|\bcustomer service\b",
    re.I,
)


# --- helpers ---------------------------------------------------------------
def _api(method, path, body=None, timeout=8):
    """Authenticated request to the client-local API. Returns parsed JSON or None."""
    if not CLIENT_API_SECRET:
        return None
    url = CLIENT_API_URL + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", "Bearer " + CLIENT_API_SECRET)
    req.add_header("User-Agent", "whatsapp-handoff/1.0")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:  # pragma: no cover - network best-effort
        print(f"[handoff] api {method} {path} failed (ignored): {e}")
        return None


def _bridge_send(chat_id, message, timeout=15):
    """Post a message to the admin group via the Baileys bridge. Returns messageId."""
    url = BRIDGE_URL + "/send"
    data = json.dumps({"chatId": chat_id, "message": message}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            res = json.loads(resp.read().decode("utf-8"))
            if res.get("success") is False:
                return None
            return res.get("messageId")
    except Exception as e:  # pragma: no cover
        print(f"[handoff] bridge send failed (ignored): {e}")
        return None


def _session_context(session_id):
    """Look up the customer chat_id, name, latest inbound message id + text from the
    read-only state.db. Returns a dict or None. Never writes/locks state.db."""
    try:
        conn = sqlite3.connect(f"file:{STATE_DB_PATH}?mode=ro", uri=True, timeout=3)
        conn.row_factory = sqlite3.Row
        try:
            srow = conn.execute(
                "SELECT chat_id, chat_type, display_name FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            if not srow or not srow["chat_id"]:
                return None
            # Latest inbound (user) message in this session → the question + badge anchor.
            mrow = conn.execute(
                "SELECT id, content FROM messages WHERE session_id = ? AND role = 'user' "
                "AND active = 1 AND (tool_call_id IS NULL OR tool_call_id = '') "
                "ORDER BY id DESC LIMIT 1",
                (session_id,),
            ).fetchone()
            return {
                "chat_id": srow["chat_id"],
                "chat_type": srow["chat_type"],
                "display_name": srow["display_name"],
                "source_msg_id": (mrow["id"] if mrow else None),
                "question": (mrow["content"] if mrow else None),
            }
        finally:
            conn.close()
    except Exception as e:  # pragma: no cover
        print(f"[handoff] session context lookup failed (ignored): {e}")
        return None


def _admin_group():
    """Resolve an admin group JID to post the handoff card to. Uses the plugin's
    state.yaml `admin_groups` (first entry). Falls back to env HANDOFF_ADMIN_GROUP."""
    env = os.environ.get("HANDOFF_ADMIN_GROUP")
    if env:
        return env
    try:
        from . import plugin as _plugin  # reuse the plugin's state loader

        groups = _plugin.load_set_from_file(_plugin.ADMIN_GROUPS_FILE)
        return sorted(groups)[0] if groups else None
    except Exception:
        return None


def _kb_best_score(question, chat_context_internal=False):
    """Best-effort KB score for the customer's question (customer tier). Returns the
    top hit's score, or None if unavailable. Used as a secondary escalation signal."""
    if not question or KB_SCORE_FLOOR <= 0:
        return None
    q = urllib.request.quote(question[:500])
    res = _api("GET", f"/api/v1/kb/search?q={q}&audience_max=customer&limit=1")
    if not res or not res.get("ok"):
        return None
    hits = res.get("hits") or []
    return hits[0]["score"] if hits else 0.0


def _format_card(ref, name, question, suggested, confidence):
    conf_pct = f"{round(confidence * 100)}%" if confidence is not None else "?"
    lines = [
        f"🎫 *#{ref}* · Customer needs a reply",
        f"👤 {name}",
        f'❓ "{(question or "").strip()[:600]}"',
        "",
        f"💡 *Suggested reply* (AI confidence {conf_pct}):",
        (suggested or "").strip()[:1200] or "(no suggestion)",
        "",
        '↩️ *Quote-reply this message* to send. Type "ok" to send as-is, '
        "or write your own reply and I'll relay it.",
    ]
    return "\n".join(lines)


# --- resolve (admin quote-reply → send answer to customer) -----------------
# "Approve as-is" triggers: an admin quote-reply whose text is one of these means
# "send the suggested reply verbatim". Anything else is treated as a correction.
_APPROVE_WORDS = {"ok", "okay", "ok.", "yes", "y", "send", "approve", "approved", "👍", "✅", "ok!"}


def try_resolve_from_quote(event):
    """If this inbound message is an admin quote-reply to an open handoff card,
    resolve the ticket (send the answer to the customer) and return a short status
    string to post back to the admin group. Returns None if it's not a handoff
    quote-reply (so the caller lets normal handling continue).

    Correlation is by message id: quotedMessageId == ticket.handoffMsgId. This is
    called from the pre_gateway_dispatch hook for messages in an admin group.
    """
    try:
        raw = getattr(event, "raw_message", None)
        if not isinstance(raw, dict):
            return None
        quoted_id = raw.get("quotedMessageId")
        if not quoted_id:
            return None

        # Find the OPEN ticket whose handoff card == the quoted message (by id).
        corr = _api(
            "GET",
            "/api/v1/tickets/by-handoff/" + urllib.request.quote(str(quoted_id)),
        )
        if not corr or not corr.get("ok") or not corr.get("ticket"):
            return None
        ticket = corr["ticket"]

        admin_reply = (getattr(event, "text", "") or "").strip()
        raw_prefix = admin_reply.lower().startswith("raw:")
        if raw_prefix:
            admin_reply = admin_reply[4:].strip()

        if admin_reply.lower() in _APPROVE_WORDS:
            body = {"mode": "approve", "via": "whatsapp"}
        else:
            # Correction: rewrite into the bot's voice unless the admin used `raw:`.
            body = {
                "mode": "custom",
                "text": admin_reply,
                "rewrite": not raw_prefix,
                "via": "whatsapp",
            }
        res = _api("POST", f"/api/v1/tickets/{ticket['id']}/resolve", body=body)
        if res and res.get("ok"):
            return f"✅ Sent to {ticket.get('customerName') or 'customer'} (#{ticket['ref']})."
        err = (res or {}).get("error", "unknown error")
        return f"⚠️ Couldn't send (#{ticket['ref']}): {err}"
    except Exception as e:  # pragma: no cover
        print(f"[handoff] resolve-from-quote failed (ignored): {e}")
        return None


# --- the hook --------------------------------------------------------------
def transform_llm_output(response_text=None, session_id="", model="", platform="", **kwargs):
    """See module docstring. Returns the (possibly replaced) reply text, or None to
    leave it unchanged. Only acts on WhatsApp."""
    try:
        text = response_text or ""
        # Only WhatsApp. Other platforms pass through untouched (return None).
        if str(platform).lower() != "whatsapp":
            return None

        # Parse + strip the confidence marker.
        m = _CONF_RE.search(text)
        confidence = None
        if m:
            try:
                confidence = max(0.0, min(1.0, float(m.group(1))))
            except ValueError:
                confidence = None
        stripped = _CONF_RE.sub("", text).strip()

        ctx = _session_context(session_id) or {}
        question = ctx.get("question")

        # Decide whether to escalate.
        explicit_human = bool(question and _HUMAN_RE.search(question))
        low_conf = confidence is not None and confidence < CONFIDENCE_THRESHOLD
        kb_score = _kb_best_score(question) if (low_conf or explicit_human) else None
        kb_weak = kb_score is not None and kb_score < KB_SCORE_FLOOR

        escalate = low_conf or explicit_human or kb_weak
        if not escalate or not ctx.get("chat_id"):
            # Normal path: send the AI's reply (marker stripped).
            return stripped or None

        # --- escalate: create ticket + post to admin group ---
        name = ctx.get("display_name") or ctx["chat_id"].split("@")[0]
        admin_group = _admin_group()

        created = _api(
            "POST",
            "/api/v1/tickets",
            body={
                "customerChat": ctx["chat_id"],
                "customerName": name,
                "question": question or "(question unavailable)",
                "suggestedReply": stripped,
                "confidence": (round(confidence * 100) if confidence is not None else None),
                "sourceMsgId": ctx.get("source_msg_id"),
                "adminGroup": admin_group,
                "channel": "whatsapp",
            },
        )
        if not created or not created.get("ok"):
            # Couldn't create the ticket — fail safe by sending the AI's reply so the
            # customer isn't left hanging (better than silence).
            print("[handoff] ticket create failed; falling back to auto-reply")
            return stripped or None

        ticket = created["ticket"]

        # Post the handoff card to the admin group and correlate by message id.
        if admin_group:
            card = _format_card(ticket["ref"], name, question, stripped, confidence)
            handoff_msg_id = _bridge_send(admin_group, card)
            if handoff_msg_id:
                _api(
                    "PATCH",
                    f"/api/v1/tickets/{ticket['id']}/handoff",
                    body={"handoffMsgId": handoff_msg_id},
                )
            else:
                print("[handoff] posted no admin card (bridge send failed)")
        else:
            print("[handoff] no admin group configured; ticket created but not posted")

        # Replace the outgoing reply with a holding message so the low-confidence
        # answer never reaches the customer.
        return HOLDING_MESSAGE

    except Exception as e:  # pragma: no cover - never break the reply path
        print(f"[handoff] hook crashed (ignored): {e}")
        # Best effort: strip any marker so the customer never sees it.
        try:
            return _CONF_RE.sub("", response_text or "").strip() or None
        except Exception:
            return None
