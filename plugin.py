import os
import json
import time
import asyncio
import yaml
import threading

PLUGIN_DIR = os.path.dirname(__file__)

# ---------------------------------------------------------------------------
# Consolidated runtime state (single file, hot-editable).
# Replaces the old whitelist.txt / admins.txt / admin_groups.txt / paused_chats.json.
# Sections:
#   root_admin      : str  — permanent admin, never removable
#   admins          : list — extra admins (/admin add|remove)
#   reply_whitelist : list — chats the bot REPLIES in (/whitelist add|remove);
#                            non-whitelisted chats are still stored silently
#   admin_groups    : list — groups where ops/slash commands are allowed
#   no_mention_groups : list — whitelisted groups EXEMPT from the mention
#                            requirement (bot replies without a mention).
#                            Default: whitelisted groups require a @mention.
#                            (/no-mention add|remove). Keep core config.yaml
#                            require_mention: false so messages still reach here.
#   paused_chats    : dict — chat_id -> reason (pause/resume + handoff)
# ---------------------------------------------------------------------------
STATE_FILE = os.path.join(PLUGIN_DIR, "state.yaml")
_STATE_LOCK = threading.Lock()

# --- cupbots-hub sync bridge (optional) -------------------------------------
# Mirrors local state.yaml changes UP to the hub (fire-and-forget). Entirely
# optional: if hub_sync isn't importable or the hub isn't configured, these calls
# are no-ops and NEVER affect message handling. The local write above each call is
# always the source of truth; the hub is a best-effort mirror.
#
# hub_sync now ships INSIDE this plugin (in-package import), so it's available
# even on standalone boxes with no separate hub-sync install. The credentials
# still live in ~/.hermes/hub-sync/.env (written by the repo's install.sh for
# hub-managed clients) — we load that .env if present, else hub_sync stays
# disabled (enabled() is False without HUB_URL/HUB_CLIENT_SECRET) and no-ops.
_HUB_SYNC = None
try:
    # Load the hub .env (if a hub client set one up) so HUB_URL / HUB_CLIENT_SECRET
    # reach hub_sync. Absent on standalone boxes — that's fine, hub_sync no-ops.
    _envp = os.path.join(os.path.expanduser("~/.hermes"), "hub-sync", ".env")
    if os.path.exists(_envp):
        for _l in open(_envp, "r", encoding="utf-8"):
            _l = _l.strip()
            if _l and not _l.startswith("#") and "=" in _l:
                _k, _v = _l.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))
    # Prefer the in-package hub_sync (ships with this plugin); fall back to a
    # legacy standalone install at ~/.hermes/hub-sync for older layouts.
    try:
        from . import hub_sync as _HUB_SYNC  # type: ignore  # noqa: N816
    except Exception:
        _hs_dir = os.path.join(os.path.expanduser("~/.hermes"), "hub-sync")
        if os.path.isdir(_hs_dir):
            import sys as _sys
            if _hs_dir not in _sys.path:
                _sys.path.insert(0, _hs_dir)
            import hub_sync as _HUB_SYNC  # noqa: N816
except Exception as _e:
    print(f"[whatsapp-listener] hub-sync not loaded (ignored): {_e}")
    _HUB_SYNC = None


def _hub_flag_for(section):
    if _HUB_SYNC is None:
        return None
    return _HUB_SYNC.SECTION_FLAG.get(section)


def _hub_push_section_diff(section, old_set, new_set):
    """Fire-and-forget: push whichever jids gained/lost this section's flag."""
    if _HUB_SYNC is None:
        return
    flag = _hub_flag_for(section)
    if not flag:
        return
    try:
        import time as _t
        now = int(_t.time())
        deltas = []
        for j in (new_set - old_set):
            deltas.append({"jid": j, flag: True, "updatedAt": now})
        for j in (old_set - new_set):
            deltas.append({"jid": j, flag: False, "updatedAt": now})
        if deltas:
            _HUB_SYNC.push_deltas(deltas, source="plugin")
    except Exception as e:
        print(f"[whatsapp-listener] hub push (section) ignored: {e}")


def _hub_push_paused_diff(old_map, new_map):
    """Fire-and-forget: push paused/resumed jids to the hub."""
    if _HUB_SYNC is None:
        return
    try:
        import time as _t
        now = int(_t.time())
        deltas = []
        for j, reason in new_map.items():
            if j not in old_map:
                deltas.append({"jid": j, "paused": True, "pauseReason": str(reason), "updatedAt": now})
        for j in old_map:
            if j not in new_map:
                deltas.append({"jid": j, "paused": False, "updatedAt": now})
        if deltas:
            _HUB_SYNC.push_deltas(deltas, source="plugin")
    except Exception as e:
        print(f"[whatsapp-listener] hub push (paused) ignored: {e}")


# jids we've already attempted a name-backfill for this process (once each).
_HUB_NAME_SEEN = set()


def _hub_backfill_name(chat_id, sender_name=None):
    """Fire-and-forget: on an incoming message, push a readable name for this chat
    to the hub. Runs at most ONCE per jid per process (hot-path safe). No-op if
    hub-sync is off or the jid was already handled.

    - GROUP (`@g.us`): resolve the group's subject via the local WA endpoint.
    - DM (`@lid` / `@s.whatsapp.net`): use `sender_name` (the sender's WhatsApp
      pushName, already on the event as source.user_name) if present. WhatsApp does
      NOT give us your saved contact name or the phone number — pushName is the best
      available, and only when the sender set one. Users can override any name in the
      hub UI (source='human' wins)."""
    if _HUB_SYNC is None or not chat_id:
        return
    if chat_id in _HUB_NAME_SEEN:
        return
    is_group = str(chat_id).endswith("@g.us")
    # For a DM with no pushName there's nothing useful to send — skip so we don't
    # create a numeric-only row. (Try again next process in case a name shows up.)
    sender_name = (sender_name or "").strip() or None
    if not is_group and not sender_name:
        return
    _HUB_NAME_SEEN.add(chat_id)
    try:
        import time as _t
        delta = {"jid": chat_id, "updatedAt": int(_t.time())}
        if is_group:
            # group subject resolved by hub_sync via the local /chat endpoint.
            _HUB_SYNC.push_deltas([delta], source="plugin", resolve_names=True)
        else:
            # DM: use the pushName from the event; don't hit the /chat endpoint.
            delta["name"] = sender_name
            _HUB_SYNC.push_deltas([delta], source="plugin", resolve_names=False)
    except Exception as e:
        print(f"[whatsapp-listener] hub name backfill ignored: {e}")

# Logical section names used in place of the old per-file constants, so the
# existing call sites (load_set_from_file(WHITELIST_FILE), etc.) keep working.
WHITELIST_FILE = "reply_whitelist"
ADMINS_FILE = "admins"
ADMIN_GROUPS_FILE = "admin_groups"
# Default behaviour: whitelisted GROUPS require a direct @mention before the bot
# replies (safe default). This allowlist names the groups that are EXEMPT — the
# bot replies to every message in them without a mention (/no-mention add|remove).
# Only meaningful for chats already in reply_whitelist. DMs are never mention-gated.
NO_MENTION_FILE = "no_mention_groups"


def _load_state() -> dict:
    """Load the consolidated state.yaml (returns {} if missing/broken)."""
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if isinstance(data, dict):
                return data
    except Exception as e:
        print(f"[whatsapp-listener] Failed to read state.yaml: {e}")
    return {}


def _save_state(state: dict) -> None:
    """Persist the consolidated state.yaml, preserving the header comments."""
    try:
        header = (
            "# whatsapp-listener consolidated state (hot-editable at runtime)\n"
            "# root_admin: permanent admin, never removable\n"
            "# admins: extra admins (/admin add|remove)\n"
            "# reply_whitelist: chats the bot REPLIES in (/whitelist add|remove); others are still stored silently\n"
            "# admin_groups: groups where ops/slash commands are allowed\n"
            "# no_mention_groups: whitelisted groups EXEMPT from the mention requirement (/no-mention add|remove)\n"
            "#   Default: whitelisted GROUPS require a direct @mention; listed groups reply without one.\n"
            "#   NOTE: keep core config.yaml `require_mention: false`. If core has require_mention: true,\n"
            "#   core drops non-mention group messages upstream before this plugin runs, so the plugin can\n"
            "#   neither reply nor silently store them and this list can never fire.\n"
            "# paused_chats: chat_id -> reason (managed by pause/resume + handoff)\n"
        )
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            f.write(header)
            yaml.safe_dump(state, f, sort_keys=False, allow_unicode=True, default_flow_style=False)
    except Exception as e:
        print(f"[whatsapp-listener] Failed to write state.yaml: {e}")


# Root admin now lives in state.yaml (falls back to the historical default so
# an empty/missing file never locks the operator out).
ROOT_ADMIN_ID = str(_load_state().get("root_admin") or "YOUR_NUMBER@s.whatsapp.net")

# Detect the gateway/bot identity (the number used by this WhatsApp gateway)
# so we can match user mentions like "@<bot-number>".
def _load_bot_identity_hint() -> dict:
    try:
        # creds.json lives under ~/.hermes/whatsapp/session/creds.json
        creds_path = os.path.join(os.path.expanduser("~/.hermes"), "whatsapp", "session", "creds.json")
        if not os.path.exists(creds_path):
            return {}
        import json as _json
        me = _json.load(open(creds_path, "r", encoding="utf-8")).get("me") or {}
        bot_id = me.get("id") or ""
        bot_name = (me.get("name") or "").strip()
        bot_numeric = str(bot_id).split("@", 1)[0]
        bot_lid = me.get("lid") or ""
        bot_lid_numeric = str(bot_lid).split("@", 1)[0] if bot_lid else ""
        return {
            "bot_id": bot_id, 
            "bot_name": bot_name, 
            "bot_numeric": bot_numeric,
            "bot_lid": bot_lid,
            "bot_lid_numeric": bot_lid_numeric.split(":")[0] if bot_lid_numeric else ""
        }
    except Exception:
        return {}


_BOT_HINT = _load_bot_identity_hint()


def _extract_mention_meta(event) -> dict:
    """Best-effort extraction of mention-related fields from the WhatsApp event.

    The WhatsApp bridge may strip the visible mention from `event.text` and only
    provide mention metadata separately. We try to locate common keys/attributes.
    """
    try:
        # If event is dict-like.
        if isinstance(event, dict):
            items = event.items()
            out = {}
            for k, v in items:
                if not (k and isinstance(k, str)):
                    continue
                k_l = k.lower()
                if "mention" in k_l or "mentioned" in k_l or "mentions" in k_l:
                    out[k] = v
            return out

        # Otherwise inspect attributes.
        out = {}
        for k in dir(event):
            if not isinstance(k, str):
                continue
            k_l = k.lower()
            if not ("mention" in k_l or "mentioned" in k_l or "mentions" in k_l):
                continue
            try:
                v = getattr(event, k)
                out[k] = v
            except Exception:
                pass
        return out
    except Exception:
        return {}


def _event_indicates_bot_mention(event, *, bot_numeric: str, bot_name: str, bot_id: str = "", bot_lid_numeric: str = "") -> bool:
    """Check mention metadata (if any) to decide if the bot was tagged."""
    try:
        # Direct check for mentionedIds or direct replies from WhatsApp bridge
        try:
            if hasattr(event, "raw_message") and isinstance(event.raw_message, dict):
                # 1. Check if the user explicitly replied to a message sent by the bot
                quoted_participant = event.raw_message.get("quotedParticipant")
                if quoted_participant:
                    q_str = str(quoted_participant)
                    if (bot_numeric and bot_numeric in q_str) or \
                       (bot_id and bot_id in q_str) or \
                       (bot_lid_numeric and bot_lid_numeric in q_str):
                        return True
                
                # 2. Check if the bot was mentioned in the text (via metadata)
                mentioned = event.raw_message.get("mentionedIds") or []
                for m in mentioned:
                    m_str = str(m)
                    if bot_numeric and bot_numeric in m_str:
                        return True
                    if bot_id and bot_id in m_str:
                        return True
                    if bot_lid_numeric and bot_lid_numeric in m_str:
                        return True
        except Exception:
            pass
            
        meta = _extract_mention_meta(event)
        if not meta:
            return False

        # Normalize candidate strings.
        needles = set()
        if bot_numeric:
            needles.add(bot_numeric)
            needles.add("@" + bot_numeric)
        if bot_name:
            needles.add(bot_name.lower())
            needles.add("@" + bot_name.lower())
        if bot_id:
            needles.add(bot_id)

        def val_to_str(v) -> str:
            try:
                if isinstance(v, (list, tuple, set)):
                    return " ".join([str(x) for x in v])
                return str(v)
            except Exception:
                return ""

        for _, v in meta.items():
            # bool-like directly
            if isinstance(v, bool):
                if v:
                    return True
                continue

            s = val_to_str(v)
            s_l = s.lower()
            for n in needles:
                n_l = n.lower()
                if n_l and n_l in s_l:
                    return True

        return False
    except Exception:
        return False

def load_set_from_file(section):
    """Read a list-valued section of state.yaml as a set.

    `section` is one of the WHITELIST_FILE / ADMINS_FILE / ADMIN_GROUPS_FILE
    logical names (kept for call-site compatibility with the old .txt files).
    """
    with _STATE_LOCK:
        state = _load_state()
    items = state.get(section) or []
    if not isinstance(items, list):
        return set()
    return {str(x).strip() for x in items if str(x).strip()}

def save_set_to_file(section, data_set):
    """Write a set back into a list-valued section of state.yaml (atomic-ish)."""
    new_set = {str(x).strip() for x in data_set if str(x).strip()}
    with _STATE_LOCK:
        state = _load_state()
        old_set = {str(x).strip() for x in (state.get(section) or []) if str(x).strip()}
        state[section] = sorted(new_set)
        _save_state(state)
    # Mirror the change to the hub AFTER the local write succeeds (best-effort).
    _hub_push_section_diff(section, old_set, new_set)

def get_all_admins():
    admins = load_set_from_file(ADMINS_FILE)
    # Always include the root admin
    if ROOT_ADMIN_ID and ROOT_ADMIN_ID != "YOUR_NUMBER@s.whatsapp.net":
        admins.add(ROOT_ADMIN_ID)
    return admins

def load_paused_chats() -> dict:
    with _STATE_LOCK:
        state = _load_state()
    pc = state.get("paused_chats") or {}
    return pc if isinstance(pc, dict) else {}

def save_paused_chats(paused_chats: dict):
    with _STATE_LOCK:
        state = _load_state()
        old_map = dict(state.get("paused_chats") or {})
        state["paused_chats"] = paused_chats
        _save_state(state)
    # Mirror pause/resume to the hub AFTER the local write succeeds (best-effort).
    _hub_push_paused_diff(old_map, dict(paused_chats or {}))

def is_chat_in_handoff(chat_id: str) -> tuple:
    """Check if the chat has any open order in a handoff state.
    Returns (is_in_handoff, reason_or_empty)
    """
    if not chat_id:
        return False, ""
    try:
        db_path = "/mnt/storage/projects/carbongpt/yltc-whatsapp-ordering-ai/data/concierge.db"
        if not os.path.exists(db_path):
            return False, ""
        
        import sqlite3
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            chat_norm = chat_id.split("@")[0]
            
            # Find client for this chat
            client_row = conn.execute(
                "SELECT id, business_name FROM clients WHERE whatsapp_chat_id = ? OR whatsapp_chat_id LIKE ?",
                (chat_id, f"%{chat_norm}%")
            ).fetchone()
            
            if client_row:
                client_id = client_row["id"]
                
                # Check for an active handoff order
                order_row = conn.execute(
                    "SELECT id, status FROM orders WHERE client_id = ? AND (human_handoff = 1 OR status = 'handoff') "
                    "ORDER BY updated_at DESC LIMIT 1",
                    (client_id,)
                ).fetchone()
                
                if order_row:
                    # Let's find the audit note / reason
                    audit_row = conn.execute(
                        "SELECT note FROM order_audit_logs WHERE order_id = ? AND action = 'handoff' "
                        "ORDER BY id DESC LIMIT 1",
                        (order_row["id"],)
                    ).fetchone()
                    reason = audit_row["note"] if audit_row else "Flagged for handoff"
                    return True, f"Order #{order_row['id']} handoff: {reason}"
                    
    except Exception:
        pass
    return False, ""

def is_manually_paused(chat_id: str) -> tuple:
    try:
        paused_chats = load_paused_chats()
        if chat_id in paused_chats:
            return True, paused_chats[chat_id]
    except Exception:
        pass
    return False, ""

_KNOWN_PAUSED_CHATS = set()

def _init_known_paused():
    global _KNOWN_PAUSED_CHATS
    if _KNOWN_PAUSED_CHATS:
        return
    try:
        # 1. Manual pauses
        paused_chats = load_paused_chats()
        for cid in paused_chats.keys():
            _KNOWN_PAUSED_CHATS.add(cid)
            _KNOWN_PAUSED_CHATS.add(cid.split("@")[0])
        # 2. Database handoffs
        db_path = "/mnt/storage/projects/carbongpt/yltc-whatsapp-ordering-ai/data/concierge.db"
        if os.path.exists(db_path):
            import sqlite3
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT whatsapp_chat_id FROM clients c "
                    "JOIN orders o ON o.client_id = c.id "
                    "WHERE o.human_handoff = 1 OR o.status = 'handoff'"
                ).fetchall()
                for r in rows:
                    if r["whatsapp_chat_id"]:
                        _KNOWN_NORM = r["whatsapp_chat_id"].split("@")[0]
                        _KNOWN_FULL = r["whatsapp_chat_id"]
                        _KNOWN_PAUSED_CHATS.add(_KNOWN_FULL)
                        _KNOWN_PAUSED_CHATS.add(_KNOWN_NORM)
    except Exception as e:
        print(f"[whatsapp-listener] Error pre-populating paused set: {e}")

def on_pre_gateway_dispatch(event, gateway, session_store, **kwargs):
    try:
        import traceback
        import sys
        from gateway.config import Platform
        
        _init_known_paused()
        
        source = getattr(event, "source", None)
        if source is None:
            return None
            
        # Instead of calling .platform (which crashes if source is just a dict or string),
        # safely check if this event came from WhatsApp
        platform_val = getattr(source, "platform", None)
        if platform_val is None and isinstance(source, dict):
            platform_val = source.get("platform")
            
        if platform_val != Platform.WHATSAPP and str(platform_val) != "whatsapp":
            return None

        chat_id = getattr(source, "chat_id", "") if not isinstance(source, dict) else source.get("chat_id", "")
        user_id = getattr(source, "user_id", "") if not isinstance(source, dict) else source.get("user_id", "")
        user_name = getattr(source, "user_name", None) if not isinstance(source, dict) else source.get("user_name")
        text = (getattr(event, "text", "") or "").strip()

        # Best-effort: backfill this chat's name to the hub (once per jid).
        # sender_name (pushName) is used for DMs; groups resolve their subject.
        _hub_backfill_name(chat_id, sender_name=user_name)

        adapter = gateway.adapters.get(Platform.WHATSAPP)

        # Apply profile-scoped monkey-patch to filter out transcription echoes dynamically and bubble errors
        if adapter and not hasattr(adapter, "_patched_transcription_echo"):
            _original_send = adapter.send
            adapter._original_send = _original_send
            
            async def new_send(*args, **kwargs):
                chat_id = "unknown"
                try:
                    chat_id = kwargs.get("chat_id")
                    if not chat_id and len(args) > 0:
                        chat_id = args[0]
                    content = kwargs.get("content")
                    if not content and len(args) > 1:
                        content = args[1]

                    # 1. Silently drop transcription echo messages for yltc profile to keep voice silent
                    if isinstance(content, str) and content.startswith('🎙️ "') and content.endswith('"'):
                        _profile_name = "default"
                        try:
                            import json as _json
                            import os as _os
                            routes_path = _os.path.expanduser("~/.hermes/plugins/whatsapp-profile-router/profile_routes.json")
                            if _os.path.exists(routes_path):
                                with open(routes_path, "r", encoding="utf-8") as rf:
                                    _routes = _json.load(rf)
                                if chat_id in _routes:
                                    _profile_name = _routes[chat_id].get("profile") or "default"
                        except Exception:
                            pass
                            
                        if _profile_name == "yltc":
                            from gateway.platforms.base import SendResult
                            return SendResult(success=True)
                            
                    return await _original_send(*args, **kwargs)
                except Exception as e:
                    import traceback
                    tb_str = traceback.format_exc()
                    print(f"!!! Error in WhatsApp new_send: {e}\n{tb_str}")
                    try:
                        admin_chat_id = "120363408762983588@g.us"
                        error_msg = (
                            "⚠️ *Hermes Outbound Send Error*\n\n"
                            f"An error occurred while sending a message to chat `{chat_id}`:\n\n"
                            f"Type: `{type(e).__name__}`\n"
                            f"Error: `{e}`\n\n"
                            f"```\n{tb_str[:1200]}\n```"
                        )
                        await _original_send(chat_id=admin_chat_id, content=error_msg)
                    except Exception as admin_err:
                        print(f"!!! Failed to send send_error alert to admin group: {admin_err}")
                    raise e
                
            adapter.send = new_send
            adapter._patched_transcription_echo = True

        # Apply profile-scoped monkey-patch to capture background agent execution errors
        if adapter and not hasattr(adapter, "_patched_process_message_background"):
            _original_process = adapter._process_message_background
            
            async def new_process_message_background(event, session_key):
                try:
                    return await _original_process(event, session_key)
                except Exception as e:
                    import traceback
                    tb_str = traceback.format_exc()
                    print(f"!!! Error in background message handling: {e}\n{tb_str}")
                    try:
                        chat_id = getattr(getattr(event, "source", None), "chat_id", "unknown")
                        user_id = getattr(getattr(event, "source", None), "user_id", "unknown")
                        text_val = getattr(event, "text", "")
                        
                        admin_chat_id = "120363408762983588@g.us"
                        error_msg = (
                            "🚨 *Hermes Background Processing Error*\n\n"
                            f"An unhandled exception occurred in the agent loop for chat `{chat_id}` (User: `{user_id}`):\n\n"
                            f"Input Text: {text_val!r}\n"
                            f"Error Type: `{type(e).__name__}`\n"
                            f"Error Message: `{e}`\n\n"
                            f"```\n{tb_str[:1200]}\n```"
                        )
                        target_send = getattr(adapter, "_original_send", adapter.send)
                        await target_send(chat_id=admin_chat_id, content=error_msg)
                    except Exception as notify_err:
                        print(f"!!! Failed to send process_error alert to admin group: {notify_err}")
                    raise e
                    
            adapter._process_message_background = new_process_message_background
            adapter._patched_process_message_background = True

        def send_msg(msg):
            if adapter:
                asyncio.create_task(adapter.send(chat_id, msg))

        # ---------------------------------------------------------
        # BLOCK SLASHER IN CLIENT GROUPS (Strict Stealth Boundary)
        # ---------------------------------------------------------
        # Resolve sender identity -> admin status early, so admins (incl. root)
        # can run slash commands anywhere, even in stealth client groups.
        try:
            from gateway.whatsapp_identity import expand_whatsapp_aliases
            _early_aliases = expand_whatsapp_aliases(user_id)
            _early_aliases = {f"{alias}@s.whatsapp.net" for alias in _early_aliases}
            _early_aliases.add(user_id)
        except Exception:
            _early_aliases = {user_id}
        sender_is_admin = bool(_early_aliases & get_all_admins())

        admin_groups = load_set_from_file(ADMIN_GROUPS_FILE)
        is_client_group = ("g.us" in chat_id) and (chat_id not in admin_groups)
        if is_client_group and text.startswith("/") and not sender_is_admin:
            return {
                "action": "skip",
                "reason": "Silently dropped slash command in client group to maintain stealth/humanness."
            }

        # ---------------------------------------------------------
        # COMMAND: /wajid
        # ---------------------------------------------------------
        if text.lower() == "/wajid":
            send_msg(f"📱 *WAJID Info*\n\nChat ID: {chat_id}\nYour ID: {user_id}")
            return {"action": "skip", "reason": "Intercepted /wajid command"}

        # ---------------------------------------------------------
        # COMMAND: login  (self-service magic link → hub portal)
        # ---------------------------------------------------------
        # A contact asks to sign in; the HUB mints a single-use link and we send it
        # back over WhatsApp. Only known employees are eligible (hub checks). This
        # works in DMs and whitelisted chats. The link goes to THIS chat.
        _login_text = text.lower().strip()
        _login_triggers = {"login", "/login", "sign in", "signin", "my claims", "submit claim", "new claim", "claim"}
        if _login_text in _login_triggers:
            if _HUB_SYNC is None:
                # Hub sync not configured on this box — nothing to offer.
                return {"action": "skip", "reason": "login requested but hub-sync unavailable"}
            # Deep-link intent: claim-related phrases jump straight to the new-claim
            # form; plain login lands on the portal dashboard.
            _redirect = None
            if _login_text in {"submit claim", "new claim", "claim"}:
                _redirect = "/portal/claims/new"
            elif _login_text == "my claims":
                _redirect = "/portal/claims"
            try:
                # Try the raw sender id plus its expanded aliases (waJid <-> lid),
                # since employees are keyed by waJid but senders may be @lid.
                candidates = sorted(_early_aliases)
                url, reason = _HUB_SYNC.request_login_link(user_id, candidates=candidates, redirect=_redirect)
            except Exception as e:
                print(f"[whatsapp-listener] login link error: {e}")
                url, reason = None, "error"
            if url:
                send_msg(
                    "🔐 *Sign in*\nTap to open your portal (link expires in ~10 min, one-time use):\n"
                    + url
                )
            elif reason == "not-eligible":
                send_msg("Sorry, this number isn't set up for portal access. Please contact your admin.")
            else:
                send_msg("Couldn't create a sign-in link right now. Please try again shortly.")
            return {"action": "skip", "reason": f"Intercepted login command ({reason or 'ok'})"}

        # Use Hermes' built-in identity expander to map @lid to @s.whatsapp.net
        try:
            from gateway.whatsapp_identity import expand_whatsapp_aliases
            user_aliases = expand_whatsapp_aliases(user_id)
            # expand_whatsapp_aliases strips the @s.whatsapp.net suffix. 
            # We must re-append it so it matches our ROOT_ADMIN_ID format!
            user_aliases = {f"{alias}@s.whatsapp.net" for alias in user_aliases}
            # Also keep the raw user_id just in case
            user_aliases.add(user_id)
        except Exception as e:
            # Fallback if the bridge map fails
            user_aliases = {user_id}

        # DEBUG: Tell the admin what the bot actually saw so we know why it failed
        if text.lower().startswith("/whitelist") and not bool(user_aliases & get_all_admins()):
            send_msg(f"🚫 Unauthorized. Only admins can use this command.\n\nDebug Info:\nYour ID: {user_id}\nAliases found: {list(user_aliases)}\nAdmins: {list(get_all_admins())}")
            return {"action": "skip", "reason": "Unauthorized /whitelist attempt"}

        is_admin = bool(user_aliases & get_all_admins())

        # ---------------------------------------------------------
        # ADMIN GROUP INTERCEPT: pause, resume, paused (Strict Admin Chat Gate)
        # ---------------------------------------------------------
        admin_groups = load_set_from_file(ADMIN_GROUPS_FILE)
        is_admin_group = (chat_id in admin_groups)
        if is_admin_group:
            import re
            text_strip = text.strip()
            text_lower = text_strip.lower()
            
            # Match resume to a titled session: "resume <client> <session_title>" or "/resume <client> <session_title>"
            # Note: We must check this BEFORE the standard resume_match because resume_match would greedily consume the title as part of the client name!
            resume_session_match = re.match(r"^(?:/)?(?:resume|unpause)\s+(\S+)\s+(.+)$", text_strip, re.IGNORECASE)
            
            # Match pause: "pause <client>" or "/pause <client>"
            pause_match = re.match(r"^(?:/)?pause\s+(.+)$", text_lower, re.IGNORECASE)
            # Match resume: "resume <client>" or "unpause <client>" or "/resume <client>" or "/unpause <client>"
            resume_match = re.match(r"^(?:/)?(?:resume|unpause)\s+(.+)$", text_lower, re.IGNORECASE)
            # Match paused: "paused" or "/paused"
            paused_match = (text_lower in {"paused", "/paused", "list paused", "/list paused"})
            # Match sync: "sync" or "sync names" or "/sync" or "/syncnames" or "/sync-names"
            sync_match = (text_lower in {"sync", "/sync", "sync names", "sync-names", "/syncnames", "/sync-names"})
            
            # Match new session: "new <client>" or "/new <client>"
            new_session_match = re.match(r"^(?:/)?new\s+(.+)$", text_strip, re.IGNORECASE)
            # Match title session: "title <client> <name>" or "/title <client> <name>"
            title_match = re.match(r"^(?:/)?title\s+(\S+)\s+(.+)$", text_strip, re.IGNORECASE)
            
            if resume_session_match:
                client_query = resume_session_match.group(1).strip()
                title_name = resume_session_match.group(2).strip()
                db_path = "/mnt/storage/projects/carbongpt/yltc-whatsapp-ordering-ai/data/concierge.db"
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        clients_db = db_conn.execute(
                            "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name = ?",
                            (client_query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name LIKE ?",
                                (f"%{client_query}%",)
                            ).fetchall()
                        
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT business_name FROM clients WHERE is_active=1").fetchall()
                            client_names_str = "\n".join([f"• {c['business_name']}" for c in all_clients])
                            send_msg(f"❌ *No active client found* matching '{client_query}'.\n\n💡 *Available client names:*\n{client_names_str}")
                        elif len(clients_db) > 1:
                            matches_str = "\n".join([f"- {c['business_name']}" for c in clients_db])
                            send_msg(f"🔍 *Multiple matches found for '{client_query}':*\n{matches_str}\n\n_Please try again with a more specific name._")
                        else:
                            client = clients_db[0]
                            target_chat_id = client["whatsapp_chat_id"]
                            biz_name = client["business_name"]
                            
                            if not target_chat_id:
                                send_msg(f"⚠️ *Client '{biz_name}' has no mapped WhatsApp Chat ID.*")
                            else:
                                from hermes_state import SessionDB
                                db = SessionDB()
                                target_id = db.resolve_session_by_title(title_name)
                                if not target_id:
                                    sess = db.get_session(title_name)
                                    if sess:
                                        target_id = sess["id"]
                                
                                if not target_id:
                                    send_msg(f"❌ *No named session found* matching *\"{title_name}\"*.")
                                else:
                                    switched_count = 0
                                    with session_store._lock:
                                        session_store._ensure_loaded_locked()
                                        for key in list(session_store._entries.keys()):
                                            if target_chat_id in key:
                                                session_store.switch_session(key, target_id)
                                                switched_count += 1
                                    
                                    if switched_count > 0:
                                        send_msg(f"🔄 *Session Switched* for *{biz_name}* to *\"{title_name}\"* ({switched_count} session(s) switched).")
                                    else:
                                        from gateway.session import build_session_key, SessionSource
                                        from gateway.config import Platform
                                        safe_source = SessionSource(
                                            platform=Platform.WHATSAPP,
                                            chat_id=target_chat_id,
                                            user_id="default_user",
                                            chat_type="group" if "g.us" in target_chat_id else "dm"
                                        )
                                        session_key = build_session_key(
                                            safe_source,
                                            group_sessions_per_user=getattr(gateway.config, "extra", {}).get("group_sessions_per_user", True),
                                            thread_sessions_per_user=getattr(gateway.config, "extra", {}).get("thread_sessions_per_user", False),
                                        )
                                        session_store.get_or_create_session(safe_source)
                                        session_store.switch_session(session_key, target_id)
                                        send_msg(f"🔄 *Session Switched* for *{biz_name}* to *\"{title_name}\"* (created and switched new default session).")
                except Exception as e:
                    send_msg(f"❌ *Error switching session:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group resume session command"}
            
            elif pause_match:
                query = pause_match.group(1).strip()
                db_path = "/mnt/storage/projects/carbongpt/yltc-whatsapp-ordering-ai/data/concierge.db"
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        
                        # Find client
                        clients_db = db_conn.execute(
                            "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name = ?",
                            (query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name LIKE ?",
                                (f"%{query}%",)
                            ).fetchall()
                            
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT business_name FROM clients WHERE is_active=1").fetchall()
                            client_names_str = "\n".join([f"• {c['business_name']}" for c in all_clients])
                            send_msg(
                                f"❌ *No active client found* matching '{query}'.\n\n"
                                f"💡 *Available client names in your DB:*\n{client_names_str}"
                            )
                        elif len(clients_db) > 1:
                            matches_str = "\n".join([f"- {c['business_name']}" for c in clients_db])
                            send_msg(f"🔍 *Multiple matches found for '{query}':*\n{matches_str}\n\n_Please try again with a more specific name._")
                        else:
                            client = clients_db[0]
                            target_chat_id = client["whatsapp_chat_id"]
                            biz_name = client["business_name"]
                            
                            if not target_chat_id:
                                send_msg(f"⚠️ *Client '{biz_name}' has no mapped WhatsApp Chat ID.*")
                            else:
                                paused_chats = load_paused_chats()
                                paused_chats[target_chat_id] = biz_name
                                save_paused_chats(paused_chats)
                                send_msg(f"⏸️ *AI Auto-Reply Paused* for *{biz_name}*.")
                except Exception as e:
                    send_msg(f"❌ *Error pausing client:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group pause command"}
                
            elif resume_match:
                query = resume_match.group(1).strip()
                db_path = "/mnt/storage/projects/carbongpt/yltc-whatsapp-ordering-ai/data/concierge.db"
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        
                        # Find client
                        clients_db = db_conn.execute(
                            "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name = ?",
                            (query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name LIKE ?",
                                (f"%{query}%",)
                            ).fetchall()
                            
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT business_name FROM clients WHERE is_active=1").fetchall()
                            client_names_str = "\n".join([f"• {c['business_name']}" for c in all_clients])
                            send_msg(
                                f"❌ *No active client found* matching '{query}'.\n\n"
                                f"💡 *Available client names in your DB:*\n{client_names_str}"
                            )
                        elif len(clients_db) > 1:
                            matches_str = "\n".join([f"- {c['business_name']}" for c in clients_db])
                            send_msg(f"🔍 *Multiple matches found for '{query}':*\n{matches_str}\n\n_Please try again with a more specific name._")
                        else:
                            client = clients_db[0]
                            target_chat_id = client["whatsapp_chat_id"]
                            biz_name = client["business_name"]
                            
                            if not target_chat_id:
                                send_msg(f"⚠️ *Client '{biz_name}' has no mapped WhatsApp Chat ID.*")
                            else:
                                paused_chats = load_paused_chats()
                                if target_chat_id in paused_chats:
                                    del paused_chats[target_chat_id]
                                    save_paused_chats(paused_chats)
                                    send_msg(f"▶️ *AI Auto-Reply Resumed* for *{biz_name}*.")
                                else:
                                    send_msg(f"ℹ️ *Client '{biz_name}' is not currently manually paused.*")
                except Exception as e:
                    send_msg(f"❌ *Error resuming client:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group resume command"}
            
            elif new_session_match:
                query = new_session_match.group(1).strip()
                db_path = "/mnt/storage/projects/carbongpt/yltc-whatsapp-ordering-ai/data/concierge.db"
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        clients_db = db_conn.execute(
                            "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name = ?",
                            (query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name LIKE ?",
                                (f"%{query}%",)
                            ).fetchall()
                        
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT business_name FROM clients WHERE is_active=1").fetchall()
                            client_names_str = "\n".join([f"• {c['business_name']}" for c in all_clients])
                            send_msg(f"❌ *No active client found* matching '{query}'.\n\n💡 *Available client names:*\n{client_names_str}")
                        elif len(clients_db) > 1:
                            matches_str = "\n".join([f"- {c['business_name']}" for c in clients_db])
                            send_msg(f"🔍 *Multiple matches found for '{query}':*\n{matches_str}\n\n_Please try again with a more specific name._")
                        else:
                            client = clients_db[0]
                            target_chat_id = client["whatsapp_chat_id"]
                            biz_name = client["business_name"]
                            
                            if not target_chat_id:
                                send_msg(f"⚠️ *Client '{biz_name}' has no mapped WhatsApp Chat ID.*")
                            else:
                                reset_keys = []
                                with session_store._lock:
                                    session_store._ensure_loaded_locked()
                                    for key in session_store._entries.keys():
                                        if target_chat_id in key:
                                            reset_keys.append(key)
                                
                                if reset_keys:
                                    for key in reset_keys:
                                        session_store.reset_session(key)
                                    send_msg(f"✨ *Clean Session Started* for *{biz_name}*.\nReset {len(reset_keys)} active user-session(s) in that group chat.")
                                else:
                                    from gateway.session import build_session_key, SessionSource
                                    from gateway.config import Platform
                                    safe_source = SessionSource(
                                        platform=Platform.WHATSAPP,
                                        chat_id=target_chat_id,
                                        user_id="default_user",
                                        chat_type="group" if "g.us" in target_chat_id else "dm"
                                    )
                                    session_key = build_session_key(
                                        safe_source,
                                        group_sessions_per_user=getattr(gateway.config, "extra", {}).get("group_sessions_per_user", True),
                                        thread_sessions_per_user=getattr(gateway.config, "extra", {}).get("thread_sessions_per_user", False),
                                    )
                                    session_store.get_or_create_session(safe_source)
                                    session_store.reset_session(session_key)
                                    send_msg(f"✨ *Clean Session Started* for *{biz_name}* (created new default session).")
                except Exception as e:
                    send_msg(f"❌ *Error starting clean session:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group new session command"}

            elif title_match:
                client_query = title_match.group(1).strip()
                title_name = title_match.group(2).strip()
                db_path = "/mnt/storage/projects/carbongpt/yltc-whatsapp-ordering-ai/data/concierge.db"
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        clients_db = db_conn.execute(
                            "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name = ?",
                            (client_query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1 AND business_name LIKE ?",
                                (f"%{client_query}%",)
                            ).fetchall()
                        
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT business_name FROM clients WHERE is_active=1").fetchall()
                            client_names_str = "\n".join([f"• {c['business_name']}" for c in all_clients])
                            send_msg(f"❌ *No active client found* matching '{client_query}'.\n\n💡 *Available client names:*\n{client_names_str}")
                        elif len(clients_db) > 1:
                            matches_str = "\n".join([f"- {c['business_name']}" for c in clients_db])
                            send_msg(f"🔍 *Multiple matches found for '{client_query}':*\n{matches_str}\n\n_Please try again with a more specific name._")
                        else:
                            client = clients_db[0]
                            target_chat_id = client["whatsapp_chat_id"]
                            biz_name = client["business_name"]
                            
                            if not target_chat_id:
                                send_msg(f"⚠️ *Client '{biz_name}' has no mapped WhatsApp Chat ID.*")
                            else:
                                from hermes_state import SessionDB
                                db = SessionDB()
                                titled_count = 0
                                with session_store._lock:
                                    session_store._ensure_loaded_locked()
                                    for key, entry in session_store._entries.items():
                                        if target_chat_id in key:
                                            db.set_session_title(entry.session_id, title_name)
                                            titled_count += 1
                                
                                if titled_count > 0:
                                    send_msg(f"🏷️ *Session Titled* for *{biz_name}* as *\"{title_name}\"* ({titled_count} session(s) titled).")
                                else:
                                    send_msg(f"⚠️ *No active AI session found* currently running for *{biz_name}* to title.")
                except Exception as e:
                    send_msg(f"❌ *Error titling session:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group title command"}
                
            elif paused_match:
                db_path = "/mnt/storage/projects/carbongpt/yltc-whatsapp-ordering-ai/data/concierge.db"
                try:
                    paused_chats = load_paused_chats()
                    manual_lines = []
                    for cid, bname in paused_chats.items():
                        manual_lines.append(f"- *{bname}* (Manually Paused)")
                        
                    # Also look up database handoffs
                    handoff_lines = []
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        handoffs = db_conn.execute(
                            "SELECT c.business_name, o.id, o.updated_at FROM orders o "
                            "JOIN clients c ON o.client_id = c.id "
                            "WHERE o.human_handoff = 1 OR o.status = 'handoff' "
                            "ORDER BY o.updated_at DESC"
                        ).fetchall()
                        for h in handoffs:
                            handoff_lines.append(f"- *{h['business_name']}* (Handoff State, Order #{h['id']})")
                            
                    all_lines = manual_lines + handoff_lines
                    if not all_lines:
                        send_msg("🟢 *No AI chats are currently paused.* All bots are fully active.")
                    else:
                        list_str = "\n".join(all_lines)
                        send_msg(f"⏸️ *Currently Paused AI Chats:*\n{list_str}")
                except Exception as e:
                    send_msg(f"❌ *Error listing paused chats:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group paused list command"}
                
            elif sync_match:
                db_path = "/mnt/storage/projects/carbongpt/yltc-whatsapp-ordering-ai/data/concierge.db"
                try:
                    import sqlite3
                    import urllib.request
                    
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        rows = db_conn.execute("SELECT id, business_name, whatsapp_chat_id FROM clients WHERE is_active=1").fetchall()
                        
                        updates = []
                        for r in rows:
                            target_chat_id = r["whatsapp_chat_id"]
                            if not target_chat_id:
                                continue
                            
                            url = f"http://127.0.0.1:3000/chat/{target_chat_id}"
                            try:
                                req = urllib.request.Request(url, method="GET")
                                with urllib.request.urlopen(req, timeout=3) as resp:
                                    data = json.loads(resp.read().decode("utf-8"))
                                    if data and data.get("name"):
                                        real_whatsapp_name = data["name"]
                                        old_name = r["business_name"]
                                        if real_whatsapp_name != old_name:
                                            updates.append((real_whatsapp_name, r["id"], old_name))
                            except Exception:
                                pass
                                
                        if not updates:
                            send_msg("🔄 *WhatsApp group sync complete.*\nAll database client names are already perfectly aligned with your active WhatsApp groups!")
                        else:
                            for new_name, cid, old_name in updates:
                                db_conn.execute("UPDATE clients SET business_name=? WHERE id=?", (new_name, cid))
                            db_conn.commit()
                            
                            updates_str = "\n".join([f"- *{old_name}* ➔ *{new_name}*" for new_name, _, old_name in updates])
                            send_msg(f"🔄 *WhatsApp group names synced with DB:*\n\n{updates_str}")
                except Exception as e:
                    send_msg(f"❌ *Error syncing names:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group sync command"}

        # ---------------------------------------------------------
        # COMMAND: /whitelist add|remove <id>
        # ---------------------------------------------------------
        if text.lower().startswith("/whitelist"):
            
            parts = text.split()
            wl = load_set_from_file(WHITELIST_FILE)
            
            if len(parts) >= 3:
                action = parts[1].lower()
                target_id = parts[2]
                
                if action == "add":
                    wl.add(target_id)
                    save_set_to_file(WHITELIST_FILE, wl)
                    send_msg(f"✅ Added to whitelist:\n{target_id}")
                elif action == "remove":
                    wl.discard(target_id)
                    save_set_to_file(WHITELIST_FILE, wl)
                    send_msg(f"❌ Removed from whitelist:\n{target_id}")
                else:
                    send_msg("Usage: /whitelist add <id> OR /whitelist remove <id>")
            elif len(parts) == 2:
                action = parts[1].lower()
                if action == "remove" and "g.us" in chat_id:
                    wl.discard(chat_id)
                    save_set_to_file(WHITELIST_FILE, wl)
                    send_msg(f"❌ Removed from whitelist:\n{chat_id}")
                else:
                    send_msg("Usage: /whitelist add <id> OR /whitelist remove <id>")
            else:
                # Bare /whitelist in a group: auto-add this group
                if "g.us" in chat_id:
                    if chat_id in wl:
                        send_msg(f"ℹ️ Already whitelisted:\n{chat_id}")
                    else:
                        wl.add(chat_id)
                        save_set_to_file(WHITELIST_FILE, wl)
                        send_msg(f"✅ Whitelisted this group:\n{chat_id}")
                else:
                    send_msg(f"Usage: /whitelist add <id> OR /whitelist remove <id>\nCurrent whitelisted chats: {len(wl)}")
                
            return {"action": "skip", "reason": "Intercepted /whitelist command"}

        # ---------------------------------------------------------
        # COMMAND: /no-mention add|remove <id>
        # By default whitelisted GROUPS require a direct @mention before the bot
        # replies (safe default). Groups added here are EXEMPT — the bot replies
        # to every message without a mention. Requires core require_mention:
        # false (otherwise core drops non-mention messages upstream).
        # ---------------------------------------------------------
        if text.lower().startswith("/no-mention"):
            if not is_admin:
                send_msg("🚫 Unauthorized. Only admins can use this command.")
                return {"action": "skip", "reason": "Unauthorized /no-mention attempt"}

            parts = text.split()
            nm = load_set_from_file(NO_MENTION_FILE)

            if len(parts) >= 3:
                action = parts[1].lower()
                target_id = parts[2]

                if action == "add":
                    nm.add(target_id)
                    save_set_to_file(NO_MENTION_FILE, nm)
                    send_msg(f"✅ No-mention mode ON for:\n{target_id}\n\n"
                             "(Group must also be in the reply whitelist. Bot now "
                             "replies to every message here, no @mention needed.)")
                elif action == "remove":
                    nm.discard(target_id)
                    save_set_to_file(NO_MENTION_FILE, nm)
                    send_msg(f"❌ No-mention mode OFF for:\n{target_id}\n\n"
                             "(Back to the default: bot only replies here when directly @mentioned.)")
                else:
                    send_msg("Usage: /no-mention add <id> OR /no-mention remove <id>")
            else:
                send_msg(f"Usage: /no-mention add <id> OR /no-mention remove <id>\n"
                         f"Current no-mention groups: {len(nm)}")

            return {"action": "skip", "reason": "Intercepted /no-mention command"}

        # ---------------------------------------------------------
        # COMMAND: /admin add|remove <id>
        # ---------------------------------------------------------
        if text.lower().startswith("/admin"):
            if not is_admin:
                send_msg("🚫 Unauthorized. Only admins can use this command.")
                return {"action": "skip", "reason": "Unauthorized /admin attempt"}
            
            parts = text.split()
            admins = load_set_from_file(ADMINS_FILE)
            
            if len(parts) >= 3:
                action = parts[1].lower()
                target_id = parts[2]
                
                if action == "add":
                    admins.add(target_id)
                    save_set_to_file(ADMINS_FILE, admins)
                    send_msg(f"✅ Added new admin:\n{target_id}")
                elif action == "remove":
                    if target_id == ROOT_ADMIN_ID:
                        send_msg("❌ Cannot remove the Root Admin.")
                    else:
                        admins.discard(target_id)
                        save_set_to_file(ADMINS_FILE, admins)
                        send_msg(f"❌ Removed admin:\n{target_id}")
                else:
                    send_msg("Usage: /admin add <id> OR /admin remove <id>")
            else:
                all_admins = get_all_admins()
                send_msg(f"Usage: /admin add <id> OR /admin remove <id>\nCurrent admins: {len(all_admins)}")
                
            return {"action": "skip", "reason": "Intercepted /admin command"}

        # ---------------------------------------------------------
        # SILENT LISTENER LOGIC
        # ---------------------------------------------------------
        whitelist = load_set_from_file(WHITELIST_FILE)
        # DRY routing: treat any chat_id present in the router's profile_routes.json
        # as whitelisted for the purpose of deciding whether Hermes should reply.
        # This lets you edit only profile_routes.json.
        routes_file = os.path.join(os.path.dirname(PLUGIN_DIR), "whatsapp-profile-router", "profile_routes.json")
        routes_chat_ids = set()
        try:
            if os.path.exists(routes_file):
                with open(routes_file, "r", encoding="utf-8") as rf:
                    routes_data = json.load(rf)
                if isinstance(routes_data, dict):
                    routes_chat_ids = {str(k) for k in routes_data.keys() if str(k).strip()}
        except Exception:
            routes_chat_ids = set()
        
        # Check if the chat itself (e.g. the group ID or DM ID) is whitelisted
        is_whitelisted = (chat_id in whitelist) or (chat_id in routes_chat_ids)

        # In a group chat, we also allow the message if the user speaking is explicitly whitelisted
        if not is_whitelisted and user_aliases:
            is_whitelisted = bool(user_aliases & whitelist)

        # ---------------------------------------------------------
        # COMMAND: /keyword-research <keyword>
        # Runs the keyword-opportunity-research skill. We rewrite the message
        # into a natural-language prompt (the skill is selected by description)
        # and force it through to the agent (return None). Gated to whitelisted
        # chats / admins so it can't be triggered by randoms (it's expensive:
        # many web fetches + LLM calls).
        # ---------------------------------------------------------
        if text.lower().startswith("/keyword-research"):
            if not (is_whitelisted or is_admin):
                send_msg("🚫 /keyword-research is restricted to whitelisted chats.")
                return {"action": "skip", "reason": "Unauthorized /keyword-research"}
            keyword = text[len("/keyword-research"):].strip()
            if not keyword:
                send_msg("Usage: /keyword-research <keyword or niche>\n\n"
                         "Example: /keyword-research employee commute form for GHG scope 3")
                return {"action": "skip", "reason": "Empty /keyword-research"}
            send_msg(f"🔎 Running deep opportunity research on *{keyword}*.\n"
                     "This takes a few minutes (SEO + Reddit/X mining + willingness-to-pay). "
                     "I'll reply with the brief, scorecard, and the one-feature wedge.")
            # Rewrite so the agent loads keyword-opportunity-research by description.
            event.text = (
                "Use the keyword-opportunity-research skill to do deep opportunity "
                f"research on the keyword/niche: \"{keyword}\". Cover keyword volume "
                "and long-tail, competitiveness, Reddit and X complaint mining, "
                "willingness to pay and pricing, then give an opportunity scorecard "
                "and the single stripped-down feature people would pay for."
            )
            return None  # fall through to the agent (bypasses mention-gating)

        is_group = "g.us" in chat_id
        text_l = text.lower()

        bot_numeric = (_BOT_HINT.get("bot_numeric") or "").strip()
        bot_name = (_BOT_HINT.get("bot_name") or "").strip().lower()

        # Mention patterns that WhatsApp commonly surfaces as plain text:
        # - @<bot-number>
        # - @<bot-name>
        # - sometimes the plain name without '@'
        bot_mentioned = False
        if bot_numeric:
            bot_mentioned = bot_mentioned or (f"@{bot_numeric}" in text_l)
        if bot_name:
            bot_mentioned = bot_mentioned or (f"@{bot_name}" in text_l)

        # Backwards compat with older setup.
        bot_mentioned = bot_mentioned or ("@hermes" in text_l) or ("cupbots" in text_l)

        # Second-stage: WhatsApp bridge may strip the visible @mention from
        # `event.text` but still provide mention metadata separately.
        if not bot_mentioned:
            bot_mentioned = _event_indicates_bot_mention(
                event,
                bot_numeric=bot_numeric,
                bot_name=bot_name,
                bot_id=_BOT_HINT.get("bot_id") or "",
                bot_lid_numeric=_BOT_HINT.get("bot_lid_numeric") or "",
            )

        # Debug only for routed groups.
        try:
            is_routed_group = chat_id in routes_chat_ids
            if is_routed_group:
                debug_path = os.path.join(os.path.expanduser("~/.hermes/logs"), "whatsapp_mention_debug.log")
                with open(debug_path, "a", encoding="utf-8") as df:
                    meta = _extract_mention_meta(event)
                    meta_small = {}
                    for k, v in meta.items():
                        try:
                            s = repr(v)
                            if len(s) > 140:
                                s = s[:140] + "..."
                            meta_small[k] = s
                        except Exception:
                            pass
                    df.write(
                        f"{time.time()} chat={chat_id} text={text!r} bot_numeric={bot_numeric!r} bot_name={bot_name!r} bot_mentioned={bot_mentioned} meta={meta_small}\n"
                    )
        except Exception:
            pass
            
        # Temporary extreme debug: dump the ENTIRE event dict structure to see where mentions hide
        if "g.us" in chat_id:
            try:
                debug_path = os.path.join(os.path.expanduser("~/.hermes/logs"), "whatsapp_mention_debug.log")
                with open(debug_path, "a", encoding="utf-8") as df:
                    if isinstance(event, dict):
                        df.write(f"RAW EVENT: {list(event.keys())}\n")
                        if "raw" in event:
                            df.write(f"RAW PAYLOAD KEYS: {list(event['raw'].keys()) if isinstance(event['raw'], dict) else 'not dict'}\n")
                    elif hasattr(event, "__dict__"):
                        df.write(f"RAW EVENT ATTRS: {list(event.__dict__.keys())}\n")
                        if hasattr(event, "raw_message") and isinstance(event.raw_message, dict):
                            df.write(f"RAW PAYLOAD KEYS: {list(event.raw_message.keys())}\n")
                            if "mentionedIds" in event.raw_message:
                                df.write(f"FOUND MENTIONS IN EVENT.RAW_MESSAGE: {event.raw_message['mentionedIds']}\n")
            except Exception:
                pass

        # Check if another user was mentioned in the group
        mentions_other_user = False
        if is_group:
            try:
                raw_msg = getattr(event, "raw_message", None)
                if isinstance(raw_msg, dict):
                    mentioned_ids = raw_msg.get("mentionedIds") or []
                    for m in mentioned_ids:
                        m_str = str(m)
                        is_bot_id = False
                        if bot_numeric and bot_numeric in m_str:
                            is_bot_id = True
                        if bot_name and bot_name in m_str.lower():
                            is_bot_id = True
                        
                        bot_id_val = _BOT_HINT.get("bot_id")
                        if bot_id_val and str(bot_id_val) in m_str:
                            is_bot_id = True
                            
                        bot_lid_val = _BOT_HINT.get("bot_lid_numeric")
                        if bot_lid_val and str(bot_lid_val) in m_str:
                            is_bot_id = True
                        
                        if not is_bot_id:
                            mentions_other_user = True
                            break
            except Exception:
                pass

        # Silent-listener behaviour + SAFE DEFAULT: in groups the bot only replies
        # when directly @mentioned. A group is EXEMPT from that requirement only if
        # it's explicitly listed in no_mention_groups (allowlist) — then it also
        # needs to be whitelisted / routed to actually be allowed to reply.
        # Requires core require_mention: false — otherwise core drops non-mention
        # group messages before this plugin ever runs (they can't even be stored).
        is_explicitly_whitelisted = (chat_id in whitelist) or (chat_id in routes_chat_ids)
        no_mention_groups = load_set_from_file(NO_MENTION_FILE)
        exempt_from_mention = is_group and (chat_id in no_mention_groups)
        allow_without_mention = exempt_from_mention and is_explicitly_whitelisted

        should_reply = is_whitelisted and (not is_group or bot_mentioned or allow_without_mention)

        # Do not reply if another user in the group is mentioned
        if is_group and mentions_other_user:
            should_reply = False

        # Check manual pause and database handoff state
        is_paused, pause_reason = is_manually_paused(chat_id)
        if not is_paused:
            is_paused, pause_reason = is_chat_in_handoff(chat_id)

        chat_norm = chat_id.split("@")[0] if chat_id else ""

        if is_paused:
            should_reply = False
            # Ensure the chat is recorded as paused
            _KNOWN_PAUSED_CHATS.add(chat_id)
            if chat_norm:
                _KNOWN_PAUSED_CHATS.add(chat_norm)
        else:
            # Transition detection: check if this chat was previously paused/handed off
            was_paused = (chat_id in _KNOWN_PAUSED_CHATS) or (chat_norm and chat_norm in _KNOWN_PAUSED_CHATS)
            if was_paused:
                print(f"[whatsapp-listener] Transition detected! Chat {chat_id} unpaused/resumed. Resetting session to force clean AI context.")
                try:
                    from gateway.session import build_session_key, SessionSource
                    from gateway.config import Platform
                    
                    safe_source = SessionSource(
                        platform=Platform.WHATSAPP,
                        chat_id=chat_id,
                        user_id=user_id,
                        chat_type="group" if is_group else "dm"
                    )
                    session_key = build_session_key(
                        safe_source,
                        group_sessions_per_user=getattr(gateway.config, "extra", {}).get("group_sessions_per_user", True),
                        thread_sessions_per_user=getattr(gateway.config, "extra", {}).get("thread_sessions_per_user", False),
                    )
                    
                    # Force session reset programmatically!
                    session_store.reset_session(session_key)
                except Exception as reset_err:
                    print(f"[whatsapp-listener] Error resetting session on transition: {reset_err}")
                
                # Remove from known paused chats now that transition is handled
                _KNOWN_PAUSED_CHATS.discard(chat_id)
                if chat_norm:
                    _KNOWN_PAUSED_CHATS.discard(chat_norm)

        # Let default Hermes handle it (sends to LLM)
        if should_reply:
            return None

        # Silently save to SQLite without triggering LLM
        # Use the global `build_session_key` function since `gateway` doesn't expose it safely
        try:
            from gateway.session import build_session_key
            from gateway.session import SessionSource
            from gateway.config import Platform
            
            # Reconstruct a proper SessionSource to guarantee build_session_key won't crash
            safe_source = SessionSource(
                platform=Platform.WHATSAPP,
                chat_id=chat_id,
                user_id=user_id,
                chat_type="group" if is_group else "dm"
            )
            session_key = build_session_key(
                safe_source,
                group_sessions_per_user=getattr(gateway.config, "extra", {}).get("group_sessions_per_user", True),
                thread_sessions_per_user=getattr(gateway.config, "extra", {}).get("thread_sessions_per_user", False),
            )
        except Exception:
            safe_source = None

        if not safe_source:
            return {"action": "skip", "reason": "Failed to create SessionSource"}
        
        session_entry = session_store.get_or_create_session(safe_source)
        
        try:
            from hermes_state import SessionDB
            db = SessionDB()
            db.append_message(
                session_id=session_entry.session_id,
                role="user",
                content=text
            )
        except Exception as err:
            pass

        return {
            "action": "skip",
            "reason": f"WhatsApp listener: saved silently for {chat_id}"
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        # Ensure we ALWAYS skip if there's a crash, to guarantee silent failure instead of an accidental reply
        return {"action": "skip", "reason": f"Plugin crashed: {e}"}
