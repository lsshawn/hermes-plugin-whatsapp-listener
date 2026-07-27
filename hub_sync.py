"""
hub_sync — client-side WhatsApp contacts sync for cupbots-hub.

Self-contained, stdlib-only (no pip deps) so it drops onto any Hermes box. Used
by BOTH sides of the sync:

  * The whatsapp-listener plugin imports `push_deltas()` / `push_flag()` and calls
    them fire-and-forget after every local state.yaml write. These NEVER raise and
    NEVER block message handling — the bot writes state.yaml first and syncs best
    effort, so a Cloudflare/hub outage can't affect it.

  * The cron (`pull.py`) calls `pull_and_apply()` to fetch the hub's merged map and
    rewrite the relevant state.yaml sections. A failed pull is a no-op (local state
    untouched).

Conflict model: per-JID LAST-WRITE-WINS on `updated_at` (unix seconds). The plugin
sends DELTAS (never the whole map), so an in-chat edit can't clobber a hub-UI edit
and vice-versa. Human toggles in the hub use now() and thus win over older deltas.

Config comes from env (see .env.example):
  HUB_URL            e.g. https://hub.cupbots.com
  HUB_CLIENT_SECRET  the client's shared secret (same one the broker uses)
  STATE_FILE         path to the plugin's state.yaml
  WA_NAME_ENDPOINT   optional, default http://127.0.0.1:3000/chat/{chat_id}
"""

import json
import os
import threading
import time
import urllib.request
import urllib.error

# --- config ----------------------------------------------------------------

def _cfg(key, default=None):
    return os.environ.get(key, default)

HUB_URL = (_cfg("HUB_URL", "") or "").rstrip("/")
HUB_CLIENT_SECRET = _cfg("HUB_CLIENT_SECRET", "") or ""
STATE_FILE = _cfg("STATE_FILE", "") or ""
WA_NAME_ENDPOINT = _cfg("WA_NAME_ENDPOINT", "http://127.0.0.1:3000/chat/{chat_id}")
_TIMEOUT = float(_cfg("HUB_SYNC_TIMEOUT", "4") or "4")

# state.yaml section  <->  hub contact flag
SECTION_FLAG = {
    "reply_whitelist": "whitelisted",
    "paused_chats": "paused",       # dict section (jid -> reason), special-cased
    "no_mention_groups": "noMention",
    "admins": "isAdmin",
}
LIST_SECTIONS = ("reply_whitelist", "no_mention_groups", "admins")

_enabled = bool(HUB_URL and HUB_CLIENT_SECRET)
_name_cache = {}          # jid -> resolved name (avoids re-hitting the local endpoint)
_name_cache_lock = threading.Lock()


# --- profile routing (config.yaml gateway.profile_routes) -------------------
# The client's Hermes multiplexes one WhatsApp gateway across several profiles
# (cgpt, yltc, ...). Routing lives in ~/.hermes/config.yaml under
# `gateway.profile_routes` (native, once multiplexing is enabled). We READ it —
# never write it: config.yaml is authoritative and hand-edited on the client.
#
# What we can map RELIABLY is GROUP jids (@g.us): a route keys on a group's
# chat_id, so a group belongs to exactly one profile. Individual jids
# (@s.whatsapp.net / @lid) have no route of their own and may appear across
# groups in DIFFERENT profiles — so we emit NO profile claim for them and let
# the hub derive per-chat profile from its own chat/session data. Hence the
# wire carries `profiles` as a LIST (0..n), never a single value.
HERMES_HOME = os.path.expanduser(os.environ.get("HERMES_HOME") or "~/.hermes")
CONFIG_YAML = os.path.join(HERMES_HOME, "config.yaml")

_routes_cache = {}        # {chat_id: profile}
_routes_cache_key = None  # config.yaml mtime — reparse only on change
_routes_lock = threading.Lock()


def _mtime(path):
    try:
        return os.stat(path).st_mtime_ns
    except OSError:
        return 0


def _parse_config_routes():
    """Return {chat_id: profile} from config.yaml gateway.profile_routes.

    Only WhatsApp routes keyed on a concrete chat_id are usable here (group
    jids). Routes without chat_id (e.g. Discord guild-only) are skipped."""
    try:
        import yaml
        with open(CONFIG_YAML, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception:
        return {}
    gw = cfg.get("gateway")
    raw = gw.get("profile_routes") if isinstance(gw, dict) else None
    if not raw and isinstance(cfg.get("profile_routes"), list):
        raw = cfg.get("profile_routes")  # tolerate a top-level form too
    out = {}
    for entry in raw or []:
        if not isinstance(entry, dict):
            continue
        platform = (entry.get("platform") or "").lower()
        if platform and platform not in ("whatsapp", "whatsapp_cloud"):
            continue
        chat_id = entry.get("chat_id")
        profile = entry.get("profile")
        if chat_id and profile:
            out[str(chat_id)] = str(profile)
    return out


def load_routes():
    """{chat_id: profile}, cached and reparsed only when config.yaml changes.
    config.yaml gateway.profile_routes is the single source of truth."""
    global _routes_cache, _routes_cache_key
    key = _mtime(CONFIG_YAML)
    with _routes_lock:
        if key == _routes_cache_key:
            return _routes_cache
        _routes_cache = _parse_config_routes()
        _routes_cache_key = key
        return _routes_cache


def profiles_for_jid(jid):
    """Profiles a jid maps to, as a list (0..n).

    Group jids resolve to their single routed profile. Individual jids get an
    empty list — hub-sync can't know their profile set, so the hub derives it.
    """
    if not jid:
        return []
    routes = load_routes()
    prof = routes.get(jid)
    return [prof] if prof else []


def distinct_profiles():
    """All profile names referenced by any route (for the profiles reconcile)."""
    return sorted({p for p in load_routes().values() if p})


def enabled():
    return _enabled


# --- low-level HTTP (stdlib, never raises to the caller of push_*) ----------

def _request(method, path, body=None, query=None):
    """Do one authenticated request to the hub. Returns parsed JSON or None."""
    if not _enabled:
        return None
    url = HUB_URL + path
    if query:
        from urllib.parse import urlencode
        url += "?" + urlencode(query)
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", "Bearer " + HUB_CLIENT_SECRET)
    # Cloudflare's WAF 403s the default "Python-urllib/x" agent — identify as our
    # own client so managed bot rules let the request through.
    req.add_header("User-Agent", "cupbots-hub-sync/1.0")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


# --- name resolution (lazy, cached) ----------------------------------------

def resolve_name(jid):
    """Best-effort display name for a jid via the local WA endpoint. Cached.
    Returns None on any failure (never raises)."""
    if not jid:
        return None
    with _name_cache_lock:
        if jid in _name_cache:
            return _name_cache[jid]
    name = None
    try:
        url = WA_NAME_ENDPOINT.format(chat_id=jid)
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data and data.get("name"):
                name = str(data["name"])
    except Exception:
        name = None
    with _name_cache_lock:
        _name_cache[jid] = name
    return name


# --- magic-link login (plugin -> hub, synchronous) -------------------------

def request_login_link(jid, candidates=None, redirect=None):
    """Ask the hub to mint a magic-login link for a WhatsApp contact.

    Returns (url, reason):
      - (url, None)          on success
      - (None, 'not-eligible')  jid isn't a known employee of this client
      - (None, 'client-unreachable' | 'disabled' | 'error')  otherwise

    `candidates` (optional): alternate jids to try in order (e.g. the identity
    expander's @lid → @s.whatsapp.net aliases), since employees are keyed by waJid
    but a sender may arrive as @lid. First eligible wins.

    `redirect` (optional): a portal-scoped deep-link path (e.g. '/portal/claims/new')
    so the tapped link lands the contact on a specific page. Ignored by the hub if
    it isn't under /portal.
    """
    if not _enabled:
        return None, "disabled"
    tries = [jid] + [j for j in (candidates or []) if j and j != jid]
    last = "error"
    for j in tries:
        body = {"jid": j}
        if redirect:
            body["redirect"] = redirect
        try:
            res = _request("POST", "/api/v1/wa-login/request", body=body)
        except Exception as e:
            print("[hub_sync] login request failed:", e)
            last = "client-unreachable"
            continue
        if res and res.get("ok") and res.get("url"):
            return res["url"], None
        last = (res or {}).get("error", "error")
    return None, last


# --- push (plugin -> hub), fire-and-forget ---------------------------------

def _post_deltas(deltas, source):
    try:
        _request("POST", "/api/v1/contacts", body={"source": source, "deltas": deltas})
    except Exception as e:
        # Best effort: log once, never propagate. The local write already succeeded.
        print("[hub_sync] push failed (ignored):", e)


def push_deltas(deltas, source="plugin", resolve_names=True):
    """Fire-and-forget: POST a list of per-JID deltas in a background thread.
    Each delta: {jid, updatedAt, and any of whitelisted/paused/noMention/isAdmin/
    name/pauseReason}. Returns immediately; never raises."""
    if not _enabled or not deltas:
        return
    now = int(time.time())
    prepared = []
    for d in deltas:
        d = dict(d)
        d.setdefault("updatedAt", now)
        if resolve_names and not d.get("name") and d.get("jid"):
            n = resolve_name(d["jid"])
            if n:
                d["name"] = n
        # Tag with the routed profile(s). A list (0..n): group jids resolve to
        # one profile; individual jids stay empty (hub derives per-chat). Only
        # attach when non-empty so we never overwrite a hub-derived value with
        # "no opinion". Older hubs simply ignore the field.
        if not d.get("profiles"):
            profs = profiles_for_jid(d.get("jid"))
            if profs:
                d["profiles"] = profs
        prepared.append(d)
    threading.Thread(target=_post_deltas, args=(prepared, source), daemon=True).start()


def push_flag(jid, flag, value, reason=None, source="plugin"):
    """Convenience: push a single flag change for one jid."""
    d = {"jid": jid, flag: bool(value), "updatedAt": int(time.time())}
    if reason is not None:
        d["pauseReason"] = reason
    push_deltas([d], source=source)


# --- whatsapp connection status (relink watcher -> hub) --------------------

def _post_status(body):
    try:
        _request("POST", "/api/v1/wa-status", body=body)
    except Exception as e:
        # Best effort: a hub/Cloudflare outage must never affect the bot.
        print("[hub_sync] status push failed (ignored):", e)


def push_status(status, qr=None, bot_user=None):
    """Fire-and-forget: report the WhatsApp connection status to the hub.

    `status` is one of 'connected' | 'unlinked' | 'disconnected'. `qr` (raw
    string) is sent ONLY while unlinked — it's a login handshake, so it rotates
    ~every 20s and the hub clears it on connect. Returns immediately; never
    raises. No-op if hub-sync isn't configured."""
    if not _enabled:
        return
    body = {"status": status, "ts": int(time.time())}
    if qr:
        body["qr"] = qr
    if bot_user:
        body["botUser"] = bot_user
    threading.Thread(target=_post_status, args=(body,), daemon=True).start()

    # Also mirror over the live-push WebSocket (best-effort) so an open hub tab
    # flips the status pill instantly, not on its next poll. HTTP above stays the
    # source of truth for persistence; this is a fast-path notification only.
    try:
        import hub_push  # type: ignore
        hub_push.push_status(status, qr=qr, bot_user=bot_user)
    except Exception:
        pass


def push_state_section(section, jids, source="plugin"):
    """Push a whole list-section as booleans for the given jids (used right after
    save_set_to_file). Only sets the ONE flag for this section per jid; other flags
    are untouched by the hub because deltas are partial."""
    flag = SECTION_FLAG.get(section)
    if not flag:
        return
    now = int(time.time())
    deltas = [{"jid": j, flag: True, "updatedAt": now} for j in jids]
    push_deltas(deltas, source=source)


# --- pull (hub -> plugin), applied to config.yaml --------------------------
# DRY: the hub now reads/writes the SAME single source as the plugin and the
# gateway — config.yaml gateway.profile_routes + whatsapp_admins — via the
# plugin's config_routes.py atomic ruamel writer. state.yaml is retired.

def _config_routes():
    """Import the plugin's config_routes module (in-package). Returns None if
    unavailable so callers can no-op gracefully."""
    try:
        from . import config_routes as _cr  # normal in-package import
        return _cr
    except Exception:
        try:
            import config_routes as _cr  # fallback when run as a loose module
            return _cr
        except Exception:
            return None


def pull_and_apply(state_file=None):
    """Fetch the hub's merged contact map and reconcile it into config.yaml
    gateway.profile_routes (reply/no_mention/paused/pause_reason) + whatsapp_admins
    (extra). Also honors a per-contact `profile` so the hub can MOVE a chat's
    profile (the end goal). Returns a summary dict, or {'ok': False} on any failure
    (config.yaml is left UNTOUCHED — the bot keeps its last-known config).

    Per-chat fields live on the route entry keyed by chat_id. Admin identity
    (root/extra) lives in whatsapp_admins; root is never overwritten by the hub.
    `state_file` is accepted for signature compatibility but ignored."""
    if not _enabled:
        return {"ok": False, "reason": "not-configured"}
    cr = _config_routes()
    if cr is None:
        return {"ok": False, "reason": "config_routes-unavailable"}

    try:
        res = _request("GET", "/api/v1/contacts")
    except Exception as e:
        print("[hub_sync] pull failed (config.yaml untouched):", e)
        return {"ok": False, "reason": "unreachable"}

    if not res or not res.get("ok"):
        return {"ok": False, "reason": "bad-response"}

    contacts = res.get("contacts", [])
    n_reply = n_nomention = n_paused = n_profile = 0
    extra_admins = []

    for c in contacts:
        jid = c.get("jid")
        if not jid:
            continue
        is_group = str(jid).endswith("@g.us")
        try:
            fields = {}
            fields["reply"] = bool(c.get("whitelisted"))
            fields["no_mention"] = bool(c.get("noMention"))
            fields["paused"] = bool(c.get("paused"))
            fields["pause_reason"] = (c.get("pauseReason") or "") if c.get("paused") else ""
            # Hub can move a chat's profile (only if it sent one).
            prof = c.get("profile")
            if prof:
                fields["profile"] = str(prof)
                n_profile += 1
            # Only create a route entry if the hub actually flags something for it
            # (avoid materializing a route for every contact the hub knows).
            meaningful = (fields["reply"] or fields["no_mention"] or fields["paused"]
                          or "profile" in fields)
            cr.set_route_fields(jid, create_if_missing=meaningful, **fields)
            if fields["reply"]:
                n_reply += 1
            if fields["no_mention"]:
                n_nomention += 1
            if fields["paused"]:
                n_paused += 1
        except Exception as e:
            print(f"[hub_sync] pull: failed to apply contact {jid} (ignored): {e}")
        # Admin flag → whatsapp_admins.extra (root is managed separately, never here).
        if c.get("isAdmin"):
            extra_admins.append(jid)

    # Reconcile the admin extra list (root left untouched).
    try:
        # Don't demote the root admin into extra; exclude it if present.
        root = (cr.load_admins() or {}).get("root") or ""
        extra_admins = sorted({j for j in extra_admins if j and j != root})
        cr.set_admins(extra=extra_admins)
    except Exception as e:
        print(f"[hub_sync] pull: failed to apply admins (ignored): {e}")

    return {
        "ok": True,
        "reply": n_reply,
        "no_mention": n_nomention,
        "paused": n_paused,
        "profile_moves": n_profile,
        "admins": len(extra_admins),
    }


def push_full_state(state_file=None, source="plugin"):
    """Reconcile: push the client's ENTIRE current per-chat config (from
    config.yaml routes + whatsapp_admins) up as deltas so the hub catches anything
    a dropped fire-and-forget POST missed. Last-write-wins on the hub side; called
    occasionally by the cron, not on the hot path. `state_file` ignored."""
    if not _enabled:
        return
    cr = _config_routes()
    if cr is None:
        return
    now = int(time.time())
    by_jid = {}
    try:
        for r in cr.load_routes():
            jid = r.get("chat_id")
            if not jid:
                continue
            e = by_jid.setdefault(jid, {"jid": jid, "updatedAt": now})
            if r.get("reply") is True:
                e["whitelisted"] = True
            if r.get("no_mention") is True:
                e["noMention"] = True
            if r.get("paused") is True:
                e["paused"] = True
                e["pauseReason"] = r.get("pause_reason") or "paused"
            # Report the chat's current profile so the hub can tag the contact
            # (drives the sidebar profile filter + per-contact dropdown default).
            # Skip 'default'/unset — a null profile on the hub means "unassigned".
            prof = r.get("profile")
            if prof and str(prof) != "default":
                e["profile"] = str(prof)
        for jid in (cr.load_admins() or {}).get("extra") or []:
            by_jid.setdefault(jid, {"jid": jid, "updatedAt": now})["isAdmin"] = True
    except Exception as e:
        print(f"[hub_sync] push_full_state read failed (ignored): {e}")
        return
    if by_jid:
        push_deltas(list(by_jid.values()), source=source, resolve_names=False)


# --- profiles reconcile (plugin -> hub) ------------------------------------

def _post_profiles(body):
    try:
        _request("POST", "/api/v1/profiles", body=body)
    except Exception as e:
        # Best effort; the hub simply keeps its last-known profile table. Older
        # hubs without this route 404 — swallowed here, never propagated.
        print("[hub_sync] profiles push failed (ignored):", e)


def push_profiles(source="plugin"):
    """Reconcile: report the full profile picture from config.yaml routes so the
    hub can render a profile filter/switcher (even for a profile with no synced
    contacts yet) and derive per-chat profiles authoritatively.

    Sends both the distinct profile list and the raw chat_id -> profile routes.
    Fire-and-forget; called occasionally by the cron (like push_full_state),
    never on the hot path. No-op if hub-sync isn't configured or no routes
    exist. config.yaml stays authoritative — this only READS it."""
    if not _enabled:
        return
    routes = load_routes()
    body = {
        "source": source,
        "updatedAt": int(time.time()),
        "profiles": distinct_profiles(),
        "routes": [{"chatId": c, "profile": p} for c, p in sorted(routes.items())],
    }
    if not body["profiles"] and not body["routes"]:
        return
    threading.Thread(target=_post_profiles, args=(body,), daemon=True).start()
