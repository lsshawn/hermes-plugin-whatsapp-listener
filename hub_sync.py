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
#
# Migration window: before you move the mappings into config.yaml, fall back to
# the legacy whatsapp-profile-router/profile_routes.json so nothing goes blank.
HERMES_HOME = os.path.expanduser(os.environ.get("HERMES_HOME") or "~/.hermes")
CONFIG_YAML = os.path.join(HERMES_HOME, "config.yaml")
LEGACY_ROUTES_JSON = os.path.join(
    HERMES_HOME, "plugins", "whatsapp-profile-router", "profile_routes.json"
)

_routes_cache = {}        # {chat_id: profile}
_routes_cache_key = None  # (config_mtime, legacy_mtime) — reparse only on change
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


def _parse_legacy_routes():
    """Return {chat_id: profile} from the old profile_routes.json (fallback)."""
    try:
        with open(LEGACY_ROUTES_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    out = {}
    if isinstance(data, dict):
        for chat_id, meta in data.items():
            profile = (meta or {}).get("profile") if isinstance(meta, dict) else None
            if chat_id and profile:
                out[str(chat_id)] = str(profile)
    return out


def load_routes():
    """{chat_id: profile}, cached and reparsed only when a source file changes.
    config.yaml wins; legacy JSON fills gaps during the migration window."""
    global _routes_cache, _routes_cache_key
    key = (_mtime(CONFIG_YAML), _mtime(LEGACY_ROUTES_JSON))
    with _routes_lock:
        if key == _routes_cache_key:
            return _routes_cache
        merged = dict(_parse_legacy_routes())  # base
        merged.update(_parse_config_routes())  # config.yaml overrides
        _routes_cache = merged
        _routes_cache_key = key
        return merged


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


# --- pull (hub -> plugin), applied to state.yaml ---------------------------

def _load_yaml(path):
    import yaml
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _dump_yaml(path, data):
    """Write state.yaml preserving the section order the plugin expects."""
    import yaml
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True, default_flow_style=False)
    os.replace(tmp, path)  # atomic


def pull_and_apply(state_file=None):
    """Fetch the hub's merged contact map and rewrite the managed state.yaml
    sections. Returns a summary dict, or {'ok': False} on any failure (in which
    case state.yaml is left UNTOUCHED — the bot keeps its last-known config).

    Managed sections: reply_whitelist, no_mention_groups, admins (lists) and
    paused_chats (dict jid->reason). `root_admin` and any other keys are preserved
    verbatim — never touched by the hub."""
    path = state_file or STATE_FILE
    if not _enabled or not path:
        return {"ok": False, "reason": "not-configured"}

    try:
        res = _request("GET", "/api/v1/contacts")
    except Exception as e:
        print("[hub_sync] pull failed (state.yaml untouched):", e)
        return {"ok": False, "reason": "unreachable"}

    if not res or not res.get("ok"):
        return {"ok": False, "reason": "bad-response"}

    contacts = res.get("contacts", [])
    state = _load_yaml(path)

    # Rebuild managed list sections from the merged map.
    new_whitelist, new_nomention, new_admins = [], [], []
    new_paused = {}
    for c in contacts:
        jid = c.get("jid")
        if not jid:
            continue
        if c.get("whitelisted"):
            new_whitelist.append(jid)
        if c.get("noMention"):
            new_nomention.append(jid)
        if c.get("isAdmin"):
            new_admins.append(jid)
        if c.get("paused"):
            new_paused[jid] = c.get("pauseReason") or "paused"

    state["reply_whitelist"] = sorted(new_whitelist)
    state["no_mention_groups"] = sorted(new_nomention)
    state["admins"] = sorted(new_admins)
    state["paused_chats"] = new_paused
    # root_admin + anything else in `state` is left exactly as-is.

    _dump_yaml(path, state)
    return {
        "ok": True,
        "whitelist": len(new_whitelist),
        "no_mention": len(new_nomention),
        "admins": len(new_admins),
        "paused": len(new_paused),
    }


def push_full_state(state_file=None, source="plugin"):
    """Reconcile: push the client's ENTIRE current state.yaml up as deltas so the
    hub catches anything a dropped fire-and-forget POST missed. Safe because the
    hub merges last-write-wins; called occasionally by the cron, not on the hot path."""
    path = state_file or STATE_FILE
    if not _enabled or not path:
        return
    state = _load_yaml(path)
    now = int(time.time())
    # Collect a union of all managed jids with their flags.
    by_jid = {}
    for section in LIST_SECTIONS:
        flag = SECTION_FLAG[section]
        for j in (state.get(section) or []):
            by_jid.setdefault(j, {"jid": j, "updatedAt": now})[flag] = True
    for j, reason in (state.get("paused_chats") or {}).items():
        e = by_jid.setdefault(j, {"jid": j, "updatedAt": now})
        e["paused"] = True
        e["pauseReason"] = reason
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
