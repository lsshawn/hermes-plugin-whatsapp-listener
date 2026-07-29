import os
import json
import time
import asyncio
import yaml
import threading

PLUGIN_DIR = os.path.dirname(__file__)


def _hermes_home() -> str:
    """Hermes home dir. Honors $HERMES_HOME (the gateway sets it) and falls back
    to ~/.hermes. Used for config.yaml reads so the plugin and config_routes.py
    agree on the same file (and so tests can point at a temp home)."""
    return os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")


def _config_path() -> str:
    return os.path.join(_hermes_home(), "config.yaml")

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


# --- multiplex profile resolution -------------------------------------------
# The listener runs in pre_gateway_dispatch, BEFORE the gateway's own routing.
# When it creates/touches a session with a bare build_session_key() (no profile)
# it lands in the DEFAULT `agent:main` namespace + default state.db. Under
# gateway.multiplex_profiles that PRE-CREATES the wrong session for a routed
# group, so the agent then runs as DEFAULT (with default's skills/memory/creds)
# instead of the routed profile — the root cause of cross-profile data leaks.
# These helpers let the listener resolve the routed profile the SAME way the
# gateway does, so its session key + DB write match the gateway's.
_LISTENER_ROUTES_CACHE = {"key": None, "map": {}, "routes": {}, "admins": None}


def _refresh_routes_cache() -> None:
    """Reparse config.yaml (WhatsApp routes + whatsapp_admins) into the cache when
    config.yaml mtime changed. Populates:
      - map: chat_id -> profile (str)
      - routes: chat_id -> full route dict (reply/no_mention/paused/pause_reason/...)
      - admins: {'root': str, 'extra': [str]}
    This is the DRY replacement for the old state.yaml reads — one config.yaml,
    re-read live per message (mtime-gated so it's cheap)."""
    cfg_path = _config_path()
    try:
        mtime = os.stat(cfg_path).st_mtime_ns
    except OSError:
        return
    if _LISTENER_ROUTES_CACHE["key"] == mtime:
        return
    m, routes = {}, {}
    admins = {"root": "", "extra": []}
    try:
        import yaml
        cfg = yaml.safe_load(open(cfg_path)) or {}
        gw = cfg.get("gateway") if isinstance(cfg.get("gateway"), dict) else {}
        for r in (gw.get("profile_routes") or []):
            if not isinstance(r, dict):
                continue
            plat = (r.get("platform") or "").lower()
            if plat and plat not in ("whatsapp", "whatsapp_cloud"):
                continue
            cid = r.get("chat_id")
            if not cid:
                continue
            cid = str(cid)
            routes[cid] = dict(r)
            prof = r.get("profile")
            if prof:
                m[cid] = str(prof)
        wa = cfg.get("whatsapp_admins")
        if isinstance(wa, dict):
            admins = {
                "root": str(wa.get("root") or "").strip(),
                "extra": [str(x).strip() for x in (wa.get("extra") or []) if str(x).strip()],
            }
    except Exception:
        m, routes = {}, {}
        admins = {"root": "", "extra": []}
    _LISTENER_ROUTES_CACHE["key"] = mtime
    _LISTENER_ROUTES_CACHE["map"] = m
    _LISTENER_ROUTES_CACHE["routes"] = routes
    _LISTENER_ROUTES_CACHE["admins"] = admins


def _chat_id_candidates(chat_id: str) -> list:
    """chat_id plus its WhatsApp identity aliases, most-specific first.

    A DM's chat_id IS the peer's JID, and WhatsApp may deliver it in either the
    phone form (``60123...@s.whatsapp.net``) or the newer LID form
    (``2806...@lid``) for the SAME person. profile_routes stores one of them, so
    a raw exact-match silently misses the other and the chat resolves to
    'default' — running the wrong profile (see the block comment above). Group
    JIDs (@g.us) are stable and never aliased, so they short-circuit.

    Mirrors the alias expansion already used for admin identity below.
    """
    raw = str(chat_id)
    if not raw or "@g.us" in raw:
        return [raw]
    out = [raw]
    try:
        from gateway.whatsapp_identity import expand_whatsapp_aliases
        for alias in sorted(expand_whatsapp_aliases(raw)):
            # expand_whatsapp_aliases returns bare identifiers; restore both
            # suffix forms so either style of profile_routes entry matches.
            for cand in (f"{alias}@s.whatsapp.net", f"{alias}@lid", alias):
                if cand not in out:
                    out.append(cand)
    except Exception:
        pass
    return out


def _route_for_chat(chat_id: str) -> dict:
    """Full route entry dict for a chat_id (reply/no_mention/paused/pause_reason/
    profile/...), or {} if unrouted. Cached on config.yaml mtime. This is the DRY
    single-source lookup that replaces state.yaml's reply_whitelist /
    no_mention_groups / paused_chats sections.

    Alias-aware: matches @lid and @s.whatsapp.net forms of the same DM peer."""
    if not chat_id:
        return {}
    _refresh_routes_cache()
    routes = _LISTENER_ROUTES_CACHE.get("routes", {})
    for cand in _chat_id_candidates(chat_id):
        hit = routes.get(cand)
        if hit:
            return hit
    return {}


def _wa_admins() -> dict:
    """{'root': str, 'extra': [str]} from config.yaml whatsapp_admins (mtime-cached)."""
    _refresh_routes_cache()
    return _LISTENER_ROUTES_CACHE.get("admins") or {"root": "", "extra": []}


def _multiplex_on() -> bool:
    try:
        import yaml
        cfg = yaml.safe_load(open(_config_path())) or {}
        gw = cfg.get("gateway")
        return bool(isinstance(gw, dict) and gw.get("multiplex_profiles"))
    except Exception:
        return False


def _profile_for_chat(chat_id: str) -> str:
    """Return the profile a chat_id routes to (from config.yaml profile_routes),
    or 'default'. Cached on config.yaml mtime. WhatsApp routes only."""
    if not chat_id:
        return "default"
    cfg_path = os.path.join(os.path.expanduser("~/.hermes"), "config.yaml")
    try:
        mtime = os.stat(cfg_path).st_mtime_ns
    except OSError:
        return "default"
    _refresh_routes_cache()
    m = _LISTENER_ROUTES_CACHE["map"]
    for cand in _chat_id_candidates(chat_id):
        prof = m.get(cand)
        if prof:
            return prof
    return "default"


def _profile_key_kwarg(chat_id: str) -> dict:
    """{'profile': <name>} for build_session_key when multiplex routes this chat;
    empty dict otherwise (so single-profile behavior is byte-identical)."""
    if not _multiplex_on():
        return {}
    prof = _profile_for_chat(chat_id)
    return {"profile": prof} if prof and prof != "default" else {}


# Profiles that are safe to run in a GROUP chat: only those confined by
# config.yaml `profile_fs_allowlist` (their file access is boxed to their own
# dirs, e.g. cgpt, yltc). An UNRESTRICTED profile (personal, apps-coder, ideas,
# or any future profile with full file/terminal/session_search) must NEVER serve
# a group — a second human in the group could otherwise pull the operator's
# private data out of it. Such profiles are allowed only in DMs (1:1). Cached on
# config.yaml mtime.
_GROUPSAFE_CACHE = {"key": None, "set": set()}


def _group_safe_profiles() -> set:
    """Set of profile names allowed to run in a GROUP: the keys of
    config.yaml `profile_fs_allowlist` (fs-confined profiles)."""
    cfg_path = os.path.join(os.path.expanduser("~/.hermes"), "config.yaml")
    try:
        mtime = os.stat(cfg_path).st_mtime_ns
    except OSError:
        return set()
    if _GROUPSAFE_CACHE["key"] != mtime:
        s = set()
        try:
            import yaml
            cfg = yaml.safe_load(open(cfg_path)) or {}
            allow = cfg.get("profile_fs_allowlist")
            if isinstance(allow, dict):
                s = {str(k).strip().lower() for k in allow.keys()}
        except Exception:
            s = set()
        _GROUPSAFE_CACHE["key"] = mtime
        _GROUPSAFE_CACHE["set"] = s
    return _GROUPSAFE_CACHE["set"]


def _profile_allowed_in_group(profile: str, chat_id: str = "") -> bool:
    """True if `profile` may serve a GROUP chat. Unrestricted profiles are
    DM-only. 'default' is treated as group-safe here because its WhatsApp surface
    is separately locked (platform_toolsets: messaging-only) — but a group with no
    route shouldn't reach the reply path anyway.

    PER-ROUTE OPT-IN: a single group may be exempted by setting
    ``group_safe: true`` on its gateway.profile_routes entry. Use ONLY for a
    group the operator has verified is solo (no other humans) — an unrestricted
    profile there has full terminal + filesystem reach, so a second member could
    pull private data out of its replies. Scoped per chat_id ON PURPOSE: it does
    NOT make the profile group-safe anywhere else, so a newly added group still
    fails closed until explicitly vouched for.
    """
    p = (profile or "default").strip().lower()
    if p == "default":
        return True
    if p in _group_safe_profiles():
        return True
    if chat_id and _route_for_chat(chat_id).get("group_safe") is True:
        return True
    return False


def _scoped_session_db(chat_id: str):
    """A SessionDB pointed at the routed profile's state.db (so the listener's
    silent-save writes to the SAME db the gateway will use), or the default DB."""
    from hermes_state import SessionDB
    if _multiplex_on():
        prof = _profile_for_chat(chat_id)
        if prof and prof != "default":
            from pathlib import Path
            pdir = Path(os.path.expanduser("~/.hermes")) / "profiles" / prof
            if pdir.is_dir():
                return SessionDB(db_path=pdir / "state.db")
    return SessionDB()


def _build_silent_content(event, text: str) -> str:
    """Build the message content to store on the SILENT path, capturing text +
    voice (transcribed) + attachments — so the agent has real context of chats it
    doesn't reply in.

    Reuses CORE helpers so the stored format matches the reply path exactly:
      - voice: transcribe_audio() (core's Groq/STT-backed sync transcriber) — the
        transcript is stored as a quoted line, same as _enrich_message_with_
        transcription does on the reply path.
      - other attachments (image/video/doc/file): _build_media_placeholder(event)
        yields "[User sent an image: <url>]" etc. (core format; media itself is
        already cached by the WhatsApp bridge).

    FAIL-SOFT: any import/STT/media error falls back to the plain text we have.
    Never raises — the caller must always be able to store SOMETHING and skip.
    These core symbols are private-ish; wrapping them keeps a future hermes
    update rename from breaking silent-save (see DESIGN-dry-config.md)."""
    base = (text or "").strip()
    try:
        media_urls = getattr(event, "media_urls", None) or []
    except Exception:
        media_urls = []
    if not media_urls:
        return base

    parts = []
    try:
        from gateway.run import (
            _event_media_is_stt_input,
            _event_media_is_image,
            _event_media_is_audio,
            _event_media_is_video,
        )
    except Exception:
        _event_media_is_stt_input = _event_media_is_image = None
        _event_media_is_audio = _event_media_is_video = None

    # 1) Voice/audio → transcribe inline (Groq STT via core's sync transcriber).
    transcribed_any = False
    if _event_media_is_stt_input is not None:
        try:
            from tools.transcription_tools import transcribe_audio
        except Exception:
            transcribe_audio = None
        for i, path in enumerate(media_urls):
            try:
                if not _event_media_is_stt_input(event, i):
                    continue
                if transcribe_audio is None:
                    parts.append("[User sent a voice message]")
                    continue
                result = transcribe_audio(path)
                if isinstance(result, dict) and result.get("success") and result.get("transcript"):
                    parts.append(f'🎙️ "{result["transcript"].strip()}"')
                    transcribed_any = True
                else:
                    parts.append("[voice message could not be transcribed]")
            except Exception:
                parts.append("[voice message could not be transcribed]")

    # 2) Non-STT attachments → core placeholder ([User sent an image: <url>] etc.)
    #    Skip indices we already handled as voice to avoid double-counting.
    try:
        from gateway.run import _build_media_placeholder
        # _build_media_placeholder covers ALL media_urls; only use it when there's
        # a non-audio attachment, else we'd duplicate the voice line.
        has_non_audio = False
        for i in range(len(media_urls)):
            try:
                is_audio = _event_media_is_audio(event, i) if _event_media_is_audio else False
                is_stt = _event_media_is_stt_input(event, i) if _event_media_is_stt_input else False
                if not (is_audio or is_stt):
                    has_non_audio = True
                    break
            except Exception:
                has_non_audio = True
                break
        if has_non_audio:
            ph = _build_media_placeholder(event)
            if ph:
                # If we transcribed voice, only append placeholders for the
                # non-audio lines to avoid restating the audio url.
                if transcribed_any or parts:
                    for line in ph.splitlines():
                        if "audio" not in line.lower():
                            parts.append(line)
                else:
                    parts.append(ph)
    except Exception:
        pass

    all_parts = [p for p in parts if p]
    if base:
        all_parts.append(base)
    return "\n".join(all_parts) if all_parts else base


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


# NOTE: _save_state() was removed with the DRY migration — the plugin no longer
# WRITES state.yaml. All writes go to config.yaml via config_routes.py. _load_state()
# is kept only as a read-only LEGACY fallback in _root_admin() for boxes not yet
# migrated (state.yaml still present); it no-ops once state.yaml is gone.


# Root admin now lives in config.yaml `whatsapp_admins.root` (read live, mtime-
# cached). Falls back to the legacy state.yaml root_admin (during/after migration)
# and finally the historical placeholder so an empty config never locks the
# operator out.
def _root_admin() -> str:
    root = (_wa_admins().get("root") or "").strip()
    if root:
        return root
    # legacy fallback: state.yaml (only present pre-migration)
    try:
        legacy = str(_load_state().get("root_admin") or "").strip()
        if legacy:
            return legacy
    except Exception:
        pass
    return "YOUR_NUMBER@s.whatsapp.net"


# Back-compat shim: some call sites still read ROOT_ADMIN_ID as a value. It's now
# resolved live via _root_admin(); this module-level snapshot is only a fallback
# default and should not be relied on for the current root (use _root_admin()).
ROOT_ADMIN_ID = _root_admin()

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


_BOT_HINT_CACHE = {"hint": {}, "mtime": None}


def _bot_hint() -> dict:
    """Bot identity, reloaded when creds.json changes.

    Loading this once at import raced the WhatsApp bridge: on a boot where the
    session dir is still empty (e.g. after a relink) the hint stayed {} for the
    whole process lifetime, so every group mention-match silently failed until
    someone restarted the gateway. Re-stat the file (cheap, mtime-gated) so a
    late-written or re-paired creds.json is picked up on the next message.
    """
    try:
        creds_path = os.path.join(
            os.path.expanduser("~/.hermes"), "whatsapp", "session", "creds.json"
        )
        mtime = os.path.getmtime(creds_path) if os.path.exists(creds_path) else None
    except Exception:
        mtime = None
    if mtime != _BOT_HINT_CACHE["mtime"] or (not _BOT_HINT_CACHE["hint"] and mtime):
        _BOT_HINT_CACHE["hint"] = _load_bot_identity_hint()
        _BOT_HINT_CACHE["mtime"] = mtime
    return _BOT_HINT_CACHE["hint"]


class _BotHintProxy(dict):
    """Keeps the existing `_BOT_HINT.get(...)` call sites working while making
    every read go through the mtime-checked loader above."""

    def get(self, key, default=None):
        return _bot_hint().get(key, default)

    def __getitem__(self, key):
        return _bot_hint()[key]

    def __contains__(self, key):
        return key in _bot_hint()

    def __bool__(self):
        return bool(_bot_hint())


_BOT_HINT = _BotHintProxy()


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

# ---------------------------------------------------------------------------
# DRY reads: derive whitelist / no_mention / admins from config.yaml routes.
# state.yaml is retired — these read the SAME single source (config.yaml
# gateway.profile_routes + whatsapp_admins), live via the mtime-cached
# _refresh_routes_cache(). Function names/signatures are preserved so existing
# call sites are unchanged.
# ---------------------------------------------------------------------------
def load_set_from_file(section):
    """Return a set of chat_ids/jids for a logical section, derived from
    config.yaml. `section` is one of WHITELIST_FILE / NO_MENTION_FILE /
    ADMINS_FILE / ADMIN_GROUPS_FILE (names kept for call-site compatibility).

    - reply_whitelist  -> chat_ids whose route has reply: true
    - no_mention_groups -> chat_ids whose route has no_mention: true
    - admins           -> whatsapp_admins.extra (root added by get_all_admins)
    - admin_groups     -> DROPPED (decision 2): always empty set.
    """
    _refresh_routes_cache()
    routes = _LISTENER_ROUTES_CACHE.get("routes", {})
    if section == WHITELIST_FILE:
        return {cid for cid, r in routes.items() if r.get("reply") is True}
    if section == NO_MENTION_FILE:
        return {cid for cid, r in routes.items() if r.get("no_mention") is True}
    if section == ADMINS_FILE:
        return set(_wa_admins().get("extra") or [])
    # ADMIN_GROUPS_FILE and anything else: dropped / unused.
    return set()

def get_all_admins():
    admins = set(_wa_admins().get("extra") or [])
    root = _root_admin()
    if root and root != "YOUR_NUMBER@s.whatsapp.net":
        admins.add(root)
    return admins

def load_paused_chats() -> dict:
    """chat_id -> pause_reason for chats paused via route.paused: true.
    This is the ONLY pause source (config.yaml, authored in cupbots-hub)."""
    _refresh_routes_cache()
    routes = _LISTENER_ROUTES_CACHE.get("routes", {})
    out = {}
    for cid, r in routes.items():
        if r.get("paused") is True:
            out[cid] = r.get("pause_reason") or "paused"
    return out

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
        # Pauses come ONLY from route.paused in config.yaml (see the pause note
        # in on_pre_gateway_dispatch). The former app.db handoff scan was
        # dropped 2026-07-28.
        paused_chats = load_paused_chats()
        for cid in paused_chats.keys():
            _KNOWN_PAUSED_CHATS.add(cid)
            _KNOWN_PAUSED_CHATS.add(cid.split("@")[0])
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

        # Live push: send the just-arrived inbound message to the hub so an open tab
        # renders it INSTANTLY (no follow-up GET). Advisory preview — the hub
        # reconciles to the canonical state.db row later. `text` may be empty for
        # media/voice; the hub shows a placeholder and the fetch fills it in.
        # Fire-and-forget; never fatal (docs live-push-design.md).
        try:
            from . import hub_push
            hub_push.notify_new_message(chat_id, text=text, role="user")
        except Exception:
            pass

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
                            _profile_name = _route_for_chat(chat_id).get("profile") or "default"
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

            # HANDOFF RESOLVE: if this is an admin quote-reply to an open handoff
            # card, send the answer to the customer and stop. Correlated by message
            # id (quotedMessageId == ticket.handoffMsgId), not by parsing text.
            try:
                from .handoff import try_resolve_from_quote
                _resolve_status = try_resolve_from_quote(event)
                if _resolve_status:
                    send_msg(_resolve_status)
                    return {"action": "skip", "reason": "Resolved handoff ticket from admin quote-reply"}
            except Exception as _e:
                print(f"[whatsapp-listener] handoff resolve intercept failed (ignored): {_e}")

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
                db_path = os.environ.get('APP_DB', '/mnt/storage/projects/company-os/data/yltc-profile.db')
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        clients_db = db_conn.execute(
                            "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name = ?",
                            (client_query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name LIKE ?",
                                (f"%{client_query}%",)
                            ).fetchall()
                        
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT display_name AS business_name FROM account WHERE is_active=1").fetchall()
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
                db_path = os.environ.get('APP_DB', '/mnt/storage/projects/company-os/data/yltc-profile.db')
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        
                        # Find client
                        clients_db = db_conn.execute(
                            "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name = ?",
                            (query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name LIKE ?",
                                (f"%{query}%",)
                            ).fetchall()
                            
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT display_name AS business_name FROM account WHERE is_active=1").fetchall()
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
                                old = load_paused_chats()
                                try:
                                    from . import config_routes as _cr
                                    _cr.set_route_fields(target_chat_id, paused=True, pause_reason=biz_name)
                                    _hub_push_paused_diff(old, {**old, target_chat_id: biz_name})
                                    send_msg(f"⏸️ *AI Auto-Reply Paused* for *{biz_name}*.")
                                except Exception as we:
                                    send_msg(f"⚠️ Couldn't update config.yaml: {we}")
                except Exception as e:
                    send_msg(f"❌ *Error pausing client:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group pause command"}
                
            elif resume_match:
                query = resume_match.group(1).strip()
                db_path = os.environ.get('APP_DB', '/mnt/storage/projects/company-os/data/yltc-profile.db')
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        
                        # Find client
                        clients_db = db_conn.execute(
                            "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name = ?",
                            (query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name LIKE ?",
                                (f"%{query}%",)
                            ).fetchall()
                            
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT display_name AS business_name FROM account WHERE is_active=1").fetchall()
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
                                old = load_paused_chats()
                                if target_chat_id in old:
                                    try:
                                        from . import config_routes as _cr
                                        _cr.set_route_fields(target_chat_id, paused=False, pause_reason="")
                                        new_map = {k: v for k, v in old.items() if k != target_chat_id}
                                        _hub_push_paused_diff(old, new_map)
                                        send_msg(f"▶️ *AI Auto-Reply Resumed* for *{biz_name}*.")
                                    except Exception as we:
                                        send_msg(f"⚠️ Couldn't update config.yaml: {we}")
                                else:
                                    send_msg(f"ℹ️ *Client '{biz_name}' is not currently manually paused.*")
                except Exception as e:
                    send_msg(f"❌ *Error resuming client:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group resume command"}
            
            elif new_session_match:
                query = new_session_match.group(1).strip()
                db_path = os.environ.get('APP_DB', '/mnt/storage/projects/company-os/data/yltc-profile.db')
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        clients_db = db_conn.execute(
                            "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name = ?",
                            (query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name LIKE ?",
                                (f"%{query}%",)
                            ).fetchall()
                        
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT display_name AS business_name FROM account WHERE is_active=1").fetchall()
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
                db_path = os.environ.get('APP_DB', '/mnt/storage/projects/company-os/data/yltc-profile.db')
                try:
                    import sqlite3
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        clients_db = db_conn.execute(
                            "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name = ?",
                            (client_query,)
                        ).fetchall()
                        if not clients_db:
                            clients_db = db_conn.execute(
                                "SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1 AND display_name LIKE ?",
                                (f"%{client_query}%",)
                            ).fetchall()
                        
                        if not clients_db:
                            all_clients = db_conn.execute("SELECT display_name AS business_name FROM account WHERE is_active=1").fetchall()
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
                try:
                    # Pauses come ONLY from route.paused in config.yaml. The
                    # app.db handoff scan was dropped 2026-07-28.
                    paused_chats = load_paused_chats()
                    all_lines = []
                    for cid, reason in paused_chats.items():
                        all_lines.append(f"- *{cid}* ({reason})")

                    if not all_lines:
                        send_msg("🟢 *No AI chats are currently paused.* All bots are fully active.")
                    else:
                        list_str = "\n".join(all_lines)
                        send_msg(f"⏸️ *Currently Paused AI Chats:*\n{list_str}")
                except Exception as e:
                    send_msg(f"❌ *Error listing paused chats:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group paused list command"}
                
            elif sync_match:
                db_path = os.environ.get('APP_DB', '/mnt/storage/projects/company-os/data/yltc-profile.db')
                try:
                    import sqlite3
                    import urllib.request
                    
                    with sqlite3.connect(db_path) as db_conn:
                        db_conn.row_factory = sqlite3.Row
                        rows = db_conn.execute("SELECT id, display_name AS business_name, whatsapp_chat_id FROM account WHERE is_active=1").fetchall()
                        
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
                                db_conn.execute("UPDATE account SET display_name=? WHERE id=?", (new_name, cid))
                            db_conn.commit()
                            
                            updates_str = "\n".join([f"- *{old_name}* ➔ *{new_name}*" for new_name, _, old_name in updates])
                            send_msg(f"🔄 *WhatsApp group names synced with DB:*\n\n{updates_str}")
                except Exception as e:
                    send_msg(f"❌ *Error syncing names:* {e}")
                return {"action": "skip", "reason": "Intercepted admin group sync command"}

        # ---------------------------------------------------------
        # COMMAND: /whitelist add|remove <id>
        # Now writes config.yaml gateway.profile_routes[chat].reply (DRY: single
        # source). Adding a chat with no route entry creates one (profile 'default'
        # until routed). Effective LIVE on the next message (mtime-cached reread).
        # ---------------------------------------------------------
        if text.lower().startswith("/whitelist"):
            parts = text.split()

            def _set_reply(cid, val):
                try:
                    from . import config_routes as _cr
                    _cr.set_route_fields(cid, reply=val)
                    # Mirror to hub AFTER the local write (best-effort).
                    if val:
                        _hub_push_section_diff(WHITELIST_FILE, set(), {cid})
                    else:
                        _hub_push_section_diff(WHITELIST_FILE, {cid}, set())
                    return True
                except Exception as e:
                    print(f"[whatsapp-listener] /whitelist config write failed: {e}")
                    send_msg(f"⚠️ Couldn't update config.yaml: {e}")
                    return False

            if len(parts) >= 3:
                action = parts[1].lower()
                target_id = parts[2]
                if action == "add":
                    if _set_reply(target_id, True):
                        send_msg(f"✅ Added to whitelist:\n{target_id}")
                elif action == "remove":
                    if _set_reply(target_id, False):
                        send_msg(f"❌ Removed from whitelist:\n{target_id}")
                else:
                    send_msg("Usage: /whitelist add <id> OR /whitelist remove <id>")
            elif len(parts) == 2:
                action = parts[1].lower()
                if action == "remove" and "g.us" in chat_id:
                    if _set_reply(chat_id, False):
                        send_msg(f"❌ Removed from whitelist:\n{chat_id}")
                else:
                    send_msg("Usage: /whitelist add <id> OR /whitelist remove <id>")
            else:
                # Bare /whitelist in a group: auto-add this group
                if "g.us" in chat_id:
                    if _route_for_chat(chat_id).get("reply") is True:
                        send_msg(f"ℹ️ Already whitelisted:\n{chat_id}")
                    elif _set_reply(chat_id, True):
                        send_msg(f"✅ Whitelisted this group:\n{chat_id}")
                else:
                    n = len(load_set_from_file(WHITELIST_FILE))
                    send_msg(f"Usage: /whitelist add <id> OR /whitelist remove <id>\nCurrent whitelisted chats: {n}")

            return {"action": "skip", "reason": "Intercepted /whitelist command"}

        # ---------------------------------------------------------
        # COMMAND: /no-mention add|remove <id>
        # By default whitelisted GROUPS require a direct @mention before the bot
        # replies (safe default). Groups added here are EXEMPT — the bot replies
        # to every message without a mention. Requires core require_mention:
        # false (otherwise core drops non-mention messages upstream).
        # Now writes config.yaml route.no_mention (DRY).
        # ---------------------------------------------------------
        if text.lower().startswith("/no-mention"):
            if not is_admin:
                send_msg("🚫 Unauthorized. Only admins can use this command.")
                return {"action": "skip", "reason": "Unauthorized /no-mention attempt"}

            parts = text.split()

            def _set_no_mention(cid, val):
                try:
                    from . import config_routes as _cr
                    _cr.set_route_fields(cid, no_mention=val)
                    _hub_push_section_diff(NO_MENTION_FILE,
                                           set() if val else {cid},
                                           {cid} if val else set())
                    return True
                except Exception as e:
                    print(f"[whatsapp-listener] /no-mention config write failed: {e}")
                    send_msg(f"⚠️ Couldn't update config.yaml: {e}")
                    return False

            if len(parts) >= 3:
                action = parts[1].lower()
                target_id = parts[2]
                if action == "add":
                    if _set_no_mention(target_id, True):
                        send_msg(f"✅ No-mention mode ON for:\n{target_id}\n\n"
                                 "(Group must also be whitelisted / reply: true. Bot now "
                                 "replies to every message here, no @mention needed.)")
                elif action == "remove":
                    if _set_no_mention(target_id, False):
                        send_msg(f"❌ No-mention mode OFF for:\n{target_id}\n\n"
                                 "(Back to the default: bot only replies here when directly @mentioned.)")
                else:
                    send_msg("Usage: /no-mention add <id> OR /no-mention remove <id>")
            else:
                n = len(load_set_from_file(NO_MENTION_FILE))
                send_msg(f"Usage: /no-mention add <id> OR /no-mention remove <id>\n"
                         f"Current no-mention groups: {n}")

            return {"action": "skip", "reason": "Intercepted /no-mention command"}

        # ---------------------------------------------------------
        # COMMAND: /admin add|remove <id>
        # Now writes config.yaml whatsapp_admins.extra (DRY). Root admin
        # (whatsapp_admins.root) is never removable.
        # ---------------------------------------------------------
        if text.lower().startswith("/admin"):
            if not is_admin:
                send_msg("🚫 Unauthorized. Only admins can use this command.")
                return {"action": "skip", "reason": "Unauthorized /admin attempt"}

            parts = text.split()

            if len(parts) >= 3:
                action = parts[1].lower()
                target_id = parts[2]
                extra = set(_wa_admins().get("extra") or [])
                if action == "add":
                    extra.add(target_id)
                    try:
                        from . import config_routes as _cr
                        _cr.set_admins(extra=sorted(extra))
                        send_msg(f"✅ Added new admin:\n{target_id}")
                    except Exception as e:
                        send_msg(f"⚠️ Couldn't update config.yaml: {e}")
                elif action == "remove":
                    if target_id == _root_admin():
                        send_msg("❌ Cannot remove the Root Admin.")
                    else:
                        extra.discard(target_id)
                        try:
                            from . import config_routes as _cr
                            _cr.set_admins(extra=sorted(extra))
                            send_msg(f"❌ Removed admin:\n{target_id}")
                        except Exception as e:
                            send_msg(f"⚠️ Couldn't update config.yaml: {e}")
                else:
                    send_msg("Usage: /admin add <id> OR /admin remove <id>")
            else:
                all_admins = get_all_admins()
                send_msg(f"Usage: /admin add <id> OR /admin remove <id>\nCurrent admins: {len(all_admins)}")

            return {"action": "skip", "reason": "Intercepted /admin command"}

        # ---------------------------------------------------------
        # SILENT LISTENER LOGIC
        # ---------------------------------------------------------
        # Single source of truth: config.yaml gateway.profile_routes with
        # reply: true. (Formerly this was OR'd against the deprecated
        # whatsapp-profile-router/profile_routes.json, which meant a route could
        # reply even with reply: false / unset. reply is now authoritative.)
        whitelist = load_set_from_file(WHITELIST_FILE)
        routes_chat_ids = whitelist

        # Check if the chat itself (e.g. the group ID or DM ID) is whitelisted
        is_whitelisted = chat_id in whitelist

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

        # Pause state. SINGLE SOURCE OF TRUTH: route.paused in config.yaml
        # (authored in cupbots-hub, synced down). The old second gate — an
        # app.db query for orders with human_handoff=1 OR status='handoff' —
        # was retired 2026-07-28: it muted a chat indefinitely with no
        # operator-visible signal, and matched human_handoff=1 even on
        # already-resolved (confirmed/cancelled) orders, so a single stale
        # escalation silenced the group forever. To mute a chat now, set
        # paused: true on its gateway.profile_routes entry.
        is_paused, pause_reason = is_manually_paused(chat_id)

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
            # LIVE PROFILE ROUTING (no gateway restart): stamp source.profile from
            # the mtime-live route map BEFORE handing off. The gateway's profile
            # resolution checks source.profile FIRST (priority 1 in
            # _resolve_profile_home_for_source), before its own BOOT-CACHED
            # profile_routes (priority 2). By setting it here — where we reparse
            # config.yaml on every message — a profile change in config.yaml takes
            # effect on the NEXT message with no restart. Only stamp for a real
            # routed profile under multiplex; leave unset otherwise so single-
            # profile / default behavior is byte-identical.
            _resolved_profile = _profile_for_chat(chat_id) if _multiplex_on() else "default"

            # GROUP SAFETY (hard rule): an UNRESTRICTED profile (personal,
            # apps-coder, ideas, …) must never run in a GROUP — a second human in
            # the group could pull the operator's private data out of its replies.
            # Only fs-allowlisted profiles (cgpt, yltc) or default (separately
            # locked) may serve groups; unrestricted profiles are DM-only. This
            # holds even for an admin @mention. Fall through to silent-save.
            if is_group and not _profile_allowed_in_group(_resolved_profile, chat_id):
                print(
                    f"[whatsapp-listener] BLOCKED reply: profile '{_resolved_profile}' "
                    f"is not group-safe (unrestricted); chat={chat_id}. "
                    f"Unrestricted profiles are DM-only. Saving silently."
                )
                should_reply = False
            else:
                try:
                    _pk = _profile_key_kwarg(chat_id)  # {} unless multiplex + non-default route
                    if _pk and getattr(source, "profile", None) != _pk["profile"]:
                        source.profile = _pk["profile"]
                except Exception:
                    pass
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
            # MULTIPLEX FIX: stamp the routed profile so the session key lands
            # in the correct namespace (agent:<profile>) and the gateway reuses
            # THIS session instead of a default one. Also stamp source.profile
            # so get_or_create_session/store agree.
            _pk = _profile_key_kwarg(chat_id)
            if _pk:
                try:
                    safe_source.profile = _pk["profile"]
                except Exception:
                    pass
            session_key = build_session_key(
                safe_source,
                group_sessions_per_user=getattr(gateway.config, "extra", {}).get("group_sessions_per_user", True),
                thread_sessions_per_user=getattr(gateway.config, "extra", {}).get("thread_sessions_per_user", False),
                **_pk,
            )
        except Exception:
            safe_source = None

        if not safe_source:
            return {"action": "skip", "reason": "Failed to create SessionSource"}

        # FULL-CONTEXT silent capture (decision 5): store text + voice + attachments
        # so the agent has real context of chats it doesn't reply in. Reuses CORE
        # helpers (no new media schema): _build_media_placeholder for attachments,
        # transcribe_audio for voice. Everything is fail-soft — on any error we
        # fall back to whatever text we already have and still store a row.
        content_to_store = _build_silent_content(event, text)

        # Use scoped DB (routed profile's state.db) for BOTH session creation
        # AND message appending. Previously session_store.get_or_create_session()
        # always hit the default profile's state.db, leaking default memory/history
        # (IBKR, agenda, etc.) into routed cgpt/yltc chats.
        db = _scoped_session_db(chat_id)
        # Routed profile for this chat (live). Stored on the session so its rows are
        # correctly tagged. NOTE: create_session persists `profile_name` (NOT the
        # `profile` column) — verified against hermes_state._insert_session_row.
        _routed_profile = _profile_for_chat(chat_id) if _multiplex_on() else "default"
        try:
            # Look up existing session by session_key in the scoped DB.
            source_str = f"whatsapp:{chat_id}"
            row = db.find_session_by_peer(
                source=source_str,
                session_key=session_key,
                chat_id=chat_id,
                chat_type="group" if is_group else "dm",
            )
            if row:
                session_id = row["id"]
            else:
                # Create a new session in the scoped DB.
                import uuid
                from datetime import datetime
                now = datetime.utcnow()
                session_id = f"{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
                _create_kwargs = dict(
                    session_key=session_key,
                    chat_id=chat_id,
                    chat_type="group" if is_group else "dm",
                    user_id=user_id,
                )
                if _routed_profile and _routed_profile != "default":
                    _create_kwargs["profile_name"] = _routed_profile
                db.create_session(session_id, source_str, **_create_kwargs)
            db.append_message(
                session_id=session_id,
                role="user",
                content=content_to_store,
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
