"""Single source of truth for WhatsApp per-chat config: read/write the root
``~/.hermes/config.yaml`` (gateway.profile_routes + whatsapp_admins + skills.disabled).

This module REPLACES the plugin's old ``state.yaml``. Everything a chat needs —
which profile serves it (``profile``), whether the bot replies (``reply``),
whether a group is exempt from the @mention requirement (``no_mention``), and
whether it's manually paused (``paused`` / ``pause_reason``) — lives on that
chat's entry under ``gateway.profile_routes`` in config.yaml. Operator identity
(``root``/``extra`` admins) lives in a top-level ``whatsapp_admins`` block.

Design notes
------------
* **ruamel.yaml, not yaml.safe_dump.** config.yaml is a ~1000-line, hand-annotated
  file. safe_dump would reflow every comment. ruamel round-trips comments and
  layout, and we only touch the keys we mean to.
* **Atomic write.** Write to a temp file in the same dir, then ``os.replace``.
* **Reads are hot.** The plugin re-reads on config.yaml mtime (see plugin.py
  ``_LISTENER_ROUTES_CACHE``); this module just does raw reads/writes. The mtime
  caching lives in the plugin so per-message reads stay cheap.
* **Only WhatsApp routes.** We ignore/skip routes whose ``platform`` isn't a
  WhatsApp variant, so we never clobber Telegram/Discord routing.

If ruamel isn't importable we fall back to PyYAML for READS (so the plugin still
functions), but WRITES require ruamel — a write attempt without it raises, which
callers treat as "couldn't persist" (fire-and-forget for hub; user-visible error
for in-chat commands).
"""

import os
import tempfile
import threading
from typing import Any, Dict, List, Optional

HERMES_HOME = os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")
CONFIG_PATH = os.path.join(HERMES_HOME, "config.yaml")

_WA_PLATFORMS = {"whatsapp", "whatsapp_cloud"}
_WRITE_LOCK = threading.Lock()

# Per-chat route fields this module manages (besides name/platform/chat_id/profile).
_PER_CHAT_FLAGS = ("reply", "no_mention", "paused", "pause_reason")


# --------------------------------------------------------------------------- io
def _ruamel():
    """Return a configured ruamel YAML instance, or None if unavailable."""
    try:
        from ruamel.yaml import YAML
    except Exception:
        return None
    y = YAML()
    y.preserve_quotes = True
    # width 80 matches the file's existing prose wrapping. NOTE: ruamel re-folds
    # multi-line plain scalars differently from the PyYAML that first wrote this
    # file, so the FIRST write reflows some prose blocks (cosmetic, semantically
    # identical — verified deep-equal). The migration normalizes config.yaml once
    # up front so every subsequent write is a MINIMAL diff. Long flow scalars
    # (api keys) are single tokens and never wrap regardless of width.
    y.width = 80
    # Match the existing config.yaml style: block sequences are indented under
    # their key with the dash at offset 2 (e.g. "  profile_routes:\n    - name:").
    # mapping=2, sequence=4, offset=2 reproduces that; the wrong values dedent
    # every list in the file and produce a huge spurious diff.
    y.indent(mapping=2, sequence=4, offset=2)
    return y


def _load_raw():
    """Load config.yaml preserving structure (ruamel doc) if possible, else a
    plain dict via PyYAML. Returns (doc, using_ruamel)."""
    y = _ruamel()
    if y is not None:
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return y.load(f), True
        except Exception:
            pass
    # Read-only fallback.
    try:
        import yaml
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return (yaml.safe_load(f) or {}), False
    except Exception:
        return {}, False


def _dump_atomic(doc) -> None:
    """Atomically write the ruamel doc back to config.yaml (preserving comments)."""
    y = _ruamel()
    if y is None:
        raise RuntimeError(
            "ruamel.yaml is required to write config.yaml but is not installed"
        )
    d = os.path.dirname(CONFIG_PATH) or "."
    fd, tmp = tempfile.mkstemp(prefix=".config.yaml.", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            y.dump(doc, f)
        os.replace(tmp, CONFIG_PATH)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


# ------------------------------------------------------------------ read helpers
def _is_wa(route: Any) -> bool:
    if not isinstance(route, dict):
        return False
    plat = str(route.get("platform") or "").lower()
    return plat in _WA_PLATFORMS


def load_routes() -> List[Dict[str, Any]]:
    """All WhatsApp route entries (as plain dicts). Read-only; safe on any box."""
    doc, _ = _load_raw()
    gw = doc.get("gateway") if isinstance(doc, dict) else None
    routes = (gw or {}).get("profile_routes") or []
    out = []
    for r in routes:
        if _is_wa(r):
            out.append({k: r.get(k) for k in r.keys()})
    return out


def route_for_chat(chat_id: str) -> Dict[str, Any]:
    """The WhatsApp route entry for chat_id as a plain dict, or {} if unrouted."""
    if not chat_id:
        return {}
    for r in load_routes():
        if str(r.get("chat_id")) == str(chat_id):
            return r
    return {}


def load_admins() -> Dict[str, Any]:
    """Return {'root': <jid|''>, 'extra': [<jid>, ...]} from whatsapp_admins."""
    doc, _ = _load_raw()
    wa = doc.get("whatsapp_admins") if isinstance(doc, dict) else None
    if not isinstance(wa, dict):
        return {"root": "", "extra": []}
    root = str(wa.get("root") or "").strip()
    extra = [str(x).strip() for x in (wa.get("extra") or []) if str(x).strip()]
    return {"root": root, "extra": extra}


# ----------------------------------------------------------------- write helpers
def _gateway_routes_node(doc):
    """Return the live ruamel profile_routes sequence node, creating the parent
    gateway map if needed. Mutating the returned node mutates the doc."""
    gw = doc.get("gateway")
    if gw is None:
        from ruamel.yaml.comments import CommentedMap
        gw = CommentedMap()
        doc["gateway"] = gw
    routes = gw.get("profile_routes")
    if routes is None:
        from ruamel.yaml.comments import CommentedSeq
        routes = CommentedSeq()
        gw["profile_routes"] = routes
    return routes


def _find_route_node(routes, chat_id: str):
    for r in routes:
        if isinstance(r, dict) and str(r.get("chat_id")) == str(chat_id) and _is_wa(r):
            return r
    return None


def set_route_fields(chat_id: str, *, profile: Optional[str] = None,
                     reply: Optional[bool] = None,
                     no_mention: Optional[bool] = None,
                     paused: Optional[bool] = None,
                     pause_reason: Optional[str] = None,
                     name: Optional[str] = None,
                     create_if_missing: bool = True) -> bool:
    """Set one or more fields on a WhatsApp chat's route entry (create it if it
    doesn't exist and create_if_missing). Only the passed (non-None) fields are
    written. Returns True if a change was persisted.

    Atomic + comment-preserving. Thread-safe (module-level lock)."""
    with _WRITE_LOCK:
        doc, ok = _load_raw()
        if not ok or not isinstance(doc, dict):
            # We loaded via the PyYAML fallback (no ruamel) or the file is broken —
            # we cannot safely round-trip a write.
            raise RuntimeError("cannot write config.yaml (ruamel unavailable or file unreadable)")

        routes = _gateway_routes_node(doc)
        node = _find_route_node(routes, chat_id)

        if node is None:
            if not create_if_missing:
                return False
            from ruamel.yaml.comments import CommentedMap
            node = CommentedMap()
            node["name"] = name or f"wa-{str(chat_id).split('@')[0][:16]}"
            node["platform"] = "whatsapp"
            node["chat_id"] = str(chat_id)
            # A newly onboarded chat with no explicit profile stays on 'default'
            # unless a profile is supplied — matches "unrouted => default" today.
            node["profile"] = profile or "default"
            routes.append(node)
            changed = True
        else:
            changed = False
            if name is not None and node.get("name") != name:
                node["name"] = name
                changed = True
            if profile is not None and node.get("profile") != profile:
                node["profile"] = profile
                changed = True

        for key, val in (("reply", reply), ("no_mention", no_mention),
                         ("paused", paused), ("pause_reason", pause_reason)):
            if val is None:
                continue
            if node.get(key) != val:
                node[key] = val
                changed = True

        if changed:
            _dump_atomic(doc)
        return changed


def remove_route(chat_id: str) -> bool:
    """Delete a WhatsApp chat's route entry entirely. Returns True if removed."""
    with _WRITE_LOCK:
        doc, ok = _load_raw()
        if not ok or not isinstance(doc, dict):
            raise RuntimeError("cannot write config.yaml (ruamel unavailable or file unreadable)")
        gw = doc.get("gateway") or {}
        routes = gw.get("profile_routes")
        if not routes:
            return False
        idx = None
        for i, r in enumerate(routes):
            if isinstance(r, dict) and str(r.get("chat_id")) == str(chat_id) and _is_wa(r):
                idx = i
                break
        if idx is None:
            return False
        del routes[idx]
        _dump_atomic(doc)
        return True


def get_skills_disabled() -> List[str]:
    """Return the global ``skills.disabled`` list from config.yaml ([] if unset)."""
    doc, _ = _load_raw()
    skills = doc.get("skills") if isinstance(doc, dict) else None
    if not isinstance(skills, dict):
        return []
    disabled = skills.get("disabled")
    if isinstance(disabled, str):
        disabled = [disabled]
    return [str(x).strip() for x in (disabled or []) if str(x).strip()]


def set_skills_disabled(names: List[str]) -> bool:
    """Replace the global ``skills.disabled`` list. Returns True if a change was
    persisted. Comment-preserving + atomic, same contract as the route writers.

    Hermes core reads this list (agent/skill_utils.py get_disabled_skills), so a
    write here takes effect on the gateway's next skills scan — no restart."""
    new = sorted({str(x).strip() for x in names if str(x).strip()})
    with _WRITE_LOCK:
        doc, ok = _load_raw()
        if not ok or not isinstance(doc, dict):
            raise RuntimeError("cannot write config.yaml (ruamel unavailable or file unreadable)")
        skills = doc.get("skills")
        if not isinstance(skills, dict):
            from ruamel.yaml.comments import CommentedMap
            skills = CommentedMap()
            doc["skills"] = skills
        cur = skills.get("disabled")
        if isinstance(cur, str):
            cur = [cur]
        cur_norm = sorted({str(x).strip() for x in (cur or []) if str(x).strip()})
        if cur_norm == new:
            return False
        from ruamel.yaml.comments import CommentedSeq
        seq = CommentedSeq()
        for x in new:
            seq.append(x)
        skills["disabled"] = seq
        _dump_atomic(doc)
        return True


def set_admins(root: Optional[str] = None, extra: Optional[List[str]] = None) -> bool:
    """Write the whatsapp_admins block. Only passed fields change. Returns True
    if a change was persisted."""
    with _WRITE_LOCK:
        doc, ok = _load_raw()
        if not ok or not isinstance(doc, dict):
            raise RuntimeError("cannot write config.yaml (ruamel unavailable or file unreadable)")
        wa = doc.get("whatsapp_admins")
        if not isinstance(wa, dict):
            from ruamel.yaml.comments import CommentedMap
            wa = CommentedMap()
            doc["whatsapp_admins"] = wa
        changed = False
        if root is not None and str(wa.get("root") or "") != str(root):
            wa["root"] = str(root)
            changed = True
        if extra is not None:
            new_extra = [str(x).strip() for x in extra if str(x).strip()]
            if list(wa.get("extra") or []) != new_extra:
                from ruamel.yaml.comments import CommentedSeq
                seq = CommentedSeq()
                for x in new_extra:
                    seq.append(x)
                wa["extra"] = seq
                changed = True
        if changed:
            _dump_atomic(doc)
        return changed
