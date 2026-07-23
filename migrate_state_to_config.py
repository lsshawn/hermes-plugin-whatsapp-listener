#!/usr/bin/env python3
"""One-time migration: fold plugins/whatsapp-listener/state.yaml into config.yaml.

Steps:
 1. Normalize config.yaml once (ruamel canonical form) so later writes are
    minimal-diff. Verified semantically identical to the original.
 2. For each state.yaml section, set the equivalent per-route field / admin:
      reply_whitelist    -> route.reply = true      (create route if missing)
      no_mention_groups  -> route.no_mention = true
      paused_chats       -> route.paused = true + pause_reason
      root_admin/admins  -> whatsapp_admins.root / .extra
    admin_groups is DROPPED (decision 2).
 3. Back up state.yaml -> state.yaml.migrated.bak, then delete state.yaml.

Idempotent-ish: re-running just re-asserts the same fields. Run with the gateway
venv python and HERMES_HOME set (or from ~/.hermes).

    HERMES_HOME=~/.hermes ~/.hermes/hermes-agent/venv/bin/python \
        ~/.hermes/plugins/whatsapp-listener/migrate_state_to_config.py
"""
import os
import sys

HERMES_HOME = os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")
PLUGIN_DIR = os.path.join(HERMES_HOME, "plugins", "whatsapp-listener")
STATE_FILE = os.path.join(PLUGIN_DIR, "state.yaml")
CONFIG_PATH = os.path.join(HERMES_HOME, "config.yaml")

sys.path.insert(0, PLUGIN_DIR)
import config_routes as C  # noqa: E402


def _load_state():
    import yaml
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def main():
    state = _load_state()
    if not state:
        print("No state.yaml found (or empty) — nothing to migrate.")
        return

    # --- 1. normalize config.yaml once (canonical ruamel form) ---
    doc, ok = C._load_raw()
    if not ok:
        print("ERROR: could not load config.yaml via ruamel; aborting.")
        sys.exit(1)
    C._dump_atomic(doc)
    print("config.yaml normalized (canonical form).")

    # existing routed chat_ids (for reporting which get created)
    existing = {r.get("chat_id") for r in C.load_routes()}

    whitelist = [str(x) for x in (state.get("reply_whitelist") or [])]
    no_mention = set(str(x) for x in (state.get("no_mention_groups") or []))
    paused = state.get("paused_chats") or {}
    root = str(state.get("root_admin") or "").strip()
    admins = [str(x).strip() for x in (state.get("admins") or []) if str(x).strip()]

    created, updated = [], []

    # --- 2a. whitelist -> route.reply = true ---
    for cid in whitelist:
        nm = cid in no_mention
        C.set_route_fields(cid, reply=True, no_mention=(True if nm else None),
                           create_if_missing=True)
        (created if cid not in existing else updated).append(cid)

    # --- 2b. no_mention entries that weren't in the whitelist (edge) ---
    for cid in no_mention:
        if cid not in whitelist:
            C.set_route_fields(cid, no_mention=True, create_if_missing=True)
            (created if cid not in existing else updated).append(cid)

    # --- 2c. paused (MANUAL) -> route.paused = true + reason ---
    for cid, reason in paused.items():
        C.set_route_fields(str(cid), paused=True,
                           pause_reason=str(reason) if reason else "paused",
                           create_if_missing=True)
        (created if str(cid) not in existing else updated).append(str(cid))

    # --- 2d. admins -> whatsapp_admins (root + extra; exclude root from extra) ---
    extra = sorted({a for a in admins if a and a != root})
    C.set_admins(root=root or None, extra=extra)

    print(f"reply/no_mention/paused applied. routes created: {sorted(set(created))}")
    print(f"routes updated (already existed): {sorted(set(updated))}")
    print(f"whatsapp_admins: root={root!r} extra={extra}")

    # --- 3. back up + delete state.yaml ---
    bak = STATE_FILE + ".migrated.bak"
    os.replace(STATE_FILE, bak)
    print(f"state.yaml -> {bak} (deleted from active path).")
    print("DONE.")


if __name__ == "__main__":
    main()
