# hermes-plugin-whatsapp-listener

Turn a WhatsApp account into a controllable [Hermes](https://github.com/NousResearch/hermes-agent)
bot that **listens everywhere but only replies where you allow** — with optional
central management, connection monitoring, and QR relink from
[cupbots-hub](https://hub.cupbots.com).

## Why install this

Out of the box, a Hermes WhatsApp bot either replies to everything or nothing.
This plugin gives you the in-between that real deployments need:

- **Silent listener.** Every message is saved to Hermes' history (so the agent
  has context), but the bot only *replies* in chats you whitelist. Drop it into
  busy groups without it talking over people.
- **In-chat control.** Manage behaviour from WhatsApp itself:
  `/whitelist`, `/pause`, `/resume`, `/admin`, `/no-mention` — no SSH, no config
  files. A permanent `root_admin` can never be locked out.
- **Group mention-gating.** Whitelisted groups require an `@mention` by default;
  exempt specific groups with `/no-mention` so the bot answers freely there.
- **Never goes dark on an outage.** All state is written locally first — the bot
  keeps working even if the network, hub, or Cloudflare is down.

### Optional: connect to cupbots-hub

If you run [cupbots-hub](https://hub.cupbots.com), this plugin also:

- **Heartbeats connection status** to the hub — see at a glance which client's
  WhatsApp is online, reconnecting, or down.
- **Auto-relink by QR.** When WhatsApp logs the bot out, the plugin surfaces a
  fresh linking QR **in the hub UI** and reconnects on scan — no SSH, no server
  access. It backs up the old session first, so a relink is always recoverable.
- **Central contact management.** Whitelist / pause / no-mention flags sync both
  ways with the hub (last-write-wins), so you can manage many bots from one place.

The hub bits are entirely optional — without them the plugin runs fully
standalone and the hub features simply no-op.

## Install

```bash
hermes plugins install lsshawn/-hermes-plugin-whatsapp-listener
hermes plugins enable whatsapp-listener
```

Set your admin number in the plugin's `state.yaml`:

```yaml
root_admin: 60123456789@s.whatsapp.net
```

Restart the gateway:

```bash
systemctl --user restart hermes-gateway.service
```

That's it for a standalone bot. See [`after-install.md`](after-install.md) for the
optional cupbots-hub connection.

## Update

```bash
hermes plugins update whatsapp-listener
```

## Files

| file | purpose |
|---|---|
| `plugin.py` | core: silent-listener, whitelist/pause/no-mention, in-chat commands |
| `__init__.py` | registers the `pre_gateway_dispatch` hook + starts the relink watcher |
| `relink_watcher.py` | connection-status heartbeat + auto QR relink (hub) |
| `hub_sync.py` | hub sync library — no-ops if the hub isn't configured |
| `plugin.yaml` | plugin manifest |

## How it stays resilient

Local `state.yaml` is always the source of truth and is written **before** any
hub call. Every hub interaction is fire-and-forget and guarded — a hub or network
outage can never block or break message handling. When the hub returns, queued
changes reconcile by timestamp.
