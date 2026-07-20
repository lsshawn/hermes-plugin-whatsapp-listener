# whatsapp-listener installed ✅

WhatsApp silent-listener + whitelist/pause/no-mention control, with optional
cupbots-hub integration (central contact management, connection-status
monitoring, and QR relink from the hub).

## 1. Enable + set your admin

If it wasn't auto-enabled:

```
hermes plugins enable whatsapp-listener
```

Set `root_admin` (your WhatsApp number) in the plugin's `state.yaml` — this is the
permanent admin that can run ops commands:

```
~/.hermes/plugins/whatsapp-listener/state.yaml
  root_admin: 60123456789@s.whatsapp.net
```

Then restart the gateway:

```
systemctl --user restart hermes-gateway.service
```

That's everything for a **standalone** WhatsApp bot. The hub bits below are
optional — without them the plugin works fully and the relink watcher just
no-ops its status pushes.

## 2. (Optional) Connect this client to cupbots-hub

To manage contacts centrally, monitor connection status, and relink via QR from
the hub, install the hub layer (systemd pull-timer + credentials):

```
git clone git@github.com:lsshawn/cupbots-hub-sync.git
cd cupbots-hub-sync
HUB_CLIENT_SECRET=hubc_xxx ./install.sh
systemctl --user restart hermes-gateway.service
```

`HUB_CLIENT_SECRET` is shown once in the hub Admin when this client is registered.
Requires cupbots-hub to expose the wa-status endpoints (recent release).

## Updating

- **Plugin:** `hermes plugins update whatsapp-listener`
- **Hub layer:** `cd cupbots-hub-sync && git pull && ./install.sh`
