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

## 3. (Optional) Human-in-the-loop handoff + knowledge base

The plugin can escalate low-confidence AI replies to a human instead of auto-
sending them. When on, the AI ends each customer reply with a confidence marker
(injected via `config.yaml` `platform_hints.whatsapp`, added on install); the
`transform_llm_output` hook parses+strips it and, when confidence is low (or the
customer asks for a human), creates a **ticket** on the client-local API and posts
a **handoff card** to your admin group. An admin **quote-replies** that card —
`ok` sends the AI's suggestion verbatim; anything else is relayed (rephrased into
the bot's voice unless prefixed `raw:`). Resolution is correlated by message id.

Requires the client-local API (`client-api`, exposes `/api/v1/tickets` +
`/api/v1/kb`) running on this box. Configure via env (e.g. in the gateway's
environment or hub-sync `.env`):

```
CLIENT_API_URL=http://127.0.0.1:8787      # the client-api on this box
HUB_CLIENT_SECRET=hubc_xxx                # same secret the API + broker use
BRIDGE_URL=http://127.0.0.1:3000          # Baileys bridge (default)
STATE_DB_PATH=~/.hermes/state.db          # default
HANDOFF_CONFIDENCE_THRESHOLD=0.6          # escalate below this (default 0.6)
HANDOFF_ADMIN_GROUP=<jid>@g.us            # optional; else first admin_groups entry
HANDOFF_KB_SCORE_FLOOR=0.0                # >0 also escalates on weak KB match
```

Knowledge base: put markdown under `docs/kb/{customer,internal,public}/*.md` with
an `audience:` frontmatter tag, then `POST /api/v1/kb/reingest` on the box. The AI
retrieves customer-tier chunks for replies; internal chunks are never served to a
customer channel (enforced in the retrieval query).

## Updating

- **Plugin:** `hermes plugins update whatsapp-listener`
- **Hub layer:** `cd cupbots-hub-sync && git pull && ./install.sh`
