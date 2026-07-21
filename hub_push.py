"""
hub_push — outbound live-push WebSocket to the hub (docs live-push-design.md,
Rollout step 2). The plugin dials OUT to the hub's /ws/client and pushes small
"something changed" events UP so an open hub tab updates in real time instead of
polling every few seconds.

Direction: this is client -> hub NOTIFY ONLY. The hub never sends commands back
this way — flag toggles / sends still arrive as normal HTTP over the tunnel. So
we only ever WRITE to the socket (plus consume the runtime's ping/pong).

Design (mirrors relink_watcher: an idempotent daemon thread; never fatal):
  • One asyncio loop in a daemon thread owns the connection.
  • Reconnect with exponential backoff + jitter; reset on a clean connect.
  • A bounded offline queue buffers events while disconnected, flushed on connect,
    so a brief hub/Cloudflare blip doesn't drop notifications.
  • Every event carries a uuid `id` so the hub/browser can dedupe re-deliveries.
  • Thread-safe entry points (notify_new_message / push_status) are called from the
    gateway's sync message hook and the relink watcher — they just enqueue.

Push is an OPTIMIZATION. If it's down, the hub falls back to its poll; nothing
here may ever raise into the caller or block message handling. Requires the
`websockets` package (present in the Hermes venv). Auth reuses HUB_CLIENT_SECRET.
"""

import os
import json
import time
import uuid
import queue
import threading
import asyncio

_MAX_QUEUE = 500          # bounded offline buffer (drop-oldest beyond this)
_BACKOFF_MIN = 1.0        # seconds
_BACKOFF_MAX = 30.0
_PING_INTERVAL = 45.0     # app-level keepalive (hub also auto-responds at runtime)

_started = False
_start_lock = threading.Lock()
_q: "queue.Queue[str]" = queue.Queue(maxsize=_MAX_QUEUE)
_hub_sync = None


def _ws_url():
    """Derive the ws(s):// endpoint from hub_sync's HUB_URL."""
    base = (getattr(_hub_sync, "HUB_URL", "") or "").rstrip("/")
    if not base:
        return ""
    if base.startswith("https://"):
        return "wss://" + base[len("https://"):] + "/ws/client"
    if base.startswith("http://"):
        return "ws://" + base[len("http://"):] + "/ws/client"
    return ""


def _enqueue(kind, data):
    """Build an envelope and enqueue it (drop-oldest if the buffer is full).
    Safe to call from any thread; never raises."""
    if _hub_sync is None or not _hub_sync.enabled():
        return
    try:
        env = {
            "v": 1,
            "type": kind,
            "ts": int(time.time()),
            "id": uuid.uuid4().hex,
            "data": data or {},
        }
        payload = json.dumps(env)
        try:
            _q.put_nowait(payload)
        except queue.Full:
            # Drop the oldest to make room — newest events matter most.
            try:
                _q.get_nowait()
            except queue.Empty:
                pass
            try:
                _q.put_nowait(payload)
            except queue.Full:
                pass
    except Exception:
        pass  # never let a notification break the caller


# --- public, thread-safe entry points --------------------------------------

# Monotonic synthetic ids for pushed message previews: NEGATIVE so they never
# collide with real state.db row ids (positive). The hub renders the preview
# instantly and reconciles to the real row on the next fetch. Guarded by a lock
# since notify_new_message is called from the gateway's message-hook thread.
_syn_lock = threading.Lock()
_syn_id = 0


def _next_syn_id():
    global _syn_id
    with _syn_lock:
        _syn_id -= 1
        return _syn_id


def notify_new_message(chat_id, text=None, role="user", ts=None):
    """An inbound message arrived in `chat_id` — push it to the hub for instant,
    GET-free rendering. `text` (the message body) is OPTIONAL: when present the hub
    appends it immediately; when absent this degrades to a bare nudge and the hub
    does a targeted fetch. The preview is advisory (synthetic negative id), so the
    canonical state.db row reconciles it later. Called from the inbound hook."""
    if not chat_id:
        return
    data = {"chatId": str(chat_id)}
    if text is not None:
        data["message"] = {
            "id": _next_syn_id(),
            "role": role,
            "content": text,
            "timestamp": int(ts if ts is not None else time.time()),
        }
    _enqueue("new_message", data)


def push_status(status, qr=None, bot_user=None):
    """Mirror a wa_status change over the live socket (in addition to the existing
    HTTP heartbeat, which remains the source of truth for persistence)."""
    data = {"status": status}
    if qr is not None:
        data["qr"] = qr
    if bot_user is not None:
        data["botUser"] = bot_user
    _enqueue("wa_status", data)


# --- connection loop (own thread) ------------------------------------------

async def _run():
    import websockets

    url = _ws_url()
    if not url:
        return
    secret = getattr(_hub_sync, "HUB_CLIENT_SECRET", "") or ""
    headers = [("Authorization", "Bearer " + secret)]
    backoff = _BACKOFF_MIN

    while True:
        if _gateway_shutting_down():
            return
        try:
            # additional_headers is the modern kwarg (websockets >= 14);
            # fall back to extra_headers for older releases.
            try:
                ws_ctx = websockets.connect(url, additional_headers=headers, open_timeout=10)
            except TypeError:
                ws_ctx = websockets.connect(url, extra_headers=headers, open_timeout=10)

            async with ws_ctx as ws:
                backoff = _BACKOFF_MIN  # clean connect → reset backoff
                await ws.send(json.dumps({
                    "v": 1, "type": "hello", "ts": int(time.time()),
                    "id": uuid.uuid4().hex, "data": {"pluginVersion": _plugin_version()},
                }))
                await _pump(ws)
        except Exception:
            # any connect/IO error → backoff and retry
            pass

        if _gateway_shutting_down():
            return
        # Exponential backoff with 50% jitter.
        import random
        delay = min(_BACKOFF_MAX, backoff)
        await asyncio.sleep(delay / 2 + random.random() * (delay / 2))
        backoff = min(_BACKOFF_MAX, backoff * 2)


async def _pump(ws):
    """Flush queued events and keep the socket alive until it drops. Runs a small
    app-level ping so intermediaries don't reap an idle connection (the hub answers
    via runtime auto-response, which never wakes/charges its Durable Object)."""
    loop = asyncio.get_event_loop()
    last_ping = time.time()
    while True:
        if _gateway_shutting_down():
            await ws.close()
            return
        # Drain the queue without blocking the loop (queue.get is blocking → executor).
        try:
            payload = await loop.run_in_executor(None, _q.get, True, 1.0)
        except Exception:
            payload = None
        if payload is not None:
            try:
                await ws.send(payload)
            except Exception:
                # Send failed → connection is gone. Requeue so we don't lose it.
                try:
                    _q.put_nowait(payload)
                except Exception:
                    pass
                return
        now = time.time()
        if now - last_ping >= _PING_INTERVAL:
            last_ping = now
            try:
                await ws.send(json.dumps({"v": 1, "type": "ping", "ts": int(now)}))
            except Exception:
                return


def _plugin_version():
    try:
        import os as _os
        p = _os.path.join(_os.path.dirname(__file__), "plugin.yaml")
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith("version:"):
                    return line.split(":", 1)[1].strip().strip('"')
    except Exception:
        pass
    return "unknown"


def _gateway_shutting_down():
    return os.environ.get("HERMES_GATEWAY_SHUTTING_DOWN") == "1"


def _thread_main():
    try:
        asyncio.run(_run())
    except Exception:
        pass  # daemon thread; never propagate


def start(hub_sync=None):
    """Start the outbound push thread once. Idempotent. No-ops (but still starts,
    to pick up config later) when the hub isn't configured — enqueues just drop."""
    global _started, _hub_sync
    with _start_lock:
        if _started:
            return
        _started = True
        _hub_sync = hub_sync
        if hub_sync is None:
            # Late-load config the same way the watcher does, so a hub-managed box
            # that set HUB_URL/SECRET in the env is picked up.
            try:
                import hub_sync as _hs  # type: ignore
                _hub_sync = _hs
            except Exception:
                _hub_sync = None
        if _hub_sync is None:
            return
        t = threading.Thread(target=_thread_main, name="hub-push", daemon=True)
        t.start()
        print("[whatsapp-listener] hub push started (live WebSocket → hub)")
