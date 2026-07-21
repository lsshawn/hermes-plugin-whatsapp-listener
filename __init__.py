def register(ctx) -> None:
    from .plugin import on_pre_gateway_dispatch, _HUB_SYNC
    ctx.register_hook("pre_gateway_dispatch", on_pre_gateway_dispatch)

    # Human-in-the-loop handoff: intercept the AI's reply, and on low confidence
    # create a ticket + post to the admin group instead of auto-sending. Best-effort
    # — if it can't import/register, normal replies are unaffected.
    try:
        from .handoff import transform_llm_output
        ctx.register_hook("transform_llm_output", transform_llm_output)
    except Exception as e:  # pragma: no cover - defensive
        print(f"[whatsapp-listener] handoff hook not registered (ignored): {e}")

    # Start the WhatsApp relink watcher: a daemon thread that heartbeats the
    # bridge's connection status to the hub and auto-surfaces a relink QR when the
    # bot is logged out. Best-effort and never fatal — if it can't start, the
    # plugin's message handling is unaffected. On standalone boxes (no hub) the
    # watcher still runs; its status pushes simply no-op.
    try:
        from . import relink_watcher
        relink_watcher.start(_HUB_SYNC)
    except Exception as e:  # pragma: no cover - defensive
        print(f"[whatsapp-listener] relink watcher not started (ignored): {e}")

    # Start the live-push WebSocket to the hub: a daemon thread that pushes small
    # "chat changed" + status events UP so an open hub tab updates in real time
    # (docs live-push-design.md, step 2). Best-effort; on standalone boxes (no hub)
    # or if `websockets` is unavailable it simply no-ops.
    try:
        from . import hub_push
        hub_push.start(_HUB_SYNC)
    except Exception as e:  # pragma: no cover - defensive
        print(f"[whatsapp-listener] hub push not started (ignored): {e}")
