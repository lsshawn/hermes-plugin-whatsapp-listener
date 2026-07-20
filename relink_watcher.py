"""
WhatsApp relink watcher — overlay-only, no core Hermes edits.

Runs as a daemon thread started from the plugin's register() at gateway boot and
lives for the gateway's lifetime.

  STEP 2: steady-state heartbeat. Poll the bridge /health and mirror the
  connection status UP to the hub via hub_sync.push_status().

  STEP 3: auto-handoff relink. When the bot bridge PROCESS is gone for a
  sustained window (what a WhatsApp logout actually causes — the bot-mode
  bridge exits on DisconnectReason.loggedOut), treat the box as logged out:
    1. back up + clear the stale session dir (Baileys can't re-pair over
       invalid creds — core itself says "delete session and restart"),
    2. run the bridge in --pair-only --pair-json to surface a fresh QR,
    3. pipe each rotating QR UP to the hub via push_status(status='unlinked', qr=…),
    4. on a successful scan (pair-only emits 'connected' then exits), restart
       the normal bot bridge ourselves (core does NOT auto-respawn mid-session),
    5. report 'connected' and clear the QR.

WHY "process gone" is the trigger (not just /health=disconnected): a transient
network blip keeps the bot-mode bridge process ALIVE (Baileys reconnects
internally). Only a real logout makes it call process.exit(1). So a sustained
process-absence is the reliable, unambiguous "needs relink" signal, and it
avoids ever running pair-only over a still-valid session.

Contract: this thread NEVER raises into the gateway and NEVER blocks message
handling. Every failure path logs once and keeps the watcher alive. If hub_sync
is absent, pushes are no-ops and the loop still runs harmlessly.
"""

import json
import os
import shutil
import signal
import subprocess
import threading
import time
import urllib.request

# --- cadence ---------------------------------------------------------------
_HEARTBEAT_INTERVAL = 45          # healthy heartbeat cadence (s)
_UNLINKED_INTERVAL = 12           # while unlinked: faster (QR rotates ~20s)
# Sustained process-absence before we declare a logout and start the relink.
# At a ~15s bad-poll cadence this is a ~2-3 min grace window — long enough to
# ride out restarts/reconnects, short enough to relink promptly.
_PROCESS_GONE_POLLS = 10
_BAD_POLL_INTERVAL = 15
# Hard cap on a single pair-only attempt (Baileys surfaces a QR within seconds;
# if nobody scans, we stop and retry on the next loop rather than hang forever).
_PAIR_ONLY_TIMEOUT = 300

# --- paths -----------------------------------------------------------------
_HERMES_HOME = os.path.expanduser(os.environ.get("HERMES_HOME") or "~/.hermes")
_SESSION_DIR = os.path.join(_HERMES_HOME, "whatsapp", "session")

_thread = None
_started = False
_lock = threading.Lock()
# True while a relink (pair-only) is in progress, so the heartbeat loop doesn't
# fight it or double-trigger.
_relinking = False


# ---------------------------------------------------------------------------
# bridge discovery / health
# ---------------------------------------------------------------------------

def _bridge_port():
    try:
        return int(os.getenv("WHATSAPP_BRIDGE_PORT", "3000"))
    except (TypeError, ValueError):
        return 3000


def _poll_health(port):
    """Return the bridge /health dict, or None if unreachable. Never raises."""
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d/health" % port, timeout=3) as resp:
            if resp.status == 200:
                return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None
    return None


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, ValueError):
        return False
    except PermissionError:
        return True  # exists but not ours to signal
    except OSError:
        return False


def _bridge_pid():
    """PID from the session pidfile core writes (<session>/bridge.pid), or None.

    Core writes TWO lines: the PID on line 1 and a process-start-time baseline on
    line 2 (used to detect PID recycling). We only need the PID → read line 1."""
    try:
        with open(os.path.join(_SESSION_DIR, "bridge.pid"), "r", encoding="utf-8") as f:
            first = (f.readline() or "").strip()
            return int(first) if first else None
    except Exception:
        return None


def _bridge_process_present(port):
    """True if a bridge appears to be running: reachable /health OR a live pidfile
    PID. We treat EITHER as 'process present' so we never misjudge a busy-but-alive
    bridge (slow /health) as gone."""
    if _poll_health(port) is not None:
        return True
    pid = _bridge_pid()
    if pid and _pid_alive(pid):
        return True
    return False


def _resolve_bridge_script():
    """Locate bridge.js the SAME way core does: prefer $HERMES_HOME overlay copy,
    else the install-tree copy. Returns (bridge_dir, bridge_js) or (None, None)."""
    candidates = [
        os.path.join(_HERMES_HOME, "scripts", "whatsapp-bridge"),
        os.path.join(_HERMES_HOME, "hermes-agent", "scripts", "whatsapp-bridge"),
    ]
    for d in candidates:
        js = os.path.join(d, "bridge.js")
        if os.path.isfile(js):
            return d, js
    return None, None


def _node_executable():
    """Find node the way core does, falling back to PATH."""
    for name in ("node",):
        p = shutil.which(name)
        if p:
            return p
    return "node"


# ---------------------------------------------------------------------------
# session reset (back up then clear) — decision: recoverable
# ---------------------------------------------------------------------------

def _backup_and_clear_session():
    """Move the stale session dir aside so pair-only starts clean. Matches the
    existing session.bak.relink-<ts> convention already on the box. Returns the
    backup path, or None if there was nothing to back up / on failure."""
    try:
        if not os.path.isdir(_SESSION_DIR):
            return None
        # Timestamp without Date.now()-style helpers being unavailable here (this
        # is plain Python, so time.strftime is fine).
        stamp = time.strftime("%Y%m%d-%H%M%S")
        backup = _SESSION_DIR + ".bak.relink-" + stamp
        shutil.move(_SESSION_DIR, backup)
        os.makedirs(_SESSION_DIR, exist_ok=True)
        print("[whatsapp-listener] session backed up to %s and cleared for relink" % backup)
        return backup
    except Exception as e:
        print("[whatsapp-listener] session backup/clear failed (relink aborted):", e)
        return None


# ---------------------------------------------------------------------------
# stop / start the bot bridge (using core's own conventions)
# ---------------------------------------------------------------------------

def _stop_bot_bridge(port):
    """Best-effort stop of any live bot bridge before pair-only takes the session.
    In the logout case the process is already gone; this covers the alive-but-
    wedged case. Only signals the pidfile PID (never a recycled/unknown PID)."""
    pid = _bridge_pid()
    if pid and _pid_alive(pid):
        try:
            os.kill(pid, signal.SIGTERM)
            print("[whatsapp-listener] sent SIGTERM to bot bridge pid %d for relink" % pid)
        except Exception as e:
            print("[whatsapp-listener] could not stop bot bridge pid %d:" % pid, e)
    # Give it a moment to release port 3000.
    for _ in range(10):
        if _poll_health(port) is None:
            break
        time.sleep(0.5)


def _start_bot_bridge(port):
    """Relaunch the normal bot-mode bridge after a successful relink. Core does
    NOT auto-respawn it mid-session, so we do it ourselves with the same CLI the
    adapter uses. Detached, logging to the same bridge.log."""
    bridge_dir, bridge_js = _resolve_bridge_script()
    if not bridge_js:
        print("[whatsapp-listener] cannot restart bot bridge: bridge.js not found")
        return False
    mode = os.getenv("WHATSAPP_MODE", "bot")
    log_path = os.path.join(_HERMES_HOME, "whatsapp", "bridge.log")
    try:
        log_fh = open(log_path, "a", encoding="utf-8")
        subprocess.Popen(
            [
                _node_executable(), bridge_js,
                "--port", str(port),
                "--session", _SESSION_DIR,
                "--mode", mode,
            ],
            cwd=bridge_dir,
            stdout=log_fh,
            stderr=log_fh,
            env=dict(os.environ),
            start_new_session=True,
        )
        print("[whatsapp-listener] bot bridge restarted after relink (mode=%s)" % mode)
        return True
    except Exception as e:
        print("[whatsapp-listener] failed to restart bot bridge:", e)
        return False


# ---------------------------------------------------------------------------
# the relink itself: run pair-only, pipe QR up, restore bot on scan
# ---------------------------------------------------------------------------

def _run_relink(hub_sync, port):
    """Perform one auto-handoff relink. Returns True if it reconnected."""
    global _relinking
    _relinking = True
    proc = None
    try:
        bridge_dir, bridge_js = _resolve_bridge_script()
        if not bridge_js:
            print("[whatsapp-listener] relink: bridge.js not found, aborting")
            return False

        # 1. Make sure no bot bridge holds the session/port, then reset session.
        _stop_bot_bridge(port)
        _backup_and_clear_session()

        # 2. Launch pair-only --pair-json (same invocation the dashboard uses).
        env = dict(os.environ)
        env["WHATSAPP_MODE"] = os.getenv("WHATSAPP_MODE", "bot")
        env["WHATSAPP_DM_POLICY"] = "pairing"
        proc = subprocess.Popen(
            [
                _node_executable(), bridge_js,
                "--pair-only", "--pair-json",
                "--session", _SESSION_DIR,
            ],
            cwd=bridge_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=True,
            env=env,
        )
        print("[whatsapp-listener] relink: pair-only started, waiting for QR…")

        # 3. Stream its JSON events; push each QR up; detect connect.
        connected = False
        deadline = time.time() + _PAIR_ONLY_TIMEOUT
        # readline blocks; a watchdog thread kills the proc at the deadline so we
        # never hang the (daemon) watcher permanently.
        def _watchdog():
            while time.time() < deadline and proc.poll() is None:
                time.sleep(1)
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
        wd = threading.Thread(target=_watchdog, name="relink-watchdog", daemon=True)
        wd.start()

        if proc.stdout is not None:
            for line in proc.stdout:
                raw = (line or "").strip()
                if not raw:
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                event = str(payload.get("event") or "")
                if event == "qr":
                    qr = str(payload.get("qr") or "").strip()
                    if qr and hub_sync is not None:
                        try:
                            hub_sync.push_status("unlinked", qr=qr)
                        except Exception as e:
                            print("[whatsapp-listener] relink QR push error (ignored):", e)
                elif event == "connected":
                    connected = True
                    user = payload.get("user")
                    print("[whatsapp-listener] relink: scanned & connected:",
                          (user or {}).get("name") if isinstance(user, dict) else None)
                    break
                elif event == "error":
                    print("[whatsapp-listener] relink pair-only error:", payload.get("error"))

        # pair-only exits shortly after 'connected'; make sure it's gone.
        try:
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass

        if not connected:
            print("[whatsapp-listener] relink: no scan within timeout; will retry")
            return False

        # 4. Restore the normal bot bridge (core won't).
        _start_bot_bridge(port)

        # 5. Confirm it came up, then report connected (clears the hub QR).
        for _ in range(20):
            if _poll_health(port) is not None and str((_poll_health(port) or {}).get("status")) == "connected":
                break
            time.sleep(1)
        if hub_sync is not None:
            try:
                hub_sync.push_status("connected")
            except Exception:
                pass
        print("[whatsapp-listener] relink complete")
        return True
    except Exception as e:
        print("[whatsapp-listener] relink failed (ignored):", e)
        return False
    finally:
        _relinking = False


# ---------------------------------------------------------------------------
# main watch loop
# ---------------------------------------------------------------------------

def _gateway_shutting_down():
    """Avoid starting a relink while the gateway itself is stopping. Best-effort:
    if we can't tell, assume not shutting down."""
    return os.environ.get("HERMES_GATEWAY_SHUTTING_DOWN") == "1"


def _watch_loop(hub_sync):
    port = _bridge_port()
    gone_polls = 0
    last_reported = None
    last_push_ts = 0.0

    while True:
        try:
            if _relinking:
                # A relink is running; it owns status reporting. Idle briefly.
                time.sleep(_UNLINKED_INTERVAL)
                continue

            health = _poll_health(port)
            connected = health is not None and str(health.get("status")) == "connected"
            present = _bridge_process_present(port)

            if connected:
                gone_polls = 0
                status = "connected"
            elif not present:
                # Bridge process appears gone — the logout signature.
                gone_polls += 1
                status = "unlinked" if gone_polls >= _PROCESS_GONE_POLLS else "disconnected"
            else:
                # Alive but not connected → transient; do not escalate.
                gone_polls = 0
                status = "disconnected"

            now = time.time()
            changed = status != last_reported
            keepalive_due = (now - last_push_ts) >= _HEARTBEAT_INTERVAL
            if hub_sync is not None and (changed or keepalive_due):
                try:
                    hub_sync.push_status(status)
                except Exception as e:
                    print("[whatsapp-listener] status push error (ignored):", e)
                last_reported = status
                last_push_ts = now

            # Trigger auto-handoff once we're confident it's a logout.
            if status == "unlinked" and not _gateway_shutting_down():
                print("[whatsapp-listener] bridge gone for %d polls — starting auto relink" % gone_polls)
                if _run_relink(hub_sync, port):
                    gone_polls = 0
                    last_reported = "connected"
                    last_push_ts = time.time()
                # Whether it succeeded or timed out, fall through and keep looping.

            # Cadence: fast while unlinked/relinking, slow when healthy.
            if status == "connected":
                time.sleep(_HEARTBEAT_INTERVAL)
            elif status == "unlinked":
                time.sleep(_UNLINKED_INTERVAL)
            else:
                time.sleep(_BAD_POLL_INTERVAL)
        except Exception as e:
            print("[whatsapp-listener] watcher loop error (ignored):", e)
            time.sleep(_HEARTBEAT_INTERVAL)


def _load_hub_sync():
    """Import hub_sync so the watcher can push status. Self-sufficient: works
    whether or not the plugin passed its module in. hub_sync ships INSIDE this
    plugin, so we prefer the in-package copy; the credentials live in
    ~/.hermes/hub-sync/.env (written by the repo's install.sh for hub clients),
    which we load if present. Returns the module, or None if unavailable.
    Standalone boxes with no .env still load the module; it just no-ops
    (enabled() is False without HUB_URL/HUB_CLIENT_SECRET)."""
    try:
        # Load hub .env (hub-managed clients) so HUB_URL / HUB_CLIENT_SECRET exist.
        envp = os.path.join(_HERMES_HOME, "hub-sync", ".env")
        if os.path.exists(envp):
            for line in open(envp, "r", encoding="utf-8"):
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        # Prefer the in-package hub_sync (ships with this plugin); fall back to a
        # legacy standalone install at ~/.hermes/hub-sync for older layouts.
        try:
            from . import hub_sync as _hs  # type: ignore
            return _hs
        except Exception:
            hs_dir = os.path.join(_HERMES_HOME, "hub-sync")
            if os.path.isdir(hs_dir):
                import sys as _sys
                if hs_dir not in _sys.path:
                    _sys.path.insert(0, hs_dir)
                import hub_sync as _hs
                return _hs
        return None
    except Exception as e:
        print("[whatsapp-listener] relink watcher: hub_sync not loaded (ignored):", e)
        return None


def start(hub_sync=None):
    """Start the watcher daemon thread once. Idempotent.

    `hub_sync` may be passed in (e.g. the plugin's already-imported module); if
    omitted or None, the watcher loads hub_sync itself so it works on clients that
    haven't applied the plugin push-patch. Status pushes are skipped if hub_sync
    is unavailable — the watcher (and its auto-relink) still run."""
    global _thread, _started
    with _lock:
        if _started:
            return
        _started = True
        if hub_sync is None:
            hub_sync = _load_hub_sync()
        _thread = threading.Thread(
            target=_watch_loop,
            args=(hub_sync,),
            name="whatsapp-relink-watcher",
            daemon=True,
        )
        _thread.start()
        print("[whatsapp-listener] relink watcher started (heartbeat %ds, auto-relink on sustained bridge loss)"
              % _HEARTBEAT_INTERVAL)
