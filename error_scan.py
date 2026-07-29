"""
error_scan — post-mortem failure detection for the Hermes gateway.

The problem this solves: the two failure modes that most often leave the user
staring at silence produce NO application-level error, because the process is
already gone when they happen:

  * OOM kill — SIGKILL from the kernel. No handler runs, no traceback, nothing
    written. The gateway simply stops mid-turn and systemd restarts it.
  * hard restart/crash — same effect: whatever turn was in flight is dropped.

Neither can be caught with try/except from inside the process that dies. The only
way to report them is to look BACKWARD on the next startup, which is what this
module does: it reads the systemd journal for this unit, finds kill/failure events
since the last scan, and pushes them to cupbots-hub as `oom`/`crash` rows.

Idempotency is essential — this runs on every boot and must not re-report the same
kill each time. Two guards:
  1. A watermark file records the last-scanned timestamp; only newer events count.
  2. Each row's id is a stable hash of (unit, event timestamp, kind), so even if
     the watermark is lost the hub's insert-or-ignore dedupes the row.

Stdlib only, and every path is best-effort: a failure in the error reporter must
never take down the gateway it's reporting for.
"""

import hashlib
import json
import os
import re
import subprocess
import time

HERMES_HOME = os.path.expanduser(os.environ.get("HERMES_HOME") or "~/.hermes")
UNIT = os.environ.get("HERMES_GATEWAY_UNIT", "hermes-gateway.service")
WATERMARK = os.path.join(HERMES_HOME, "state", "error_scan.json")

# How far back to look on a first run (no watermark yet). Bounded so a fresh
# install doesn't import weeks of ancient history into the hub.
_FIRST_RUN_LOOKBACK_SECONDS = 6 * 3600

# journal lines we care about, mapped to a failure kind.
_PATTERNS = (
    # systemd's own words for an OOM. Checked before the generic signal line so an
    # OOM is never mis-filed as a plain crash.
    ("oom", re.compile(r"killed by the OOM killer|oom-kill|result 'oom-kill'", re.I)),
    ("crash", re.compile(r"Main process exited, code=killed, status=9/KILL", re.I)),
    ("crash", re.compile(r"Failed with result '(signal|core-dump|timeout)'", re.I)),
)


def _read_watermark():
    try:
        with open(WATERMARK, "r", encoding="utf-8") as f:
            return float((json.load(f) or {}).get("last_scan") or 0)
    except Exception:
        return 0.0


def _write_watermark(ts):
    try:
        os.makedirs(os.path.dirname(WATERMARK), exist_ok=True)
        tmp = WATERMARK + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"last_scan": ts}, f)
        os.replace(tmp, WATERMARK)   # atomic: a crash mid-write can't corrupt it
    except Exception as e:
        print("[error_scan] could not persist watermark (ignored):", e)


def _stable_id(unit, ts_us, kind):
    """Deterministic id so the same journal event always maps to the same row."""
    h = hashlib.sha256(f"{unit}|{ts_us}|{kind}".encode("utf-8")).hexdigest()
    return h[:32]


def _journal_since(since_epoch):
    """Kill/failure events from the unit's journal since `since_epoch`.

    Uses `-o json` so we get an authoritative per-entry timestamp instead of
    parsing the human-readable prefix (which omits the year and is locale-shaped).
    Returns [] on any failure — a box without systemd/journalctl just reports
    nothing rather than erroring.
    """
    cmd = [
        "journalctl", "-u", UNIT, "-o", "json", "--no-pager",
        "--since", f"@{int(since_epoch)}",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    except Exception as e:
        print("[error_scan] journalctl unavailable (ignored):", e)
        return []
    if proc.returncode != 0:
        return []

    events = []
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except Exception:
            continue
        msg = entry.get("MESSAGE")
        if not isinstance(msg, str):
            continue
        for kind, pat in _PATTERNS:
            if pat.search(msg):
                try:
                    ts_us = int(entry.get("__REALTIME_TIMESTAMP") or 0)
                except Exception:
                    ts_us = 0
                events.append({
                    "kind": kind,
                    "ts_us": ts_us,
                    "occurred_at": int(ts_us / 1_000_000) if ts_us else int(time.time()),
                    "message": msg.strip(),
                })
                break   # first matching pattern wins (OOM outranks crash)
    return events


def _dedupe(events):
    """Collapse events describing ONE death into a single row.

    A single OOM emits several lines (`killed by the OOM killer`, then
    `code=killed, status=9/KILL`, then `result 'oom-kill'`). Reporting three rows
    for one kill would be noise, so we group by (kind-ish, second) and keep the
    most specific: an `oom` in the same window suppresses the generic `crash`.
    """
    by_second = {}
    for e in events:
        bucket = e["occurred_at"] // 10        # 10s window covers the burst
        cur = by_second.get(bucket)
        if cur is None:
            by_second[bucket] = e
        elif cur["kind"] != "oom" and e["kind"] == "oom":
            by_second[bucket] = e              # upgrade crash -> oom
    return sorted(by_second.values(), key=lambda e: e["occurred_at"])


def scan_and_report(hub_sync=None, block=True):
    """Find kill/failure events since the last scan and push them to the hub.

    Called once at gateway startup. `block=True` (default) sends synchronously —
    at boot a daemon thread can lose the race against the rest of startup, and
    this runs once so the cost is trivial.

    Returns the number of events reported. Never raises.
    """
    try:
        if hub_sync is None:
            hub_sync = _load_hub_sync()
        if hub_sync is None or not hub_sync.enabled():
            return 0

        now = time.time()
        since = _read_watermark() or (now - _FIRST_RUN_LOOKBACK_SECONDS)
        # Re-scan a small overlap: journald can order an entry slightly after the
        # watermark we wrote. Stable ids make the overlap harmless.
        events = _dedupe(_journal_since(max(0, since - 60)))
        # Drop anything at or before the watermark proper (the overlap's purpose is
        # only to catch late-ordered entries, not to re-report old ones).
        events = [e for e in events if e["occurred_at"] > since - 60]

        reports = []
        for e in events:
            kind = e["kind"]
            summary = (
                "Gateway killed by the OOM killer — the turn in flight was lost"
                if kind == "oom"
                else "Gateway died unexpectedly — the turn in flight was lost"
            )
            reports.append({
                "id": _stable_id(UNIT, e["ts_us"], kind),
                "kind": kind,
                "summary": summary,
                "detail": e["message"],
                "source": "startup-scan",
                "occurred_at": e["occurred_at"],
            })

        if reports:
            hub_sync.push_errors(reports, block=block)
            print(f"[error_scan] reported {len(reports)} prior failure(s) to hub")

        _write_watermark(now)
        return len(reports)
    except Exception as e:
        # The reporter itself must never be the thing that breaks startup.
        print("[error_scan] scan failed (ignored):", e)
        return 0


def _load_hub_sync():
    """Import hub_sync with hub credentials loaded, mirroring relink_watcher."""
    try:
        envp = os.path.join(HERMES_HOME, "hub-sync", ".env")
        if os.path.exists(envp):
            for line in open(envp, "r", encoding="utf-8"):
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        try:
            from . import hub_sync as _hs   # type: ignore
            return _hs
        except Exception:
            import hub_sync as _hs          # loose-module fallback
            return _hs
    except Exception as e:
        print("[error_scan] hub_sync not loaded (ignored):", e)
        return None


if __name__ == "__main__":
    # Manual run: `python error_scan.py` reports what it would find.
    n = scan_and_report()
    print(f"reported {n} event(s)")
