"""Heartbeat scanner — v0.2 evaluates predicates against new successful runs.

v0.3 will extend this scanner with missed-run detection (compare expected
cron fire times against actual run records).

The scanner runs in a background thread (interval from config). Each pass:

  1. Reads the predicate configuration from config.json (predicates: { cron_id: [...] }).
  2. For each cron_id with predicates, looks up the most-recent finished run
     in cron/runs/<cron_id>.jsonl.
  3. If the run is newer than the last one we processed AND finished within
     the lookback window AND status was 'ok', evaluates all predicates.
  4. If any predicate fails, calls watchdog.handle_failure(failure_source='predicate')
     which runs the same retry-or-alert logic as a webhook failure.

State:
  - "last processed run timestamp per cron" lives in-memory (a small dict).
    Lost on restart, which is fine — the lookback window prevents
    re-evaluating very old runs after a restart.
  - "file_grew last-known size per (cron, predicate_index)" lives in the
    predicate_history SQLite table (persistent — needed for file_grew
    to be useful at all).
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

import db as db_mod
import openclaw_lookup as oc
import predicates as pred_mod


class HeartbeatScanner:
    """One per Watchdog. Owns the in-memory 'last processed' map."""

    def __init__(self, watchdog):
        self.wd = watchdog
        self._lock = threading.Lock()
        # cron_id -> ts (ms) of last processed finished run
        self._last_processed: dict[str, int] = {}
        # Initialize from the latest run we see on startup so we don't re-process
        # historical runs (would generate false alarms for any expired predicate).
        self._seed_last_processed()

    def _seed_last_processed(self) -> None:
        runs_dir = self.wd.cfg["heartbeat"]["runs_dir_path"]
        for cron_id in self._configured_predicate_crons():
            last = oc.last_run_for(cron_id, runs_dir)
            if last:
                self._last_processed[cron_id] = int(last.get("ts", 0))

    def _configured_predicate_crons(self) -> list[str]:
        preds = self.wd.cfg.get("predicates", {}) or {}
        out = []
        for k, v in preds.items():
            if k.startswith("_"):    # skip _comment-like keys
                continue
            if k.startswith("00000000"):   # skip the example placeholder UUID
                continue
            if isinstance(v, list) and v:
                out.append(k)
        return out

    def scan_once(self) -> dict:
        """One scan pass. Returns stats dict written to heartbeat_scans table."""
        t_start = time.time()
        cfg = self.wd.cfg
        runs_dir = cfg["heartbeat"]["runs_dir_path"]
        tz_name = cfg["server"]["timezone"]
        lookback_seconds = float(cfg["heartbeat"]["lookback_hours"]) * 3600
        now_ms = int(time.time() * 1000)

        crons_checked = 0
        predicates_failed = 0

        for cron_id in self._configured_predicate_crons():
            crons_checked += 1
            preds = cfg["predicates"][cron_id]
            last_run = oc.last_run_for(cron_id, runs_dir)
            if not last_run:
                continue

            run_ts = int(last_run.get("ts", 0))
            run_status = last_run.get("status")
            run_id = last_run.get("sessionId") or str(run_ts)

            # Skip runs we've already processed
            with self._lock:
                last_ts = self._last_processed.get(cron_id, 0)
            if run_ts <= last_ts:
                continue

            # Skip runs that fell out of the lookback window
            if (now_ms - run_ts) / 1000 > lookback_seconds:
                with self._lock:
                    self._last_processed[cron_id] = run_ts
                continue

            # Mark processed BEFORE evaluating to avoid re-running the
            # predicates if scan triggers a retry and a new finished run
            # arrives mid-evaluation.
            with self._lock:
                self._last_processed[cron_id] = run_ts

            # Predicates only kick in on status=ok runs. status=error already
            # went through the webhook path.
            if run_status != "ok":
                continue

            # Evaluate predicates in sequence; stop on first failure
            for i, pred in enumerate(preds):
                ok, msg = pred_mod.evaluate(
                    pred,
                    tz_name=tz_name,
                    state_get=lambda key, cid=cron_id, idx=i: self._predicate_state_get(cid, idx, key),
                    state_set=lambda key, val, cid=cron_id, idx=i: self._predicate_state_set(cid, idx, key, val),
                )
                if not ok:
                    predicates_failed += 1
                    self.wd.handle_failure(
                        cron_id=cron_id,
                        failed_run_id=run_id,
                        error=msg,
                        failure_source="predicate",
                    )
                    break  # one alert per scan-cycle per cron

        duration_ms = int((time.time() - t_start) * 1000)
        with self.wd._lock:
            self.wd.conn.execute(
                "INSERT INTO heartbeat_scans (scanned_at, crons_checked, "
                "missed_detected, predicates_failed, duration_ms) "
                "VALUES (?, ?, ?, ?, ?)",
                (datetime.now(timezone.utc).isoformat(),
                 crons_checked, 0, predicates_failed, duration_ms),
            )

        return {
            "crons_checked": crons_checked,
            "predicates_failed": predicates_failed,
            "missed_detected": 0,    # v0.3
            "duration_ms": duration_ms,
        }

    def _predicate_state_get(self, cron_id: str, idx: int, key: str) -> dict | None:
        row = self.wd.conn.execute(
            "SELECT state_json FROM predicate_history "
            "WHERE cron_id=? AND predicate_index=?",
            (cron_id, idx),
        ).fetchone()
        if not row:
            return None
        try:
            return json.loads(row["state_json"])
        except (json.JSONDecodeError, KeyError):
            return None

    def _predicate_state_set(self, cron_id: str, idx: int, key: str, val: dict) -> None:
        self.wd.conn.execute(
            "INSERT INTO predicate_history (cron_id, predicate_index, state_json, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(cron_id, predicate_index) DO UPDATE SET "
            "state_json=excluded.state_json, updated_at=excluded.updated_at",
            (cron_id, idx, json.dumps(val), datetime.now(timezone.utc).isoformat()),
        )

    def run_forever(self, stop_event: threading.Event | None = None) -> None:
        interval_seconds = int(self.wd.cfg["heartbeat"]["interval_minutes"]) * 60
        while True:
            try:
                stats = self.scan_once()
                if stats["predicates_failed"]:
                    import sys
                    sys.stderr.write(
                        f"[heartbeat] {stats['predicates_failed']} predicate(s) failed "
                        f"across {stats['crons_checked']} crons\n"
                    )
            except Exception as e:
                import sys, traceback
                sys.stderr.write(f"[heartbeat] scan failed: {type(e).__name__}: {e}\n")
                traceback.print_exc(file=sys.stderr)
            if stop_event and stop_event.wait(interval_seconds):
                return
            else:
                time.sleep(interval_seconds)
