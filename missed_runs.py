"""Missed-or-failed run detection.

Reads OpenClaw's `cron/jobs.json` to enumerate enabled crons + their
schedules, computes expected fire times in a [since, until) window via
cron_parser, and checks `cron/runs/<id>.jsonl` for an actual finished
run within +/- grace_minutes of each expected fire.

For each expected fire one of three things is true:
  * A run within +/- grace exists with status="ok"  -> skipped (success)
  * A run within +/- grace exists with status != "ok" -> reported as
    "errored", carrying the actual run's status string
  * No run within +/- grace exists -> reported as "missed"

Crons do NOT need to be wired into the retry-watchdog (no failure-alert
webhook required). This module reads OpenClaw's filesystem state
directly and reports on it — purely informational.

The `fire_now` operation calls `openclaw cron run <id>` via the existing
oc.cron_run helper. The watchdog does not track or retry this; it's a
one-shot manual catch-up.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import cron_parser
import openclaw_lookup as oc


def find_missed(*, jobs_json_path: str, runs_dir: str,
                since: datetime, until: datetime,
                default_tz: str,
                grace_minutes: int = 5,
                expected_webhook_url: str | None = None) -> list[dict[str, Any]]:
    """For each enabled cron, return entries in [since, until) where the
    expected fire either didn't run or only produced errored runs.

    See module docstring for the kind=missed vs kind=errored semantics.

    If `expected_webhook_url` is provided, each entry gains a
    `wired_to_watchdog` boolean indicating whether the cron's
    failure-alert webhook is currently pointed at us. The UI uses this
    to surface errored crons that aren't being watched as one-click
    Wire candidates.

    Crons with unparseable schedules are reported once with a synthetic
    entry carrying schedule_parse_error so the operator can see them in
    the UI rather than silently dropping them.
    """
    if since.tzinfo is None or until.tzinfo is None:
        raise ValueError("since/until must be timezone-aware")

    grace = timedelta(minutes=grace_minutes)
    out: list[dict[str, Any]] = []

    for j in oc.read_jobs_with_alerts(jobs_json_path):
        if not j.get("enabled"):
            continue
        cron_id = j.get("cron_id")
        if not cron_id:
            continue
        schedule_expr = j.get("schedule") or ""
        tz_name = j.get("timezone") or default_tz

        # Wiring status: is this cron's failure-alert webhook pointed at us?
        wired_here = False
        if expected_webhook_url:
            fa = j.get("failure_alert") or {}
            if fa.get("mode") == "webhook" and fa.get("to") == expected_webhook_url:
                wired_here = True

        try:
            expr = cron_parser.parse(schedule_expr)
        except (ValueError, KeyError) as e:
            out.append({
                "cron_id": cron_id,
                "name": j.get("name"),
                "agent": j.get("agent"),
                "schedule": schedule_expr,
                "timezone": tz_name,
                "expected_at_iso": None,
                "expected_at_ms": None,
                "last_actual_run_iso": None,
                "last_actual_run_status": None,
                "wired_to_watchdog": wired_here,
                "schedule_parse_error": str(e),
            })
            continue

        try:
            expected = cron_parser.fire_times(expr, tz_name, since, until)
        except Exception as e:
            out.append({
                "cron_id": cron_id,
                "name": j.get("name"),
                "agent": j.get("agent"),
                "schedule": schedule_expr,
                "timezone": tz_name,
                "expected_at_iso": None,
                "expected_at_ms": None,
                "last_actual_run_iso": None,
                "last_actual_run_status": None,
                "wired_to_watchdog": wired_here,
                "schedule_parse_error": f"fire_times: {e}",
            })
            continue

        if not expected:
            continue

        # Pull every finished run that COULD plausibly match any expected
        # fire in the window. Widen by +/- grace on each side.
        since_ms = int((since - grace).timestamp() * 1000)
        until_ms = int((until + grace).timestamp() * 1000)
        runs = oc.all_runs_for(cron_id, runs_dir, since_ms, until_ms)

        last_actual = runs[-1] if runs else None

        for expected_dt in expected:
            expected_ms = int(expected_dt.timestamp() * 1000)
            lo = expected_ms - int(grace.total_seconds() * 1000)
            hi = expected_ms + int(grace.total_seconds() * 1000)

            # All runs within +/- grace of this expected fire, sorted by ts
            window_runs = [
                r for r in runs
                if isinstance(r.get("ts"), (int, float)) and lo <= r["ts"] <= hi
            ]

            if not window_runs:
                kind = "missed"
                matched = None
            elif any((r.get("status") or "").lower() == "ok" for r in window_runs):
                # At least one run within the window succeeded -> skip; nothing
                # is wrong even if other tries errored before the success.
                continue
            else:
                # All runs in the window errored. Use the most recent for
                # display purposes.
                kind = "errored"
                matched = window_runs[-1]

            entry = {
                "cron_id": cron_id,
                "name": j.get("name"),
                "agent": j.get("agent"),
                "schedule": schedule_expr,
                "timezone": tz_name,
                "expected_at_iso": expected_dt.isoformat(),
                "expected_at_ms": expected_ms,
                "kind": kind,
                "matched_run_iso": None,
                "matched_run_status": None,
                "last_actual_run_iso": None,
                "last_actual_run_status": None,
                "wired_to_watchdog": wired_here,
                "is_future": expected_dt > datetime.now(expected_dt.tzinfo),
            }

            if matched is not None:
                m_ts = matched.get("ts")
                if isinstance(m_ts, (int, float)):
                    entry["matched_run_iso"] = datetime.fromtimestamp(
                        m_ts / 1000, tz=ZoneInfo(tz_name)
                    ).isoformat()
                entry["matched_run_status"] = matched.get("status")

            if last_actual is not None:
                last_ts = last_actual.get("ts")
                if isinstance(last_ts, (int, float)):
                    last_dt = datetime.fromtimestamp(
                        last_ts / 1000, tz=ZoneInfo(tz_name)
                    )
                    entry["last_actual_run_iso"] = last_dt.isoformat()
                    entry["last_actual_run_status"] = last_actual.get("status")
            out.append(entry)

    # Sort by expected_at descending so the most recent miss is at the top
    out.sort(key=lambda e: e.get("expected_at_ms") or 0, reverse=True)
    return out
