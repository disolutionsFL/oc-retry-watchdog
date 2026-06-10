"""Missed / failed / skipped run detection.

Reads OpenClaw's `cron/jobs.json` to enumerate enabled crons + their
schedules, computes expected fire times in a window via cron_parser,
and matches each expected fire to a `cron/runs/<id>.jsonl` record
using the run record's `runAtMs` field — which is the scheduled fire
time as recorded by OpenClaw itself, NOT the finish time. This is the
authoritative source; we don't approximate via timestamp grace windows.

Schema notes (verified against C3PO production records 2026-06-10):

  Each `finished` record has:
    ts          finish time (ms since epoch)
    runAtMs     scheduled fire time (ms since epoch) -- the key we match on
    status      "ok" | "error" | "skipped"
    error       error text (status="error") or reason (status="skipped"
                e.g. "disabled")
    summary     agent-written summary on success/error
    durationMs  ts - runAtMs (effectively)

A cron scheduled for 10:00 that takes 5 minutes has runAtMs ~ 10:00:00
and ts ~ 10:05:00. Matching on runAtMs makes "duration delta" irrelevant.

For each expected fire one of FOUR things is true:

  * A matching record exists with status="ok"      -> skipped (success)
  * A matching record exists with status="error"   -> reported as "errored"
  * A matching record exists with status="skipped" -> reported as "skipped"
  * No matching record exists                      -> reported as "missed"

Crons do NOT need to be wired into the retry-watchdog (no failure-alert
webhook required). This module reads OpenClaw's filesystem state
directly and reports on it — purely informational.

The `fire_now` operation calls `openclaw cron run <id>` via the existing
oc.cron_run helper. The watchdog does not track or retry this; it's a
one-shot manual catch-up.

History note: an earlier v0.7/v0.8 implementation used a +/- grace window
on the `ts` field instead of matching `runAtMs`. That produced false
"missed" classifications when a cron took longer than the grace to run
(common for 35B-model agents). Replaced 2026-06-10 with runAtMs matching.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import cron_parser
import openclaw_lookup as oc


def _read_openclaw_timeouts(openclaw_config_path: str) -> tuple[int, dict]:
    """Return (default_timeout_seconds, per_agent_timeout_overrides).

    Pulls `agents.defaults.timeoutSeconds` as the global default and any
    `agents.list[].timeoutSeconds` as per-agent overrides. Falls back to
    1800s (30 min) on any read/parse failure -- safe value matching the
    documented default.
    """
    import os
    from pathlib import Path
    p = Path(os.path.expanduser(openclaw_config_path))
    if not p.exists():
        return 1800, {}
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 1800, {}
    ag = d.get("agents") or {}
    defaults = ag.get("defaults") or {}
    default_to = int(defaults.get("timeoutSeconds") or 1800)
    overrides: dict[str, int] = {}
    for a in (ag.get("list") or []):
        if isinstance(a, dict) and "timeoutSeconds" in a and a.get("id"):
            overrides[a["id"]] = int(a["timeoutSeconds"])
    return default_to, overrides


def find_missed(*, jobs_json_path: str, runs_dir: str,
                since: datetime, until: datetime,
                default_tz: str,
                openclaw_config_path: str | None = None,
                match_tolerance_seconds: int = 60,
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

    tol_ms = match_tolerance_seconds * 1000
    default_timeout_sec, agent_timeouts = _read_openclaw_timeouts(
        openclaw_config_path or "~/.openclaw/openclaw.json")
    now = datetime.now(since.tzinfo)
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

        # Per-cron timeout from openclaw.json (per-agent override > default).
        # Used to classify "no record yet" as either still-running (within
        # timeout) or definitively missed (past timeout).
        agent_id = j.get("agent") or ""
        timeout_sec = agent_timeouts.get(agent_id, default_timeout_sec)

        try:
            displayable_fires = cron_parser.fire_times(
                expr, tz_name, since, until)
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

        if not displayable_fires:
            continue

        # Fetch all finished runs whose runAtMs could plausibly match any
        # displayable fire. Widen the window by the match tolerance on
        # each side. Note we filter by ts here (the field all_runs_for
        # uses) but apply runAtMs matching below -- ts is always >=
        # runAtMs so a window on ts is a safe lower bound on runAtMs.
        first_fire = displayable_fires[0]
        last_fire = displayable_fires[-1]
        runs_since_ms = int((first_fire - timedelta(seconds=match_tolerance_seconds)).timestamp() * 1000)
        # Add timeout to upper bound so we catch records whose ts is
        # delayed by the longest possible run duration.
        runs_until_ms = int((last_fire + timedelta(seconds=timeout_sec + match_tolerance_seconds + 60)).timestamp() * 1000)
        runs = oc.all_runs_for(cron_id, runs_dir, runs_since_ms, runs_until_ms)

        last_actual = runs[-1] if runs else None

        for expected_dt in displayable_fires:
            expected_ms = int(expected_dt.timestamp() * 1000)

            # Authoritative match: find a run record whose runAtMs is
            # within tolerance of the expected fire time. This is the
            # field OpenClaw itself writes for "what fire was this run
            # scheduled for", so it does NOT drift with run duration.
            matched = None
            for r in runs:
                rA = r.get("runAtMs")
                if isinstance(rA, (int, float)) and abs(rA - expected_ms) <= tol_ms:
                    matched = r
                    break

            if matched is None:
                # No record. Decide between "still running" and "missed"
                # using the cron's configured timeout. If we're inside the
                # timeout window, the run could still finish; we skip it
                # (not a problem yet). If we're past it, definitively missed.
                seconds_since_fire = (now - expected_dt).total_seconds()
                if seconds_since_fire <= timeout_sec:
                    # Still potentially running; don't surface as a problem.
                    continue
                kind = "missed"
            else:
                status = (matched.get("status") or "").lower()
                if status == "ok":
                    continue   # success -- not a problem, don't include
                elif status == "skipped":
                    kind = "skipped"
                elif status == "error":
                    kind = "errored"
                else:
                    # Unknown status -- surface as errored so the operator sees it
                    kind = "errored"

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
