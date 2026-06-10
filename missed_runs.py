"""Missed / failed / skipped run detection.

Reads OpenClaw's `cron/jobs.json` to enumerate enabled crons + their
schedules, computes expected fire times in a window via cron_parser,
and matches each expected fire to a `cron/runs/<id>.jsonl` record
using the run record's `runAtMs` field as the actual run-start
timestamp + the run's `status` field for classification.

Schema notes (verified against C3PO production records 2026-06-10):

  Each `finished` record has:
    ts          finish time (ms since epoch)
    runAtMs     ACTUAL RUN-START time the scheduler dispatched the run
                (ms since epoch). NOT the schedule's mathematical fire
                time -- runs queue, so a 10:00 cron that the scheduler
                couldn't pick up until 10:04:52 has runAtMs ~ 10:04:52.
    status      "ok" | "error" | "skipped"
    error       error text (status="error") or reason (status="skipped"
                e.g. "disabled")
    summary     agent-written summary on success/error
    durationMs  ts - runAtMs

Matching algorithm: each scheduled fire E "owns" the half-open interval
`[E - lead_tolerance, next_E)` -- or for the final fire in our window,
`[E - lead_tolerance, E + timeout_ms + queue_cushion]`. Any finished
run whose `runAtMs` falls in that interval is the run for E. Runs are
walked in `runAtMs` ascending order alongside fires (also ascending), so
each run is claimed at most once (sliding pointer, O(n+m)).

For each expected fire one of FOUR things is true:

  * Matched run, status="ok"      -> success (silently filtered out)
  * Matched run, status="error"   -> reported as "errored"
  * Matched run, status="skipped" -> reported as "skipped"
  * No matched run                -> "still running" if within timeout,
                                     else reported as "missed"

The "still running" cutoff comes from `agents.defaults.timeoutSeconds`
in openclaw.json (with optional per-agent `agents.list[].timeoutSeconds`
overrides), defaulting to 1800s.

Crons do NOT need to be wired into the retry-watchdog (no failure-alert
webhook required). This module reads OpenClaw's filesystem state
directly and reports on it -- purely informational.

History notes:
  v0.7/v0.8     : matched on `ts` (finish time) with a +/-5min grace
                  window. Marked successful runs as MISSED when they
                  took longer than the grace to finish.
  v0.8.1 (init) : matched on `runAtMs` with a tight +/-60s tolerance
                  assuming runAtMs was the scheduled fire time. Wrong:
                  runAtMs is actual start time, which drifts by queue
                  delay (multiple minutes on busy schedulers).
  v0.8.2        : current. Each fire owns an interval bounded by the
                  next fire or by the cron's timeout, accommodating
                  any scheduler queue delay up to the timeout.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import cron_parser
import openclaw_lookup as oc


# Default queue cushion past timeout for the final fire's match interval.
# After E + timeout + cushion, if nothing has runAtMs in the interval,
# we call it missed.
_QUEUE_CUSHION_SEC = 600
# Default lead tolerance (the scheduler can fire a few seconds before E
# due to clock skew). 60s is a generous bound; we never need more lead
# than this in practice.
_DEFAULT_LEAD_TOL_SEC = 60


def assign_runs_to_fires(
    fires_sorted: list[datetime],
    runs_sorted_by_runat: list[dict[str, Any]],
    timeout_sec: int,
    lead_tol_sec: int = _DEFAULT_LEAD_TOL_SEC,
    queue_cushion_sec: int = _QUEUE_CUSHION_SEC,
) -> list[dict[str, Any] | None]:
    """Match each scheduled fire to at most one finished run record.

    Each fire E owns the half-open interval `[E - lead_tol, next_E)` --
    or for the final fire, `[E - lead_tol, E + timeout + cushion]`. The
    earliest unassigned run whose `runAtMs` falls in that interval is
    claimed; later runs cannot match an earlier fire.

    Returns a list aligned 1:1 with `fires_sorted` where each entry is
    the matched run record, or None if no run matched.

    Both inputs must be pre-sorted ascending. `runs_sorted_by_runat`
    must contain only records whose `runAtMs` is a number; callers
    should filter accordingly.
    """
    lead_tol_ms = lead_tol_sec * 1000
    timeout_ms = timeout_sec * 1000
    cushion_ms = queue_cushion_sec * 1000
    out: list[dict[str, Any] | None] = [None] * len(fires_sorted)
    ptr = 0
    n_runs = len(runs_sorted_by_runat)
    for i, expected_dt in enumerate(fires_sorted):
        expected_ms = int(expected_dt.timestamp() * 1000)
        lower = expected_ms - lead_tol_ms
        if i + 1 < len(fires_sorted):
            upper = int(fires_sorted[i + 1].timestamp() * 1000) - lead_tol_ms
        else:
            upper = expected_ms + timeout_ms + cushion_ms

        # Advance past runs that are too old to match any future fire
        # (their runAtMs is below this fire's lower bound, and fires
        # ascend, so they can't match later fires either).
        while ptr < n_runs and runs_sorted_by_runat[ptr]["runAtMs"] < lower:
            ptr += 1
        if ptr < n_runs and runs_sorted_by_runat[ptr]["runAtMs"] < upper:
            out[i] = runs_sorted_by_runat[ptr]
            ptr += 1
    return out


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
        # Used both to bound the per-fire match interval and to classify
        # "no record yet" as either still-running (within timeout) or
        # definitively missed (past timeout).
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

        # Pull finished runs spanning the widest plausible match interval:
        # from before the first fire (lead tolerance) to past the last
        # fire + timeout + cushion. Filtered by `ts` (the field
        # all_runs_for uses), which is always >= runAtMs, so the window
        # is a safe upper bound for runAtMs.
        fires_sorted = sorted(displayable_fires)
        first_fire = fires_sorted[0]
        last_fire = fires_sorted[-1]
        lead_tol_sec = max(match_tolerance_seconds, _DEFAULT_LEAD_TOL_SEC)
        runs_since_ms = int((first_fire - timedelta(seconds=lead_tol_sec)).timestamp() * 1000)
        runs_until_ms = int((last_fire + timedelta(
            seconds=timeout_sec + _QUEUE_CUSHION_SEC + 60)).timestamp() * 1000)
        runs = oc.all_runs_for(cron_id, runs_dir, runs_since_ms, runs_until_ms)
        runs_with_runat = sorted(
            (r for r in runs if isinstance(r.get("runAtMs"), (int, float))),
            key=lambda r: r["runAtMs"],
        )
        last_actual = runs[-1] if runs else None

        matches = assign_runs_to_fires(
            fires_sorted, runs_with_runat, timeout_sec, lead_tol_sec=lead_tol_sec)

        for expected_dt, matched in zip(fires_sorted, matches):
            expected_ms = int(expected_dt.timestamp() * 1000)

            if matched is None:
                # No record claimed by this fire. Decide between "still
                # running" (within timeout + queue cushion) and "missed".
                seconds_since_fire = (now - expected_dt).total_seconds()
                if seconds_since_fire <= timeout_sec + _QUEUE_CUSHION_SEC:
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


def classify_fires(
    fires_sorted: list[datetime],
    runs_with_runat: list[dict[str, Any]],
    timeout_sec: int,
    now: datetime,
    lead_tol_sec: int = _DEFAULT_LEAD_TOL_SEC,
) -> dict[str, int]:
    """Bucket each fire in `fires_sorted` into a status category.

    Returns a dict with counts: ok / error / skipped / missed / running /
    upcoming. Total always equals len(fires_sorted).

      upcoming  the fire time is in the future (now < expected)
      running   no record matched but the fire is within timeout+cushion
      ok/error/skipped  a record matched, classified by its status field
      missed    no record matched and the fire is past timeout+cushion

    Used by the schedules panel for the per-cron today's-fires breakdown.
    """
    matches = assign_runs_to_fires(
        fires_sorted, runs_with_runat, timeout_sec, lead_tol_sec=lead_tol_sec)
    out = {"ok": 0, "error": 0, "skipped": 0, "missed": 0,
           "running": 0, "upcoming": 0}
    for expected_dt, matched in zip(fires_sorted, matches):
        if matched is not None:
            status = (matched.get("status") or "").lower()
            if status in out:
                out[status] += 1
            else:
                out["error"] += 1
            continue
        if expected_dt > now:
            out["upcoming"] += 1
            continue
        if (now - expected_dt).total_seconds() <= timeout_sec + _QUEUE_CUSHION_SEC:
            out["running"] += 1
        else:
            out["missed"] += 1
    return out


def get_agent_timeout(openclaw_config_path: str, agent_id: str | None) -> int:
    """Public accessor for the per-cron timeout. Server uses this when
    computing schedules-panel breakdowns."""
    default_to, overrides = _read_openclaw_timeouts(openclaw_config_path)
    if agent_id and agent_id in overrides:
        return overrides[agent_id]
    return default_to
