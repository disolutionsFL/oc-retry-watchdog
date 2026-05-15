"""Helpers for invoking the openclaw CLI and reading its on-disk state.

v0.1 uses:
  - `openclaw cron show <id>` to refresh cron name/schedule/agent
  - `openclaw cron run <id>` to fire a retry
  - reads jobs.json + run jsonls directly for heartbeat scanning (v0.3)

The CLI exit shape is best-effort — we parse stdout permissively and
treat parse misses as missing metadata, not errors.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any


def cron_show(openclaw_cli: str, cron_id: str, timeout_seconds: int = 10
              ) -> tuple[dict[str, str | None], str]:
    """Return ({name, schedule, agent}, raw_stdout). All values may be None on parse miss."""
    try:
        result = subprocess.run(
            [os.path.expanduser(openclaw_cli), "cron", "show", cron_id],
            capture_output=True, text=True, timeout=timeout_seconds,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
        return ({"name": None, "schedule": None, "agent": None}, f"(exec error: {e})")

    out = (result.stdout or "") + "\n" + (result.stderr or "")
    parsed: dict[str, str | None] = {"name": None, "schedule": None, "agent": None}
    for line in out.splitlines():
        m = re.match(r"\s*name\s*[:=]\s*(.+)$", line, re.IGNORECASE)
        if m: parsed["name"] = m.group(1).strip()
        m = re.match(r"\s*schedule\s*[:=]\s*(.+)$", line, re.IGNORECASE)
        if m: parsed["schedule"] = m.group(1).strip()
        m = re.match(r"\s*agent\s*[:=]\s*(.+)$", line, re.IGNORECASE)
        if m: parsed["agent"] = m.group(1).strip()
    return parsed, out


def cron_run(openclaw_cli: str, cron_id: str, timeout_seconds: int = 30
             ) -> tuple[bool, str | None, str]:
    """Fire `openclaw cron run <id>`. Returns (success, run_id_if_extractable, raw_output).

    Note: many CLIs print the new runId on stdout. We try to extract it but
    don't fail if we can't.
    """
    try:
        result = subprocess.run(
            [os.path.expanduser(openclaw_cli), "cron", "run", cron_id],
            capture_output=True, text=True, timeout=timeout_seconds,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
        return (False, None, f"(exec error: {e})")

    out = (result.stdout or "") + "\n" + (result.stderr or "")
    run_id = None
    m = re.search(r"\b(run[-_ ]?id|started run)[\s:=]+([a-f0-9]{8}-[a-f0-9-]+)", out, re.IGNORECASE)
    if m:
        run_id = m.group(2)
    return (result.returncode == 0, run_id, out)


def read_jobs_json(path: str) -> list[dict[str, Any]]:
    p = Path(os.path.expanduser(path))
    if not p.exists():
        return []
    data = json.loads(p.read_text(encoding="utf-8"))
    jobs = data.get("jobs", data) if isinstance(data, dict) else data
    if isinstance(jobs, dict):
        jobs = list(jobs.values())
    return [j for j in jobs if isinstance(j, dict)]


def read_jobs_with_alerts(jobs_json_path: str) -> list[dict[str, Any]]:
    """Read jobs.json and normalize each entry to a flat dict for the admin
    view: cron_id, name, schedule, timezone, agent, enabled, failure_alert."""
    out: list[dict[str, Any]] = []
    for j in read_jobs_json(jobs_json_path):
        sched = j.get("schedule") or {}
        if not isinstance(sched, dict):
            sched = {}
        out.append({
            "cron_id": j.get("id"),
            "name": j.get("name"),
            "schedule": sched.get("expr"),
            "timezone": sched.get("tz"),
            "agent": j.get("agentId"),
            "enabled": bool(j.get("enabled")),
            "failure_alert": j.get("failureAlert") if isinstance(j.get("failureAlert"), dict) else None,
        })
    return out


def cron_wire_webhook(openclaw_cli: str, cron_id: str, webhook_url: str,
                      after: int = 1, timeout_seconds: int = 30
                      ) -> tuple[bool, str]:
    """Run `openclaw cron edit` to add a failure-alert webhook to a cron.
    Returns (success, combined_output)."""
    try:
        result = subprocess.run(
            [os.path.expanduser(openclaw_cli), "cron", "edit", cron_id,
             "--failure-alert", "--failure-alert-mode", "webhook",
             "--failure-alert-to", webhook_url,
             "--failure-alert-after", str(after)],
            capture_output=True, text=True, timeout=timeout_seconds,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
        return False, f"(exec error: {e})"
    return result.returncode == 0, (result.stdout or "") + (result.stderr or "")


def cron_unwire(openclaw_cli: str, cron_id: str, timeout_seconds: int = 30
                ) -> tuple[bool, str]:
    """Remove failure-alert from a cron."""
    try:
        result = subprocess.run(
            [os.path.expanduser(openclaw_cli), "cron", "edit", cron_id,
             "--no-failure-alert"],
            capture_output=True, text=True, timeout=timeout_seconds,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
        return False, f"(exec error: {e})"
    return result.returncode == 0, (result.stdout or "") + (result.stderr or "")


def last_run_for(cron_id: str, runs_dir: str) -> dict[str, Any] | None:
    """Return the most recent 'finished' record from cron/runs/<id>.jsonl, or None."""
    p = Path(os.path.expanduser(runs_dir)) / f"{cron_id}.jsonl"
    if not p.exists():
        return None
    last = None
    try:
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if r.get("action") == "finished":
                if last is None or r.get("ts", 0) > last.get("ts", 0):
                    last = r
    except OSError:
        return None
    return last
