"""OpenClaw Retry Watchdog daemon.

v0.1 — webhook receive + retry + alert + read-only API + minimal UI page.
v0.2 — predicate verification (post-success side-effect checks).
v0.3 — background predicate scanner running on a fixed interval + full UI
       + OpenClaw admin wiring (Wire / Unwire crons from the dashboard).
       Note: the v0.3 milestone was originally scoped to also include
       missed-run detection (catching crons that never fired); that half
       did not ship and remains in the Roadmap.
v0.4 — per-model tuning registry, offline-model detection, context-budget awareness.
v0.5 — healthcheck framework with pre-retry enforcement + AI-assisted suggestions.
v0.6 — failure-mode explanations: AI diagnosis on alert emails + on-demand UI button.
v0.7 — missed/failed cron run detection (jobs.json direct read, Fire/Wire one-click)
       + collapsible all-schedules panel with agent filter. Stdlib cron parser.
v0.8 — explain-missed-run: AI diagnosis + live healthcheck states + refire
       recommendation for each row in the missed/failed panel.
v0.8.1 — correctness: match expected fires to runs by `runAtMs` instead of
       a +/- ts grace window. Adds "skipped" status. Uses the cron's
       configured timeoutSeconds (from openclaw.json agents.defaults)
       to distinguish "still running" from "missed".

Config is loaded from $RETRY_WATCHDOG_CONFIG, --config <path>, or ./config.json
in that order of precedence.

Threading: stdlib ThreadingHTTPServer. The DB connection is shared across
threads (SQLite with WAL + check_same_thread=False); each handler is short
so contention is minimal. The cron-info refresh runs in a background thread.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import db as db_mod
import alert as alert_mod
import openclaw_lookup as oc
import heartbeat as heartbeat_mod
import ai as ai_mod
import predicates as predicates_mod
import missed_runs as missed_runs_mod

VERSION = "0.8.1"
_START_TIME = time.time()
WEB_DIR = Path(__file__).parent / "web"


# ---------- Config loading ----------

DEFAULT_CONFIG = {
    "server": {
        "port": 9095,
        "ui_bind": "0.0.0.0",
        "webhook_bind": "127.0.0.1",
        "timezone": "America/New_York",
        # Optional override. When the daemon binds an internal port that's
        # fronted by a portproxy (e.g. WSL2 -> Windows), set this to the
        # externally-reachable URL the openclaw cron will actually POST to.
        # Empty -> auto-compute from `port` as "http://localhost:<port>/webhook".
        "webhook_url": "",
    },
    "db": {"path": "~/.openclaw/retry-watchdog/retry.db"},
    "alert": {
        "default_recipient": "",
        "sender_binary": "gog-send",
        "sender_account": "",
        "sender_env": {},
    },
    "retries": {"default_max": 1, "enabled": True},
    "heartbeat": {
        "interval_minutes": 5,
        "lookback_hours": 6,
        "jobs_json_path": "~/.openclaw/cron/jobs.json",
        "runs_dir_path": "~/.openclaw/cron/runs",
        # grace_period_minutes was reserved for missed-run detection (see
        # heartbeat.py docstring + README Roadmap). Not declared here so the
        # default surface matches what the code actually reads.
    },
    "openclaw_cli": "openclaw",
    "openclaw_instance_name": "",
    "ui_url": "http://localhost:9095/",
    "predicates": {},
    "healthchecks": {},
    "ai": {
        # When the operator enables AI in Settings, the predicate editor gets
        # a "Suggest with AI" button. Primary + fallback models are picked
        # from openclaw.json's configured providers.
        "openclaw_config_path": "~/.openclaw/openclaw.json",
        "max_tokens": 1024,
        "timeout_seconds": 60,
    },
}


def load_config(path: str | None) -> tuple[dict[str, Any], Path | None]:
    """Load config from path / env / default. Returns (cfg, source_path).
    source_path is the file we actually loaded (or None if no file was found
    and we fell back to built-in defaults). Callers needing to write the
    config back (e.g. the predicate editor) use the source_path."""
    if path:
        cfg_path = Path(os.path.expanduser(path))
    elif os.environ.get("RETRY_WATCHDOG_CONFIG"):
        cfg_path = Path(os.path.expanduser(os.environ["RETRY_WATCHDOG_CONFIG"]))
    else:
        cfg_path = Path(__file__).parent / "config.json"

    if not cfg_path.exists():
        print(f"[config] no config at {cfg_path} — using built-in defaults", file=sys.stderr)
        return DEFAULT_CONFIG, None

    user = json.loads(cfg_path.read_text(encoding="utf-8"))
    merged = _deep_merge(DEFAULT_CONFIG, user)
    return merged, cfg_path


def _deep_merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(a.get(k), dict):
            out[k] = _deep_merge(a[k], v)
        else:
            out[k] = v
    return out


def settings_defaults_for_db(cfg: dict) -> dict[str, str]:
    """Subset of config that seeds the SQLite settings table on first run."""
    return {
        "default_alert_recipient": cfg["alert"].get("default_recipient", ""),
        "default_max_retries": str(int(cfg["retries"].get("default_max", 1))),
        "sender_account": cfg["alert"].get("sender_account", ""),
        # AI is off until the operator picks a primary model in Settings
        "ai_enabled": "0",
        "ai_primary_model": "",
        "ai_fallback_model": "",
    }


# ---------- Time helpers ----------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_iso_date(tz_name: str) -> str:
    return datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")


# ---------- Retry / alert flow ----------

class Watchdog:
    """Container for shared state, called by handlers via the module-level singleton."""

    def __init__(self, cfg: dict, cfg_path=None):
        self.cfg = cfg
        self.cfg_path = cfg_path   # Path or None; needed for PUT predicates write-back
        self.conn = db_mod.connect(cfg["db"]["path"])
        db_mod.init_schema(self.conn, settings_defaults_for_db(cfg))
        self._lock = threading.Lock()
        # In-memory state for healthcheck file_grew predicates. Healthchecks
        # run inline on retry decisions (not via the scanner) so we don't use
        # the predicate_history table for them — keeps the audit trail of
        # post-success predicate state cleaner.
        self._healthcheck_state: dict = {}
        # Auto-register predicate-configured crons so they appear in the UI
        # before their first failure/test-alert. Metadata refreshes in the
        # background via cron_info_refresh_loop.
        for cron_id, preds in (cfg.get("predicates") or {}).items():
            if cron_id.startswith("_") or cron_id.startswith("00000000"):
                continue
            if isinstance(preds, list) and preds:
                db_mod.upsert_cron(self.conn, cron_id, {
                    "default_max_retries": int(cfg["retries"].get("default_max", 1)),
                })
        self.scanner = heartbeat_mod.HeartbeatScanner(self)

    # ----- helpers

    def _settings(self) -> dict[str, str]:
        return db_mod.all_settings(self.conn)

    def _default_recipient(self) -> str:
        return self._settings().get("default_alert_recipient", "")

    def _default_max_retries(self) -> int:
        return int(self._settings().get("default_max_retries", "1"))

    def _resolve_recipient(self, cron: dict) -> str:
        return cron.get("alert_recipient") or self._default_recipient()

    def _ensure_cron(self, cron_id: str) -> dict:
        with self._lock:
            row = db_mod.upsert_cron(self.conn, cron_id, {
                "default_max_retries": self._default_max_retries(),
            })
            if not row.get("name"):
                # Refresh metadata in-process (fast — single CLI call). If slow, we
                # could push to a background thread.
                meta, _ = oc.cron_show(self.cfg["openclaw_cli"], cron_id)
                db_mod.update_cron_meta(self.conn, cron_id,
                                        meta["name"], meta["schedule"], meta["agent"])
                row = db_mod.upsert_cron(self.conn, cron_id, {
                    "default_max_retries": self._default_max_retries(),
                })
        return row

    def _today_retries(self, cron_id: str) -> int:
        """Count of retries the watchdog has ACTUALLY fired today (outcome='queued').

        Crucially excludes:
          - declined-dependency-down  (a down dependency isn't the cron's fault;
                                        the retry was skipped, not consumed)
          - declined-over-limit       (we didn't retry; we just alerted again)
          - declined-disabled         (didn't retry)
          - declined-error            (couldn't retry due to a CLI failure)

        Only outcome='queued' means `openclaw cron run` was actually invoked,
        which is what should consume budget against max_retries.
        """
        tz = self.cfg["server"]["timezone"]
        today = today_iso_date(tz)
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM retry_events WHERE cron_id=? "
            "AND received_at >= ? AND received_at < ? AND outcome = 'queued'",
            (cron_id, today + "T00:00:00", today + "T23:59:59.999999"),
        ).fetchone()
        return int(row["c"] if row else 0)

    def _evaluate_healthchecks(self, cron_id: str) -> tuple[bool, dict | None]:
        """Evaluate all healthchecks for this cron. Returns (all_passed, first_failure_detail).

        first_failure_detail (when not None) is a dict with:
          index        index in the healthcheck list (0-based)
          type         the predicate type that failed
          description  the operator-supplied description (shown in alert)
          error        the predicate's failure message
        """
        hcs = (self.cfg.get("healthchecks") or {}).get(cron_id) or []
        if not isinstance(hcs, list) or not hcs:
            return True, None
        tz_name = self.cfg["server"]["timezone"]
        for i, hc in enumerate(hcs):
            try:
                ok, msg = predicates_mod.evaluate(
                    hc,
                    tz_name=tz_name,
                    state_get=lambda key, cid=cron_id, idx=i:
                        self._healthcheck_state.get((cid, idx)),
                    state_set=lambda key, val, cid=cron_id, idx=i:
                        self._healthcheck_state.__setitem__((cid, idx), val),
                )
            except Exception as e:
                return False, {"index": i, "type": hc.get("type", "?"),
                               "description": hc.get("description", ""),
                               "error": f"{type(e).__name__}: {e}"}
            if not ok:
                return False, {"index": i, "type": hc.get("type", "?"),
                               "description": hc.get("description", ""),
                               "error": msg}
        return True, None

    # ----- main entry points

    def handle_failure(
        self, *, cron_id: str, failed_run_id: str | None, error: str,
        failure_source: str = "webhook",
    ) -> dict:
        cron = self._ensure_cron(cron_id)
        retries_today = self._today_retries(cron_id)
        max_retries = int(cron.get("max_retries") or self._default_max_retries())
        enabled = bool(cron.get("enabled"))

        if not enabled:
            db_mod.insert_retry_event(self.conn, cron_id, failed_run_id, now_iso(),
                                      None, None, "declined-disabled", failure_source,
                                      error, "cron disabled in watchdog")
            return {"ok": True, "action": "declined", "reason": "disabled"}

        # Healthcheck gating: if any pre-retry healthcheck fails, skip the
        # retry — a dependency is down and retrying won't help. Doesn't count
        # against max_retries (failure isn't the cron's fault). Fires a
        # dependency-unhealthy alert with the failing healthcheck's details.
        hc_passed, hc_failure = self._evaluate_healthchecks(cron_id)
        if not hc_passed:
            return self._fire_dependency_alert(
                cron, failed_run_id, error, failure_source, hc_failure
            )

        if retries_today < max_retries:
            return self._fire_retry(cron, failed_run_id, error, failure_source)
        return self._fire_alert(cron, failed_run_id, error, failure_source, retries_today)

    def manual_retry(self, cron_id: str) -> dict:
        cron = self._ensure_cron(cron_id)
        return self._fire_retry(cron, None, None, "manual")

    # ----- AI predicate suggestion -----

    def suggest_predicates(self, cron_id: str) -> dict:
        return self._suggest_checks(cron_id, kind="predicates")

    def suggest_healthchecks(self, cron_id: str) -> dict:
        return self._suggest_checks(cron_id, kind="healthchecks")

    def _suggest_checks(self, cron_id: str, *, kind: str) -> dict:
        """Ask the configured AI model to suggest predicates or healthchecks
        for this cron. `kind` selects the system prompt + which existing
        list to send as context."""
        s = self._settings()
        if s.get("ai_enabled", "0") not in ("1", "true", "True"):
            raise RuntimeError("AI is not enabled in settings")
        primary = s.get("ai_primary_model", "").strip()
        fallback = s.get("ai_fallback_model", "").strip()
        if not primary:
            raise RuntimeError("no primary AI model selected in settings")

        # Gather context: cron metadata + prompt + recent summaries +
        # any predicates already configured
        cfg_ai = self.cfg.get("ai") or {}
        oc_cfg_path = cfg_ai.get("openclaw_config_path", "~/.openclaw/openclaw.json")
        max_tokens = int(cfg_ai.get("max_tokens", 1024))
        timeout = int(cfg_ai.get("timeout_seconds", 60))

        jobs = oc.read_jobs_with_alerts(self.cfg["heartbeat"]["jobs_json_path"])
        job = next((j for j in jobs if j.get("cron_id") == cron_id), None)
        if not job:
            raise RuntimeError(f"cron {cron_id} not found in jobs.json")

        # Pull the message body from raw jobs.json (read_jobs_with_alerts
        # doesn't include payload by design)
        raw_jobs = oc.read_jobs_json(self.cfg["heartbeat"]["jobs_json_path"])
        raw = next((j for j in raw_jobs if j.get("id") == cron_id), None) or {}
        prompt_text = ((raw.get("payload") or {}).get("message") or "")

        # Recent run summaries from cron/runs/<id>.jsonl
        recent: list[str] = []
        runs_dir = self.cfg["heartbeat"]["runs_dir_path"]
        latest = oc.last_run_for(cron_id, runs_dir)
        if latest:
            recent.append(latest.get("summary") or "")

        existing_preds = (self.cfg.get(kind) or {}).get(cron_id) or []

        tuning_overrides = cfg_ai.get("tunings") or {}

        tried: list[dict] = []
        for slot, key in [("primary", primary), ("fallback", fallback)]:
            if not key:
                continue
            mdef = ai_mod.get_model_endpoint(oc_cfg_path, key,
                                              agent_id=job.get("agent"))
            if not mdef:
                tried.append({"slot": slot, "key": key, "error": "model not found in openclaw.json"})
                continue
            # Fast pre-check: don't burn the full chat-completion timeout on
            # a host that's down. If /v1/models doesn't respond within 3s,
            # skip this model and try the next.
            if not ai_mod.is_endpoint_reachable(mdef["base_url"],
                                                mdef.get("api_key"),
                                                timeout_seconds=3):
                tried.append({"slot": slot, "key": key,
                              "error": "endpoint unreachable (3s ping failed)"})
                continue
            tuning = ai_mod.resolve_tuning(key, tuning_overrides)
            # Bound max_tokens by what the model itself supports (from
            # openclaw.json), then reserve openclaw's compaction tail.
            requested = int(tuning.get("max_tokens", max_tokens) or max_tokens)
            budget = ai_mod.compute_context_budget(mdef, requested)
            effective_max_tokens = budget["capped_max_tokens"]
            # build_messages may add a tuning-specific prefix (e.g. /no_think
            # for GLM) so we rebuild per attempt
            # The cron's CONFIGURED model (from its jobs.json payload.model)
            # — not the same as the model we're using to generate the
            # suggestion. The AI needs to know this so it can suggest
            # health-checking the right endpoint.
            cron_payload_model = None
            cron_model_endpoint = None
            for raw in raw_jobs:
                if raw.get("id") == cron_id:
                    cron_payload_model = (raw.get("payload") or {}).get("model")
                    break
            if cron_payload_model:
                cron_mdef = ai_mod.get_model_endpoint(oc_cfg_path, cron_payload_model)
                if cron_mdef:
                    cron_model_endpoint = cron_mdef.get("base_url")
            messages = ai_mod.build_messages(
                cron_name=job.get("name") or cron_id,
                agent=job.get("agent") or "?",
                schedule=job.get("schedule") or "?",
                cron_prompt=prompt_text,
                recent_summaries=recent,
                existing_predicates=existing_preds,
                tuning=tuning,
                kind=kind,
                model_endpoint=cron_model_endpoint,
                model_id=(cron_payload_model.split("/", 1)[-1]
                          if cron_payload_model and "/" in cron_payload_model
                          else cron_payload_model),
            )
            ok, content = ai_mod.chat_completion(
                base_url=mdef["base_url"],
                model=mdef["model_id"],
                messages=messages,
                api_key=mdef.get("api_key"),
                tuning=tuning,
                max_tokens=effective_max_tokens,
                timeout_seconds=timeout,
            )
            if not ok:
                tried.append({"slot": slot, "key": key,
                              "tuning": tuning.get("_source", "?"),
                              "budget": budget,
                              "error": content[:300]})
                continue
            preds, err = ai_mod.parse_predicates(content)
            # parse_predicates returns (None, err) on parse failure and
            # ([], "") on a valid-but-empty array. Treat empty array as
            # success — the model legitimately concluded there are no
            # dependencies worth checking (correct for cleanup crons,
            # local-only tasks, etc.).
            if preds is None:
                tried.append({"slot": slot, "key": key,
                              "tuning": tuning.get("_source", "?"),
                              "budget": budget,
                              "error": f"parse: {err}",
                              "raw_first_300": content[:300]})
                continue
            return {"ok": True, "predicates": preds, "model_used": key,
                    "tuning": tuning.get("_source", "?"),
                    "budget": budget,
                    "slot": slot, "tried": tried}
        return {"ok": False, "predicates": [], "model_used": None,
                "error": "all configured models failed", "tried": tried}

    # ----- OpenClaw integration admin -----

    def _expected_webhook_url(self) -> str:
        """The webhook URL we'd want each OpenClaw cron to be wired to.

        Prefers config `server.webhook_url` (lets WSL+portproxy deployments
        point at the external port rather than the internal bind). Falls
        back to `http://localhost:<server.port>/webhook`."""
        explicit = (self.cfg["server"].get("webhook_url") or "").strip()
        if explicit:
            return explicit
        port = int(self.cfg["server"]["port"])
        return f"http://localhost:{port}/webhook"

    def list_openclaw_jobs(self) -> dict:
        """Return all OpenClaw jobs annotated with watchdog integration status,
        plus orphans (crons in our DB but not in OpenClaw)."""
        jobs_path = self.cfg["heartbeat"]["jobs_json_path"]
        jobs = oc.read_jobs_with_alerts(jobs_path)
        expected = self._expected_webhook_url()

        in_db = {r["cron_id"] for r in self.conn.execute("SELECT cron_id FROM crons").fetchall()}
        in_openclaw = {j["cron_id"] for j in jobs if j.get("cron_id")}
        cfg_preds = self.cfg.get("predicates", {}) or {}

        for j in jobs:
            fa = j.get("failure_alert") or {}
            to_url = fa.get("to")
            mode = fa.get("mode")
            j["webhook_url"] = to_url
            j["webhook_mode"] = mode
            j["webhook_after"] = fa.get("after")
            j["webhook_wired_here"] = bool(mode == "webhook" and to_url == expected)
            j["webhook_wired_elsewhere"] = bool(mode == "webhook" and to_url and to_url != expected)
            j["in_watchdog_db"] = j["cron_id"] in in_db
            preds = cfg_preds.get(j["cron_id"])
            j["predicates_count"] = len(preds) if isinstance(preds, list) else 0

        # Orphans: cron_ids in our DB that no longer exist in jobs.json
        orphans = []
        for cid in sorted(in_db - in_openclaw):
            row = self.conn.execute("SELECT * FROM crons WHERE cron_id=?", (cid,)).fetchone()
            if not row:
                continue
            d = dict(row)
            preds = cfg_preds.get(cid)
            d["predicates_count"] = len(preds) if isinstance(preds, list) else 0
            orphans.append(d)

        return {
            "jobs": jobs,
            "orphans": orphans,
            "expected_webhook": expected,
        }

    def wire_openclaw_cron(self, cron_id: str) -> tuple[bool, str]:
        """Run `openclaw cron edit` to set the failure-alert webhook to our URL.
        Also auto-registers the cron in our DB so it shows in the main list."""
        ok, output = oc.cron_wire_webhook(
            self.cfg["openclaw_cli"], cron_id, self._expected_webhook_url(),
            after=1,
        )
        if ok:
            with self._lock:
                db_mod.upsert_cron(self.conn, cron_id, {
                    "default_max_retries": int(self.cfg["retries"].get("default_max", 1)),
                })
                # Refresh metadata so name/schedule populate
                meta, _ = oc.cron_show(self.cfg["openclaw_cli"], cron_id)
                db_mod.update_cron_meta(self.conn, cron_id,
                                        meta.get("name"), meta.get("schedule"), meta.get("agent"))
        return ok, output

    def unwire_openclaw_cron(self, cron_id: str) -> tuple[bool, str]:
        """Remove the failure-alert webhook from a cron. Watchdog DB row
        stays (history + predicates intact) — delete separately if desired."""
        return oc.cron_unwire(self.cfg["openclaw_cli"], cron_id)

    # ----- Missed-run detection (v0.7) -----

    def find_missed_runs(self, *, day_iso: str | None = None) -> dict:
        """Return the list of missed/errored/skipped cron runs for a day.

        `day_iso` is YYYY-MM-DD in `server.timezone`. Default is today.
        Crons do NOT need to be registered in the watchdog — this reads
        OpenClaw's jobs.json + runs/<id>.jsonl directly.

        Matching is by `runAtMs` (the scheduled fire time the OpenClaw
        scheduler records in each run record), with the run's `status`
        field driving classification (ok / error / skipped). Crons with
        no run record for an expected fire are classified as MISSED only
        once they're past their configured `agents.defaults.timeoutSeconds`
        (30 min by default); within that window they may still be running
        and are silently filtered out.

        For today (and any future day), `until` is capped at `now` so
        expected fires that simply haven't happened yet aren't reported.
        """
        tz_name = self.cfg["server"]["timezone"]
        tz = ZoneInfo(tz_name)
        if day_iso:
            try:
                day = datetime.strptime(day_iso, "%Y-%m-%d").date()
            except ValueError:
                raise RuntimeError(f"invalid day {day_iso!r}, expected YYYY-MM-DD")
        else:
            day = datetime.now(tz).date()

        since = datetime(day.year, day.month, day.day, 0, 0, tzinfo=tz)
        until = since + timedelta(days=1)

        # For today (and any future day), cap `until` at now() so we don't
        # report "missed" for fire times that simply haven't happened yet.
        now = datetime.now(tz)
        effective_until = min(until, now)
        if effective_until <= since:
            return {
                "day": day.isoformat(),
                "timezone": tz_name,
                "now_iso": now.isoformat(),
                "missed": [],
            }

        oc_cfg_path = (self.cfg.get("ai") or {}).get(
            "openclaw_config_path", "~/.openclaw/openclaw.json")
        missed = missed_runs_mod.find_missed(
            jobs_json_path=self.cfg["heartbeat"]["jobs_json_path"],
            runs_dir=self.cfg["heartbeat"]["runs_dir_path"],
            since=since,
            until=effective_until,
            default_tz=tz_name,
            openclaw_config_path=oc_cfg_path,
            expected_webhook_url=self._expected_webhook_url(),
        )
        return {
            "day": day.isoformat(),
            "timezone": tz_name,
            "now_iso": now.isoformat(),
            "missed": missed,
        }

    def list_cron_schedules(self) -> dict:
        """Return every cron in jobs.json with computed today's fires +
        next fire time, organized for the schedule-view panel.

        Reads jobs.json fresh on every call -- this is intentionally
        real-time so it reflects schedule changes immediately.
        """
        import cron_parser
        tz_name = self.cfg["server"]["timezone"]
        tz = ZoneInfo(tz_name)
        now = datetime.now(tz)
        today_start = datetime(now.year, now.month, now.day, 0, 0, tzinfo=tz)
        tomorrow_start = today_start + timedelta(days=1)
        # Look ahead 7 days for the next fire if today has none left.
        lookahead_end = today_start + timedelta(days=7)

        expected_url = self._expected_webhook_url()
        runs_dir = self.cfg["heartbeat"]["runs_dir_path"]
        out = []
        for j in oc.read_jobs_with_alerts(self.cfg["heartbeat"]["jobs_json_path"]):
            cron_id = j.get("cron_id")
            schedule_expr = (j.get("schedule") or "").strip()
            tz_for_cron = j.get("timezone") or tz_name

            today_fires_iso: list[str] = []
            next_fire_iso: str | None = None
            parse_error: str | None = None
            try:
                expr = cron_parser.parse(schedule_expr)
                today_fires = cron_parser.fire_times(
                    expr, tz_for_cron, today_start, tomorrow_start)
                today_fires_iso = [d.isoformat() for d in today_fires]
                # First future fire (in the lookahead window)
                future = cron_parser.fire_times(
                    expr, tz_for_cron, now, lookahead_end)
                if future:
                    next_fire_iso = future[0].isoformat()
            except (ValueError, KeyError) as e:
                parse_error = str(e)

            # Last actual run time (if any)
            last_actual_iso = None
            last_actual_status = None
            last = oc.last_run_for(cron_id, runs_dir) if cron_id else None
            if last and isinstance(last.get("ts"), (int, float)):
                last_actual_iso = datetime.fromtimestamp(
                    last["ts"] / 1000, tz=ZoneInfo(tz_for_cron)
                ).isoformat()
                last_actual_status = last.get("status")

            fa = j.get("failure_alert") or {}
            wired_here = (
                fa.get("mode") == "webhook"
                and fa.get("to") == expected_url
            )

            out.append({
                "cron_id": cron_id,
                "name": j.get("name"),
                "agent": j.get("agent"),
                "schedule": schedule_expr,
                "timezone": tz_for_cron,
                "enabled": bool(j.get("enabled")),
                "today_fires": today_fires_iso,
                "today_fire_count": len(today_fires_iso),
                "next_fire_iso": next_fire_iso,
                "last_actual_run_iso": last_actual_iso,
                "last_actual_run_status": last_actual_status,
                "wired_to_watchdog": wired_here,
                "schedule_parse_error": parse_error,
            })
        return {
            "timezone": tz_name,
            "now_iso": now.isoformat(),
            "today": today_start.date().isoformat(),
            "schedules": out,
        }

    def _evaluate_all_healthchecks(self, cron_id: str) -> list[dict]:
        """Evaluate every healthcheck for this cron and return per-check state.

        Unlike `_evaluate_healthchecks`, this does NOT short-circuit on the
        first failure — used by the missed-run Explain modal so the operator
        sees every dependency's current state at a glance.

        Returns a list of {index, type, description, ok, message}.
        Empty list if no healthchecks are configured for the cron.
        """
        hcs = (self.cfg.get("healthchecks") or {}).get(cron_id) or []
        if not isinstance(hcs, list) or not hcs:
            return []
        tz_name = self.cfg["server"]["timezone"]
        out = []
        for i, hc in enumerate(hcs):
            entry = {
                "index": i,
                "type": hc.get("type", "?"),
                "description": hc.get("description", ""),
                "ok": False,
                "message": "",
            }
            try:
                ok, msg = predicates_mod.evaluate(
                    hc,
                    tz_name=tz_name,
                    state_get=lambda key, cid=cron_id, idx=i:
                        self._healthcheck_state.get((cid, idx)),
                    state_set=lambda key, val, cid=cron_id, idx=i:
                        self._healthcheck_state.__setitem__((cid, idx), val),
                )
                entry["ok"] = bool(ok)
                entry["message"] = msg
            except Exception as e:
                entry["ok"] = False
                entry["message"] = f"{type(e).__name__}: {e}"
            out.append(entry)
        return out

    def explain_missed_run(self, cron_id: str, expected_at_ms: int,
                           match_tolerance_seconds: int = 60) -> dict:
        """Explain one row in the Missed & failed cron runs panel.

        Reads cron metadata from jobs.json, looks for an actual run record
        within +/- grace_minutes of expected_at. If a run is found and at
        least one of them errored (kind=errored case), feeds the error /
        summary into the AI failure-mode diagnoser. If no run is found
        (kind=missed case), returns a structured no-data explanation.

        Also evaluates the cron's currently-configured healthchecks so the
        operator sees dependency state at the same time. Empty list if no
        healthchecks are configured for this cron.

        Returns:
          {
            ok:                True iff AI produced a diagnosis.
            kind:              "missed" | "errored"
            cron:              {cron_id, name, agent, schedule}
            matched_run:       {ts_iso, status, summary_excerpt} | None
            ai:                {cause, next_step, confidence, category} | None
            ai_error:          str (empty if ok)
            ai_model_used:     str | None
            healthchecks:      [{description, ok, message}, ...]
            healthchecks_configured: int
            suggested_action:  Derived recommendation string.
          }
        """
        jobs_path = self.cfg["heartbeat"]["jobs_json_path"]
        runs_dir = self.cfg["heartbeat"]["runs_dir_path"]

        # Look up the cron metadata in jobs.json.
        job = next(
            (j for j in oc.read_jobs_with_alerts(jobs_path)
             if j.get("cron_id") == cron_id),
            None,
        )
        if not job:
            raise RuntimeError(f"cron {cron_id!r} not found in jobs.json")

        cron = {
            "cron_id": cron_id,
            "name": job.get("name") or cron_id,
            "agent": job.get("agent") or "?",
            "schedule": job.get("schedule") or "?",
            "max_retries": 0,    # unused but _try_explain_failure may read it
        }

        # Look for the run record whose runAtMs is within tolerance of
        # the expected fire time. Same authoritative match used by the
        # panel itself (find_missed).
        tol_ms = match_tolerance_seconds * 1000
        # Fetch a generous window of records around the expected fire
        # (filtered by ts here; matched by runAtMs below).
        lo = expected_at_ms - 3600_000     # 1h before
        hi = expected_at_ms + 4 * 3600_000  # 4h after
        candidate_runs = oc.all_runs_for(cron_id, runs_dir, lo, hi)

        matched = None
        for r in candidate_runs:
            rA = r.get("runAtMs")
            if isinstance(rA, (int, float)) and abs(rA - expected_at_ms) <= tol_ms:
                matched = r
                break

        if matched is None:
            kind = "missed"
        else:
            status = (matched.get("status") or "").lower()
            if status == "ok":
                kind = "succeeded"   # operator clicked Explain on a row that
                                     # the panel shouldn't show; tell them.
            elif status == "skipped":
                kind = "skipped"
            elif status == "error":
                kind = "errored"
            else:
                kind = "errored"

        matched_block = None
        if matched is not None:
            m_ts = matched.get("ts")
            ts_iso = None
            if isinstance(m_ts, (int, float)):
                ts_iso = datetime.fromtimestamp(
                    m_ts / 1000, tz=ZoneInfo(self.cfg["server"]["timezone"])
                ).isoformat()
            summary = matched.get("summary") or ""
            if len(summary) > 1500:
                summary = "...(truncated)...\n" + summary[-1500:]
            matched_block = {
                "ts_iso": ts_iso,
                "status": matched.get("status"),
                "summary_excerpt": summary,
            }

        # Evaluate healthchecks now -- whether or not we have an AI call to make.
        hc_results = self._evaluate_all_healthchecks(cron_id)
        hc_configured = len(hc_results)

        ai_diag = None
        ai_err = ""
        ai_model_used = None
        suggested_action = ""

        if kind == "errored":
            # Prefer the error field, fall back to summary.
            error_text = ""
            if matched:
                err = matched.get("error")
                summary = matched.get("summary")
                if isinstance(err, str) and err:
                    error_text = err
                    if isinstance(summary, str) and summary:
                        error_text += "\n\nRun summary excerpt:\n" + summary
                elif isinstance(summary, str) and summary:
                    error_text = summary
            if not error_text:
                error_text = "(no error / summary text recorded)"
            ai_diag, ai_err, ai_model_used = self._try_explain_failure(
                cron=cron,
                error=error_text,
                failure_source="missed-runs-panel",
                retry_history=[],
                run_log_excerpt=error_text,
            )
        elif kind == "skipped":
            reason = (matched.get("error") if matched else None) or "(no reason recorded)"
            ai_err = (f"OpenClaw recorded this fire with status=skipped (reason: "
                      f"{reason}). The run was not executed by design -- AI "
                      f"diagnosis isn't meaningful here. Common causes: the "
                      f"cron was disabled at fire time; the gateway was busy "
                      f"with a long-running prior task; sessionTarget rules "
                      f"prevented a new session.")
        elif kind == "missed":
            ai_err = ("no run record exists for this expected fire and the "
                      "configured agent timeout has elapsed; AI diagnosis is "
                      "not meaningful for missed fires. Common causes: the "
                      "OpenClaw gateway or its host was not running at the "
                      "expected time; the cron schedule has a recently-"
                      "changed expression; the cron was disabled and re-"
                      "enabled mid-day. Check the gateway journal around "
                      "the expected fire time.")
        elif kind == "succeeded":
            ai_err = ("a status=ok run exists for this expected fire; the "
                      "panel should have filtered it out. Refresh to re-check.")

        # Derive a re-fire recommendation from the AI category (when present),
        # adjusted by current healthcheck state.
        if kind == "missed":
            suggested_action = (
                "Manual catch-up: click Fire to run `openclaw cron run "
                "<id>` once. If the host or gateway was down at the expected "
                "time and is up now, this should succeed on the first try."
            )
        elif kind == "skipped":
            suggested_action = (
                "OpenClaw intentionally skipped this fire (typically because "
                "the cron was disabled or another constraint prevented it). "
                "Re-firing manually with Fire will run it now; if the same "
                "reason (e.g. disabled) still applies, the manual run will "
                "also be skipped."
            )
        elif kind == "succeeded":
            suggested_action = "Already succeeded -- no action needed."
        elif ai_diag and ai_diag.get("category"):
            cat = (ai_diag.get("category") or "").lower()
            cat_advice = {
                "model":     "Re-fire now -- model-side errors are usually transient. If it errors again with the same category, the model is genuinely unhealthy and a retry won't help until it recovers.",
                "network":   "Verify the upstream service is reachable, then re-fire.",
                "config":    "Do NOT re-fire yet -- a config issue will repeat. Fix the config (or wire/adjust the relevant healthcheck) before retrying.",
                "data":      "Inspect the input data before re-firing -- a repeat run is likely to hit the same error if the data is bad.",
                "code":      "Do NOT re-fire yet -- a code-level issue needs a deploy or workaround. Re-firing will just repeat the error.",
                "dependency":"Wait for the dependency to recover (check healthcheck state below), then re-fire. Re-firing now will likely fail the same way.",
                "unknown":   "Review the AI diagnosis below and the healthcheck state, then decide. When in doubt, re-fire once -- many errors are transient.",
            }
            suggested_action = cat_advice.get(cat, cat_advice["unknown"])
            # If healthchecks are configured and any are failing, take that
            # information over the AI category — a known-failing dependency
            # is more authoritative than a model's guess.
            if hc_results and any(not h["ok"] for h in hc_results):
                failed_names = ", ".join(
                    (h["description"] or h["type"]) for h in hc_results if not h["ok"]
                )
                suggested_action = (
                    f"DO NOT re-fire yet. {len(hc_results) - sum(1 for h in hc_results if h['ok'])} "
                    f"healthcheck(s) currently FAILING: {failed_names}. Fix the dependency "
                    f"first, then re-fire. (AI also suggests: {suggested_action})"
                )
        else:
            # AI errored or disabled. Generic advice.
            if hc_results and any(not h["ok"] for h in hc_results):
                suggested_action = (
                    "One or more healthchecks are FAILING (see below). Fix the "
                    "dependency first, then re-fire."
                )
            else:
                suggested_action = (
                    "AI diagnosis unavailable. Inspect the run summary below "
                    "and decide based on the visible error. If healthchecks "
                    "pass and the error looks transient, re-fire once."
                )

        return {
            "ok": ai_diag is not None,
            "kind": kind,
            "cron": cron,
            "matched_run": matched_block,
            "ai": ai_diag,
            "ai_error": ai_err,
            "ai_model_used": ai_model_used,
            "healthchecks": hc_results,
            "healthchecks_configured": hc_configured,
            "suggested_action": suggested_action,
        }

    def fire_cron_now(self, cron_id: str) -> dict:
        """Manually fire `openclaw cron run <id>` for a cron the user picked
        from the missed-runs view. Does NOT register the cron in the
        watchdog DB and does NOT record a retry event — this is a one-shot
        catch-up, not part of the retry flow."""
        ok, run_id, raw = oc.cron_run(self.cfg["openclaw_cli"], cron_id)
        return {
            "ok": ok,
            "cron_id": cron_id,
            "run_id": run_id,
            "output": (raw or "")[:1500],
        }

    def delete_cron(self, cron_id: str) -> bool:
        """Remove a cron from the watchdog DB. Used for orphan cleanup.
        Also strips its predicate config. Retry/alert history rows are kept
        for forensics (they reference cron_id but have no FK cascade in v1)."""
        with self._lock:
            cur = self.conn.execute("DELETE FROM crons WHERE cron_id=?", (cron_id,))
            removed_crons = cur.rowcount > 0
            # Strip predicates too
            if self.cfg_path is not None:
                try:
                    on_disk = json.loads(self.cfg_path.read_text(encoding="utf-8"))
                    dirty = False
                    for key in ("predicates", "healthchecks"):
                        section = on_disk.get(key) or {}
                        if isinstance(section, dict) and cron_id in section:
                            del section[cron_id]
                            on_disk[key] = section
                            self.cfg[key] = section
                            dirty = True
                    if dirty:
                        tmp = self.cfg_path.with_suffix(self.cfg_path.suffix + ".tmp")
                        tmp.write_text(json.dumps(on_disk, indent=2), encoding="utf-8")
                        os.replace(tmp, self.cfg_path)
                except Exception as e:
                    sys.stderr.write(f"[delete_cron] config edit failed: {e}\n")
        return removed_crons

    def update_predicates(self, cron_id: str, predicates: list[dict]) -> dict:
        return self._update_checks(cron_id, predicates, kind="predicates")

    def update_healthchecks(self, cron_id: str, healthchecks: list[dict]) -> dict:
        return self._update_checks(cron_id, healthchecks, kind="healthchecks")

    def _update_checks(self, cron_id: str, items: list[dict], *, kind: str) -> dict:
        """Replace the predicate or healthcheck list for a cron. Persists to
        config.json on disk and updates the in-memory cfg. Auto-registers
        the cron in the DB if not already known.

        Pass an empty list to remove all entries for the cron.
        `kind` is "predicates" or "healthchecks" (top-level keys in config).
        """
        if self.cfg_path is None:
            raise RuntimeError(f"no config file in use; {kind} edits require a config.json")
        if kind not in ("predicates", "healthchecks"):
            raise RuntimeError(f"unknown check kind: {kind!r}")

        with self._lock:
            db_mod.upsert_cron(self.conn, cron_id, {
                "default_max_retries": int(self.cfg["retries"].get("default_max", 1)),
            })

        with self._lock:
            on_disk = json.loads(self.cfg_path.read_text(encoding="utf-8"))
            section = on_disk.get(kind) or {}
            if not isinstance(section, dict):
                section = {}
            if items:
                section[cron_id] = items
            elif cron_id in section:
                del section[cron_id]
            on_disk[kind] = section

            tmp = self.cfg_path.with_suffix(self.cfg_path.suffix + ".tmp")
            tmp.write_text(json.dumps(on_disk, indent=2), encoding="utf-8")
            os.replace(tmp, self.cfg_path)

            self.cfg[kind] = section

        return items

    def test_alert(self, cron_id: str) -> dict:
        cron = self._ensure_cron(cron_id)
        return self._fire_alert(cron, None, "This is a test alert.", "test", 0,
                                subject_override=(
                                    f"[OpenClaw Retry Watchdog] Test alert — "
                                    f"{cron.get('name') or cron_id}"
                                ),
                                notes="test")

    def _fire_retry(self, cron: dict, failed_run_id: str | None, error: str | None,
                    failure_source: str) -> dict:
        ok, run_id, raw = oc.cron_run(self.cfg["openclaw_cli"], cron["cron_id"])
        outcome = "queued" if ok else "declined-error"
        notes = None if ok else f"openclaw cron run failed: {raw[:500]}"
        db_mod.insert_retry_event(
            self.conn, cron["cron_id"], failed_run_id, now_iso(),
            now_iso() if ok else None, run_id, outcome, failure_source,
            error, notes,
        )
        return {"ok": True, "action": "retried" if ok else "retry-failed",
                "run_id": run_id, "notes": notes}

    def _fire_alert(self, cron: dict, failed_run_id: str | None, error: str | None,
                    failure_source: str, retries_today: int,
                    subject_override: str | None = None,
                    notes: str | None = None,
                    outcome: str = "declined-over-limit") -> dict:
        cfg = self.cfg
        recipient = self._resolve_recipient(cron)
        if not recipient:
            db_mod.insert_retry_event(
                self.conn, cron["cron_id"], failed_run_id, now_iso(),
                None, None, "declined-error", failure_source, error,
                "no alert recipient configured",
            )
            return {"ok": False, "action": "alert-skipped", "reason": "no recipient"}

        history = db_mod.recent_retry_events(self.conn, cron["cron_id"], limit=10)
        subject = subject_override or (
            f"[OpenClaw Cron Failure] {cron.get('name') or cron['cron_id']} — "
            f"max retries exhausted"
        )

        # Best-effort AI diagnosis. Short timeout so a slow model never
        # blocks the alert email from getting out. If it fails, we record
        # the reason in the email body so the operator knows AI was tried
        # but didn't help — not that the feature was simply off.
        # Skip on test alerts — diagnosing the sentinel "This is a test
        # alert" string burns AI tokens for zero signal.
        run_log_excerpt = self._read_failed_run_excerpt(cron["cron_id"])
        if failure_source == "test":
            diagnosis, diag_err, diag_model_key = None, "", None
        else:
            diagnosis, diag_err, diag_model_key = self._try_explain_failure(
                cron=cron, error=error or "", failure_source=failure_source,
                retry_history=history, run_log_excerpt=run_log_excerpt,
            )

        body = alert_mod.format_failure_body(
            cron=cron,
            error=error or "(no error text)",
            failure_source=failure_source,
            retry_history=history,
            suggested_cron_run=f"{cfg['openclaw_cli']} cron run {cron['cron_id']}",
            ui_url=cfg.get("ui_url", "http://localhost:9095/"),
            diagnosis=diagnosis,
            diagnosis_unavailable_reason=diag_err if not diagnosis else None,
            run_log_excerpt=run_log_excerpt,
        )

        ok, err = alert_mod.send_email(
            sender_binary=cfg["alert"]["sender_binary"],
            sender_account=cfg["alert"]["sender_account"],
            recipient=recipient,
            subject=subject,
            body=body,
            extra_env=cfg["alert"].get("sender_env") or {},
        )

        # Record the alert and the originating retry-decision row.
        # `outcome` differentiates declined-over-limit from
        # declined-dependency-down in the audit trail.
        if failure_source != "test":
            db_mod.insert_retry_event(
                self.conn, cron["cron_id"], failed_run_id, now_iso(),
                None, None, outcome, failure_source, error,
                notes or f"retries_today={retries_today}",
            )
        alert_event_id = db_mod.insert_alert_event(
            self.conn, cron["cron_id"], now_iso(), recipient,
            subject, body, 1 if ok else 0, None if ok else err, notes,
        )

        # Cache the diagnosis attempt against the alert row so the UI can
        # show it later without re-burning the AI call. We store BOTH
        # successful and failed attempts -- a recorded failure tells the
        # operator "AI was tried" vs "AI was never run" when they click
        # Explain on an old row.
        if diagnosis or diag_err:
            db_mod.upsert_explanation(
                self.conn,
                event_kind="alert", event_id=alert_event_id,
                cron_id=cron["cron_id"], created_at=now_iso(),
                model_key=diag_model_key,
                cause=(diagnosis or {}).get("cause"),
                next_step=(diagnosis or {}).get("next_step"),
                confidence=(diagnosis or {}).get("confidence"),
                category=(diagnosis or {}).get("category"),
                error=diag_err if not diagnosis else None,
            )

        return {"ok": ok, "action": "alerted", "recipient": recipient,
                "error": None if ok else err,
                "diagnosis": diagnosis,
                "diagnosis_error": diag_err if not diagnosis else None}

    # ----- AI failure-mode explanation -----

    def _read_failed_run_excerpt(self, cron_id: str, tail_chars: int = 1500) -> str:
        """Pull the tail of the most recent finished run for this cron, for
        inclusion in the alert email + AI diagnosis input. Returns empty
        string if no run log is available (the file is missing, the run
        record has no summary, etc.) — this is a best-effort lookup."""
        try:
            runs_dir = self.cfg["heartbeat"]["runs_dir_path"]
            latest = oc.last_run_for(cron_id, runs_dir)
        except Exception:
            return ""
        if not latest:
            return ""
        # Prefer "summary" (what the agent wrote at the end); fall back to
        # truncated raw record. Truncate to tail so token budgets stay sane.
        text = latest.get("summary") or json.dumps(latest)[:5000]
        if len(text) > tail_chars:
            text = "...(truncated)...\n" + text[-tail_chars:]
        return text

    def _try_explain_failure(self, *, cron: dict, error: str,
                              failure_source: str, retry_history: list[dict],
                              run_log_excerpt: str
                              ) -> tuple[dict | None, str, str | None]:
        """Best-effort: try primary then fallback AI model, short timeout.
        Returns (diagnosis_or_None, error_message_or_empty, model_key_or_None).

        On success: (diagnosis, "", model_key).
        On every form of failure: (None, "<short reason>", None).
        Safe to call when AI is disabled — returns ("AI disabled in Settings")."""
        s = self._settings()
        if s.get("ai_enabled", "0") not in ("1", "true", "True"):
            return None, "AI disabled in Settings", None
        primary = s.get("ai_primary_model", "").strip()
        fallback = s.get("ai_fallback_model", "").strip()
        if not primary:
            return None, "no primary AI model configured", None

        cfg_ai = self.cfg.get("ai") or {}
        oc_cfg_path = cfg_ai.get("openclaw_config_path", "~/.openclaw/openclaw.json")
        # Tight bound -- this runs inline on the alert path. We never block
        # the email send for more than ~8s even if both models are slow.
        explain_timeout = int(cfg_ai.get("explain_timeout_seconds",
                                          cfg_ai.get("timeout_seconds", 8)))
        if explain_timeout > 15:
            explain_timeout = 15
        explain_max_tokens = int(cfg_ai.get("explain_max_tokens", 512))
        tuning_overrides = cfg_ai.get("tunings") or {}

        # Pull the cron's payload prompt + its configured model endpoint so
        # we can mention it in the AI input (helps the model spot wired-to-
        # the-wrong-endpoint cases).
        jobs_path = self.cfg["heartbeat"]["jobs_json_path"]
        raw_jobs = oc.read_jobs_json(jobs_path)
        raw = next((j for j in raw_jobs if j.get("id") == cron["cron_id"]), None) or {}
        prompt_text = ((raw.get("payload") or {}).get("message") or "")
        cron_payload_model = (raw.get("payload") or {}).get("model")
        cron_model_endpoint = None
        if cron_payload_model:
            cron_mdef = ai_mod.get_model_endpoint(oc_cfg_path, cron_payload_model)
            if cron_mdef:
                cron_model_endpoint = cron_mdef.get("base_url")

        last_err = ""
        for slot, key in [("primary", primary), ("fallback", fallback)]:
            if not key:
                continue
            mdef = ai_mod.get_model_endpoint(oc_cfg_path, key)
            if not mdef:
                last_err = f"{slot} model not found in openclaw.json"
                continue
            # Short ping so we fail fast on dead endpoints
            if not ai_mod.is_endpoint_reachable(mdef["base_url"],
                                                 mdef.get("api_key"),
                                                 timeout_seconds=2):
                last_err = f"{slot} endpoint unreachable"
                continue
            tuning = ai_mod.resolve_tuning(key, tuning_overrides)
            messages = ai_mod.build_explain_messages(
                cron_name=cron.get("name") or cron["cron_id"],
                agent=cron.get("agent") or "?",
                schedule=cron.get("schedule") or "?",
                cron_prompt=prompt_text,
                error=error,
                failure_source=failure_source,
                retry_history=retry_history,
                recent_run_summary=run_log_excerpt,
                model_id=(cron_payload_model.split("/", 1)[-1]
                          if cron_payload_model and "/" in cron_payload_model
                          else cron_payload_model),
                model_endpoint=cron_model_endpoint,
                tuning=tuning,
            )
            try:
                diagnosis, err = ai_mod.explain_failure(
                    base_url=mdef["base_url"],
                    model=mdef["model_id"],
                    messages=messages,
                    api_key=mdef.get("api_key"),
                    tuning=tuning,
                    max_tokens=explain_max_tokens,
                    timeout_seconds=explain_timeout,
                )
            except Exception as e:
                last_err = f"{slot} {type(e).__name__}: {e}"
                continue
            if diagnosis is not None:
                return diagnosis, "", key
            last_err = f"{slot} parse/call failed: {err[:200]}"
        return None, last_err or "all configured models failed", None

    def explain_event(self, cron_id: str, event_kind: str,
                       event_id: int, *, force: bool = False) -> dict:
        """On-demand failure-mode explanation for a past retry or alert
        event. Caches in the explanations table so repeat clicks don't
        re-burn the AI call. Pass force=True to regenerate."""
        if event_kind not in ("retry", "alert"):
            raise RuntimeError(f"invalid event_kind: {event_kind!r}")

        if not force:
            cached = db_mod.get_explanation(self.conn, event_kind, event_id)
            if cached and (cached.get("cause") or cached.get("error")):
                return {"ok": bool(cached.get("cause")),
                        "cached": True,
                        "cause": cached.get("cause"),
                        "next_step": cached.get("next_step"),
                        "confidence": cached.get("confidence"),
                        "category": cached.get("category"),
                        "model_key": cached.get("model_key"),
                        "created_at": cached.get("created_at"),
                        "error": cached.get("error")}

        # Fetch the underlying event so we have the error text + context
        if event_kind == "retry":
            evt = db_mod.get_retry_event(self.conn, event_id)
        else:
            evt = db_mod.get_alert_event(self.conn, event_id)
        if not evt or evt.get("cron_id") != cron_id:
            raise RuntimeError(f"{event_kind} event {event_id} not found for {cron_id}")

        cron = self._ensure_cron(cron_id)
        # For alert events we don't have a structured `error` column —
        # the body has the error text. For retry events we do.
        error_text = (evt.get("error")
                       or evt.get("subject", "")
                       or "(no error text recorded)")
        failure_source = evt.get("failure_source") or "alert"
        history = db_mod.recent_retry_events(self.conn, cron_id, limit=10)
        run_log_excerpt = self._read_failed_run_excerpt(cron_id)

        diagnosis, diag_err, diag_model_key = self._try_explain_failure(
            cron=cron, error=error_text, failure_source=failure_source,
            retry_history=history, run_log_excerpt=run_log_excerpt,
        )

        db_mod.upsert_explanation(
            self.conn,
            event_kind=event_kind, event_id=event_id,
            cron_id=cron_id, created_at=now_iso(),
            model_key=diag_model_key,
            cause=(diagnosis or {}).get("cause"),
            next_step=(diagnosis or {}).get("next_step"),
            confidence=(diagnosis or {}).get("confidence"),
            category=(diagnosis or {}).get("category"),
            error=diag_err if not diagnosis else None,
        )
        return {"ok": bool(diagnosis), "cached": False,
                "cause": (diagnosis or {}).get("cause"),
                "next_step": (diagnosis or {}).get("next_step"),
                "confidence": (diagnosis or {}).get("confidence"),
                "category": (diagnosis or {}).get("category"),
                "model_key": diag_model_key,
                "error": diag_err if not diagnosis else None}

    def _fire_dependency_alert(self, cron: dict, failed_run_id: str | None,
                               original_error: str | None,
                               failure_source: str,
                               hc_failure: dict) -> dict:
        """Alert path when a pre-retry healthcheck fails. Skips the retry,
        marks the retry_events row as declined-dependency-down, and uses a
        distinct alert subject so the operator can filter these out from
        the more common max-retries-exhausted alerts."""
        cron_name = cron.get("name") or cron["cron_id"]
        subject = f"[OpenClaw Cron Failure] {cron_name} — dependency unhealthy"
        # Stitch healthcheck details into the body's error section so the
        # operator immediately sees what was checked + how it failed
        hc_desc = hc_failure.get("description") or hc_failure.get("type") or "(unnamed)"
        augmented_error = (
            f"Pre-retry healthcheck failed — retry was SKIPPED.\n"
            f"\n"
            f"  Healthcheck #{hc_failure['index'] + 1}: {hc_desc}\n"
            f"  Check type: {hc_failure.get('type')}\n"
            f"  Reason:     {hc_failure.get('error')}\n"
            f"\n"
            f"Original cron error:\n"
            f"  {original_error or '(none)'}\n"
            f"\n"
            f"The watchdog declined to retry because a dependency this cron uses "
            f"appears to be down. Fix the dependency, then trigger a manual retry "
            f"from the watchdog UI or run the cron yourself."
        )
        notes = f"healthcheck#{hc_failure['index']}: {hc_desc} — {hc_failure.get('error', '')[:120]}"
        return self._fire_alert(
            cron, failed_run_id, augmented_error,
            failure_source=failure_source,
            retries_today=0,
            subject_override=subject,
            notes=notes,
            outcome="declined-dependency-down",
        )


# Module-level singleton, set in main()
WATCHDOG: Watchdog | None = None


# ---------- HTTP handlers ----------

class Handler(BaseHTTPRequestHandler):
    server_version = f"oc-retry-watchdog/{VERSION}"

    # Quiet the default access log; we'll log meaningfully ourselves.
    def log_message(self, format: str, *args: Any) -> None:
        if self.path.startswith("/api/") or self.path == "/webhook":
            sys.stderr.write(f"[{self.log_date_time_string()}] {self.command} {self.path}\n")

    def _send_json(self, code: int, body: Any) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(data)

    def _send_text(self, code: int, body: str, ctype: str = "text/plain") -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", f"{ctype}; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> Any:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return None

    def _serve_static(self, rel: str) -> None:
        p = WEB_DIR / rel
        if not p.exists() or not p.is_file():
            self._send_text(404, "Not found")
            return
        ctype = {".html": "text/html", ".js": "application/javascript",
                 ".css": "text/css"}.get(p.suffix, "application/octet-stream")
        self._send_text(200, p.read_text(encoding="utf-8"), ctype=ctype)

    # ----- routing

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        assert WATCHDOG is not None
        path = urllib.parse.urlparse(self.path).path

        if path == "/" or path == "/index.html":
            self._serve_static("index.html")
        elif path.startswith("/web/"):
            self._serve_static(path[len("/web/"):])
        elif path in ("/app.js", "/style.css"):
            self._serve_static(path.lstrip("/"))
        elif path == "/api/health":
            self._send_json(200, {"ok": True, "version": VERSION,
                                  "uptime_seconds": int(time.time() - _START_TIME)})
        elif path == "/api/settings":
            s = WATCHDOG._settings()
            self._send_json(200, {
                "default_alert_recipient": s.get("default_alert_recipient", ""),
                "default_max_retries": int(s.get("default_max_retries", "1")),
                "sender_account": s.get("sender_account", ""),
                "openclaw_instance_name": WATCHDOG.cfg.get("openclaw_instance_name", ""),
                "ai_enabled": s.get("ai_enabled", "0") in ("1", "true", "True"),
                "ai_primary_model": s.get("ai_primary_model", ""),
                "ai_fallback_model": s.get("ai_fallback_model", ""),
                "daemon_version": VERSION,
                "daemon_uptime_seconds": int(time.time() - _START_TIME),
            })
        elif path == "/api/ai/models":
            oc_path = WATCHDOG.cfg["ai"]["openclaw_config_path"]
            models = ai_mod.read_openclaw_models(oc_path)
            # Annotate each with its resolved tuning so the operator can see
            # at a glance which knobs will apply.
            overrides = (WATCHDOG.cfg.get("ai") or {}).get("tunings") or {}
            ai_default_mt = int((WATCHDOG.cfg.get("ai") or {}).get("max_tokens", 1024))
            for m in models:
                t = ai_mod.resolve_tuning(m["key"], overrides)
                m["tuning_name"] = t.get("name", t.get("_source", "?"))
                m["tuning_source"] = t.get("_source", "?")
                m["tuning_notes"] = t.get("notes", "")
                # Pull compaction settings (per-agent override resolved at
                # call-time inside suggest_predicates; for display we use
                # the global default).
                full = ai_mod.get_model_endpoint(oc_path, m["key"])
                if full:
                    requested = int(t.get("max_tokens", ai_default_mt) or ai_default_mt)
                    budget = ai_mod.compute_context_budget(full, requested)
                    m["context_window"] = full.get("context_window")
                    m["model_max_tokens"] = full.get("max_tokens")
                    m["compaction"] = full.get("compaction") or {}
                    m["effective_max_tokens"] = budget["capped_max_tokens"]
                    m["input_headroom_tokens"] = budget["input_headroom_tokens"]
            # Parallel-ping all endpoints so the dropdown can mark offline
            # models. Bounded by ~2s wall-clock even if every endpoint is
            # down (parallel timeouts).
            try:
                avail = ai_mod.check_models_availability(models, timeout_seconds=2)
                for m in models:
                    m["online"] = bool(avail.get(m["key"], False))
            except Exception as e:
                # Don't break the endpoint over an availability check
                sys.stderr.write(f"[/api/ai/models] availability check failed: {e}\n")
                for m in models:
                    m["online"] = None  # unknown
            self._send_json(200, models)
        elif path == "/api/ai/tunings":
            # Built-in registry + active overrides — useful for operators
            # adding new model families or debugging "why didn't it pick the
            # right knobs?"
            overrides = (WATCHDOG.cfg.get("ai") or {}).get("tunings") or {}
            self._send_json(200, {
                "default": ai_mod.DEFAULT_TUNING,
                "builtin": ai_mod.BUILTIN_TUNINGS,
                "overrides": overrides,
            })
        elif path == "/api/crons":
            tz = WATCHDOG.cfg["server"]["timezone"]
            crons = db_mod.list_crons_with_counts(WATCHDOG.conn, today_iso_date(tz))
            # Annotate each cron with predicate + healthcheck counts/descriptions
            cfg_preds = WATCHDOG.cfg.get("predicates", {}) or {}
            cfg_hcs = WATCHDOG.cfg.get("healthchecks", {}) or {}
            for c in crons:
                preds = cfg_preds.get(c["cron_id"])
                if isinstance(preds, list):
                    c["predicates_count"] = len(preds)
                    c["predicates_descriptions"] = [
                        p.get("description", p.get("type", "?")) for p in preds
                    ]
                else:
                    c["predicates_count"] = 0
                    c["predicates_descriptions"] = []
                hcs = cfg_hcs.get(c["cron_id"])
                if isinstance(hcs, list):
                    c["healthchecks_count"] = len(hcs)
                    c["healthchecks_descriptions"] = [
                        h.get("description", h.get("type", "?")) for h in hcs
                    ]
                else:
                    c["healthchecks_count"] = 0
                    c["healthchecks_descriptions"] = []
            self._send_json(200, crons)
        elif path.startswith("/api/crons/") and path.endswith("/history"):
            cron_id = path.split("/")[3]
            retries = db_mod.recent_retry_events(WATCHDOG.conn, cron_id)
            alerts = db_mod.recent_alert_events(WATCHDOG.conn, cron_id)
            # Annotate with cached-explanation availability so the UI can
            # render the Explain button in its "view cached" state without
            # an extra round-trip per row.
            for r in retries:
                r["has_explanation"] = bool(
                    db_mod.get_explanation(WATCHDOG.conn, "retry", r["id"])
                )
            for a in alerts:
                a["has_explanation"] = bool(
                    db_mod.get_explanation(WATCHDOG.conn, "alert", a["id"])
                )
            self._send_json(200, {
                "retry_events": retries,
                "alert_events": alerts,
            })
        elif path.startswith("/api/crons/") and path.endswith("/predicates"):
            cron_id = path.split("/")[3]
            preds = (WATCHDOG.cfg.get("predicates") or {}).get(cron_id, [])
            self._send_json(200, preds if isinstance(preds, list) else [])
        elif path.startswith("/api/crons/") and path.endswith("/healthchecks"):
            cron_id = path.split("/")[3]
            hcs = (WATCHDOG.cfg.get("healthchecks") or {}).get(cron_id, [])
            self._send_json(200, hcs if isinstance(hcs, list) else [])
        elif path == "/api/heartbeat":
            rows = WATCHDOG.conn.execute(
                "SELECT * FROM heartbeat_scans ORDER BY id DESC LIMIT 50"
            ).fetchall()
            self._send_json(200, [dict(r) for r in rows])
        elif path == "/api/openclaw-jobs":
            self._send_json(200, WATCHDOG.list_openclaw_jobs())
        elif path == "/api/cron-schedules":
            self._send_json(200, WATCHDOG.list_cron_schedules())
        elif path == "/api/missed-runs":
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            day = (qs.get("day") or [None])[0]
            try:
                result = WATCHDOG.find_missed_runs(day_iso=day)
            except RuntimeError as e:
                self._send_json(400, {"ok": False, "error": str(e)})
                return
            self._send_json(200, result)
        else:
            self._send_text(404, "Not found")

    def do_POST(self) -> None:
        assert WATCHDOG is not None
        path = urllib.parse.urlparse(self.path).path
        body = self._read_json()

        if path == "/webhook":
            if not isinstance(body, dict):
                self._send_json(400, {"ok": False, "error": "invalid json"})
                return
            cron_id = body.get("jobId") or body.get("cron_id") or body.get("id")
            if not cron_id:
                self._send_json(400, {"ok": False, "error": "missing jobId/cron_id"})
                return
            result = WATCHDOG.handle_failure(
                cron_id=cron_id,
                failed_run_id=body.get("runId") or body.get("run_id"),
                error=body.get("error") or body.get("message") or "",
                failure_source="webhook",
            )
            self._send_json(200, result)
            return

        m = path.split("/")
        if len(m) >= 5 and m[1] == "api" and m[2] == "crons":
            cron_id = m[3]
            action = m[4]
            if action == "retry-now":
                self._send_json(200, WATCHDOG.manual_retry(cron_id))
                return
            if action == "test-alert":
                self._send_json(200, WATCHDOG.test_alert(cron_id))
                return

        if path == "/api/heartbeat/scan-now":
            stats = WATCHDOG.scanner.scan_once()
            self._send_json(200, stats)
            return

        # POST /api/check/test — dry-run a single check (predicate or
        # healthcheck — same schema). Used by the editor's per-card Test
        # button to validate a definition before saving.
        if path == "/api/check/test":
            if not isinstance(body, dict):
                self._send_json(400, {"ok": False, "message": "expected JSON object"})
                return
            if not body.get("type"):
                self._send_json(400, {"ok": False, "message": "missing 'type'"})
                return
            tz_name = WATCHDOG.cfg["server"]["timezone"]
            try:
                # Ephemeral in-memory state for file_grew (which is rarely
                # used here but supported). State doesn't persist between
                # test calls — that's fine for a test button.
                _scratch: dict = {}
                ok, msg = predicates_mod.evaluate(
                    body,
                    tz_name=tz_name,
                    state_get=lambda k: _scratch.get(k),
                    state_set=lambda k, v: _scratch.__setitem__(k, v),
                )
            except Exception as e:
                self._send_json(200, {"ok": False,
                                      "message": f"{type(e).__name__}: {e}"})
                return
            self._send_json(200, {"ok": bool(ok), "message": msg})
            return

        # POST /api/crons/<id>/predicates/suggest
        # POST /api/crons/<id>/healthchecks/suggest
        if (len(m) >= 6 and m[1] == "api" and m[2] == "crons"
                and m[4] in ("predicates", "healthchecks") and m[5] == "suggest"):
            cron_id = m[3]
            kind = m[4]
            try:
                if kind == "predicates":
                    result = WATCHDOG.suggest_predicates(cron_id)
                else:
                    result = WATCHDOG.suggest_healthchecks(cron_id)
            except RuntimeError as e:
                self._send_json(400, {"ok": False, "error": str(e)})
                return
            self._send_json(200, result)
            return

        # POST /api/crons/<cron_id>/history/<kind>/<event_id>/explain
        # Generate (or fetch cached) AI explanation for a past retry or
        # alert event. Body may include {"force": true} to regenerate.
        if (len(m) >= 8 and m[1] == "api" and m[2] == "crons"
                and m[4] == "history" and m[5] in ("retry", "alert")
                and m[7] == "explain"):
            cron_id = m[3]
            kind = m[5]
            try:
                event_id = int(m[6])
            except ValueError:
                self._send_json(400, {"ok": False, "error": "event_id must be int"})
                return
            force = bool(isinstance(body, dict) and body.get("force"))
            try:
                result = WATCHDOG.explain_event(cron_id, kind, event_id, force=force)
            except RuntimeError as e:
                self._send_json(400, {"ok": False, "error": str(e)})
                return
            self._send_json(200, result)
            return

        # POST /api/openclaw-jobs/<id>/wire  | /unwire
        if len(m) >= 5 and m[1] == "api" and m[2] == "openclaw-jobs":
            cron_id = m[3]
            action = m[4]
            if action == "wire":
                ok, output = WATCHDOG.wire_openclaw_cron(cron_id)
                self._send_json(200 if ok else 500,
                                {"ok": ok, "action": "wired" if ok else "wire-failed",
                                 "output": output[:1000]})
                return
            if action == "unwire":
                ok, output = WATCHDOG.unwire_openclaw_cron(cron_id)
                self._send_json(200 if ok else 500,
                                {"ok": ok, "action": "unwired" if ok else "unwire-failed",
                                 "output": output[:1000]})
                return

        # POST /api/missed-runs/<cron_id>/fire
        # One-shot manual catch-up for a cron the operator picked from the
        # Missed Runs view. Calls `openclaw cron run <id>` directly; no
        # retry-event row, no watchdog DB registration.
        if (len(m) >= 5 and m[1] == "api" and m[2] == "missed-runs"
                and m[4] == "fire"):
            cron_id = m[3]
            result = WATCHDOG.fire_cron_now(cron_id)
            self._send_json(200 if result.get("ok") else 500, result)
            return

        # POST /api/missed-runs/<cron_id>/explain
        # Body: {"expected_at_ms": <int>, "grace_minutes": <int>}
        # Returns AI failure-mode diagnosis + healthcheck states + a
        # suggested action for whether to re-fire.
        if (len(m) >= 5 and m[1] == "api" and m[2] == "missed-runs"
                and m[4] == "explain"):
            cron_id = m[3]
            if not isinstance(body, dict):
                self._send_json(400, {"ok": False,
                                      "error": "expected JSON object"})
                return
            expected = body.get("expected_at_ms")
            if not isinstance(expected, (int, float)):
                self._send_json(400, {"ok": False,
                                      "error": "expected_at_ms (number) is required"})
                return
            tol = body.get("match_tolerance_seconds")
            tol_int = 60
            if isinstance(tol, (int, float)):
                tol_int = max(0, int(tol))
            try:
                result = WATCHDOG.explain_missed_run(
                    cron_id, int(expected),
                    match_tolerance_seconds=tol_int)
            except RuntimeError as e:
                self._send_json(400, {"ok": False, "error": str(e)})
                return
            self._send_json(200, result)
            return

        self._send_text(404, "Not found")

    def do_DELETE(self) -> None:
        assert WATCHDOG is not None
        path = urllib.parse.urlparse(self.path).path
        m = path.split("/")
        # DELETE /api/crons/<id> — remove from watchdog DB (orphan cleanup)
        if len(m) == 4 and m[1] == "api" and m[2] == "crons":
            cron_id = m[3]
            removed = WATCHDOG.delete_cron(cron_id)
            self._send_json(200, {"ok": True, "removed": removed})
            return
        self._send_text(404, "Not found")

    def do_PATCH(self) -> None:
        assert WATCHDOG is not None
        path = urllib.parse.urlparse(self.path).path
        body = self._read_json()
        if not isinstance(body, dict):
            self._send_json(400, {"ok": False, "error": "invalid json"})
            return

        m = path.split("/")
        if len(m) == 4 and m[1] == "api" and m[2] == "crons":
            cron_id = m[3]
            updated = db_mod.patch_cron(WATCHDOG.conn, cron_id, body)
            if updated is None:
                self._send_json(404, {"ok": False, "error": "cron not found"})
                return
            self._send_json(200, updated)
            return

        if path == "/api/settings":
            allowed = {"default_alert_recipient", "default_max_retries",
                       "ai_enabled", "ai_primary_model", "ai_fallback_model"}
            for k, v in body.items():
                if k not in allowed:
                    continue
                if k == "ai_enabled":
                    v = "1" if v in (True, "true", "True", "1", 1) else "0"
                db_mod.set_setting(WATCHDOG.conn, k, str(v))
            self._send_json(200, WATCHDOG._settings())
            return

        self._send_text(404, "Not found")

    def do_PUT(self) -> None:
        assert WATCHDOG is not None
        path = urllib.parse.urlparse(self.path).path
        body = self._read_json()

        m = path.split("/")
        if (len(m) >= 5 and m[1] == "api" and m[2] == "crons"
                and m[4] in ("predicates", "healthchecks")):
            cron_id = m[3]
            kind = m[4]
            if not isinstance(body, list):
                self._send_json(400, {"ok": False, "error": f"expected JSON array of {kind}"})
                return
            for i, p in enumerate(body):
                if not isinstance(p, dict):
                    self._send_json(400, {"ok": False, "error": f"{kind}[{i}] is not an object"})
                    return
                if not p.get("type"):
                    self._send_json(400, {"ok": False, "error": f"{kind}[{i}] missing 'type'"})
                    return
            try:
                if kind == "predicates":
                    updated = WATCHDOG.update_predicates(cron_id, body)
                else:
                    updated = WATCHDOG.update_healthchecks(cron_id, body)
            except RuntimeError as e:
                self._send_json(500, {"ok": False, "error": str(e)})
                return
            self._send_json(200, updated)
            return

        self._send_text(404, "Not found")


# ---------- Background tasks ----------

def cron_info_refresh_loop(wd: Watchdog, interval_seconds: int = 300) -> None:
    """Refresh name/schedule/agent for known crons periodically."""
    while True:
        try:
            rows = wd.conn.execute("SELECT cron_id FROM crons").fetchall()
            for r in rows:
                cid = r["cron_id"]
                meta, _ = oc.cron_show(wd.cfg["openclaw_cli"], cid)
                db_mod.update_cron_meta(wd.conn, cid, meta["name"],
                                        meta["schedule"], meta["agent"])
        except Exception as e:
            sys.stderr.write(f"[cron_info_refresh] {type(e).__name__}: {e}\n")
        time.sleep(interval_seconds)


# ---------- main ----------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--init-db", action="store_true",
                    help="Initialize DB schema + seed defaults from config, then exit")
    args = ap.parse_args()

    cfg, cfg_path = load_config(args.config)
    global WATCHDOG
    WATCHDOG = Watchdog(cfg, cfg_path=cfg_path)

    if args.init_db:
        print(f"DB initialized at {os.path.expanduser(cfg['db']['path'])}")
        return 0

    port = int(cfg["server"]["port"])
    bind = cfg["server"]["ui_bind"]

    threading.Thread(
        target=cron_info_refresh_loop,
        args=(WATCHDOG,),
        daemon=True,
    ).start()

    threading.Thread(
        target=WATCHDOG.scanner.run_forever,
        daemon=True,
    ).start()

    httpd = ThreadingHTTPServer((bind, port), Handler)
    print(f"[oc-retry-watchdog v{VERSION}] listening on {bind}:{port}")
    print(f"  config:     {args.config or os.environ.get('RETRY_WATCHDOG_CONFIG') or './config.json'}")
    print(f"  db:         {os.path.expanduser(cfg['db']['path'])}")
    print(f"  recipient:  {cfg['alert'].get('default_recipient') or '(unset)'}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down...")
    return 0


if __name__ == "__main__":
    sys.exit(main())
