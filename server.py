"""OpenClaw Retry Watchdog daemon.

v0.1 — webhook receive + retry + alert + read-only API + minimal UI page.
v0.2 — adds predicate verification.
v0.3 — adds heartbeat / missed-run detection.

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
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import db as db_mod
import alert as alert_mod
import openclaw_lookup as oc
import heartbeat as heartbeat_mod
import ai as ai_mod

VERSION = "0.5.0"
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
        "grace_period_minutes": 10,
        "lookback_hours": 6,
        "jobs_json_path": "~/.openclaw/cron/jobs.json",
        "runs_dir_path": "~/.openclaw/cron/runs",
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
        tz = self.cfg["server"]["timezone"]
        today = today_iso_date(tz)
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM retry_events WHERE cron_id=? "
            "AND received_at >= ? AND received_at < ?",
            (cron_id, today + "T00:00:00", today + "T23:59:59.999999"),
        ).fetchone()
        return int(row["c"] if row else 0)

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
            messages = ai_mod.build_messages(
                cron_name=job.get("name") or cron_id,
                agent=job.get("agent") or "?",
                schedule=job.get("schedule") or "?",
                cron_prompt=prompt_text,
                recent_summaries=recent,
                existing_predicates=existing_preds,
                tuning=tuning,
                kind=kind,
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
            if not preds:
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
                    notes: str | None = None) -> dict:
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
        body = alert_mod.format_failure_body(
            cron=cron,
            error=error or "(no error text)",
            failure_source=failure_source,
            retry_history=history,
            suggested_cron_run=f"{cfg['openclaw_cli']} cron run {cron['cron_id']}",
            ui_url=cfg.get("ui_url", "http://localhost:9095/"),
        )

        ok, err = alert_mod.send_email(
            sender_binary=cfg["alert"]["sender_binary"],
            sender_account=cfg["alert"]["sender_account"],
            recipient=recipient,
            subject=subject,
            body=body,
            extra_env=cfg["alert"].get("sender_env") or {},
        )

        # Record the alert and the originating retry-decision row in one transaction.
        if failure_source != "test":
            db_mod.insert_retry_event(
                self.conn, cron["cron_id"], failed_run_id, now_iso(),
                None, None, "declined-over-limit", failure_source, error,
                f"retries_today={retries_today}",
            )
        db_mod.insert_alert_event(
            self.conn, cron["cron_id"], now_iso(), recipient,
            subject, body, 1 if ok else 0, None if ok else err, notes,
        )
        return {"ok": ok, "action": "alerted", "recipient": recipient,
                "error": None if ok else err}


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
            self._send_json(200, {
                "retry_events": db_mod.recent_retry_events(WATCHDOG.conn, cron_id),
                "alert_events": db_mod.recent_alert_events(WATCHDOG.conn, cron_id),
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
