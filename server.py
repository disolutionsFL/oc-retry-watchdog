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

VERSION = "0.1.0"
_START_TIME = time.time()
WEB_DIR = Path(__file__).parent / "web"


# ---------- Config loading ----------

DEFAULT_CONFIG = {
    "server": {
        "port": 9095,
        "ui_bind": "0.0.0.0",
        "webhook_bind": "127.0.0.1",
        "timezone": "America/New_York",
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
    "ui_url": "http://localhost:9095/",
    "predicates": {},
    "healthchecks": {},
}


def load_config(path: str | None) -> dict[str, Any]:
    if path:
        cfg_path = Path(os.path.expanduser(path))
    elif os.environ.get("RETRY_WATCHDOG_CONFIG"):
        cfg_path = Path(os.path.expanduser(os.environ["RETRY_WATCHDOG_CONFIG"]))
    else:
        cfg_path = Path(__file__).parent / "config.json"

    if not cfg_path.exists():
        print(f"[config] no config at {cfg_path} — using built-in defaults", file=sys.stderr)
        return DEFAULT_CONFIG

    user = json.loads(cfg_path.read_text(encoding="utf-8"))
    merged = _deep_merge(DEFAULT_CONFIG, user)
    return merged


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
    }


# ---------- Time helpers ----------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_iso_date(tz_name: str) -> str:
    return datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")


# ---------- Retry / alert flow ----------

class Watchdog:
    """Container for shared state, called by handlers via the module-level singleton."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.conn = db_mod.connect(cfg["db"]["path"])
        db_mod.init_schema(self.conn, settings_defaults_for_db(cfg))
        self._lock = threading.Lock()

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
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
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
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
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
                "daemon_version": VERSION,
                "daemon_uptime_seconds": int(time.time() - _START_TIME),
            })
        elif path == "/api/crons":
            tz = WATCHDOG.cfg["server"]["timezone"]
            crons = db_mod.list_crons_with_counts(WATCHDOG.conn, today_iso_date(tz))
            self._send_json(200, crons)
        elif path.startswith("/api/crons/") and path.endswith("/history"):
            cron_id = path.split("/")[3]
            self._send_json(200, {
                "retry_events": db_mod.recent_retry_events(WATCHDOG.conn, cron_id),
                "alert_events": db_mod.recent_alert_events(WATCHDOG.conn, cron_id),
            })
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
            for k, v in body.items():
                if k not in ("default_alert_recipient", "default_max_retries"):
                    continue
                db_mod.set_setting(WATCHDOG.conn, k, str(v))
            self._send_json(200, WATCHDOG._settings())
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

    cfg = load_config(args.config)
    global WATCHDOG
    WATCHDOG = Watchdog(cfg)

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
