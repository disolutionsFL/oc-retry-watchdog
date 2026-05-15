"""SQLite schema bootstrap and small helpers.

v1 schema. Bumping `schema_version` in the meta table triggers ALTER migrations
at startup. Pure stdlib — no SQLAlchemy.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

SCHEMA_VERSION = "1"


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS crons (
    cron_id TEXT PRIMARY KEY,
    name TEXT,
    schedule TEXT,
    agent TEXT,
    enabled INTEGER DEFAULT 1,
    max_retries INTEGER DEFAULT 1,
    alert_recipient TEXT,
    first_seen_at TEXT,
    last_updated_at TEXT
);

CREATE TABLE IF NOT EXISTS retry_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cron_id TEXT NOT NULL,
    failed_run_id TEXT,
    received_at TEXT NOT NULL,
    retried_at TEXT,
    retried_run_id TEXT,
    outcome TEXT NOT NULL,
    failure_source TEXT NOT NULL,
    error TEXT,
    notes TEXT,
    FOREIGN KEY (cron_id) REFERENCES crons(cron_id)
);

CREATE INDEX IF NOT EXISTS idx_retry_events_cron_date
    ON retry_events(cron_id, received_at);

CREATE TABLE IF NOT EXISTS alert_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cron_id TEXT NOT NULL,
    triggered_at TEXT NOT NULL,
    channel TEXT NOT NULL,
    recipient TEXT NOT NULL,
    subject TEXT,
    body TEXT,
    success INTEGER NOT NULL,
    error TEXT,
    notes TEXT,
    FOREIGN KEY (cron_id) REFERENCES crons(cron_id)
);

CREATE INDEX IF NOT EXISTS idx_alert_events_cron_date
    ON alert_events(cron_id, triggered_at);

CREATE TABLE IF NOT EXISTS predicate_history (
    cron_id TEXT NOT NULL,
    predicate_index INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (cron_id, predicate_index),
    FOREIGN KEY (cron_id) REFERENCES crons(cron_id)
);

CREATE TABLE IF NOT EXISTS heartbeat_scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scanned_at TEXT NOT NULL,
    crons_checked INTEGER NOT NULL,
    missed_detected INTEGER NOT NULL,
    predicates_failed INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- AI-generated failure-mode explanations. Keyed by (event_kind, event_id)
-- so the same diagnosis can attach to either a retry_events row or an
-- alert_events row. Lets the UI re-show old explanations without paying
-- the AI call cost twice.
CREATE TABLE IF NOT EXISTS explanations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_kind TEXT NOT NULL,           -- 'retry' or 'alert'
    event_id INTEGER NOT NULL,
    cron_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    model_key TEXT,                     -- which AI model produced this
    cause TEXT,
    next_step TEXT,
    confidence TEXT,
    category TEXT,
    error TEXT,                         -- non-empty when AI call failed
    UNIQUE(event_kind, event_id)
);

CREATE INDEX IF NOT EXISTS idx_explanations_cron
    ON explanations(cron_id, created_at);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    p = Path(os.path.expanduser(str(db_path)))
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def init_schema(conn: sqlite3.Connection, defaults: dict[str, Any]) -> None:
    """Create tables (idempotent) and seed settings from `defaults` if empty.

    `defaults` is the resolved set of values from config.json that should
    populate the `settings` table on a fresh DB. UI-edited values persist
    in this table — config.json only seeds the initial state.

    Note: executescript() implicitly commits before running, so we can't
    wrap it in our manual transaction(). With isolation_level=None each
    single execute() auto-commits — fine for the small number of seed rows.
    """
    conn.executescript(SCHEMA_SQL)
    existing = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if not existing:
        conn.execute("INSERT INTO meta (key, value) VALUES (?, ?)",
                     ("schema_version", SCHEMA_VERSION))
    for k, v in defaults.items():
        conn.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
            (k, str(v) if not isinstance(v, str) else v),
        )


def get_setting(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def all_settings(conn: sqlite3.Connection) -> dict[str, str]:
    return {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM settings")}


def upsert_cron(conn: sqlite3.Connection, cron_id: str, defaults: dict[str, Any]) -> dict:
    """Insert cron if unknown, return its current row as a dict."""
    row = conn.execute("SELECT * FROM crons WHERE cron_id=?", (cron_id,)).fetchone()
    if row:
        return dict(row)
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO crons (cron_id, enabled, max_retries, alert_recipient, "
        "first_seen_at, last_updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        (cron_id, 1, int(defaults.get("default_max_retries", 1)),
         None, now, now),
    )
    row = conn.execute("SELECT * FROM crons WHERE cron_id=?", (cron_id,)).fetchone()
    return dict(row)


def update_cron_meta(conn: sqlite3.Connection, cron_id: str,
                     name: str | None, schedule: str | None, agent: str | None) -> None:
    from datetime import datetime, timezone
    conn.execute(
        "UPDATE crons SET name=COALESCE(?,name), schedule=COALESCE(?,schedule), "
        "agent=COALESCE(?,agent), last_updated_at=? WHERE cron_id=?",
        (name, schedule, agent, datetime.now(timezone.utc).isoformat(), cron_id),
    )


def patch_cron(conn: sqlite3.Connection, cron_id: str, fields: dict[str, Any]) -> dict | None:
    allowed = {"enabled", "max_retries", "alert_recipient"}
    sets, vals = [], []
    for k, v in fields.items():
        if k not in allowed:
            continue
        sets.append(f"{k}=?")
        vals.append(v)
    if not sets:
        return dict(conn.execute("SELECT * FROM crons WHERE cron_id=?", (cron_id,)).fetchone() or {})
    from datetime import datetime, timezone
    sets.append("last_updated_at=?")
    vals.append(datetime.now(timezone.utc).isoformat())
    vals.append(cron_id)
    conn.execute(f"UPDATE crons SET {', '.join(sets)} WHERE cron_id=?", vals)
    row = conn.execute("SELECT * FROM crons WHERE cron_id=?", (cron_id,)).fetchone()
    return dict(row) if row else None


def list_crons_with_counts(conn: sqlite3.Connection, today_iso_date: str) -> list[dict]:
    today_prefix = today_iso_date + "T"
    rows = conn.execute("SELECT * FROM crons ORDER BY name").fetchall()
    out = []
    for r in rows:
        cid = r["cron_id"]
        counts = conn.execute("""
            SELECT
                SUM(CASE WHEN received_at LIKE ? THEN 1 ELSE 0 END) AS retries_today,
                SUM(CASE WHEN received_at >= date('now','-30 days') THEN 1 ELSE 0 END) AS retries_30d
            FROM retry_events WHERE cron_id=?
        """, (today_prefix + "%", cid)).fetchone()
        alerts = conn.execute("""
            SELECT
                SUM(CASE WHEN triggered_at LIKE ? THEN 1 ELSE 0 END) AS alerts_today,
                SUM(CASE WHEN triggered_at >= date('now','-30 days') THEN 1 ELSE 0 END) AS alerts_30d
            FROM alert_events WHERE cron_id=?
        """, (today_prefix + "%", cid)).fetchone()
        last_retry = conn.execute(
            "SELECT retried_at FROM retry_events WHERE cron_id=? AND retried_at IS NOT NULL "
            "ORDER BY id DESC LIMIT 1", (cid,)
        ).fetchone()
        last_alert = conn.execute(
            "SELECT triggered_at FROM alert_events WHERE cron_id=? ORDER BY id DESC LIMIT 1",
            (cid,)
        ).fetchone()
        d = dict(r)
        d.update({
            "retries_today": (counts["retries_today"] or 0) if counts else 0,
            "retries_30d":   (counts["retries_30d"] or 0) if counts else 0,
            "alerts_today":  (alerts["alerts_today"] or 0) if alerts else 0,
            "alerts_30d":    (alerts["alerts_30d"] or 0) if alerts else 0,
            "last_retried_at": last_retry["retried_at"] if last_retry else None,
            "last_alerted_at": last_alert["triggered_at"] if last_alert else None,
        })
        out.append(d)
    return out


def insert_retry_event(conn: sqlite3.Connection, cron_id: str, failed_run_id: str | None,
                       received_at: str, retried_at: str | None, retried_run_id: str | None,
                       outcome: str, failure_source: str, error: str | None, notes: str | None) -> int:
    cur = conn.execute(
        "INSERT INTO retry_events (cron_id, failed_run_id, received_at, retried_at, "
        "retried_run_id, outcome, failure_source, error, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (cron_id, failed_run_id, received_at, retried_at, retried_run_id,
         outcome, failure_source, error, notes),
    )
    return cur.lastrowid


def insert_alert_event(conn: sqlite3.Connection, cron_id: str, triggered_at: str,
                       recipient: str, subject: str, body: str,
                       success: int, error: str | None, notes: str | None) -> int:
    cur = conn.execute(
        "INSERT INTO alert_events (cron_id, triggered_at, channel, recipient, "
        "subject, body, success, error, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (cron_id, triggered_at, "email", recipient, subject, body,
         success, error, notes),
    )
    return cur.lastrowid


def recent_retry_events(conn: sqlite3.Connection, cron_id: str, limit: int = 10) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM retry_events WHERE cron_id=? ORDER BY id DESC LIMIT ?",
        (cron_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def recent_alert_events(conn: sqlite3.Connection, cron_id: str, limit: int = 10) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM alert_events WHERE cron_id=? ORDER BY id DESC LIMIT ?",
        (cron_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_retry_event(conn: sqlite3.Connection, event_id: int) -> dict | None:
    row = conn.execute("SELECT * FROM retry_events WHERE id=?", (event_id,)).fetchone()
    return dict(row) if row else None


def get_alert_event(conn: sqlite3.Connection, event_id: int) -> dict | None:
    row = conn.execute("SELECT * FROM alert_events WHERE id=?", (event_id,)).fetchone()
    return dict(row) if row else None


def get_explanation(conn: sqlite3.Connection, event_kind: str,
                    event_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM explanations WHERE event_kind=? AND event_id=?",
        (event_kind, event_id),
    ).fetchone()
    return dict(row) if row else None


def upsert_explanation(conn: sqlite3.Connection, *, event_kind: str,
                       event_id: int, cron_id: str, created_at: str,
                       model_key: str | None,
                       cause: str | None, next_step: str | None,
                       confidence: str | None, category: str | None,
                       error: str | None) -> int:
    """Insert or replace an explanation. Using REPLACE on the unique
    (event_kind, event_id) constraint so re-running explain overwrites
    a prior failed attempt cleanly."""
    cur = conn.execute(
        "INSERT INTO explanations (event_kind, event_id, cron_id, created_at, "
        "model_key, cause, next_step, confidence, category, error) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(event_kind, event_id) DO UPDATE SET "
        "  cron_id=excluded.cron_id, created_at=excluded.created_at, "
        "  model_key=excluded.model_key, cause=excluded.cause, "
        "  next_step=excluded.next_step, confidence=excluded.confidence, "
        "  category=excluded.category, error=excluded.error",
        (event_kind, event_id, cron_id, created_at, model_key,
         cause, next_step, confidence, category, error),
    )
    return cur.lastrowid
