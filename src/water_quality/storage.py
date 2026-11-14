from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .models import DOMAIN_RECORD_TYPE


class DataStore:
    """Owns structured tables, idempotent writes, quarantine and batch state.

    Formal observations live in ``records``; anything that cannot satisfy the
    versioned rules goes to ``quarantine_records`` with an explanatory reason
    code and the original expression, never into formal observations.
    """

    def __init__(self, database: str) -> None:
        self.conn = sqlite3.connect(database)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_schema()
        self._migrate_schema()
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _create_schema(self) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS batches (
            batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'ingest',
            processed INTEGER NOT NULL DEFAULT 0,
            accepted INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS raw_manifests (
            digest TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            content_type TEXT NOT NULL,
            byte_count INTEGER NOT NULL,
            path TEXT,
            captured_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS records (
            record_key TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            raw_hash TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS quarantine_records (
            quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER,
            reprocess_run_id INTEGER,
            record_key TEXT,
            source TEXT,
            reason_code TEXT NOT NULL,
            error TEXT NOT NULL,
            rule_version TEXT,
            dedup_hash TEXT,
            raw_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
        );
        CREATE TABLE IF NOT EXISTS rejected_records (
            rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            source TEXT,
            error TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
        );
        CREATE TABLE IF NOT EXISTS rule_versions (
            rule_version TEXT PRIMARY KEY,
            catalog_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reprocess_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_version_from TEXT,
            rule_version_to TEXT NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            affected INTEGER NOT NULL DEFAULT 0,
            unchanged INTEGER NOT NULL DEFAULT 0,
            quarantined INTEGER NOT NULL DEFAULT 0,
            summary_json TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS reprocess_run_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            record_key TEXT NOT NULL,
            action TEXT NOT NULL,
            reason_code TEXT,
            detail TEXT,
            before_json TEXT,
            after_json TEXT,
            FOREIGN KEY(run_id) REFERENCES reprocess_runs(run_id)
        );
        """)

    def _migrate_schema(self) -> None:
        """Add columns introduced after the baseline release, when absent."""
        existing = {row["name"] for row in self.conn.execute("PRAGMA table_info(records)")}
        additions = {
            "reported_value_text": "TEXT",
            "reported_value": "REAL",
            "qualifier": "TEXT",
            "detection_limit": "REAL",
            "reported_unit": "TEXT",
            "comparison_semantics": "TEXT",
            "conversion_factor": "REAL",
            "rule_version": "TEXT",
            "rule_fingerprint": "TEXT",
            "raw_digest": "TEXT",
            "source_raw_json": "TEXT",
        }
        for column, decl in additions.items():
            if column not in existing:
                self.conn.execute(f"ALTER TABLE records ADD COLUMN {column} {decl}")

        q_existing = {row["name"] for row in self.conn.execute("PRAGMA table_info(quarantine_records)")}
        for column, decl in {
            "reason_code": "TEXT",
            "rule_version": "TEXT",
            "record_key": "TEXT",
            "status": "TEXT NOT NULL DEFAULT 'open'",
            "dedup_hash": "TEXT",
        }.items():
            if column not in q_existing:
                self.conn.execute(f"ALTER TABLE quarantine_records ADD COLUMN {column} {decl}")
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_quarantine_dedup "
            "ON quarantine_records(dedup_hash) WHERE dedup_hash IS NOT NULL"
        )

    # --- rule version registry -------------------------------------------------

    def register_rule_version(self, version: str, catalog: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO rule_versions(rule_version, catalog_json, created_at) VALUES (?, ?, ?)",
            (version, json.dumps(catalog, ensure_ascii=False, sort_keys=True), self._now()),
        )
        self.conn.commit()

    # --- batch / manifest ------------------------------------------------------

    def start_batch(self, source: str, mode: str = "ingest") -> int:
        cursor = self.conn.execute(
            "INSERT INTO batches(source, status, mode, started_at) VALUES (?, 'running', ?, ?)",
            (source, mode, self._now()),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def save_manifest(self, digest: str, source: str, content_type: str, byte_count: int, path: str | None) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO raw_manifests(digest, source, content_type, byte_count, path, captured_at) VALUES (?, ?, ?, ?, ?, ?)",
            (digest, source, content_type, byte_count, path, self._now()),
        )
        self.conn.commit()

    # --- formal observations ---------------------------------------------------

    def upsert(self, record: DOMAIN_RECORD_TYPE, source_raw: Any | None = None) -> bool:
        now = self._now()
        cursor = self.conn.execute(
            """INSERT INTO records(
                 record_key, source, observed_at, payload_json, raw_hash, first_seen_at, updated_at,
                 reported_value_text, reported_value, qualifier, detection_limit, reported_unit,
                 comparison_semantics, conversion_factor, rule_version, rule_fingerprint,
                 raw_digest, source_raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(record_key) DO UPDATE SET
                 source=excluded.source, observed_at=excluded.observed_at,
                 payload_json=excluded.payload_json, raw_hash=excluded.raw_hash,
                 updated_at=excluded.updated_at,
                 reported_value_text=excluded.reported_value_text,
                 reported_value=excluded.reported_value, qualifier=excluded.qualifier,
                 detection_limit=excluded.detection_limit, reported_unit=excluded.reported_unit,
                 comparison_semantics=excluded.comparison_semantics,
                 conversion_factor=excluded.conversion_factor,
                 rule_version=excluded.rule_version,
                 rule_fingerprint=excluded.rule_fingerprint,
                 raw_digest=excluded.raw_digest,
                 source_raw_json=excluded.source_raw_json
               WHERE records.raw_hash <> excluded.raw_hash""",
            (
                record.record_key, record.source, record.observed_at,
                json.dumps(record.payload, ensure_ascii=False, sort_keys=True),
                record.raw_hash, now, now,
                record.reported_value_text, record.reported_value, record.qualifier,
                record.detection_limit, record.reported_unit,
                record.comparison_semantics, record.conversion_factor,
                record.rule_version, record.rule_fingerprint,
                record.raw_digest,
                json.dumps(source_raw, ensure_ascii=False, sort_keys=True) if source_raw is not None else None,
            ),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def delete_record(self, record_key: str) -> None:
        self.conn.execute("DELETE FROM records WHERE record_key = ?", (record_key,))
        self.conn.commit()

    def iter_records(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM records ORDER BY record_key"))

    # --- quarantine ------------------------------------------------------------

    def quarantine(
        self,
        raw: Any,
        source: str | None,
        reason_code: str,
        error: str,
        rule_version: str | None,
        batch_id: int | None = None,
        reprocess_run_id: int | None = None,
        record_key: str | None = None,
    ) -> bool:
        """Persist a quarantine row. Returns False when an identical row already exists."""
        raw_json = json.dumps(raw, ensure_ascii=False, sort_keys=True)
        # Ingest-time duplicates of the same source expression are idempotent;
        # reprocess quarantine rows are always new audit events.
        dedup_hash = None
        if reprocess_run_id is None and batch_id is not None:
            dedup_hash = hashlib.sha256(
                f"{reason_code}|{rule_version or ''}|{raw_json}".encode("utf-8")
            ).hexdigest()
        cursor = self.conn.execute(
            """INSERT OR IGNORE INTO quarantine_records(
                 batch_id, reprocess_run_id, record_key, source, reason_code, error,
                 rule_version, dedup_hash, raw_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                batch_id, reprocess_run_id, record_key, source, reason_code, error,
                rule_version, dedup_hash, raw_json, self._now(),
            ),
        )
        inserted = cursor.rowcount > 0
        # Mirror into the legacy rejection table so pre-existing readers keep working.
        if inserted and batch_id is not None:
            self.conn.execute(
                "INSERT INTO rejected_records(batch_id, source, error, raw_json, created_at) VALUES (?, ?, ?, ?, ?)",
                (batch_id, source, f"[{reason_code}] {error}", raw_json, self._now()),
            )
        self.conn.commit()
        return inserted

    def reject(self, batch_id: int, raw: Any, source: str | None, error: str) -> None:
        """Legacy entry point retained for compatibility."""
        self.quarantine(raw, source, "UNCLASSIFIED", error, None, batch_id=batch_id)

    def list_open_quarantine(self) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM quarantine_records WHERE status='open' ORDER BY quarantine_id"
            )
        )

    def mark_quarantine_status(self, quarantine_id: int, status: str) -> None:
        self.conn.execute(
            "UPDATE quarantine_records SET status=? WHERE quarantine_id=?",
            (status, quarantine_id),
        )
        self.conn.commit()

    # --- reprocessing runs -----------------------------------------------------

    def start_reprocess_run(
        self, rule_version_from: str | None, rule_version_to: str, mode: str
    ) -> int:
        cursor = self.conn.execute(
            """INSERT INTO reprocess_runs(rule_version_from, rule_version_to, mode, status, started_at)
               VALUES (?, ?, ?, 'running', ?)""",
            (rule_version_from, rule_version_to, mode, self._now()),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def add_reprocess_item(
        self,
        run_id: int,
        record_key: str,
        action: str,
        reason_code: str | None,
        detail: str | None,
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
    ) -> None:
        self.conn.execute(
            """INSERT INTO reprocess_run_items(
                 run_id, record_key, action, reason_code, detail, before_json, after_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id, record_key, action, reason_code, detail,
                json.dumps(before, ensure_ascii=False, sort_keys=True) if before is not None else None,
                json.dumps(after, ensure_ascii=False, sort_keys=True) if after is not None else None,
            ),
        )
        self.conn.commit()

    def finish_reprocess_run(
        self, run_id: int, status: str, affected: int, unchanged: int, quarantined: int,
        summary: dict[str, Any],
    ) -> None:
        self.conn.execute(
            """UPDATE reprocess_runs SET status=?, affected=?, unchanged=?, quarantined=?,
               summary_json=?, finished_at=? WHERE run_id=?""",
            (status, affected, unchanged, quarantined,
             json.dumps(summary, ensure_ascii=False, sort_keys=True), self._now(), run_id),
        )
        self.conn.commit()

    def list_reprocess_runs(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM reprocess_runs ORDER BY run_id"))

    def list_reprocess_items(self, run_id: int) -> list[sqlite3.Row]:
        return list(
            self.conn.execute("SELECT * FROM reprocess_run_items WHERE run_id=? ORDER BY item_id", (run_id,))
        )

    # --- batch finalization / counts ------------------------------------------

    def finish_batch(self, batch_id: int, status: str, processed: int, accepted: int, rejected: int, error: str | None = None) -> None:
        self.conn.execute(
            "UPDATE batches SET status=?, processed=?, accepted=?, rejected=?, error=?, finished_at=? WHERE batch_id=?",
            (status, processed, accepted, rejected, error, self._now(), batch_id),
        )
        self.conn.commit()

    def counts(self) -> dict[str, int]:
        return {
            "records": int(self.conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]),
            "rejected": int(self.conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]),
            "quarantined": int(self.conn.execute("SELECT COUNT(*) FROM quarantine_records").fetchone()[0]),
            "batches": int(self.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]),
            "raw_manifests": int(self.conn.execute("SELECT COUNT(*) FROM raw_manifests").fetchone()[0]),
        }

    def close(self) -> None:
        self.conn.close()
