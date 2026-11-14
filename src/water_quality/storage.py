from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .errors import SettlementError
from .models import DOMAIN_RECORD_TYPE

_TERMINAL_STATUSES = {"settled", "unrecoverable"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _expiry(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(seconds, 0.0))).isoformat(timespec="microseconds")


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


class DataStore:
    """Owns structured tables, idempotent writes, raw evidence, batch ledgers and settlement."""

    def __init__(self, database: str, lease_seconds: float = 60.0) -> None:
        self.lease_seconds = max(float(lease_seconds), 0.0)
        self.conn = sqlite3.connect(database, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._ensure_schema()

    # ------------------------------------------------------------------ schema
    def _ensure_schema(self) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS batches (
            batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            digest TEXT,
            content_type TEXT,
            item_count INTEGER NOT NULL DEFAULT 0,
            processed INTEGER NOT NULL DEFAULT 0,
            accepted INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,
            changed INTEGER NOT NULL DEFAULT 0,
            attempt INTEGER NOT NULL DEFAULT 1,
            recovery_count INTEGER NOT NULL DEFAULT 0,
            lease_token TEXT,
            lease_owner TEXT,
            lease_expires_at TEXT,
            heartbeat_at TEXT,
            error TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            settled_at TEXT
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
            updated_at TEXT NOT NULL,
            first_batch_id INTEGER
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
        CREATE TABLE IF NOT EXISTS batch_inputs (
            batch_id INTEGER PRIMARY KEY,
            records_json TEXT NOT NULL,
            input_digest TEXT NOT NULL,
            item_count INTEGER NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
        );
        CREATE TABLE IF NOT EXISTS batch_items (
            batch_id INTEGER NOT NULL,
            item_index INTEGER NOT NULL,
            outcome TEXT NOT NULL,
            record_key TEXT,
            changed INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY(batch_id, item_index),
            FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
        );
        """)
        self._migrate()
        self.conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_batches_digest ON batches(digest);
        CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status);
        CREATE INDEX IF NOT EXISTS idx_rejected_batch ON rejected_records(batch_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_rejected_batch_item ON rejected_records(batch_id, item_index);
        CREATE INDEX IF NOT EXISTS idx_batch_items_outcome ON batch_items(batch_id, outcome);
        """)
        self.conn.commit()

    def _migrate(self) -> None:
        """Additive migration so databases written by earlier releases stay readable."""
        self._add_columns(
            "batches",
            {
                "digest": "TEXT",
                "content_type": "TEXT",
                "item_count": "INTEGER NOT NULL DEFAULT 0",
                "changed": "INTEGER NOT NULL DEFAULT 0",
                "attempt": "INTEGER NOT NULL DEFAULT 1",
                "recovery_count": "INTEGER NOT NULL DEFAULT 0",
                "lease_token": "TEXT",
                "lease_owner": "TEXT",
                "lease_expires_at": "TEXT",
                "heartbeat_at": "TEXT",
                "settled_at": "TEXT",
            },
        )
        self._add_columns("records", {"first_batch_id": "INTEGER"})
        self._add_columns("rejected_records", {"item_index": "INTEGER"})
        # Older releases used "completed". Preserve the historical outcome and
        # let reconciliation report the missing evidence for those rows.
        self.conn.execute("UPDATE batches SET status='settled' WHERE status='completed'")
        self.conn.execute("UPDATE batches SET status='running' WHERE status NOT IN ('running','finalizing','failed','settled','unrecoverable')")
        self.conn.commit()

    def _add_columns(self, table: str, columns: dict[str, str]) -> None:
        existing = {row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")}
        for name, ddl in columns.items():
            if name not in existing:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

    # ----------------------------------------------------------------- batches
    @staticmethod
    def _owner() -> str:
        return f"pid-{uuid.uuid4().hex[:12]}"

    def start_batch(
        self,
        source: str,
        *,
        digest: str | None = None,
        content_type: str = "application/json",
        byte_count: int = 0,
        raw_path: str | None = None,
        records: list[Any] | None = None,
        owner: str | None = None,
    ) -> int:
        """Register evidence, input snapshot and a running batch in one transaction."""
        now = _now()
        token = uuid.uuid4().hex
        records_json = json.dumps(records, ensure_ascii=False, sort_keys=True, default=str) if records is not None else None
        item_count = len(records) if records is not None else 0
        input_digest = hashlib.sha256(records_json.encode("utf-8")).hexdigest() if records_json is not None else None
        status = "finalizing" if records is not None and item_count == 0 else "running"
        with self.conn:
            if digest is not None:
                self.conn.execute(
                    "INSERT OR IGNORE INTO raw_manifests(digest, source, content_type, byte_count, path, captured_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (digest, source, content_type, int(byte_count), raw_path, now),
                )
            cursor = self.conn.execute(
                """INSERT INTO batches(source, status, digest, content_type, item_count, attempt, recovery_count,
                                        lease_token, lease_owner, lease_expires_at, heartbeat_at, started_at)
                   VALUES (?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?)""",
                (source, status, digest, content_type, item_count, token, owner or self._owner(), _expiry(self.lease_seconds), now, now),
            )
            batch_id = int(cursor.lastrowid)
            if records_json is not None:
                self.conn.execute(
                    "INSERT INTO batch_inputs(batch_id, records_json, input_digest, item_count) VALUES (?, ?, ?, ?)",
                    (batch_id, records_json, input_digest, item_count),
                )
        return batch_id

    def get_batch(self, batch_id: int) -> dict[str, Any] | None:
        return _row_to_dict(self.conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone())

    def batches_by_digest(self, digest: str) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM batches WHERE digest=? ORDER BY batch_id", (digest,)).fetchall()
        return [dict(row) for row in rows]

    def manifest(self, digest: str) -> dict[str, Any] | None:
        return _row_to_dict(self.conn.execute("SELECT * FROM raw_manifests WHERE digest=?", (digest,)).fetchone())

    def save_manifest(self, digest: str, source: str, content_type: str, byte_count: int, path: str | None) -> None:
        """Register evidence without a batch; use ``start_batch`` for the atomic path."""
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO raw_manifests(digest, source, content_type, byte_count, path, captured_at) VALUES (?, ?, ?, ?, ?, ?)",
                (digest, source, content_type, int(byte_count), path, _now()),
            )

    def input_snapshot(self, batch_id: int) -> dict[str, Any] | None:
        return _row_to_dict(self.conn.execute("SELECT * FROM batch_inputs WHERE batch_id=?", (batch_id,)).fetchone())

    def input_records(self, batch_id: int) -> list[Any]:
        row = self.input_snapshot(batch_id)
        if row is None:
            raise SettlementError("batch input snapshot is missing", {"batch_id": batch_id})
        return json.loads(row["records_json"])

    def ledger(self, batch_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT item_index, outcome, record_key, changed, error, created_at FROM batch_items WHERE batch_id=? ORDER BY item_index",
            (batch_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def rejections(self, batch_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT rejection_id, item_index, source, error, raw_json, created_at FROM rejected_records WHERE batch_id=? ORDER BY rejection_id",
            (batch_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def ledger_stats(self, batch_id: int) -> dict[str, int]:
        row = self.conn.execute(
            """SELECT COUNT(*) AS items,
                      COALESCE(SUM(CASE WHEN outcome='accepted' THEN 1 ELSE 0 END), 0) AS accepted,
                      COALESCE(SUM(CASE WHEN outcome='rejected' THEN 1 ELSE 0 END), 0) AS rejected,
                      COALESCE(SUM(CASE WHEN outcome='accepted' AND changed=1 THEN 1 ELSE 0 END), 0) AS changed
               FROM batch_items WHERE batch_id=?""",
            (batch_id,),
        ).fetchone()
        return {key: int(row[key]) for key in ("items", "accepted", "rejected", "changed")}

    # ---------------------------------------------------------------- item work
    def _upsert_record(self, record: DOMAIN_RECORD_TYPE, batch_id: int) -> bool:
        now = _now()
        cursor = self.conn.execute(
            """INSERT INTO records(record_key, source, observed_at, payload_json, raw_hash, first_seen_at, updated_at, first_batch_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(record_key) DO UPDATE SET source=excluded.source, observed_at=excluded.observed_at,
                 payload_json=excluded.payload_json, raw_hash=excluded.raw_hash, updated_at=excluded.updated_at
               WHERE records.raw_hash <> excluded.raw_hash""",
            (
                record.record_key,
                record.source,
                record.observed_at,
                json.dumps(record.payload, ensure_ascii=False, sort_keys=True),
                record.raw_hash,
                now,
                now,
                batch_id,
            ),
        )
        return cursor.rowcount > 0

    def _maybe_finalize(self, batch_id: int) -> None:
        batch = self.conn.execute("SELECT item_count, status FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if batch is None:
            raise SettlementError("batch disappeared while recording an item", {"batch_id": batch_id})
        count = int(self.conn.execute("SELECT COUNT(*) FROM batch_items WHERE batch_id=?", (batch_id,)).fetchone()[0])
        if count >= int(batch["item_count"]) and batch["status"] not in _TERMINAL_STATUSES:
            self.conn.execute("UPDATE batches SET status='finalizing' WHERE batch_id=?", (batch_id,))

    def _heartbeat(self, batch_id: int) -> None:
        self.conn.execute(
            "UPDATE batches SET heartbeat_at=?, lease_expires_at=? WHERE batch_id=? AND status NOT IN ('settled','unrecoverable')",
            (_now(), _expiry(self.lease_seconds), batch_id),
        )

    def accept_item(self, batch_id: int, item_index: int, record: DOMAIN_RECORD_TYPE) -> bool:
        """Persist a structured record and its ledger entry atomically."""
        with self.conn:
            changed = self._upsert_record(record, batch_id)
            self.conn.execute(
                "INSERT INTO batch_items(batch_id, item_index, outcome, record_key, changed, created_at) VALUES (?, ?, 'accepted', ?, ?, ?)",
                (batch_id, item_index, record.record_key, int(changed), _now()),
            )
            self._maybe_finalize(batch_id)
            self._heartbeat(batch_id)
        return changed

    def reject_item(self, batch_id: int, item_index: int, raw: Any, source: str | None, error: str) -> None:
        """Persist a rejection and its ledger entry atomically."""
        with self.conn:
            self.conn.execute(
                "INSERT INTO rejected_records(batch_id, source, error, raw_json, created_at, item_index) VALUES (?, ?, ?, ?, ?, ?)",
                (batch_id, source, error, json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str), _now(), item_index),
            )
            self.conn.execute(
                "INSERT INTO batch_items(batch_id, item_index, outcome, changed, error, created_at) VALUES (?, ?, 'rejected', 0, ?, ?)",
                (batch_id, item_index, error, _now()),
            )
            self._maybe_finalize(batch_id)
            self._heartbeat(batch_id)

    # ------------------------------------------------------------------ settle
    def ledger_problems(self, batch_id: int) -> list[str]:
        """Return human-readable inconsistencies between ledger and durable rows."""
        batch = self.conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if batch is None:
            return ["batch not found"]
        stats = self.ledger_stats(batch_id)
        expected = int(batch["item_count"])
        problems: list[str] = []
        if stats["items"] != expected:
            problems.append(f"ledger has {stats['items']} items but {expected} were registered")
        rejection_rows = self.conn.execute(
            "SELECT item_index, error FROM rejected_records WHERE batch_id=?", (batch_id,)
        ).fetchall()
        ledger_rejections = {
            item["item_index"]: item["error"] for item in self.ledger(batch_id) if item["outcome"] == "rejected"
        }
        if len(rejection_rows) != stats["rejected"]:
            problems.append(
                f"rejection table has {len(rejection_rows)} rows but ledger has {stats['rejected']} rejections"
            )
        for row in rejection_rows:
            if row["item_index"] not in ledger_rejections:
                problems.append(f"rejection row for item {row['item_index']} has no ledger entry")
            elif ledger_rejections[row["item_index"]] != row["error"]:
                problems.append(f"rejection row for item {row['item_index']} has a different reason")
        missing = self.conn.execute(
            """SELECT COUNT(*) FROM batch_items bi
               LEFT JOIN records r ON r.record_key = bi.record_key
               WHERE bi.batch_id=? AND bi.outcome='accepted' AND r.record_key IS NULL""",
            (batch_id,),
        ).fetchone()[0]
        if int(missing) != 0:
            problems.append(f"{int(missing)} accepted ledger entries have no structured record")
        if batch["status"] == "settled":
            summary = (int(batch["processed"]), int(batch["accepted"]), int(batch["rejected"]), int(batch["changed"]))
            if summary != (stats["items"], stats["accepted"], stats["rejected"], stats["changed"]):
                problems.append("settled batch summary does not match the ledger")
        return problems

    def settle_batch(self, batch_id: int) -> dict[str, int]:
        """Verify ledger/evidence consistency and mark the batch settled in one transaction."""
        with self.conn:
            batch = self.conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if batch is None:
                raise SettlementError("batch not found", {"batch_id": batch_id})
            problems = self.ledger_problems(batch_id)
            if problems:
                raise SettlementError("; ".join(problems), {"batch_id": batch_id, "problems": problems})
            stats = self.ledger_stats(batch_id)
            if batch["status"] == "settled":
                return stats
            now = _now()
            self.conn.execute(
                """UPDATE batches SET status='settled', processed=?, accepted=?, rejected=?, changed=?, error=NULL,
                   finished_at=?, settled_at=?, lease_token=NULL, lease_owner=NULL, lease_expires_at=NULL
                   WHERE batch_id=?""",
                (stats["items"], stats["accepted"], stats["rejected"], stats["changed"], now, now, batch_id),
            )
        return stats

    def fail_batch(self, batch_id: int, error: str) -> None:
        """Record an unexpected failure, keeping the ledger for later recovery."""
        stats = self.ledger_stats(batch_id)
        with self.conn:
            self.conn.execute(
                """UPDATE batches SET status='failed', processed=?, accepted=?, rejected=?, changed=?, error=?,
                   lease_token=NULL, lease_owner=NULL, lease_expires_at=NULL
                   WHERE batch_id=? AND status NOT IN ('settled','unrecoverable')""",
                (stats["items"], stats["accepted"], stats["rejected"], stats["changed"], error, batch_id),
            )

    def claim_batch(self, batch_id: int, owner: str | None = None) -> bool:
        """Atomically take an expired/absent lease; return False when a live worker owns it."""
        now = _now()
        with self.conn:
            cursor = self.conn.execute(
                """UPDATE batches SET status='running', attempt=attempt+1, recovery_count=recovery_count+1,
                   lease_token=?, lease_owner=?, lease_expires_at=?, heartbeat_at=?
                   WHERE batch_id=? AND status NOT IN ('settled','unrecoverable')
                     AND (lease_expires_at IS NULL OR lease_expires_at <= ?)""",
                (uuid.uuid4().hex, owner or self._owner(), _expiry(self.lease_seconds), now, batch_id, now),
            )
        return cursor.rowcount > 0

    def steal_batch(self, batch_id: int, owner: str | None = None) -> bool:
        """Take the lease regardless of its expiry; used only by explicit recovery."""
        now = _now()
        with self.conn:
            cursor = self.conn.execute(
                """UPDATE batches SET status='running', attempt=attempt+1, recovery_count=recovery_count+1,
                   lease_token=?, lease_owner=?, lease_expires_at=?, heartbeat_at=?
                   WHERE batch_id=? AND status NOT IN ('settled','unrecoverable')""",
                (uuid.uuid4().hex, owner or self._owner(), _expiry(self.lease_seconds), now, batch_id),
            )
        return cursor.rowcount > 0

    def mark_unrecoverable(self, batch_id: int, error: str) -> None:
        with self.conn:
            self.conn.execute(
                """UPDATE batches SET status='unrecoverable', error=?, lease_token=NULL, lease_owner=NULL,
                   lease_expires_at=NULL, finished_at=?
                   WHERE batch_id=? AND status NOT IN ('settled','unrecoverable')""",
                (error, _now(), batch_id),
            )

    def counts(self) -> dict[str, int]:
        return {
            "records": int(self.conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]),
            "total_rejected": int(self.conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]),
            "batches": int(self.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]),
            "raw_manifests": int(self.conn.execute("SELECT COUNT(*) FROM raw_manifests").fetchone()[0]),
        }

    def close(self) -> None:
        self.conn.close()
