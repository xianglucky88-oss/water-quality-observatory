from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .adjudication import AdjudicationConfig
from .decisions import adjudicate_subject
from .identity import subject_key
from .models import DOMAIN_RECORD_TYPE, WaterQualityRecord, WithdrawalDirective


class DataStore:
    """Owns structured tables, idempotent writes, rejection records, batch state
    and the traceable revision-adjudication evidence ledger.

    All adjudication writes happen inside one ``BEGIN IMMEDIATE`` transaction per
    input, so a crash can never leave a half-written logical version. Replaying the
    same input is a no-op thanks to content-addressed primary keys.
    """

    def __init__(self, database: str) -> None:
        # Autocommit isolation: every multi-statement mutation is wrapped in an
        # explicit transaction with a clearly defined atomic boundary.
        self.conn = sqlite3.connect(database, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_schema()
        self._migrate_schema()

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
        CREATE TABLE IF NOT EXISTS batches (
            batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            processed INTEGER NOT NULL DEFAULT 0,
            accepted INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            manifest_digest TEXT
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
        CREATE TABLE IF NOT EXISTS rejected_records (
            rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            source TEXT,
            error TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
        );

        -- Append-only evidence ledger: one row per distinct report ever received.
        CREATE TABLE IF NOT EXISTS report_evidence (
            report_hash TEXT PRIMARY KEY,
            record_key TEXT NOT NULL,
            subject_key TEXT NOT NULL,
            source TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            station_id TEXT NOT NULL,
            sample_id TEXT NOT NULL,
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            unit TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            batch_id INTEGER NOT NULL,
            batch_sequence INTEGER NOT NULL,
            arrival_order INTEGER NOT NULL UNIQUE,
            first_seen_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
        );
        CREATE INDEX IF NOT EXISTS idx_evidence_subject ON report_evidence(subject_key);
        CREATE INDEX IF NOT EXISTS idx_evidence_record ON report_evidence(record_key);

        -- Withdrawal instructions are themselves retained as immutable evidence.
        CREATE TABLE IF NOT EXISTS withdrawal_directives (
            directive_hash TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            target_record_key TEXT NOT NULL,
            subject_key TEXT NOT NULL,
            batch_id INTEGER NOT NULL,
            observed_at TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
        );
        CREATE INDEX IF NOT EXISTS idx_withdrawal_target ON withdrawal_directives(target_record_key);

        -- Derived, fully recomputable current state per report.
        CREATE TABLE IF NOT EXISTS report_states (
            report_hash TEXT PRIMARY KEY,
            record_key TEXT NOT NULL,
            subject_key TEXT NOT NULL,
            state TEXT NOT NULL,
            reason TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(report_hash) REFERENCES report_evidence(report_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_report_state ON report_states(subject_key, state);

        -- Derived subject-level regulatory status.
        CREATE TABLE IF NOT EXISTS subject_states (
            subject_key TEXT PRIMARY KEY,
            station_id TEXT NOT NULL,
            sample_id TEXT NOT NULL,
            metric TEXT NOT NULL,
            state TEXT NOT NULL,
            reason TEXT NOT NULL,
            confirmed_report_hash TEXT,
            updated_at TEXT NOT NULL
        );

        -- Append-only audit trail of every derived-state transition.
        CREATE TABLE IF NOT EXISTS state_transitions (
            transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_key TEXT NOT NULL,
            report_hash TEXT,
            scope TEXT NOT NULL,
            previous_state TEXT,
            new_state TEXT NOT NULL,
            reason TEXT NOT NULL,
            batch_id INTEGER,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_transitions_subject ON state_transitions(subject_key);
        """
        )

    def _migrate_schema(self) -> None:
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(batches)")}
        if "manifest_digest" not in columns:
            self.conn.execute("ALTER TABLE batches ADD COLUMN manifest_digest TEXT")

    @staticmethod
    def _now() -> str:
        # Wall-clock time is audit metadata only; it never drives adjudication.
        return datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------ batches
    def start_batch(self, source: str) -> int:
        cursor = self.conn.execute(
            "INSERT INTO batches(source, status, started_at) VALUES (?, 'running', ?)",
            (source, self._now()),
        )
        return int(cursor.lastrowid)

    def attach_manifest(self, batch_id: int, digest: str) -> None:
        self.conn.execute("UPDATE batches SET manifest_digest=? WHERE batch_id=?", (digest, batch_id))

    def save_manifest(self, digest: str, source: str, content_type: str, byte_count: int, path: str | None) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO raw_manifests(digest, source, content_type, byte_count, path, captured_at) VALUES (?, ?, ?, ?, ?, ?)",
            (digest, source, content_type, byte_count, path, self._now()),
        )

    def upsert(self, record: DOMAIN_RECORD_TYPE) -> bool:
        """Legacy structured-table upsert, kept for the original ingestion path."""
        now = self._now()
        cursor = self.conn.execute(
            """INSERT INTO records(record_key, source, observed_at, payload_json, raw_hash, first_seen_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(record_key) DO UPDATE SET source=excluded.source, observed_at=excluded.observed_at,
                 payload_json=excluded.payload_json, raw_hash=excluded.raw_hash, updated_at=?
               WHERE records.raw_hash <> excluded.raw_hash""",
            (
                record.record_key,
                record.source,
                record.observed_at,
                json.dumps(record.payload, ensure_ascii=False, sort_keys=True),
                record.raw_hash,
                now,
                now,
                now,
            ),
        )
        return cursor.rowcount > 0

    def reject(self, batch_id: int, raw: Any, source: str | None, error: str) -> None:
        self.conn.execute(
            "INSERT INTO rejected_records(batch_id, source, error, raw_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (batch_id, source, error, json.dumps(raw, ensure_ascii=False, sort_keys=True), self._now()),
        )

    def finish_batch(
        self,
        batch_id: int,
        status: str,
        processed: int,
        accepted: int,
        rejected: int,
        error: str | None = None,
    ) -> None:
        self.conn.execute(
            "UPDATE batches SET status=?, processed=?, accepted=?, rejected=?, error=?, finished_at=? WHERE batch_id=?",
            (status, processed, accepted, rejected, error, self._now(), batch_id),
        )

    # ------------------------------------------------------------- adjudication
    def _next_arrival_order(self) -> int:
        row = self.conn.execute("SELECT COALESCE(MAX(arrival_order), 0) + 1 AS next_order FROM report_evidence").fetchone()
        return int(row["next_order"])

    def _subject_evidence(self, skey: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM report_evidence WHERE subject_key=? ORDER BY arrival_order",
                (skey,),
            )
        )

    def _withdrawn_keys(self, skey: str) -> frozenset[str]:
        rows = self.conn.execute(
            "SELECT target_record_key FROM withdrawal_directives WHERE subject_key=?",
            (skey,),
        ).fetchall()
        return frozenset(row["target_record_key"] for row in rows)

    def _recompute_subject(self, skey: str, config: AdjudicationConfig, batch_id: int) -> None:
        """Deterministically recompute and persist all derived state for one subject.

        Must be called inside an open transaction. State transitions are append-only;
        report_states/subject_states only change when the derived state actually
        changes, so replays write nothing new.
        """
        evidence = self._subject_evidence(skey)
        withdrawn = self._withdrawn_keys(skey)
        result = adjudicate_subject([dict(row) for row in evidence], withdrawn, config)
        now = self._now()

        existing_states = {
            row["report_hash"]: row
            for row in self.conn.execute("SELECT * FROM report_states WHERE subject_key=?", (skey,))
        }

        for report in evidence:
            report_hash = report["report_hash"]
            decision = result["reports"][report_hash]
            previous = existing_states.get(report_hash)
            if previous is None:
                self.conn.execute(
                    "INSERT INTO report_states(report_hash, record_key, subject_key, state, reason, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (report_hash, report["record_key"], skey, decision["state"], decision["reason"], now),
                )
                self._log_transition(skey, report_hash, "report", None, decision["state"], decision["reason"], batch_id)
            elif previous["state"] != decision["state"] or previous["reason"] != decision["reason"]:
                self.conn.execute(
                    "UPDATE report_states SET state=?, reason=?, updated_at=? WHERE report_hash=?",
                    (decision["state"], decision["reason"], now, report_hash),
                )
                self._log_transition(
                    skey,
                    report_hash,
                    "report",
                    previous["state"],
                    decision["state"],
                    decision["reason"],
                    batch_id,
                )

        # Evidence is append-only; every state row still corresponds to stored evidence,
        # and withdrawn reports remain queryable through both tables and the transition log.

        first = evidence[0] if evidence else None
        confirmed_hash = None
        if result["subject_state"] == "confirmed":
            winner_row = self.conn.execute(
                """SELECT re.report_hash FROM report_evidence re
                   JOIN report_states rs ON rs.report_hash = re.report_hash
                   WHERE re.subject_key=? AND rs.state='confirmed' LIMIT 1""",
                (skey,),
            ).fetchone()
            confirmed_hash = winner_row["report_hash"] if winner_row else None

        subject_previous = self.conn.execute(
            "SELECT * FROM subject_states WHERE subject_key=?", (skey,)
        ).fetchone()
        if subject_previous is None:
            self.conn.execute(
                """INSERT INTO subject_states(subject_key, station_id, sample_id, metric, state, reason, confirmed_report_hash, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    skey,
                    first["station_id"] if first else "",
                    first["sample_id"] if first else "",
                    first["metric"] if first else "",
                    result["subject_state"],
                    result["subject_reason"],
                    confirmed_hash,
                    now,
                ),
            )
            self._log_transition(
                skey, None, "subject", None, result["subject_state"], result["subject_reason"], batch_id
            )
        elif (
            subject_previous["state"] != result["subject_state"]
            or subject_previous["reason"] != result["subject_reason"]
            or subject_previous["confirmed_report_hash"] != confirmed_hash
        ):
            self.conn.execute(
                """UPDATE subject_states SET state=?, reason=?, confirmed_report_hash=?, updated_at=?
                   WHERE subject_key=?""",
                (result["subject_state"], result["subject_reason"], confirmed_hash, now, skey),
            )
            self._log_transition(
                skey,
                None,
                "subject",
                subject_previous["state"],
                result["subject_state"],
                result["subject_reason"],
                batch_id,
            )

    def _log_transition(
        self,
        skey: str,
        report_hash: str | None,
        scope: str,
        previous_state: str | None,
        new_state: str,
        reason: str,
        batch_id: int,
    ) -> None:
        self.conn.execute(
            """INSERT INTO state_transitions(subject_key, report_hash, scope, previous_state, new_state, reason, batch_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (skey, report_hash, scope, previous_state, new_state, reason, batch_id, self._now()),
        )

    def ingest_report(
        self, record: WaterQualityRecord, batch_id: int, batch_sequence: int, config: AdjudicationConfig
    ) -> dict[str, bool]:
        """Atomically land one report and adjudicate its subject.

        Returns ``{"evidence_added": bool, "changed": bool}``. Re-delivery of the
        same content (same report hash) is an idempotent no-op that never creates a
        second logical version.
        """
        skey = subject_key(record.station_id, record.sample_id, record.metric)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            legacy_changed = self.upsert(record)
            existing = self.conn.execute(
                "SELECT 1 FROM report_evidence WHERE report_hash=?", (record.raw_hash,)
            ).fetchone()
            evidence_added = existing is None
            if evidence_added:
                self.conn.execute(
                    """INSERT INTO report_evidence(report_hash, record_key, subject_key, source, observed_at,
                         station_id, sample_id, metric, value, unit, quality_flag, content_hash,
                         batch_id, batch_sequence, arrival_order, first_seen_at, payload_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        record.raw_hash,
                        record.record_key,
                        skey,
                        record.source,
                        record.observed_at,
                        record.station_id,
                        record.sample_id,
                        record.metric,
                        record.value,
                        record.unit,
                        record.quality_flag,
                        record.content_hash,
                        batch_id,
                        batch_sequence,
                        self._next_arrival_order(),
                        self._now(),
                        json.dumps(record.payload, ensure_ascii=False, sort_keys=True),
                    ),
                )
            transitions_before = self._transition_count()
            self._recompute_subject(skey, config, batch_id)
            transitions_after = self._transition_count()
            self.conn.commit()
            return {
                "evidence_added": evidence_added,
                "changed": bool(legacy_changed or evidence_added or transitions_after > transitions_before),
            }
        except Exception:
            self.conn.rollback()
            raise

    def ingest_withdrawal(
        self, directive: WithdrawalDirective, batch_id: int, config: AdjudicationConfig
    ) -> dict[str, bool]:
        """Atomically record a withdrawal directive and re-adjudicate the subject.

        The target report must exist and belong to the withdrawing source; otherwise
        the directive is rejected (nothing is written). Re-delivery of the same
        directive is an idempotent no-op.
        """
        skey = subject_key(directive.station_id, directive.sample_id, directive.metric)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            target = self.conn.execute(
                "SELECT source FROM report_evidence WHERE record_key=?",
                (directive.target_record_key,),
            ).fetchone()
            if target is None:
                self.conn.rollback()
                raise ValueError(
                    f"withdrawal target not found: {directive.target_record_key}"
                )
            if target["source"] != directive.source:
                self.conn.rollback()
                raise ValueError("a source can only withdraw its own reports")

            existing = self.conn.execute(
                "SELECT 1 FROM withdrawal_directives WHERE directive_hash=?",
                (directive.directive_hash,),
            ).fetchone()
            directive_added = existing is None
            if directive_added:
                self.conn.execute(
                    """INSERT INTO withdrawal_directives(directive_hash, source, target_record_key, subject_key,
                         batch_id, observed_at, first_seen_at, payload_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        directive.directive_hash,
                        directive.source,
                        directive.target_record_key,
                        skey,
                        batch_id,
                        directive.observed_at,
                        self._now(),
                        json.dumps(directive.payload, ensure_ascii=False, sort_keys=True),
                    ),
                )
            transitions_before = self._transition_count()
            self._recompute_subject(skey, config, batch_id)
            transitions_after = self._transition_count()
            self.conn.commit()
            return {
                "evidence_added": directive_added,
                "changed": bool(directive_added or transitions_after > transitions_before),
            }
        except Exception:
            self.conn.rollback()
            raise

    def _transition_count(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM state_transitions").fetchone()[0])

    # ----------------------------------------------------------------- queries
    def counts(self) -> dict[str, int]:
        # Totals across the whole database. Prefixed keys so they cannot collide
        # with per-batch counters (processed/accepted/rejected) in pipeline results.
        return {
            "total_records": int(self.conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]),
            "total_rejected": int(self.conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]),
            "total_batches": int(self.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]),
            "total_raw_manifests": int(self.conn.execute("SELECT COUNT(*) FROM raw_manifests").fetchone()[0]),
            "report_evidence": int(self.conn.execute("SELECT COUNT(*) FROM report_evidence").fetchone()[0]),
            "withdrawals": int(self.conn.execute("SELECT COUNT(*) FROM withdrawal_directives").fetchone()[0]),
            "state_transitions": int(self.conn.execute("SELECT COUNT(*) FROM state_transitions").fetchone()[0]),
        }

    def close(self) -> None:
        self.conn.close()
