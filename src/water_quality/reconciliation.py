from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import (
    AmbiguousBatchError,
    BatchInProgressError,
    EvidenceError,
    InjectedFault,
    SettlementError,
    UnrecoverableBatchError,
    describe_error,
)
from .faults import FaultInjector
from .normalize import normalize
from .pipeline import IngestionPipeline
from .storage import DataStore

# Canonical states exposed by inspection and recovery reports.
NEVER_STARTED = "never_started"
IN_PROGRESS = "in_progress"
RETRYABLE = "retryable"
READY_TO_SETTLE = "ready_to_settle"
COMPLETED = "completed"
UNRECOVERABLE = "unrecoverable"

_ACTIVE = {"running", "finalizing", "failed"}


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _evidence_report(store: DataStore, batch: dict[str, Any], raw_dir: str | None) -> dict[str, Any]:
    digest = batch.get("digest")
    snapshot = store.input_snapshot(int(batch["batch_id"]))
    manifest = store.manifest(digest) if digest else None
    problems: list[str] = []

    if not digest:
        problems.append("batch has no registered evidence digest")
    if manifest is None:
        problems.append("raw manifest is missing")
    if snapshot is None:
        problems.append("input snapshot is missing")

    snapshot_matches = False
    if snapshot is not None:
        try:
            records = json.loads(snapshot["records_json"])
        except (TypeError, ValueError):
            records = None
            problems.append("input snapshot is not valid JSON")
        if records is not None:
            recomputed = hashlib.sha256(
                json.dumps(records, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            snapshot_matches = recomputed == snapshot["input_digest"]
            if not snapshot_matches:
                problems.append("input snapshot digest does not match its content")
            if int(snapshot["item_count"]) != len(records):
                problems.append("input snapshot item count is inconsistent")
            if int(snapshot["item_count"]) != int(batch["item_count"]):
                problems.append("batch item count does not match the input snapshot")

    raw_file_present: bool | None = None
    raw_file_matches: bool | None = None
    if raw_dir is not None and digest:
        path = Path(raw_dir) / f"{digest}.json"
        raw_file_present = path.is_file()
        if raw_file_present:
            raw_file_matches = hashlib.sha256(path.read_bytes()).hexdigest() == digest
            if not raw_file_matches:
                problems.append("raw file content does not match the digest")
            if manifest is not None and int(manifest["byte_count"]) != path.stat().st_size:
                problems.append("raw file size does not match the manifest")
        else:
            problems.append("raw file is missing from the landing directory")

    return {
        "digest": digest,
        "manifest_present": manifest is not None,
        "snapshot_present": snapshot is not None,
        "snapshot_matches": snapshot_matches,
        "raw_file_present": raw_file_present,
        "raw_file_matches": raw_file_matches,
        "byte_count": int(manifest["byte_count"]) if manifest is not None else None,
        "input_digest": snapshot["input_digest"] if snapshot is not None else None,
        "problems": problems,
        "ok": not problems,
    }


def _counts(store: DataStore, batch: dict[str, Any]) -> dict[str, Any]:
    stats = store.ledger_stats(int(batch["batch_id"]))
    return {
        "item_count": int(batch["item_count"]),
        "ledger_items": stats["items"],
        "accepted": stats["accepted"],
        "rejected": stats["rejected"],
        "changed": stats["changed"],
        "batch_processed": int(batch["processed"]),
        "batch_accepted": int(batch["accepted"]),
        "batch_rejected": int(batch["rejected"]),
        "batch_changed": int(batch["changed"]),
    }


def _record_exists(store: DataStore, record_key: str | None) -> bool:
    if not record_key:
        return False
    row = store.conn.execute("SELECT 1 FROM records WHERE record_key=?", (record_key,)).fetchone()
    return row is not None


def _lease_live(batch: dict[str, Any], now: datetime) -> bool:
    expiry = _parse_time(batch.get("lease_expires_at"))
    return expiry is not None and expiry > now


def _classify(store: DataStore, batch: dict[str, Any], evidence: dict[str, Any], now: datetime) -> tuple[str, str | None, bool]:
    """Return (state, reason, auto_recoverable).

    Evidence is checked before the batch status is trusted so damaged or
    missing raw material can never be reported as a successful completion.
    """
    status = batch["status"]
    batch_id = int(batch["batch_id"])
    stats = store.ledger_stats(batch_id)
    item_count = int(batch["item_count"])
    live = _lease_live(batch, now)

    if status == "unrecoverable":
        return UNRECOVERABLE, batch.get("error") or "batch was marked unrecoverable", False
    if not evidence["ok"]:
        return UNRECOVERABLE, "evidence is incomplete or inconsistent: " + "; ".join(evidence["problems"]), False

    if status == "settled":
        summary = (int(batch["processed"]), int(batch["accepted"]), int(batch["rejected"]), int(batch["changed"]))
        ledger = (stats["items"], stats["accepted"], stats["rejected"], stats["changed"])
        if summary != ledger:
            return UNRECOVERABLE, "settled batch summary does not match its ledger", False
        problems = store.ledger_problems(batch_id)
        if problems:
            return UNRECOVERABLE, "ledger cannot be reconciled: " + "; ".join(problems), False
        return COMPLETED, None, False

    if status not in _ACTIVE:
        return UNRECOVERABLE, f"unknown batch status: {status}", False

    if stats["items"] > item_count:
        return UNRECOVERABLE, "ledger contains more items than the registered input", False
    if stats["items"] < 0 or item_count != len(store.input_records(batch_id)):
        return UNRECOVERABLE, "registered item count does not match the input snapshot", False

    if live:
        return IN_PROGRESS, "another worker holds a live lease", False

    if stats["items"] == item_count:
        problems = store.ledger_problems(batch_id)
        if problems:
            return UNRECOVERABLE, "ledger cannot be reconciled: " + "; ".join(problems), False
        return READY_TO_SETTLE, "all items have durable outcomes; settlement was not completed", True

    if stats["items"] == 0:
        if item_count == 0:
            return READY_TO_SETTLE, "empty input is ready to settle", True
        return NEVER_STARTED, "no item outcome has been recorded", True
    return RETRYABLE, f"{stats['items']} of {item_count} items have durable outcomes", True


def inspect_batch(database: str, batch_id: int, raw_dir: str | None = None, lease_seconds: float = 60.0) -> dict[str, Any]:
    store = DataStore(database, lease_seconds=lease_seconds)
    try:
        batch = store.get_batch(batch_id)
        if batch is None:
            raise SettlementError("batch not found", {"batch_id": batch_id})
        return _report(store, batch, raw_dir)
    finally:
        store.close()


def _report(store: DataStore, batch: dict[str, Any], raw_dir: str | None) -> dict[str, Any]:
    batch_id = int(batch["batch_id"])
    evidence = _evidence_report(store, batch, raw_dir)
    state, reason, recoverable = _classify(store, batch, evidence, datetime.now(timezone.utc))
    ledger = store.ledger(batch_id)
    rejections = store.rejections(batch_id)
    return {
        "batch_id": batch_id,
        "source": batch["source"],
        "status": batch["status"],
        "state": state,
        "auto_recoverable": recoverable,
        "idempotent_resubmit": state in {COMPLETED, NEVER_STARTED, RETRYABLE, READY_TO_SETTLE},
        "reason": reason,
        "counts": _counts(store, batch),
        "evidence": evidence,
        "rejections": [{"item_index": row["item_index"], "error": row["error"]} for row in rejections],
        "pending_items": [
            item["item_index"]
            for item in ledger
            if item["outcome"] == "accepted" and not _record_exists(store, item["record_key"])
        ],
        "last_error": batch.get("error"),
        "attempt": int(batch["attempt"]),
        "recovery_count": int(batch["recovery_count"]),
        "lease": {
            "owner": batch.get("lease_owner"),
            "expires_at": batch.get("lease_expires_at"),
            "live": _lease_live(batch, datetime.now(timezone.utc)),
        },
    }


def find_batch_by_digest(database: str, digest: str, raw_dir: str | None = None, lease_seconds: float = 60.0) -> dict[str, Any]:
    store = DataStore(database, lease_seconds=lease_seconds)
    try:
        matches = store.batches_by_digest(digest)
        if not matches:
            raise SettlementError("no batch registered for digest", {"digest": digest})
        if len(matches) > 1:
            settled = [batch for batch in matches if batch["status"] == "settled"]
            if len(settled) == 1:
                return _report(store, settled[0], raw_dir)
            raise AmbiguousBatchError(
                "digest maps to several unsettled batches",
                {"digest": digest, "batch_ids": [int(batch["batch_id"]) for batch in matches]},
            )
        return _report(store, matches[0], raw_dir)
    finally:
        store.close()


def list_batches(database: str, raw_dir: str | None = None, lease_seconds: float = 60.0) -> list[dict[str, Any]]:
    store = DataStore(database, lease_seconds=lease_seconds)
    try:
        rows = store.conn.execute("SELECT * FROM batches ORDER BY batch_id").fetchall()
        return [_report(store, dict(row), raw_dir) for row in rows]
    finally:
        store.close()


def list_orphan_evidence(database: str, raw_dir: str | None = None) -> list[dict[str, Any]]:
    """Raw files landed in the directory with no batch referencing them."""
    if raw_dir is None:
        raise ValueError("raw_dir is required to inspect landed evidence")
    store = DataStore(database)
    try:
        registered = {row[0] for row in store.conn.execute("SELECT digest FROM batches WHERE digest IS NOT NULL")}
    finally:
        store.close()
    orphans: list[dict[str, Any]] = []
    for path in sorted(Path(raw_dir).glob("*.json")):
        digest = path.name[:-5]
        if digest in registered:
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        orphans.append(
            {
                "digest": digest,
                "path": str(path),
                "byte_count": path.stat().st_size,
                "content_matches_name": actual == digest,
            }
        )
    return orphans


def _assert_input_matches_evidence(store: DataStore, batch: dict[str, Any], records: list[Any]) -> None:
    snapshot = store.input_snapshot(int(batch["batch_id"]))
    if snapshot is None:
        raise EvidenceError("input snapshot is missing", {"batch_id": int(batch["batch_id"])})
    recomputed = hashlib.sha256(
        json.dumps(records, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    if recomputed != snapshot["input_digest"] or len(records) != int(batch["item_count"]):
        raise EvidenceError(
            "input snapshot no longer matches its registered digest",
            {"batch_id": int(batch["batch_id"]), "expected": snapshot["input_digest"], "actual": recomputed},
        )


def resume_batch(
    database: str,
    batch_id: int,
    *,
    raw_dir: str | None = None,
    faults: FaultInjector | None = None,
    force: bool = False,
    lease_seconds: float = 60.0,
) -> dict[str, Any]:
    """Explicitly resume or settle a batch without deleting anything.

    Returns the inspection report augmented with ``recovered`` and ``run``.
    Raises ``UnrecoverableBatchError`` for states that must be handled by a
    human, and ``BatchInProgressError`` when a live lease is not overridden.
    """
    store = DataStore(database, lease_seconds=lease_seconds)
    try:
        return _resume_with_store(store, batch_id, raw_dir, faults, force)
    finally:
        store.close()


def _resume_with_store(
    store: DataStore,
    batch: dict[str, Any] | int,
    raw_dir: str | None,
    faults: FaultInjector | None,
    force: bool,
) -> dict[str, Any]:
    batch_id = int(batch) if isinstance(batch, int) else int(batch["batch_id"])
    faults = faults or FaultInjector()
    batch = store.get_batch(batch_id)
    if batch is None:
        raise SettlementError("batch not found", {"batch_id": batch_id})
    report = _report(store, batch, raw_dir)
    state = report["state"]
    if state == COMPLETED:
        report.update({"recovered": False, "already_settled": True, "run": None})
        return report
    if state == UNRECOVERABLE:
        raise UnrecoverableBatchError(report["reason"] or "batch cannot be recovered automatically", report)
    if state == IN_PROGRESS and not force:
        raise BatchInProgressError(report["reason"] or "batch is in progress", report)

    if not store.claim_batch(batch_id):
        if not force:
            refreshed = store.get_batch(batch_id) or batch
            raise BatchInProgressError("batch lease was taken by another worker", _report(store, refreshed, raw_dir))
        if not store.steal_batch(batch_id):
            refreshed = store.get_batch(batch_id) or batch
            raise UnrecoverableBatchError(
                "batch reached a terminal state during recovery", _report(store, refreshed, raw_dir)
            )
    batch = store.get_batch(batch_id) or batch

    try:
        records = store.input_records(batch_id)
        _assert_input_matches_evidence(store, batch, records)
        result = IngestionPipeline(records, batch["source"], store, batch_id, normalize, faults=faults).run()
    except InjectedFault as exc:
        store.fail_batch(batch_id, describe_error(exc))
        raise
    except EvidenceError as exc:
        store.mark_unrecoverable(batch_id, describe_error(exc))
        raise UnrecoverableBatchError(str(exc), getattr(exc, "report", None)) from exc
    except Exception as exc:
        store.fail_batch(batch_id, describe_error(exc))
        raise

    final = _report(store, store.get_batch(batch_id) or batch, raw_dir)
    final.update({"recovered": True, "already_settled": False, "run": result})
    return final
