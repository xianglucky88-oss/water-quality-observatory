from __future__ import annotations

from typing import Any

from .errors import AmbiguousBatchError, BatchInProgressError, UnrecoverableBatchError, describe_error
from .faults import FaultInjector
from .normalize import normalize
from .pipeline import IngestionPipeline
from .reconciliation import COMPLETED, _resume_with_store
from .sources import FixtureSource, HttpJsonSource, RawLanding, SourceBatch
from .storage import DataStore

_UNSETTLED = {"running", "finalizing", "failed"}


def _select_resume_target(store: DataStore, digest: str, source: str) -> dict[str, Any] | None:
    matches = [batch for batch in store.batches_by_digest(digest) if batch["source"] == source]
    unsettled = [batch for batch in matches if batch["status"] in _UNSETTLED]
    if not unsettled:
        return None
    unsettled.sort(key=lambda batch: int(batch["batch_id"]))
    if len(unsettled) > 1:
        raise AmbiguousBatchError(
            "evidence digest has several unfinished batches; resume one explicitly",
            {"digest": digest, "batch_ids": [int(batch["batch_id"]) for batch in unsettled]},
        )
    return unsettled[0]


def _run(
    batch: SourceBatch,
    database: str,
    raw_dir: str | None,
    *,
    faults: FaultInjector | None = None,
    lease_seconds: float = 60.0,
    resume: bool = True,
    force: bool = False,
) -> dict[str, int]:
    landing = RawLanding(raw_dir)
    digest, raw_path = landing.save(batch)
    store = DataStore(database, lease_seconds=lease_seconds)
    try:
        target = _select_resume_target(store, digest, batch.source) if resume else None
        if target is not None:
            report = _resume_with_store(store, target, raw_dir, faults, force)
            result = report.get("run")
            if result is None:
                # Already settled between inspection and resume.
                result = _counts_result(store, report)
            result.update({"batch_id": int(target["batch_id"]), "state": report["state"], "recovered": True})
            return result

        batch_id = store.start_batch(
            batch.source,
            digest=digest,
            content_type=batch.content_type,
            byte_count=len(batch.raw_body),
            raw_path=raw_path,
            records=batch.records,
        )
        faults = faults or FaultInjector()
        try:
            faults.trigger("after_evidence", {"batch_id": batch_id, "digest": digest})
            result = IngestionPipeline(batch.records, batch.source, store, batch_id, normalize, faults=faults).run()
        except Exception as exc:
            # Release the lease so a resubmission is classified as retryable
            # rather than in progress. Settlement failures keep their ledger.
            store.fail_batch(batch_id, describe_error(exc))
            raise
        result.update({"batch_id": batch_id, "state": COMPLETED, "recovered": False})
        return result
    finally:
        store.close()


def _counts_result(store: DataStore, report: dict[str, Any]) -> dict[str, int]:
    counts = report["counts"]
    result = {
        "processed": counts["ledger_items"],
        "accepted": counts["accepted"],
        "rejected": counts["rejected"],
        "changed": counts["changed"],
        "batch_id": int(report["batch_id"]),
        "state": report["state"],
    }
    result.update(store.counts())
    return result


def run_fixture(
    fixture: str,
    database: str,
    raw_dir: str | None = None,
    *,
    faults: FaultInjector | None = None,
    lease_seconds: float = 60.0,
    resume: bool = True,
    force: bool = False,
) -> dict[str, int]:
    return _run(
        FixtureSource().fetch(fixture),
        database,
        raw_dir,
        faults=faults,
        lease_seconds=lease_seconds,
        resume=resume,
        force=force,
    )


def run_url(
    url: str,
    database: str,
    raw_dir: str | None = None,
    timeout: float = 30.0,
    records_key: str | None = None,
    *,
    faults: FaultInjector | None = None,
    lease_seconds: float = 60.0,
    resume: bool = True,
    force: bool = False,
) -> dict[str, int]:
    return _run(
        HttpJsonSource(url, timeout=timeout, records_key=records_key).fetch(),
        database,
        raw_dir,
        faults=faults,
        lease_seconds=lease_seconds,
        resume=resume,
        force=force,
    )


def recover_batch(
    database: str,
    batch_id: int | None = None,
    *,
    digest: str | None = None,
    raw_dir: str | None = None,
    faults: FaultInjector | None = None,
    lease_seconds: float = 60.0,
    force: bool = False,
) -> dict[str, Any]:
    """Explicit recovery by batch id or by the digest of landed evidence."""
    if (batch_id is None) == (digest is None):
        raise ValueError("provide exactly one of batch_id or digest")
    from .reconciliation import find_batch_by_digest, resume_batch

    if digest is not None:
        report = find_batch_by_digest(database, digest, raw_dir, lease_seconds)
        batch_id = int(report["batch_id"])
    return resume_batch(database, int(batch_id), raw_dir=raw_dir, faults=faults, force=force, lease_seconds=lease_seconds)


def verify_batch(database: str, batch_id: int, raw_dir: str | None = None, lease_seconds: float = 60.0) -> dict[str, Any]:
    from .reconciliation import inspect_batch

    return inspect_batch(database, batch_id, raw_dir, lease_seconds)


__all__ = [
    "run_fixture",
    "run_url",
    "recover_batch",
    "verify_batch",
    "BatchInProgressError",
    "UnrecoverableBatchError",
]
