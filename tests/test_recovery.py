from __future__ import annotations

import multiprocessing
from pathlib import Path

import pytest

from water_quality.application import recover_batch, run_fixture, run_url, verify_batch
from water_quality.errors import (
    AmbiguousBatchError,
    BatchInProgressError,
    CrashSignal,
    InjectedFault,
    SettlementError,
    UnrecoverableBatchError,
)
from water_quality.faults import FaultInjector
from water_quality.models import WaterQualityRecord
from water_quality.reconciliation import (
    COMPLETED,
    IN_PROGRESS,
    NEVER_STARTED,
    READY_TO_SETTLE,
    RETRYABLE,
    UNRECOVERABLE,
)
from water_quality.storage import DataStore

FIXTURE = Path(__file__).parents[1] / "fixtures" / "sample.json"
CHECKPOINTS = ("after_evidence", "after_first_write", "after_rejection", "before_finalize")

# --------------------------------------------------------------- local helpers


def _database(tmp_path: Path, name: str = "data.sqlite3") -> str:
    return str(tmp_path / name)


def _raw(tmp_path: Path) -> str:
    return str(tmp_path / "raw")


def _fixture_copy(tmp_path: Path, records: list[dict], name: str = "input.json") -> str:
    import json

    path = tmp_path / name
    path.write_text(json.dumps(records), encoding="utf-8")
    return str(path)


def _query(database: str, sql: str, params: tuple = ()) -> list[tuple]:
    store = DataStore(database, lease_seconds=0)
    try:
        return [tuple(row) for row in store.conn.execute(sql, params).fetchall()]
    finally:
        store.close()


def _all_batches(database: str, raw_dir: str) -> list[dict]:
    store = DataStore(database, lease_seconds=0)
    try:
        ids = [int(row[0]) for row in store.conn.execute("SELECT batch_id FROM batches ORDER BY batch_id")]
    finally:
        store.close()
    return [verify_batch(database, batch_id, raw_dir, lease_seconds=0) for batch_id in ids]


def _digest(database: str) -> str:
    return _query(database, "SELECT digest FROM raw_manifests")[0][0]


def _assert_batch_consistent(database: str, batch_id: int, raw_dir: str) -> dict:
    """Check structured rows, rejection rows, ledger, summary and evidence agree."""
    report = verify_batch(database, batch_id, raw_dir, lease_seconds=0)
    counts = report["counts"]
    assert report["state"] == COMPLETED, report
    assert counts["ledger_items"] == counts["item_count"]
    assert counts["batch_processed"] == counts["ledger_items"]
    assert counts["batch_accepted"] == counts["accepted"]
    assert counts["batch_rejected"] == counts["rejected"]
    assert counts["batch_changed"] == counts["changed"]
    assert len(report["rejections"]) == counts["rejected"]

    ledger = _query(database, "SELECT item_index, outcome FROM batch_items WHERE batch_id=?", (batch_id,))
    assert len(ledger) == counts["item_count"]
    assert len({index for index, _ in ledger}) == len(ledger)
    assert sum(1 for _, outcome in ledger if outcome == "accepted") == counts["accepted"]
    assert sum(1 for _, outcome in ledger if outcome == "rejected") == counts["rejected"]

    rejected_rows = _query(database, "SELECT item_index, error FROM rejected_records WHERE batch_id=?", (batch_id,))
    assert rejected_rows == [(row["item_index"], row["error"]) for row in report["rejections"]]

    accepted_keys = _query(
        database, "SELECT record_key FROM batch_items WHERE batch_id=? AND outcome='accepted'", (batch_id,)
    )
    for (key,) in accepted_keys:
        assert _query(database, "SELECT record_key FROM records WHERE record_key=?", (key,))
    assert report["pending_items"] == []

    evidence = report["evidence"]
    assert evidence["ok"], evidence
    assert evidence["manifest_present"] and evidence["snapshot_present"] and evidence["snapshot_matches"]
    if raw_dir is not None:
        assert evidence["raw_file_present"] and evidence["raw_file_matches"]
    assert report["last_error"] is None
    return report


# ------------------------------------------------------------------- baseline


def test_fixture_pipeline_is_idempotent(tmp_path):
    database = _database(tmp_path)
    first = run_fixture(str(FIXTURE), database, _raw(tmp_path))
    second = run_fixture(str(FIXTURE), database, _raw(tmp_path))
    assert first["processed"] == 4
    assert first["accepted"] == 3
    assert first["rejected"] == 1
    assert second["changed"] == 0
    assert second["records"] == 3
    assert second["batches"] == 2
    assert second["raw_manifests"] == 1
    assert second["rejected"] == 1
    _assert_batch_consistent(database, first["batch_id"], _raw(tmp_path))


def test_first_batch_records_evidence_and_ledger(tmp_path):
    database = _database(tmp_path)
    result = run_fixture(str(FIXTURE), database, _raw(tmp_path))
    store = DataStore(database, lease_seconds=0)
    try:
        batch = store.get_batch(result["batch_id"])
        snapshot = store.input_snapshot(result["batch_id"])
        manifest = store.manifest(batch["digest"])
    finally:
        store.close()
    assert batch["status"] == "settled"
    assert batch["error"] is None and batch["finished_at"]
    assert batch["processed"] == 4 and batch["accepted"] == 3 and batch["rejected"] == 1
    assert snapshot is not None and snapshot["item_count"] == 4
    assert manifest is not None and manifest["byte_count"] == len(FIXTURE.read_bytes())
    assert len(list(Path(_raw(tmp_path)).glob("*.json"))) == 1


def test_http_source_runs_end_to_end_and_lands_raw(tmp_path):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    class Handler(BaseHTTPRequestHandler):
        body = FIXTURE.read_bytes()

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(self.body)))
            self.end_headers()
            self.wfile.write(self.body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        database = _database(tmp_path, "http.sqlite3")
        result = run_url(f"http://127.0.0.1:{server.server_port}/records", database, _raw(tmp_path))
        assert result["accepted"] == 3
        assert result["rejected"] == 1
        assert len(list((tmp_path / "raw").glob("*.json"))) == 1
        _assert_batch_consistent(database, result["batch_id"], _raw(tmp_path))
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_quality_rejects_invalid_domain_value():
    from water_quality.normalize import normalize

    import json
    from water_quality.quality import validate

    fixture = json.loads(FIXTURE.read_text())
    invalid = fixture[-1]
    try:
        record = normalize(invalid, invalid["source"])
    except ValueError:
        return
    assert validate(record)


# ------------------------------------------------- interruption and recovery


@pytest.mark.parametrize("checkpoint", CHECKPOINTS)
@pytest.mark.parametrize("mode", ("raise", "crash"))
def test_each_checkpoint_is_recoverable_without_duplicates(tmp_path, checkpoint, mode):
    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises((InjectedFault, CrashSignal)) as caught:
        run_fixture(str(FIXTURE), database, raw_dir, faults=FaultInjector(f"{checkpoint}:{mode}"))
    assert caught.type.__name__ in {"InjectedFault", "CrashSignal"}

    batch_id = _query(database, "SELECT MAX(batch_id) FROM batches")[0][0]
    before = verify_batch(database, batch_id, raw_dir, lease_seconds=0)
    expected_state = {
        "after_evidence": NEVER_STARTED,
        "after_first_write": RETRYABLE,
        "after_rejection": RETRYABLE,
        "before_finalize": READY_TO_SETTLE,
    }[checkpoint]
    # Rejections are written after the accepted rows, so an interruption there
    # can already be the everything-but-settlement case.
    allowed = {expected_state}
    if checkpoint == "after_rejection":
        allowed.add(READY_TO_SETTLE)
    if mode == "crash":
        # An abrupt death leaves the durable lease behind; a healthy worker may
        # still own it until the lease expires.
        allowed.add(IN_PROGRESS)
    assert before["state"] in allowed, before
    assert before["evidence"]["ok"]
    ledger_before = before["counts"]["ledger_items"]
    accepted_before = _accepted_count(database, batch_id)
    records_before = {row[0] for row in _query(database, "SELECT record_key FROM records")}

    if mode == "crash" and before["lease"]["live"]:
        with pytest.raises(BatchInProgressError):
            recover_batch(database, batch_id, raw_dir=raw_dir, lease_seconds=0)
    recovered = recover_batch(database, batch_id, raw_dir=raw_dir, lease_seconds=0, force=True)
    assert recovered["state"] == COMPLETED
    assert recovered["recovered"] is True
    assert recovered["counts"]["ledger_items"] == 4
    assert recovered["counts"]["accepted"] == 3
    assert recovered["counts"]["rejected"] == 1
    assert recovered["counts"]["changed"] == 3
    records_after = {row[0] for row in _query(database, "SELECT record_key FROM records")}
    assert len(records_after) == 3
    assert len(records_after - records_before) == 3 - accepted_before
    assert _query(database, "SELECT COUNT(*) FROM batches") == [(1,)]
    report = _assert_batch_consistent(database, batch_id, raw_dir)
    assert report["attempt"] == 2
    assert report["recovery_count"] == 1
    assert ledger_before <= 4

    # A second explicit recovery is safe and does not create another batch.
    again = recover_batch(database, batch_id, raw_dir=raw_dir, lease_seconds=0)
    assert again["already_settled"] is True
    assert _query(database, "SELECT COUNT(*) FROM batches") == [(1,)]


def _accepted_count(database: str, batch_id: int) -> int:
    return _query(database, "SELECT COUNT(*) FROM batch_items WHERE batch_id=? AND outcome='accepted'", (batch_id,))[0][0]


def test_interruption_at_specific_points_then_recovery(tmp_path):
    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises(InjectedFault):
        run_fixture(str(FIXTURE), database, raw_dir, faults=FaultInjector("after_first_write"))
    assert _query(database, "SELECT count(*) FROM records") == [(1,)]
    assert _query(database, "SELECT count(*) FROM rejected_records") == [(0,)]

    with pytest.raises(InjectedFault):
        recover_batch(database, 1, raw_dir=raw_dir, lease_seconds=0, faults=FaultInjector("after_rejection"))
    assert _query(database, "SELECT count(*) FROM records") == [(3,)]
    assert _query(database, "SELECT count(*) FROM rejected_records") == [(1,)]
    assert _query(database, "SELECT count(*) FROM batch_items") == [(4,)]
    mid = verify_batch(database, 1, raw_dir, lease_seconds=0)
    assert mid["state"] == READY_TO_SETTLE

    recovered = recover_batch(database, 1, raw_dir=raw_dir, lease_seconds=0)
    assert recovered["state"] == COMPLETED
    assert recovered["counts"]["changed"] == 3
    assert recovered["attempt"] == 3
    assert recovered["recovery_count"] == 2
    report = _assert_batch_consistent(database, 1, raw_dir)
    assert _query(database, "SELECT count(*) FROM records") == [(3,)]
    assert _query(database, "SELECT count(*) FROM rejected_records") == [(1,)]


def test_resubmitting_same_evidence_finishes_instead_of_duplicating(tmp_path):
    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises(InjectedFault):
        run_fixture(str(FIXTURE), database, raw_dir, faults=FaultInjector("after_rejection"))
    rerun = run_fixture(str(FIXTURE), database, raw_dir)
    assert rerun["batch_id"] == 1
    assert rerun["recovered"] is True
    assert (rerun["processed"], rerun["accepted"], rerun["rejected"], rerun["changed"]) == (4, 3, 1, 3)
    assert rerun["batches"] == 1
    assert _query(database, "SELECT count(*) FROM rejected_records") == [(1,)]
    _assert_batch_consistent(database, 1, raw_dir)

    # A second full submission of settled evidence is a fresh, idempotent batch
    # with no duplicate structured rows and no duplicate rejections.
    again = run_fixture(str(FIXTURE), database, raw_dir)
    assert again["changed"] == 0
    assert again["records"] == 3
    assert again["batches"] == 2
    assert again["rejected"] == 1
    assert _query(database, "SELECT count(*) FROM rejected_records") == [(2,)]
    _assert_batch_consistent(database, again["batch_id"], raw_dir)


def test_recover_after_finalize_is_a_noop(tmp_path):
    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises(InjectedFault):
        run_fixture(str(FIXTURE), database, raw_dir, faults=FaultInjector("after_finalize"))
    report = verify_batch(database, 1, raw_dir, lease_seconds=0)
    assert report["state"] == COMPLETED
    again = recover_batch(database, 1, raw_dir=raw_dir, lease_seconds=0)
    assert again["already_settled"] is True
    assert again["recovered"] is False
    assert again["attempt"] == 1
    assert _query(database, "SELECT count(*) FROM records") == [(3,)]
    _assert_batch_consistent(database, 1, raw_dir)


def test_recovery_itself_can_be_interrupted_and_resumed(tmp_path):
    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises(InjectedFault):
        run_fixture(str(FIXTURE), database, raw_dir, faults=FaultInjector("after_evidence"))
    for checkpoint, mode in (("after_first_write", "raise"), ("after_rejection", "raise")):
        with pytest.raises((InjectedFault, CrashSignal)) as caught:
            recover_batch(database, 1, raw_dir=raw_dir, lease_seconds=0, faults=FaultInjector(f"{checkpoint}:{mode}"))
        assert caught.type.__name__ in {"InjectedFault", "CrashSignal"}
        partial = verify_batch(database, 1, raw_dir, lease_seconds=0)
        assert partial["state"] in {RETRYABLE, READY_TO_SETTLE}
        assert partial["evidence"]["ok"]
    final = recover_batch(database, 1, raw_dir=raw_dir, lease_seconds=0)
    assert final["state"] == COMPLETED
    assert final["recovery_count"] == 3
    _assert_batch_consistent(database, 1, raw_dir)


# ------------------------------------------------------- state classification


def test_never_started_and_in_progress_states(tmp_path):
    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises(InjectedFault):
        run_fixture(str(FIXTURE), database, raw_dir, faults=FaultInjector("after_evidence"))
    report = verify_batch(database, 1, raw_dir, lease_seconds=0)
    assert report["state"] == NEVER_STARTED
    assert report["auto_recoverable"] is True
    assert report["idempotent_resubmit"] is True

    # Simulate a live worker: lease deliberately long-lived.
    store = DataStore(database, lease_seconds=3600)
    try:
        store.start_batch("manual", digest=_digest(database), records=[{"only": "one"}])
    finally:
        store.close()
    with pytest.raises(BatchInProgressError):
        recover_batch(database, 2, raw_dir=raw_dir, lease_seconds=3600)
    report = verify_batch(database, 2, raw_dir, lease_seconds=3600)
    assert report["state"] == IN_PROGRESS
    assert report["lease"]["live"] is True
    assert report["auto_recoverable"] is False


def test_unrecoverable_evidence_blocks_automatic_settlement(tmp_path):
    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises(InjectedFault):
        run_fixture(str(FIXTURE), database, raw_dir, faults=FaultInjector("after_first_write"))
    digest = _digest(database)
    evidence_file = Path(raw_dir) / f"{digest}.json"
    evidence_file.write_bytes(b"tampered")

    report = verify_batch(database, 1, raw_dir, lease_seconds=0)
    assert report["state"] == UNRECOVERABLE
    assert report["auto_recoverable"] is False
    assert "raw file content does not match the digest" in report["evidence"]["problems"]
    with pytest.raises(UnrecoverableBatchError):
        recover_batch(database, 1, raw_dir=raw_dir, lease_seconds=0)
    assert _query(database, "SELECT status FROM batches WHERE batch_id=1") == [("failed",)]
    assert _query(database, "SELECT count(*) FROM records") == [(1,)]
    assert _query(database, "SELECT count(*) FROM rejected_records") == [(0,)]


def test_operator_marked_unrecoverable_is_not_silently_retried(tmp_path):
    database = _database(tmp_path)
    with pytest.raises(InjectedFault):
        run_fixture(str(FIXTURE), database, None, faults=FaultInjector("after_first_write"))
    result = {"batch_id": 1}
    store = DataStore(database, lease_seconds=0)
    try:
        store.mark_unrecoverable(result["batch_id"], "operator: evidence regenerated out of band")
    finally:
        store.close()
    report = verify_batch(database, result["batch_id"], None, lease_seconds=0)
    assert report["state"] == UNRECOVERABLE
    assert "operator" in report["last_error"]
    with pytest.raises(UnrecoverableBatchError):
        recover_batch(database, result["batch_id"], raw_dir=None, lease_seconds=0)
    # Existing structured rows are never deleted by a blocked recovery.
    assert _query(database, "SELECT count(*) FROM records") == [(1,)]


def test_settlement_refuses_inconsistent_ledger(tmp_path):
    database = _database(tmp_path)
    store = DataStore(database, lease_seconds=0)
    try:
        batch_id = store.start_batch("src", digest="digest", records=[{"a": 1}])
        record = WaterQualityRecord("src", "K", "t", "S", "X", "ph", 7.0, "ph", "good", {"x": 1}, "h")
        store.accept_item(batch_id, 0, record)
        store.conn.execute("DELETE FROM records WHERE record_key='K'")
        store.conn.commit()
        with pytest.raises(SettlementError):
            store.settle_batch(batch_id)
        assert store.get_batch(batch_id)["status"] == "finalizing"
    finally:
        store.close()


def test_rejected_rows_roll_back_atomically(tmp_path):
    database = _database(tmp_path)
    store = DataStore(database, lease_seconds=0)
    try:
        batch_id = store.start_batch("src", records=[{"a": 1}])
        with pytest.raises(RuntimeError):
            with store.conn:
                store.conn.execute(
                    "INSERT INTO rejected_records(batch_id, source, error, raw_json, created_at, item_index)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (batch_id, None, "e", "{}", "now", 0),
                )
                raise RuntimeError("rollback")
        assert store.rejections(batch_id) == []
        assert store.ledger(batch_id) == []
    finally:
        store.close()


# ---------------------------------------------------------- batch isolation


def test_recovery_never_touches_other_batches(tmp_path):
    import json

    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    records = json.loads(FIXTURE.read_text())
    healthy_path = _fixture_copy(tmp_path, records[:2], "healthy.json")
    broken_path = _fixture_copy(tmp_path, records[2:], "broken.json")

    healthy = run_fixture(healthy_path, database, raw_dir)
    assert (healthy["accepted"], healthy["rejected"], healthy["changed"]) == (2, 0, 2)
    healthy_report_before = verify_batch(database, healthy["batch_id"], raw_dir, lease_seconds=0)

    with pytest.raises(InjectedFault):
        run_fixture(broken_path, database, raw_dir, faults=FaultInjector("after_first_write"))
    broken_id = _query(database, "SELECT MAX(batch_id) FROM batches")[0][0]
    assert broken_id != healthy["batch_id"]

    records_before = set(_query(database, "SELECT record_key FROM records"))
    recover_batch(database, broken_id, raw_dir=raw_dir, lease_seconds=0)
    _assert_batch_consistent(database, broken_id, raw_dir)

    healthy_report_after = verify_batch(database, healthy["batch_id"], raw_dir, lease_seconds=0)
    assert healthy_report_after["counts"] == healthy_report_before["counts"]
    assert healthy_report_after["state"] == COMPLETED
    assert healthy_report_after["attempt"] == healthy_report_before["attempt"]
    assert healthy_report_after["status"] == "settled"
    # Only new rows from the broken batch were added.
    assert set(_query(database, "SELECT record_key FROM records")) >= records_before
    assert _query(database, "SELECT status FROM batches WHERE batch_id=?", (healthy["batch_id"],)) == [("settled",)]


def test_same_evidence_in_two_sources_stays_separate_and_consistent(tmp_path):
    import json

    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    records = json.loads(FIXTURE.read_text())
    for index, record in enumerate(records):
        record["source"] = "station-network" if index % 2 == 0 else "lab-a"
    path = _fixture_copy(tmp_path, records, "mixed.json")

    result = run_fixture(path, database, raw_dir)
    assert (result["processed"], result["accepted"], result["rejected"]) == (4, 3, 1)
    _assert_batch_consistent(database, result["batch_id"], raw_dir)


# ------------------------------------------------------------ crash semantics


def _crashing_child(database: str, raw_dir: str, queue) -> None:
    try:
        run_fixture(str(FIXTURE), database, raw_dir, faults=FaultInjector("after_first_write:crash"))
    except BaseException as exc:  # noqa: BLE001 - detached child reports nothing
        queue.put(type(exc).__name__)


def test_hard_crash_leaves_lease_and_partial_ledger(tmp_path):
    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    store = DataStore(database, lease_seconds=2)
    store.close()
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    process = context.Process(target=_crashing_child, args=(database, raw_dir, queue))
    process.start()
    process.join(timeout=20)
    assert process.exitcode is not None
    assert queue.get_nowait() in {"CrashSignal", "InjectedFault"}

    report = verify_batch(database, 1, raw_dir, lease_seconds=0)
    assert report["state"] in {IN_PROGRESS, RETRYABLE}
    assert report["counts"]["ledger_items"] == 1
    # A live lease from a dead worker can be overridden explicitly.
    recovered = recover_batch(database, 1, raw_dir=raw_dir, lease_seconds=0, force=True)
    assert recovered["state"] == COMPLETED
    _assert_batch_consistent(database, 1, raw_dir)


# --------------------------------------------------------------- CLI surface


def test_cli_keeps_fixture_and_http_public_entry_points(tmp_path):
    from water_quality.cli import main

    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    main(["--fixture", str(FIXTURE), "--database", database, "--raw-dir", raw_dir])
    main(["--verify-batch", "1", "--database", database, "--raw-dir", raw_dir])


def test_cli_reports_failure_and_supports_explicit_recovery(tmp_path, capsys):
    import json

    from water_quality.cli import main

    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises(SystemExit) as exit_info:
        main(["--fixture", str(FIXTURE), "--database", database, "--raw-dir", raw_dir,
              "--fail-at", "after_first_write"])
    assert exit_info.value.code == 1
    failure = json.loads(capsys.readouterr().out)
    assert failure["error"] == "injected fault at after_first_write"
    assert failure["report"]["checkpoint"] == "after_first_write"
    stored = verify_batch(database, 1, raw_dir, lease_seconds=0)
    assert stored["last_error"] == "InjectedFault: injected fault at after_first_write"
    assert stored["state"] == RETRYABLE

    main(["--recover-batch", "1", "--database", database, "--raw-dir", raw_dir])
    recovered = json.loads(capsys.readouterr().out)
    assert recovered["state"] == COMPLETED
    assert (recovered["counts"]["accepted"], recovered["counts"]["rejected"]) == (3, 1)


def test_cli_list_batches_and_ambiguous_digest(tmp_path, capsys):
    import json

    from water_quality.cli import main

    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises(SystemExit):
        main(["--fixture", str(FIXTURE), "--database", database, "--raw-dir", raw_dir,
              "--fail-at", "after_first_write"])
    capsys.readouterr()
    with pytest.raises(SystemExit):
        main(["--fixture", str(FIXTURE), "--database", database, "--raw-dir", raw_dir, "--legacy-batch",
              "--fail-at", "after_first_write"])
    capsys.readouterr()
    main(["--list-batches", "--database", database, "--raw-dir", raw_dir])
    reports = json.loads(capsys.readouterr().out)
    assert [report["batch_id"] for report in reports] == [1, 2]
    assert all(report["state"] == RETRYABLE for report in reports)

    with pytest.raises(AmbiguousBatchError):
        recover_batch(database, digest=_digest(database), raw_dir=raw_dir, lease_seconds=0)


def test_orphan_evidence_is_reported_not_silently_adopted(tmp_path, capsys):
    import json

    from water_quality.cli import main
    from water_quality.reconciliation import list_orphan_evidence

    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    run_fixture(str(FIXTURE), database, raw_dir)
    extra = Path(raw_dir) / ("0" * 64 + ".json")
    extra.write_bytes(b"landed but never registered")

    orphans = list_orphan_evidence(database, raw_dir)
    assert [orphan["digest"] for orphan in orphans] == ["0" * 64]
    assert orphans[0]["content_matches_name"] is False

    main(["--list-orphans", "--database", database, "--raw-dir", raw_dir])
    printed = json.loads(capsys.readouterr().out)
    assert len(printed) == 1
    # Orphan evidence never becomes a batch by inspection alone.
    assert _query(database, "SELECT COUNT(*) FROM batches") == [(1,)]


def test_second_worker_cannot_settle_a_claimed_batch(tmp_path):
    database, raw_dir = _database(tmp_path), _raw(tmp_path)
    with pytest.raises(InjectedFault):
        run_fixture(str(FIXTURE), database, raw_dir, faults=FaultInjector("before_finalize"))
    store = DataStore(database, lease_seconds=3600)
    try:
        assert store.claim_batch(1, owner="worker-a") is True
        assert store.claim_batch(1, owner="worker-b") is False
        assert store.get_batch(1)["lease_owner"] == "worker-a"
        with pytest.raises(BatchInProgressError):
            recover_batch(database, 1, raw_dir=raw_dir, lease_seconds=3600)
        assert store.steal_batch(1, owner="worker-b") is True
        assert store.get_batch(1)["lease_owner"] == "worker-b"
    finally:
        store.close()
    recovered = recover_batch(database, 1, raw_dir=raw_dir, lease_seconds=0, force=True)
    assert recovered["state"] == COMPLETED
    _assert_batch_consistent(database, 1, raw_dir)
