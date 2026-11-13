from __future__ import annotations

import json
from pathlib import Path

import pytest

from water_quality.application import (
    query_subject,
    query_subjects,
    query_summary,
    run_fixture,
)
from water_quality.storage import DataStore

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "adjudication"
CONFIG = str(Path(__file__).parents[1] / "fixtures" / "adjudication.config.json")


def _ingest(tmp_path: Path, name: str, database: Path) -> dict:
    return run_fixture(str(FIXTURE_DIR / name), str(database), str(tmp_path / "raw"), CONFIG)


def _skey(station: str, sample: str, metric: str) -> str:
    return f"{station}|{sample}|{metric}"


@pytest.fixture()
def database(tmp_path: Path) -> Path:
    return tmp_path / "adjudication.sqlite3"


def test_initial_preliminary_reports_are_pending_then_lab_confirms(database, tmp_path):
    """Scenario 1: preliminary report first, later higher-priority same-content confirmation."""
    first = _ingest(tmp_path, "01_initial.json", database)
    assert first["processed"] == 5
    assert first["accepted"] == 5
    assert first["new_evidence"] == 5

    w10 = query_subject(str(database), _skey("W-10", "S-100", "PH"))
    assert w10["subject"]["state"] == "confirmed"
    # Single station report is confirmed by precedence within the known evidence.
    assert w10["reports"][0]["state"] == "confirmed"

    _ingest(tmp_path, "02_retests_and_conflict.json", database)
    w10 = query_subject(str(database), _skey("W-10", "S-100", "PH"))
    states = {row["source"]: row["state"] for row in w10["reports"]}
    assert states["lab-a"] == "confirmed"
    assert states["station-network"] == "superseded"
    superseded = next(row for row in w10["reports"] if row["source"] == "station-network")
    assert superseded["reason"] == "same_content_duplicate_evidence"
    assert w10["subject"]["state"] == "confirmed"
    assert w10["confirmed_report"]["source"] == "lab-a"


def test_lab_retest_overturns_preliminary_value(database, tmp_path):
    """Scenario 2: a more reliable retest with a different value supersedes the preliminary one."""
    _ingest(tmp_path, "01_initial.json", database)
    _ingest(tmp_path, "02_retests_and_conflict.json", database)

    w11 = query_subject(str(database), _skey("W-11", "S-101", "AMMONIA_N"))
    by_source = {row["source"]: row for row in w11["reports"]}
    assert by_source["lab-a"]["state"] == "confirmed"
    assert by_source["lab-a"]["value"] == pytest.approx(0.9)
    assert by_source["station-network"]["state"] == "superseded"
    assert by_source["station-network"]["reason"] == "higher_priority_result_published"
    # The earlier conclusion is still traceable, not erased.
    assert by_source["station-network"]["value"] == pytest.approx(1.2)
    assert w11["confirmed_report"]["value"] == pytest.approx(0.9)


def test_identical_redelivery_creates_no_new_logical_version(database, tmp_path):
    """Scenario 3: duplicate transmission is idempotent end to end."""
    first = _ingest(tmp_path, "01_initial.json", database)
    second = _ingest(tmp_path, "01_initial.json", database)
    assert second["new_evidence"] == 0
    assert second["changed"] == 0
    assert second["report_evidence"] == first["new_evidence"]
    assert second["total_records"] == 5

    store = DataStore(str(database))
    try:
        transitions = store.conn.execute("SELECT COUNT(*) FROM state_transitions").fetchone()[0]
    finally:
        store.close()
    third = _ingest(tmp_path, "01_initial.json", database)
    store = DataStore(str(database))
    try:
        assert store.conn.execute("SELECT COUNT(*) FROM state_transitions").fetchone()[0] == transitions
    finally:
        store.close()
    assert third["new_evidence"] == 0


def test_two_equal_priority_results_enter_dispute_and_are_not_final(database, tmp_path):
    """Scenario 4: conflicting high-priority results block regulatory publication."""
    _ingest(tmp_path, "01_initial.json", database)
    _ingest(tmp_path, "02_retests_and_conflict.json", database)

    w12 = query_subject(str(database), _skey("W-12", "S-102", "LEAD"))
    assert w12["subject"]["state"] == "disputed"
    lab_rows = [row for row in w12["reports"] if row["source"].startswith("lab")]
    assert {row["state"] for row in lab_rows} == {"disputed"}
    assert sorted(row["value"] for row in lab_rows) == pytest.approx([0.03, 0.07])

    final = query_subjects(str(database), final_only=True)
    assert all(row["subject_key"] != _skey("W-12", "S-102", "LEAD") for row in final)
    summary = query_summary(str(database))
    assert summary["unresolved_disputes"] >= 1
    assert summary["final_regulatory_subjects"] == summary["subjects"]["confirmed"]


def test_withdrawal_then_resubmission_reopens_adjudication(database, tmp_path):
    """Scenario 5: withdrawal is retained as evidence; a later resubmission adjudicates anew."""
    _ingest(tmp_path, "01_initial.json", database)
    _ingest(tmp_path, "03_withdrawal.json", database)

    w13 = query_subject(str(database), _skey("W-13", "S-103", "DISSOLVED_OXYGEN"))
    assert w13["reports"][0]["state"] == "withdrawn"
    assert w13["subject"]["state"] == "pending"
    assert len(w13["withdrawals"]) == 1
    assert w13["subject"]["confirmed_report_hash"] is None

    # The withdrawal fixture itself contains a duplicated directive: still one row.
    store = DataStore(str(database))
    try:
        assert store.conn.execute("SELECT COUNT(*) FROM withdrawal_directives").fetchone()[0] == 1
    finally:
        store.close()

    _ingest(tmp_path, "04_resubmit.json", database)
    w13 = query_subject(str(database), _skey("W-13", "S-103", "DISSOLVED_OXYGEN"))
    assert w13["subject"]["state"] == "confirmed"
    states = {row["observed_at"]: row["state"] for row in w13["reports"]}
    assert states["2026-11-02T08:00:00Z"] == "withdrawn"
    new_rows = [row for row in w13["reports"] if row["observed_at"] != "2026-11-02T08:00:00Z"]
    assert {row["state"] for row in new_rows} == {"confirmed", "superseded"}
    # Full lifecycle survives in the append-only transition log.
    scopes = [(row["scope"], row["new_state"]) for row in w13["transitions"]]
    assert ("report", "withdrawn") in scopes
    assert ("subject", "confirmed") in scopes


def test_partial_failure_is_atomic_per_record_and_recovers(database, tmp_path):
    """Scenario 6: a bad record is rejected; good records still commit; rerun recovers cleanly."""
    _ingest(tmp_path, "01_initial.json", database)
    result = _ingest(tmp_path, "05_partial_failure.json", database)
    assert result["processed"] == 2
    assert result["accepted"] == 1
    assert result["rejected"] == 1

    w15 = query_subject(str(database), _skey("W-15", "S-105", "TOTAL_PHOSPHORUS"))
    assert w15["subject"]["state"] == "confirmed"

    store = DataStore(str(database))
    try:
        rejected = store.conn.execute("SELECT error FROM rejected_records").fetchall()
        assert any("numeric field is invalid" in row["error"] for row in rejected)
        # No evidence row exists for the failed record.
        assert (
            store.conn.execute(
                "SELECT COUNT(*) FROM report_evidence WHERE station_id='W-16'"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()

    # Replaying the same mixed batch never duplicates the good logical version.
    # The valid record is re-accepted but idempotent; the unparseable record has no
    # evidence hash and is rejected again (two rejection rows, one per delivery).
    rerun = _ingest(tmp_path, "05_partial_failure.json", database)
    assert rerun["accepted"] == 1
    assert rerun["new_evidence"] == 0
    assert rerun["changed"] == 0
    # The bad record cannot be deduplicated before parsing, so this delivery rejects it again.
    assert rerun["processed"] == 2
    assert rerun["rejected"] == 1
    assert rerun["report_evidence"] == result["report_evidence"]
    store = DataStore(str(database))
    try:
        # One rejection row per delivery attempt: full failure audit trail.
        assert (
            store.conn.execute(
                "SELECT COUNT(*) FROM rejected_records WHERE raw_json LIKE '%W-16%'"
            ).fetchone()[0]
            == 2
        )
        assert (
            store.conn.execute(
                "SELECT COUNT(*) FROM report_evidence WHERE station_id='W-15'"
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()


def test_crash_mid_batch_leaves_no_half_version_and_replay_is_identical(database, tmp_path, monkeypatch):
    """A simulated exception after the first commit leaves later subjects untouched; replay converges."""
    _ingest(tmp_path, "01_initial.json", database)

    from water_quality import storage as storage_module

    real_recompute = DataStore._recompute_subject
    calls = {"count": 0}

    def flaky(self, skey, config, batch_id):
        calls["count"] += 1
        if calls["count"] == 2:
            raise RuntimeError("simulated disk failure")
        return real_recompute(self, skey, config, batch_id)

    monkeypatch.setattr(storage_module.DataStore, "_recompute_subject", flaky)
    with pytest.raises(RuntimeError):
        run_fixture(str(FIXTURE_DIR / "02_retests_and_conflict.json"), str(database), str(tmp_path / "raw"), CONFIG)

    store = DataStore(str(database))
    try:
        failed_batch = store.conn.execute(
            "SELECT status FROM batches ORDER BY batch_id DESC LIMIT 1"
        ).fetchone()
        assert failed_batch["status"] == "failed"
        # Second subject's evidence must not have landed (its transaction rolled back).
        assert (
            store.conn.execute(
                "SELECT COUNT(*) FROM report_evidence WHERE station_id='W-11' AND source='lab-a'"
            ).fetchone()[0]
            == 0
        )
        # First subject's commit survived.
        assert (
            store.conn.execute(
                "SELECT COUNT(*) FROM report_evidence WHERE station_id='W-10' AND source='lab-a'"
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()

    monkeypatch.undo()
    recovered = _ingest(tmp_path, "02_retests_and_conflict.json", database)
    assert recovered["rejected"] == 0
    w11 = query_subject(str(database), _skey("W-11", "S-101", "AMMONIA_N"))
    assert w11["subject"]["state"] == "confirmed"


def test_replay_is_deterministic_across_fresh_databases(database, tmp_path):
    """Same material replayed into two independent databases yields identical states and reasons."""
    db_a = tmp_path / "a.sqlite3"
    db_b = tmp_path / "b.sqlite3"
    names = ["01_initial.json", "02_retests_and_conflict.json", "03_withdrawal.json", "04_resubmit.json"]
    for name in names:
        _ingest(tmp_path, name, db_a)
    for name in names:
        _ingest(tmp_path, name, db_b)

    def snapshot(path: Path) -> dict:
        store = DataStore(str(path))
        try:
            subjects = store.conn.execute(
                "SELECT subject_key, state, reason, confirmed_report_hash FROM subject_states ORDER BY subject_key"
            ).fetchall()
            reports = store.conn.execute(
                "SELECT record_key, state, reason FROM report_states ORDER BY record_key"
            ).fetchall()
            return {"subjects": [tuple(row) for row in subjects], "reports": [tuple(row) for row in reports]}
        finally:
            store.close()

    assert snapshot(db_a) == snapshot(db_b)

    # Replaying everything again changes nothing.
    before = snapshot(db_a)
    for name in names:
        assert _ingest(tmp_path, name, db_a)["changed"] == 0
    assert snapshot(db_a) == before


def test_late_retest_only_touches_its_own_subject(database, tmp_path):
    """Adjudication recomputes one subject at a time; other water samples are unaffected."""
    _ingest(tmp_path, "01_initial.json", database)
    before = query_subjects(str(database))
    _ingest(tmp_path, "02_retests_and_conflict.json", database)
    after = query_subjects(str(database))

    changed_keys = {"W-10|S-100|PH", "W-11|S-101|AMMONIA_N", "W-12|S-102|LEAD"}
    before_map = {row["subject_key"]: (row["state"], row["reason"]) for row in before}
    after_map = {row["subject_key"]: (row["state"], row["reason"]) for row in after}
    for key, value in before_map.items():
        if key not in changed_keys:
            assert after_map[key] == value


def test_quality_ineligible_report_never_becomes_final(database, tmp_path):
    _ingest(tmp_path, "01_initial.json", database)
    w14 = query_subject(str(database), _skey("W-14", "S-104", "PH"))
    assert w14["subject"]["state"] == "pending"
    assert w14["reports"][0]["state"] == "pending"
    assert w14["reports"][0]["reason"] == "quality_not_eligible"
    assert query_subject(str(database), _skey("W-14", "S-104", "PH"))["subject"]["confirmed_report_hash"] is None


def test_transition_history_is_independently_queryable(database, tmp_path):
    _ingest(tmp_path, "01_initial.json", database)
    _ingest(tmp_path, "02_retests_and_conflict.json", database)
    w11 = query_subject(str(database), _skey("W-11", "S-101", "AMMONIA_N"))
    report_transitions = [row for row in w11["transitions"] if row["scope"] == "report"]
    # Station report moves confirmed -> superseded when the lab result overturns it.
    station_hash = next(row["report_hash"] for row in w11["reports"] if row["source"] == "station-network")
    station_history = [row for row in report_transitions if row["report_hash"] == station_hash]
    assert [row["new_state"] for row in station_history] == ["confirmed", "superseded"]
    assert station_history[-1]["reason_label"]  # human-readable reason is present


def test_operator_output_contains_no_authentication_material(database, tmp_path):
    _ingest(tmp_path, "01_initial.json", database)
    detail = query_subject(str(database), _skey("W-10", "S-100", "PH"))
    serialized = json.dumps(detail, ensure_ascii=False)
    for secret_word in ("token", "credential", "secret", "authorization", "api_key", "bearer"):
        assert secret_word not in serialized.lower()
    summary = query_summary(str(database))
    assert set(summary["reports"]) == {"pending", "confirmed", "superseded", "disputed", "withdrawn"}
