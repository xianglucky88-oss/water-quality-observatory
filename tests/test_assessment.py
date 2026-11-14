"""Window-level evidence completeness assessment tests.

Every conclusion category is exercised against local deterministic data:
boundary instants, late arrivals, long gaps, duplicates, rejected records,
mixed metrics, zero values and threshold switching.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from water_quality.application import run_fixture
from water_quality.assessment import (
    DEFAULT_POLICY,
    SUPPORTED_METRICS,
    assess_window,
    render_summary,
)
from water_quality.cli import main as cli_main


WINDOW_START = "2026-11-01T10:00:00Z"
WINDOW_END = "2026-11-01T11:00:00Z"


class FakeClock:
    """Deterministic UTC clock so arrival ordering is fully reproducible."""

    def __init__(self, start: datetime) -> None:
        self.current = start

    def __call__(self) -> datetime:
        return self.current

    def advance(self, seconds: int) -> None:
        self.current += timedelta(seconds=seconds)


def make_record(source, observed_at, station, sample, metric, value,
                unit=None, flag="good"):
    if unit is None:
        unit = "pH" if metric == "ph" else "mg/L"
    return {
        "source": source,
        "observed_at": observed_at,
        "payload": {
            "station_id": station,
            "sample_id": sample,
            "metric": metric,
            "value": value,
            "unit": unit,
            "quality_flag": flag,
        },
    }


def ingest(tmp_path, name, records, clock, database=None):
    fixture = tmp_path / name
    fixture.write_text(json.dumps(records), encoding="utf-8")
    db = database or str(tmp_path / "store.sqlite3")
    result = run_fixture(str(fixture), db, str(tmp_path / "raw"), clock=clock)
    return db, result


def block(result, metric):
    return next(item for item in result["metrics"] if item["metric"] == metric)


def reason_codes(block_):
    return [reason["code"] for reason in block_["reasons"]]


def test_boundary_instants_are_start_inclusive_end_exclusive(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [
        make_record("s1", WINDOW_START, "W-01", "S-EDGE-START", "ph", 7.1),
        make_record("s1", WINDOW_END, "W-01", "S-EDGE-END", "ph", 7.3),
    ]
    db, _ = ingest(tmp_path, "boundary.json", records, clock)
    result = assess_window(db, "w-01", WINDOW_START, WINDOW_END, ["ph"])
    keys = [item["record_key"] for item in block(result, "ph")["usable_observations"]]
    assert any("S-EDGE-START" in key for key in keys)
    assert all("S-EDGE-END" not in key for key in keys)
    # End-exclusive record must not appear anywhere in the window evidence.
    assert all("S-EDGE-END" not in item for item in json.dumps(result, ensure_ascii=False).split("record_key")[1:])


def test_late_arrival_is_counted_but_remains_usable(tmp_path):
    early_clock = FakeClock(datetime(2026, 11, 1, 10, 30, tzinfo=timezone.utc))
    records = [
        make_record("s1", "2026-11-01T10:05:00Z", "W-01", "S-EARLY", "ph", 7.0),
        make_record("s1", "2026-11-01T10:15:00Z", "W-01", "S-LATE", "ph", 7.4),
        make_record("s1", "2026-11-01T10:25:00Z", "W-01", "S-LATE2", "ph", 7.5),
    ]
    db, _ = ingest(tmp_path, "late.json", records, early_clock)
    # A second batch with a record sampled inside the window but seen after the deadline.
    late_clock = FakeClock(datetime(2026, 11, 2, 6, 0, tzinfo=timezone.utc))
    db, _ = ingest(
        tmp_path, "late2.json",
        [make_record("s2", "2026-11-01T10:40:00Z", "W-01", "S-LATE3", "ph", 7.6)],
        late_clock, database=db,
    )
    result = assess_window(
        db, "W-01", WINDOW_START, WINDOW_END, ["ph"],
        arrival_deadline="2026-11-01T12:00:00Z",
    )
    ph = block(result, "ph")
    assert ph["counts"]["usable_observations"] == 4
    assert ph["counts"]["late_arrivals"] == 1
    late = next(item for item in ph["usable_observations"] if item["late_arrival"])
    assert late["sample_id"] == "S-LATE3"
    on_time = [item for item in ph["usable_observations"] if not item["late_arrival"]]
    assert len(on_time) == 3
    # Late arrival is informational: it never invalidates the sampled evidence.
    assert "CONFLICTING_VERSIONS" not in reason_codes(ph)


def test_long_gap_creates_evidence_gap_including_edges(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [
        make_record("s1", "2026-11-01T10:40:00Z", "W-01", "S-A", "ph", 7.0),
        make_record("s1", "2026-11-01T10:45:00Z", "W-01", "S-B", "ph", 7.1),
    ]
    db, _ = ingest(tmp_path, "gaps.json", records, clock)
    result = assess_window(
        db, "W-01", WINDOW_START, WINDOW_END, ["ph"],
        policy_overrides={"ph": {"min_observations": 2, "max_gap_seconds": 600}},
    )
    ph = block(result, "ph")
    kinds = [gap["kind"] for gap in ph["evidence_gaps"]]
    assert kinds == ["leading_edge", "trailing_edge"]
    assert all(gap["exceeds_threshold"] for gap in ph["evidence_gaps"])
    assert "GAP_EXCEEDS_MAX_INTERVAL" in reason_codes(ph)
    assert ph["status"] == "insufficient_evidence"


def test_internal_gap_measured_between_observations(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [
        make_record("s1", "2026-11-01T10:00:00Z", "W-01", "S-A", "ph", 7.0),
        make_record("s1", "2026-11-01T10:55:00Z", "W-01", "S-B", "ph", 7.1),
    ]
    db, _ = ingest(tmp_path, "internal.json", records, clock)
    result = assess_window(
        db, "W-01", WINDOW_START, WINDOW_END, ["ph"],
        policy_overrides={"ph": {"min_observations": 2, "max_gap_seconds": 600}},
    )
    internal = next(g for g in block(result, "ph")["evidence_gaps"] if g["kind"] == "internal")
    assert internal["gap_seconds"] == 55 * 60
    assert internal["exceeds_threshold"] is True


def test_missing_metric_is_distinct_from_rejected_and_zero(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    # lead: only rejected (invalid domain range). ammonia_n: nothing at all.
    records = [
        make_record("s1", "2026-11-01T10:10:00Z", "W-01", "S-BAD", "lead", 250.0),
        make_record("s1", "2026-11-01T10:20:00Z", "W-01", "S-ZERO", "dissolved_oxygen", 0.0),
    ]
    db, result = ingest(tmp_path, "missing.json", records, clock)
    assert result["rejected"] == 1
    assessed = assess_window(
        db, "W-01", WINDOW_START, WINDOW_END,
        ["lead", "ammonia_n", "dissolved_oxygen"],
        policy_overrides={
            "lead": {"min_observations": 1, "max_gap_seconds": 3600},
            "ammonia_n": {"min_observations": 1, "max_gap_seconds": 600},
            "dissolved_oxygen": {"min_observations": 1, "max_gap_seconds": 3600},
        },
    )
    lead = block(assessed, "lead")
    ammonia = block(assessed, "ammonia_n")
    do = block(assessed, "dissolved_oxygen")
    assert reason_codes(lead) == ["ONLY_REJECTED_EVIDENCE"]
    assert reason_codes(ammonia) == ["MISSING_IN_WINDOW", "GAP_EXCEEDS_MAX_INTERVAL"]
    assert lead["counts"]["rejected_records"] == 1
    assert ammonia["counts"]["rejected_records"] == 0
    # Zero is a genuine usable numeric observation, never merged with missing/invalid.
    assert do["counts"]["usable_observations"] == 1
    assert do["counts"]["zero_value_observations"] == 1
    assert do["usable_observations"][0]["is_zero_value"] is True
    assert do["status"] == "supports_pollution_assessment"


def test_duplicate_conflicts_keep_every_version_and_block_verdict(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    first = [make_record("lab-a", "2026-11-01T10:10:00Z", "W-01", "S-DUP", "lead", 0.04)]
    db, _ = ingest(tmp_path, "dup1.json", first, clock)
    # Same domain key (station+sample+metric+observed_at), repeated source, different value.
    second = [make_record("lab-a", "2026-11-01T10:10:00Z", "W-01", "S-DUP", "lead", 0.09)]
    clock.advance(3600)
    db, _ = ingest(tmp_path, "dup2.json", second, clock, database=db)
    result = assess_window(
        db, "W-01", WINDOW_START, WINDOW_END, ["lead"],
        policy_overrides={"lead": {"min_observations": 1, "max_gap_seconds": 3600}},
    )
    lead = block(result, "lead")
    assert lead["counts"]["usable_observations"] == 0
    assert lead["counts"]["conflicting_record_keys"] == 1
    conflict = lead["duplicate_conflicts"][0]
    assert conflict["distinct_version_count"] == 2
    assert conflict["values_seen"] == [0.04, 0.09]
    assert len(conflict["raw_hashes"]) == 2
    assert {ref["batch_id"] for ref in conflict["evidence"]} == {1, 2}
    assert "CONFLICTING_VERSIONS" in reason_codes(lead)


def test_exact_duplicates_are_folded_not_flagged(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    record = make_record("s1", "2026-11-01T10:10:00Z", "W-01", "S-EXACT", "ph", 7.2)
    # Same fixture path => identical raw bytes, source name and content hash.
    db, first = ingest(tmp_path, "exact.json", [record], clock)
    clock.advance(600)
    db, second = ingest(tmp_path, "exact.json", [record], clock, database=db)
    assert second["changed"] == 0
    result = assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["ph"])
    ph = block(result, "ph")
    assert ph["counts"]["usable_observations"] == 1
    assert ph["counts"]["conflicting_record_keys"] == 0
    assert ph["counts"]["exact_duplicate_versions"] == 1
    item = ph["usable_observations"][0]
    assert item["duplicate_copies"] == 1
    assert len(item["evidence"]) == 2
    assert len({ref["raw_hash"] for ref in item["evidence"]}) == 1


def test_rejected_records_are_quarantined_and_traced(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [
        make_record("s1", "2026-11-01T10:10:00Z", "W-01", "S-BAD", "ph", 99.0),
        {"source": "s1", "observed_at": "", "payload": {"station_id": "W-01"}},
    ]
    db, ingest_result = ingest(tmp_path, "reject.json", records, clock)
    assert ingest_result["rejected"] == 2
    result = assess_window(
        db, "W-01", WINDOW_START, WINDOW_END, ["ph"],
        policy_overrides={"ph": {"min_observations": 1, "max_gap_seconds": 600}},
    )
    ph = block(result, "ph")
    assert ph["counts"]["usable_observations"] == 0
    assert ph["counts"]["rejected_records"] == 1
    quarantined = ph["rejected_records"][0]
    assert quarantined["batch_id"] == 1
    assert "domain range" in quarantined["reason"]
    assert quarantined["raw_summary"]["sample_id"] == "S-BAD"
    # The unparseable rejection cannot be located in a window; reported separately.
    assert result["window_totals"]["unattributed_rejections"] == 1


def test_mixed_metrics_share_one_window_with_independent_verdicts(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [
        make_record("s1", "2026-11-01T10:00:00Z", "W-01", "S-1", "ph", 7.0),
        make_record("s1", "2026-11-01T10:20:00Z", "W-01", "S-2", "ph", 7.1),
        make_record("s1", "2026-11-01T10:40:00Z", "W-01", "S-3", "ph", 7.2),
        make_record("s1", "2026-11-01T10:05:00Z", "W-01", "S-4", "lead", 0.01),
    ]
    db, _ = ingest(tmp_path, "mixed.json", records, clock)
    result = assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["ph", "lead"])
    assert [item["metric"] for item in result["metrics"]] == ["ph", "lead"]
    assert block(result, "ph")["status"] == "supports_pollution_assessment"
    assert block(result, "lead")["status"] == "insufficient_evidence"
    assert result["overall_status"] == "insufficient_evidence"


def test_threshold_switch_changes_verdict_deterministically(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [
        make_record("s1", "2026-11-01T10:00:00Z", "W-01", "S-1", "ph", 7.0),
        make_record("s1", "2026-11-01T10:15:00Z", "W-01", "S-2", "ph", 7.1),
    ]
    db, _ = ingest(tmp_path, "threshold.json", records, clock)
    strict = assess_window(
        db, "W-01", WINDOW_START, WINDOW_END, ["ph"],
        policy_overrides={"ph": {"min_observations": 3, "max_gap_seconds": 600}},
    )
    relaxed = assess_window(
        db, "W-01", WINDOW_START, WINDOW_END, ["ph"],
        policy_overrides={"ph": {"min_observations": 2, "max_gap_seconds": 3600}},
    )
    assert block(strict, "ph")["status"] == "insufficient_evidence"
    assert "BELOW_MIN_OBSERVATIONS" in reason_codes(block(strict, "ph"))
    assert "GAP_EXCEEDS_MAX_INTERVAL" in reason_codes(block(strict, "ph"))
    assert block(relaxed, "ph")["status"] == "supports_pollution_assessment"
    # Edge gaps exist in both but only exceed the strict threshold.
    strict_gaps = [g for g in block(strict, "ph")["evidence_gaps"] if g["exceeds_threshold"]]
    relaxed_gaps = [g for g in block(relaxed, "ph")["evidence_gaps"] if g["exceeds_threshold"]]
    assert strict_gaps and not relaxed_gaps


def test_repeated_imports_and_input_order_do_not_drift_fingerprint(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records_a = [
        make_record("s1", "2026-11-01T10:00:00Z", "W-01", "S-1", "ph", 7.0),
        make_record("s1", "2026-11-01T10:20:00Z", "W-01", "S-2", "ph", 7.1),
        make_record("s1", "2026-11-01T10:40:00Z", "W-01", "S-3", "ph", 7.2),
    ]
    db, _ = ingest(tmp_path, "a.json", records_a, clock)
    first = assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["ph"])

    clock.advance(3600)
    db, reimport = ingest(tmp_path, "a.json", records_a, clock, database=db)
    assert reimport["changed"] == 0
    second = assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["ph"])
    assert second["assessment_fingerprint"] == first["assessment_fingerprint"]

    # Separate database, same content in reverse input order and a different clock.
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_clock = FakeClock(datetime(2026, 11, 3, 8, 0, tzinfo=timezone.utc))
    db2, _ = ingest(other_dir, "a.json", list(reversed(records_a)), other_clock)
    third = assess_window(db2, "W-01", WINDOW_START, WINDOW_END, ["ph"])
    assert third["assessment_fingerprint"] == first["assessment_fingerprint"]


def test_duplicate_raw_material_produces_one_manifest(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [make_record("s1", "2026-11-01T10:00:00Z", "W-01", "S-1", "ph", 7.0)]
    db, _ = ingest(tmp_path, "same.json", records, clock)
    clock.advance(10)
    db, _ = ingest(tmp_path, "same.json", records, clock, database=db)
    result = assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["ph"])
    assert len(result["provenance"]["raw_materials"]) == 1
    assert len(result["provenance"]["batches"]) == 2
    material = result["provenance"]["raw_materials"][0]
    assert material["batches"] == [1, 2]


def test_every_conclusion_carries_record_batch_and_raw_provenance(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [
        make_record("s1", "2026-11-01T10:00:00Z", "W-01", "S-1", "ph", 7.0),
        make_record("s1", "2026-11-01T10:10:00Z", "W-01", "S-2", "ph", 7.1),
        make_record("s1", "2026-11-01T10:20:00Z", "W-01", "S-3", "ph", 7.2),
        make_record("s1", "2026-11-01T10:30:00Z", "W-01", "S-BAD", "lead", 500.0),
    ]
    db, _ = ingest(tmp_path, "prov.json", records, clock)
    result = assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["ph", "lead"])
    batch_ids = {batch["batch_id"] for batch in result["provenance"]["batches"]}
    material_digests = {m["digest"] for m in result["provenance"]["raw_materials"]}
    for item in block(result, "ph")["usable_observations"]:
        for ref in item["evidence"]:
            assert ref["batch_id"] in batch_ids
            assert ref["raw_hash"]
    for rejected in block(result, "lead")["rejected_records"]:
        assert rejected["batch_id"] in batch_ids
        assert rejected["raw_summary"]["sample_id"] == "S-BAD"
    for material in result["provenance"]["raw_materials"]:
        assert material["digest"] in material_digests
        assert material["byte_count"] > 0 and material["source"]


def test_assessment_is_read_only(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [make_record("s1", "2026-11-01T10:00:00Z", "W-01", "S-1", "ph", 7.0)]
    db, _ = ingest(tmp_path, "readonly.json", records, clock)
    snapshot = Path(db).read_bytes()
    assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["ph"])
    assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["ph"])
    assert Path(db).read_bytes() == snapshot


def test_empty_window_whole_window_gap_and_uncertain_wording(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    db, _ = ingest(tmp_path, "empty.json", [], clock)
    result = assess_window(
        db, "W-01", WINDOW_START, WINDOW_END, ["ph"],
        policy_overrides={"ph": {"min_observations": 1, "max_gap_seconds": 600}},
    )
    ph = block(result, "ph")
    whole = ph["evidence_gaps"][0]
    assert whole["kind"] == "whole_window"
    assert whole["gap_seconds"] == 3600
    assert "evidence_completeness_only" in result["determination_scope"]
    assert result["overall_status"] == "insufficient_evidence"
    # No evidence means the document must never assert a pollution event.
    assert "pollution_event" not in json.dumps(result)


def test_cli_assess_outputs_summary_and_json(tmp_path, capsys):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    records = [make_record("s1", "2026-11-01T10:00:00Z", "W-01", "S-1", "ph", 7.0)]
    db, _ = ingest(tmp_path, "cli.json", records, clock)
    cli_main(["assess", "--database", db, "--station", "W-01",
              "--start", WINDOW_START, "--end", WINDOW_END,
              "--metrics", "ph", "--format", "summary"])
    summary = capsys.readouterr().out
    assert "窗口证据完整度评估" in summary
    assert "评估指纹" in summary
    assert render_summary  # import surface available

    out_file = tmp_path / "result.json"
    cli_main(["assess", "--database", db, "--station", "W-01",
              "--start", WINDOW_START, "--end", WINDOW_END,
              "--metrics", "ph", "--format", "json", "--output", str(out_file)])
    structured = json.loads(out_file.read_text(encoding="utf-8"))
    assert structured["schema_version"] == "1.0"
    assert structured["metrics"][0]["metric"] == "ph"
    assert len(structured["assessment_fingerprint"]) == 64


def test_invalid_arguments_are_rejected(tmp_path):
    clock = FakeClock(datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc))
    db, _ = ingest(tmp_path, "args.json", [], clock)
    with pytest.raises(ValueError, match="window end"):
        assess_window(db, "W-01", WINDOW_END, WINDOW_START, ["ph"])
    with pytest.raises(ValueError, match="unsupported metrics"):
        assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["mercury"])
    with pytest.raises(ValueError, match="non-negative integer"):
        assess_window(db, "W-01", WINDOW_START, WINDOW_END, ["ph"],
                      policy_overrides={"ph": {"min_observations": -1}})
    with pytest.raises(FileNotFoundError):
        assess_window(str(tmp_path / "missing.sqlite3"), "W-01",
                      WINDOW_START, WINDOW_END, ["ph"])


def test_default_policy_covers_every_supported_metric():
    assert set(DEFAULT_POLICY) == set(SUPPORTED_METRICS)
    for config in DEFAULT_POLICY.values():
        assert config["min_observations"] >= 1
        assert config["max_gap_seconds"] > 0
