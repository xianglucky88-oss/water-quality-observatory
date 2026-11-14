from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from water_quality.application import reprocess, run_fixture
from water_quality.models import WaterQualityRecord
from water_quality.normalize import normalize
from water_quality.reprocess import ACTION_QUARANTINE, ACTION_UNCHANGED, ACTION_UPDATE, ACTION_RELEASE
from water_quality.rules import (
    DEFAULT_RULES,
    MetricRule,
    RuleCatalog,
    RuleViolation,
)
from water_quality.storage import DataStore

FIXTURE = Path(__file__).parents[1] / "fixtures" / "lab_samples.json"
BASELINE = Path(__file__).parents[1] / "fixtures" / "sample.json"


def _raw_records() -> list[dict]:
    return json.loads(FIXTURE.read_text())


def _find(sample_id: str, records: list[dict]) -> dict:
    return next(r for r in records if r["payload"]["sample_id"] == sample_id)


# --------------------------------------------------------------------------- #
# Mixed units: only explicit catalog factors are converted
# --------------------------------------------------------------------------- #

def test_mixed_units_convert_through_explicit_factor_only():
    records = _raw_records()
    converted = normalize(_find("S-101", records), "lab-a")  # < 0.01 mg/L
    assert converted.reported_unit == "mg/l"
    assert converted.unit == "mg/l"

    ug_record_raw = next(r for r in records if r["payload"].get("value") == 10)
    ug_record = normalize(ug_record_raw, "lab-a")
    assert ug_record.reported_unit == "ug/l"
    assert ug_record.unit == "mg/l"
    assert ug_record.conversion_factor == 0.001
    assert ug_record.value == pytest.approx(0.01)
    assert ug_record.comparison_semantics == "exact"


def test_unknown_unit_is_quarantined_never_guessed(tmp_path):
    result = run_fixture(str(FIXTURE), str(tmp_path / "db.sqlite3"), str(tmp_path / "raw"))
    assert result["processed"] == 12
    assert result["accepted"] == 6
    assert result["rejected"] == 6

    store = DataStore(str(tmp_path / "db.sqlite3"))
    rows = store.conn.execute(
        "SELECT reason_code, error, raw_json FROM quarantine_records WHERE reason_code='UNKNOWN_UNIT'"
    ).fetchall()
    assert len(rows) == 1
    raw = json.loads(rows[0]["raw_json"])
    assert raw["payload"]["unit"] == "ppb"
    # Formal observations must never carry the unknown unit.
    formal = store.conn.execute("SELECT COUNT(*) AS c FROM records").fetchone()["c"]
    assert formal == 6
    store.close()


# --------------------------------------------------------------------------- #
# Case and whitespace tolerance
# --------------------------------------------------------------------------- #

def test_case_and_whitespace_are_normalized_but_reported_text_retained():
    records = _raw_records()
    censored = normalize(_find("S-101", records), "lab-a")
    assert censored.station_id == "W-01"
    assert censored.metric == "lead"
    assert censored.reported_unit == "mg/l"
    # The original expression stays queryable, verbatim.
    assert censored.reported_value_text == "< 0.01"
    assert censored.qualifier == "<"


def test_unicode_microgram_spelling_resolves_to_canonical_unit():
    raw = {
        "source": "lab",
        "observed_at": "2026-11-02T10:00:00Z",
        "payload": {
            "station_id": "W-9", "sample_id": "S-900", "metric": "lead",
            "value": 7, "unit": "µg/L", "quality_flag": "good",
        },
    }
    record = normalize(raw, "lab")
    assert record.reported_unit == "ug/l"
    assert record.unit == "mg/l"
    assert record.value == pytest.approx(0.007)


# --------------------------------------------------------------------------- #
# Below detection limit: never zero, limit retained
# --------------------------------------------------------------------------- #

def test_censored_result_keeps_limit_as_comparable_value_not_zero():
    records = _raw_records()
    censored = normalize(_find("S-101", records), "lab-a")
    assert censored.comparison_semantics == "left_censored"
    assert censored.value == pytest.approx(0.01)
    assert censored.value != 0
    assert censored.detection_limit == pytest.approx(0.01)
    assert censored.qualifier == "<"
    assert censored.reported_value == pytest.approx(0.01)


def test_non_detect_token_requires_explicit_detection_limit():
    records = _raw_records()
    nd = normalize(_find("S-102", records), "lab-a")
    assert nd.qualifier == "ND"
    assert nd.comparison_semantics == "left_censored"
    assert nd.value == pytest.approx(0.02)
    assert nd.reported_value is None
    assert nd.reported_value_text == "ND"

    with pytest.raises(RuleViolation) as exc:
        normalize(_find("S-103", records), "lab-a")
    assert exc.value.code == "DETECTION_LIMIT_MISSING"


def test_non_detect_in_noncanonical_unit_still_converts_limit():
    raw = {
        "source": "lab",
        "observed_at": "2026-11-02T10:15:00Z",
        "payload": {
            "station_id": "W-9", "sample_id": "S-903", "metric": "ammonia_n",
            "value": "ND", "unit": "ug/L", "quality_flag": "good",
            "detection_limit": 20,
        },
    }
    record = normalize(raw, "lab")
    assert record.qualifier == "ND"
    assert record.detection_limit == pytest.approx(20.0)  # retained in reported unit
    assert record.value == pytest.approx(0.02)  # 20 ug/L -> 0.02 mg/L, never zero
    assert record.comparison_semantics == "left_censored"


def test_embedded_lt_supplies_its_own_limit_when_field_absent():
    raw = {
        "source": "lab",
        "observed_at": "2026-11-02T10:05:00Z",
        "payload": {
            "station_id": "W-9", "sample_id": "S-901", "metric": "lead",
            "value": "<0.02", "unit": "mg/L", "quality_flag": "good",
        },
    }
    record = normalize(raw, "lab")
    assert record.detection_limit == pytest.approx(0.02)
    assert record.value == pytest.approx(0.02)
    assert record.comparison_semantics == "left_censored"


def test_detection_limit_is_kept_in_reported_unit_but_value_converted():
    raw = {
        "source": "lab",
        "observed_at": "2026-11-02T10:10:00Z",
        "payload": {
            "station_id": "W-9", "sample_id": "S-902", "metric": "lead",
            "value": "<5", "unit": "ug/L", "quality_flag": "good",
            "detection_limit": 5,
        },
    }
    record = normalize(raw, "lab")
    # The reported limit stays in its original unit...
    assert record.detection_limit == pytest.approx(5.0)
    assert record.reported_unit == "ug/l"
    # ...while the comparable value is explicitly converted to the canonical unit, never zero.
    assert record.value == pytest.approx(0.005)
    assert record.unit == "mg/l"
    assert record.comparison_semantics == "left_censored"
    assert record.qualifier == "<"


def test_implausible_detection_limit_is_quarantined(tmp_path):
    run_fixture(str(FIXTURE), str(tmp_path / "db.sqlite3"), str(tmp_path / "raw"))
    store = DataStore(str(tmp_path / "db.sqlite3"))
    rows = store.conn.execute(
        "SELECT reason_code FROM quarantine_records WHERE reason_code='DETECTION_LIMIT_INVALID'"
    ).fetchall()
    assert len(rows) == 1
    store.close()


# --------------------------------------------------------------------------- #
# Contradictory expressions
# --------------------------------------------------------------------------- #

def test_contradictory_qualifier_and_value_are_rejected():
    records = _raw_records()
    with pytest.raises(RuleViolation) as exc:
        normalize(_find("S-106", records), "lab-a")
    assert exc.value.code == "CONFLICTING_EXPRESSION"


def test_exact_value_below_own_detection_limit_is_contradiction():
    records = _raw_records()
    with pytest.raises(RuleViolation) as exc:
        normalize(_find("S-108", records), "lab-a")
    assert exc.value.code == "CONFLICTING_EXPRESSION"


def test_unparseable_value_expression_is_rejected():
    records = _raw_records()
    with pytest.raises(RuleViolation) as exc:
        normalize(_find("S-107", records), "lab-a")
    assert exc.value.code == "CONFLICTING_EXPRESSION"


# --------------------------------------------------------------------------- #
# Same water sample, two reporting styles; both survive distinctly
# --------------------------------------------------------------------------- #

def test_same_sample_accepts_two_reporting_styles(tmp_path):
    run_fixture(str(FIXTURE), str(tmp_path / "db.sqlite3"), str(tmp_path / "raw"))
    store = DataStore(str(tmp_path / "db.sqlite3"))
    rows = store.conn.execute(
        "SELECT * FROM records WHERE payload_json LIKE '%S-101%' ORDER BY observed_at"
    ).fetchall()
    assert len(rows) == 2
    semantics = {json.loads(r["payload_json"])["comparison_semantics"] for r in rows}
    assert semantics == {"exact", "left_censored"}
    values = sorted(json.loads(r["payload_json"])["value"] for r in rows)
    assert values == pytest.approx([0.01, 0.01])
    units = {json.loads(r["payload_json"])["unit"] for r in rows}
    assert units == {"mg/l"}
    store.close()


# --------------------------------------------------------------------------- #
# Traceability: query results lead back to the original expression
# --------------------------------------------------------------------------- #

def test_query_can_trace_back_to_reported_expression(tmp_path):
    db = tmp_path / "db.sqlite3"
    run_fixture(str(FIXTURE), str(db), str(tmp_path / "raw"))
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """SELECT * FROM records
           WHERE qualifier='<' AND reported_unit='mg/l'
             AND comparison_semantics='left_censored'"""
    ).fetchone()
    assert row is not None
    payload = json.loads(row["payload_json"])
    source_envelope = json.loads(row["source_raw_json"])
    assert payload["reported_value_text"] == "< 0.01"
    assert payload["detection_limit"] == 0.01
    # The exact source bytes are retained for replay/audit.
    assert source_envelope["payload"]["value"] == "  < 0.01 "
    assert source_envelope["payload"]["unit"] == " mg/L "
    assert row["rule_version"] == DEFAULT_RULES.version
    manifest = conn.execute("SELECT COUNT(*) AS c FROM raw_manifests").fetchone()["c"]
    assert manifest == 1
    conn.close()


# --------------------------------------------------------------------------- #
# Duplicate import: idempotent, no recomputation of unchanged records
# --------------------------------------------------------------------------- #

def test_duplicate_import_is_idempotent_and_raw_landing_dedupes(tmp_path):
    db = tmp_path / "db.sqlite3"
    raw_dir = tmp_path / "raw"
    first = run_fixture(str(FIXTURE), str(db), str(raw_dir))
    second = run_fixture(str(FIXTURE), str(db), str(raw_dir))
    assert first["accepted"] == 6
    assert second["accepted"] == 6
    assert second["changed"] == 0
    assert second["records"] == 6
    # Content-addressed raw landing keeps a single copy.
    assert len(list(raw_dir.glob("*.json"))) == 1

    conn = sqlite3.connect(db)
    rows = conn.execute("SELECT updated_at, first_seen_at FROM records").fetchall()
    # No row was rewritten by the replay: updated_at stays equal to first_seen_at.
    assert all(updated == seen for updated, seen in rows)
    conn.close()


def test_baseline_pipeline_still_idempotent(tmp_path):
    db = tmp_path / "baseline.sqlite3"
    first = run_fixture(str(BASELINE), str(db), str(tmp_path / "raw"))
    second = run_fixture(str(BASELINE), str(db), str(tmp_path / "raw"))
    assert first["processed"] == 4
    assert first["accepted"] == 3
    assert first["rejected"] == 1
    assert second["changed"] == 0
    assert second["records"] == 3
    assert second["batches"] == 2
    assert second["raw_manifests"] == 1


# --------------------------------------------------------------------------- #
# Rule upgrade: preview vs apply, affected identification, no silent rewrite
# --------------------------------------------------------------------------- #

UPGRADED_RULES = RuleCatalog(
    version="2026.12.0",
    metrics={
        "ph": MetricRule("ph", "ph", {"ph": 1.0}, (0.0, 14.0), 14.0),
        "dissolved_oxygen": MetricRule("dissolved_oxygen", "mg/l", {"mg/l": 1.0}, (0.0, 30.0), 30.0),
        "ammonia_n": MetricRule("ammonia_n", "mg/l", {"mg/l": 1.0, "ug/l": 0.001}, (0.0, 1000.0), 1000.0),
        # New: ppb becomes an explicitly declared unit (treated as ug/L alias factor).
        "total_phosphorus": MetricRule(
            "total_phosphorus", "mg/l",
            {"mg/l": 1.0, "ug/l": 0.001, "ppb": 0.001}, (0.0, 1000.0), 1000.0,
        ),
        # Tightened plausible detection-limit ceiling: old 0.01 mg/L lead rows affected.
        "lead": MetricRule("lead", "mg/l", {"mg/l": 1.0, "ug/l": 0.001}, (0.0, 100.0), 0.005),
    },
)


def test_rule_upgrade_preview_changes_nothing(tmp_path):
    db = tmp_path / "upgrade.sqlite3"
    run_fixture(str(FIXTURE), str(db), str(tmp_path / "raw"))
    store = DataStore(str(db))
    before = {r["record_key"]: dict(r) for r in store.iter_records()}
    quarantine_before = store.conn.execute("SELECT COUNT(*) AS c FROM quarantine_records").fetchone()["c"]

    outcome = reprocess(str(db), UPGRADED_RULES, apply=False)
    assert outcome.mode == "preview"

    store = DataStore(str(db))
    after = {r["record_key"]: dict(r) for r in store.iter_records()}
    assert set(before) == set(after)
    for key in before:
        assert before[key]["payload_json"] == after[key]["payload_json"]
        assert before[key]["rule_version"] == after[key]["rule_version"]
    assert store.conn.execute("SELECT COUNT(*) AS c FROM quarantine_records").fetchone()["c"] == quarantine_before
    store.close()


def test_rule_upgrade_preview_identifies_affected_and_unchanged_records(tmp_path):
    db = tmp_path / "upgrade2.sqlite3"
    run_fixture(str(FIXTURE), str(db), str(tmp_path / "raw"))
    outcome = reprocess(str(db), UPGRADED_RULES, apply=False)

    actions = {(i["record_key"], i["action"]) for i in outcome.items}
    quarantines = [i for i in outcome.items if i["action"] == ACTION_QUARANTINE]
    unchanged = [i for i in outcome.items if i["action"] == ACTION_UNCHANGED]
    releases = [i for i in outcome.items if i["action"] == ACTION_RELEASE]

    # The lead censored row (limit 0.01 > new ceiling 0.005) becomes invalid.
    assert any("LEAD" in key and action == ACTION_QUARANTINE for key, action in actions)
    # pH exact record is unaffected: version metadata may change, value is never recomputed.
    ph_items = [i for i in unchanged if "|PH|" in i["record_key"]]
    assert ph_items and all("derived value identical" in i["detail"] for i in ph_items)
    # The ppb quarantine row now passes and is offered for release.
    assert releases and all("TOTAL_PHOSPHORUS" in i["record_key"] for i in releases)
    assert outcome.affected >= 1
    assert outcome.quarantined == len(quarantines)
    assert len(unchanged) >= 1


def test_rule_upgrade_apply_writes_only_affected_and_keeps_audit_trail(tmp_path):
    db = tmp_path / "upgrade3.sqlite3"
    run_fixture(str(FIXTURE), str(db), str(tmp_path / "raw"))
    preview = reprocess(str(db), UPGRADED_RULES, apply=False)
    outcome = reprocess(str(db), UPGRADED_RULES, apply=True)
    assert outcome.mode == "apply"

    store = DataStore(str(db))
    run = store.conn.execute("SELECT * FROM reprocess_runs ORDER BY run_id").fetchall()
    assert [r["mode"] for r in run] == ["preview", "apply"]
    applied = run[-1]
    assert applied["affected"] == preview.affected
    assert applied["status"] == "completed"

    # Quarantined formal observation was removed from the formal table and explained.
    q = store.conn.execute(
        "SELECT reason_code, record_key, rule_version FROM quarantine_records "
        "WHERE reprocess_run_id=? AND reason_code='DETECTION_LIMIT_INVALID'",
        (outcome.run_id,),
    ).fetchall()
    assert len(q) == 1
    assert q[0]["rule_version"] == "2026.12.0"
    assert store.conn.execute(
        "SELECT COUNT(*) AS c FROM records WHERE record_key=?", (q[0]["record_key"],)
    ).fetchone()["c"] == 0

    # Released ppb row is now a formal observation under the new rules.
    released = store.conn.execute(
        "SELECT * FROM records WHERE reported_unit='ppb'"
    ).fetchall()
    assert len(released) == 1
    payload = json.loads(released[0]["payload_json"])
    assert payload["unit"] == "mg/l"
    assert payload["value"] == pytest.approx(0.0003)
    assert payload["rule_version"] == "2026.12.0"

    # Unchanged rows keep their original rule version and timestamps.
    untouched = store.conn.execute(
        "SELECT * FROM records WHERE payload_json LIKE '%\"metric\": \"ph\"%'"
    ).fetchall()
    assert all(r["rule_version"] == DEFAULT_RULES.version for r in untouched)
    store.close()


def test_rule_fingerprints_change_when_catalog_changes():
    old = DEFAULT_RULES.fingerprint("lead")
    new = UPGRADED_RULES.fingerprint("lead")
    assert old != new
    # Unchanged metric keeps an identical fingerprint.
    assert DEFAULT_RULES.fingerprint("ph") == UPGRADED_RULES.fingerprint("ph")


def test_factor_correction_produces_explicit_update_path(tmp_path):
    """A wrong-but-declared factor fixed by a new catalog rewrites the comparable value."""
    db = tmp_path / "factor.sqlite3"
    initial = RuleCatalog(
        version="2026.10.0",
        metrics={
            name: rule if name != "lead"
            else MetricRule("lead", "mg/l", {"mg/l": 1.0, "ug/l": 0.01}, (0.0, 100.0), 100.0)
            for name, rule in DEFAULT_RULES.metrics.items()
        },
    )
    corrected = RuleCatalog(
        version="2026.11.0",
        metrics={
            name: rule if name != "lead"
            else MetricRule("lead", "mg/l", {"mg/l": 1.0, "ug/l": 0.001}, (0.0, 100.0), 100.0)
            for name, rule in DEFAULT_RULES.metrics.items()
        },
    )
    fixture = tmp_path / "lead.json"
    fixture.write_text(json.dumps([
        {
            "source": "lab",
            "observed_at": "2026-11-02T11:00:00Z",
            "payload": {
                "station_id": "W-7", "sample_id": "S-700", "metric": "lead",
                "value": 10, "unit": "ug/L", "quality_flag": "good",
            },
        }
    ]))
    run_fixture(str(fixture), str(db), str(tmp_path / "raw"), catalog=initial)
    store = DataStore(str(db))
    before = store.conn.execute("SELECT * FROM records").fetchone()
    assert json.loads(before["payload_json"])["value"] == pytest.approx(0.1)
    store.close()

    preview = reprocess(str(db), corrected, apply=False)
    update = [i for i in preview.items if i["action"] == ACTION_UPDATE]
    assert len(update) == 1
    assert update[0]["before"]["value"] == pytest.approx(0.1)
    assert update[0]["after"]["value"] == pytest.approx(0.01)
    # Preview must not have touched the stored row.
    store = DataStore(str(db))
    assert json.loads(store.conn.execute("SELECT payload_json FROM records").fetchone()["payload_json"])["value"] == pytest.approx(0.1)
    store.close()

    applied = reprocess(str(db), corrected, apply=True)
    assert applied.affected == 1
    store = DataStore(str(db))
    row = store.conn.execute("SELECT * FROM records").fetchone()
    assert json.loads(row["payload_json"])["value"] == pytest.approx(0.01)
    assert row["rule_version"] == "2026.11.0"
    store.close()


# --------------------------------------------------------------------------- #
# Ingest preview writes nothing
# --------------------------------------------------------------------------- #

def test_ingest_preview_is_read_only(tmp_path):
    db = tmp_path / "preview.sqlite3"
    result = run_fixture(str(FIXTURE), str(db), str(tmp_path / "raw"), preview=True)
    assert result["mode"] == "preview"
    assert len(result["decisions"]) == 12
    assert {d["status"] for d in result["decisions"]} == {"accepted", "quarantined"}
    store = DataStore(str(db))
    assert store.conn.execute("SELECT COUNT(*) AS c FROM records").fetchone()["c"] == 0
    assert store.conn.execute("SELECT COUNT(*) AS c FROM quarantine_records").fetchone()["c"] == 0
    assert store.conn.execute("SELECT COUNT(*) AS c FROM batches").fetchone()["c"] == 0
    # Raw landing still happens for replay even in preview.
    assert store.conn.execute("SELECT COUNT(*) AS c FROM raw_manifests").fetchone()["c"] == 1
    store.close()


# --------------------------------------------------------------------------- #
# Existing normal records are not wrongly recomputed by an identical rule set
# --------------------------------------------------------------------------- #

def test_reprocess_under_same_rules_marks_everything_unchanged(tmp_path):
    db = tmp_path / "same.sqlite3"
    run_fixture(str(FIXTURE), str(db), str(tmp_path / "raw"))
    store = DataStore(str(db))
    before = {r["record_key"]: r["updated_at"] for r in store.iter_records()}
    store.close()

    outcome = reprocess(str(db), DEFAULT_RULES, apply=True)
    assert outcome.affected == 0
    assert outcome.quarantined == 0
    assert all(i["action"] == ACTION_UNCHANGED for i in outcome.items if i["action"] != ACTION_RELEASE)

    store = DataStore(str(db))
    after = {r["record_key"]: r["updated_at"] for r in store.iter_records()}
    assert before == after
    store.close()


def test_record_carries_full_reporting_and_comparison_fields():
    records = _raw_records()
    record: WaterQualityRecord = normalize(_find("S-110", records), "lab-a")
    assert record.metric == "total_phosphorus"
    assert record.reported_unit == "ug/l"
    assert record.unit == "mg/l"
    assert record.value == pytest.approx(0.2)
    assert record.conversion_factor == 0.001
    assert record.comparison_semantics == "exact"
    assert record.rule_fingerprint
    assert record.raw_hash


# --------------------------------------------------------------------------- #
# Legacy database migration
# --------------------------------------------------------------------------- #

def test_pre_upgrade_database_is_migrated_without_data_loss(tmp_path):
    db = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE batches (batch_id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, status TEXT,
            processed INTEGER, accepted INTEGER, rejected INTEGER, error TEXT, started_at TEXT, finished_at TEXT);
        CREATE TABLE raw_manifests (digest TEXT PRIMARY KEY, source TEXT, content_type TEXT,
            byte_count INTEGER, path TEXT, captured_at TEXT);
        CREATE TABLE records (record_key TEXT PRIMARY KEY, source TEXT, observed_at TEXT,
            payload_json TEXT, raw_hash TEXT, first_seen_at TEXT, updated_at TEXT);
        CREATE TABLE rejected_records (rejection_id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER,
            source TEXT, error TEXT, raw_json TEXT, created_at TEXT);
        INSERT INTO records VALUES ('W-01|S-001|PH|2026-11-01T10:00:00Z', 'fixture',
            '2026-11-01T10:00:00Z', '{"metric":"ph","value":7.2,"unit":"ph"}', 'abc',
            '2026-11-01T00:00:00+00:00', '2026-11-01T00:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()

    store = DataStore(str(db))
    columns = {r["name"] for r in store.conn.execute("PRAGMA table_info(records)")}
    assert {"rule_version", "rule_fingerprint", "source_raw_json", "comparison_semantics"} <= columns
    row = store.conn.execute("SELECT * FROM records").fetchone()
    assert row["rule_version"] is None  # legacy provenance preserved, never back-filled silently
    assert json.loads(row["payload_json"])["value"] == 7.2
    assert bool(store.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='quarantine_records'"
    ).fetchone())
    store.close()


def test_quarantine_rows_are_deduplicated_on_replay(tmp_path):
    db = tmp_path / "q.sqlite3"
    run_fixture(str(FIXTURE), str(db), str(tmp_path / "raw"))
    run_fixture(str(FIXTURE), str(db), str(tmp_path / "raw"))
    store = DataStore(str(db))
    # Each distinct failure expression appears exactly once despite two imports.
    rows = store.conn.execute(
        "SELECT reason_code, raw_json, COUNT(*) AS c FROM quarantine_records "
        "WHERE reprocess_run_id IS NULL GROUP BY reason_code, raw_json"
    ).fetchall()
    assert rows and all(r["c"] == 1 for r in rows)
    assert store.conn.execute(
        "SELECT COUNT(*) FROM quarantine_records WHERE reprocess_run_id IS NULL"
    ).fetchone()[0] == 6
    store.close()
