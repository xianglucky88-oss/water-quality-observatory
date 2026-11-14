"""Controlled re-evaluation when metric/unit rules change.

Rule upgrades must never silently rewrite history. This module first *previews*
which existing formal observations (and open quarantine rows) are affected by
a new catalog; a separate explicit apply run performs the writes. Preview
runs create no formal observation and no quarantine row.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .normalize import normalize
from .quality import validate
from .rules import RuleCatalog, RuleViolation
from .storage import DataStore

PREVIEW = "preview"
APPLY = "apply"

ACTION_UNCHANGED = "unchanged"
ACTION_UPDATE = "update"
ACTION_QUARANTINE = "quarantine"
ACTION_RELEASE = "release_from_quarantine"


@dataclass(frozen=True)
class ReprocessOutcome:
    run_id: int
    mode: str
    affected: int
    unchanged: int
    quarantined: int
    items: list[dict[str, Any]]


def _record_to_envelope(row: Any) -> dict[str, Any]:
    """Reconstruct the original source envelope from a stored formal row."""
    raw_json = row["source_raw_json"]
    if raw_json:
        return json.loads(raw_json)
    payload = json.loads(row["payload_json"])
    # Legacy rows written before source envelopes were retained.
    return {
        "source": row["source"],
        "observed_at": row["observed_at"],
        "payload": {
            "station_id": payload.get("station_id"),
            "sample_id": payload.get("sample_id"),
            "metric": payload.get("metric"),
            "value": payload.get("reported_value_text"),
            "unit": payload.get("reported_unit") or payload.get("unit"),
            "quality_flag": payload.get("quality_flag"),
            "qualifier": payload.get("qualifier"),
            "detection_limit": payload.get("detection_limit"),
        },
    }


def reprocess_records(
    store: DataStore,
    catalog: RuleCatalog,
    mode: str,
    source: str = "rule-reprocess",
) -> ReprocessOutcome:
    if mode not in (PREVIEW, APPLY):
        raise ValueError("mode must be 'preview' or 'apply'")
    store.register_rule_version(
        catalog.version,
        {m: catalog.get(m).fingerprint() for m in sorted(catalog.metrics)},
    )
    run_id = store.start_reprocess_run(None, catalog.version, mode)
    affected = unchanged = quarantined = 0
    items: list[dict[str, Any]] = []

    for row in store.iter_records():
        record_key = row["record_key"]
        before = json.loads(row["payload_json"])
        prior_version = row["rule_version"]
        envelope = _record_to_envelope(row)
        try:
            candidate = normalize(envelope, row["source"], catalog, row["raw_digest"])
            issues = validate(candidate, catalog)
            if issues:
                raise RuleViolation("OUTSIDE_DOMAIN_RANGE", "; ".join(issues))
        except RuleViolation as exc:
            quarantined += 1
            affected += 1
            items.append(
                {
                    "record_key": record_key,
                    "action": ACTION_QUARANTINE,
                    "reason_code": exc.code,
                    "detail": exc.message,
                    "before": before,
                    "after": None,
                }
            )
            if mode == APPLY:
                store.quarantine(
                    envelope, row["source"], exc.code, exc.message,
                    catalog.version, reprocess_run_id=run_id, record_key=record_key,
                )
                store.delete_record(record_key)
            store.add_reprocess_item(
                run_id, record_key, ACTION_QUARANTINE, exc.code, exc.message, before, None
            )
            continue

        # A record is "affected" only when the derived comparison semantics actually
        # change. A pure rule-version bump with identical outputs is NOT an impact:
        # the row stays unchanged and is never rewritten.
        derived_changed = (
            before.get("value") != candidate.value
            or before.get("unit") != candidate.unit
            or before.get("comparison_semantics") != candidate.comparison_semantics
            or before.get("qualifier") != candidate.qualifier
            or before.get("detection_limit") != candidate.detection_limit
            or before.get("conversion_factor") != candidate.conversion_factor
        )
        rule_metadata_changed = (
            prior_version != candidate.rule_version
            or row["rule_fingerprint"] != candidate.rule_fingerprint
        )
        if derived_changed:
            affected += 1
            items.append(
                {
                    "record_key": record_key,
                    "action": ACTION_UPDATE,
                    "reason_code": None,
                    "detail": (
                        f"rule {prior_version or 'unknown'} -> {candidate.rule_version}; "
                        f"fingerprint {row['rule_fingerprint']} -> {candidate.rule_fingerprint}"
                    ),
                    "before": before,
                    "after": candidate.payload,
                }
            )
            store.add_reprocess_item(
                run_id, record_key, ACTION_UPDATE, None,
                f"rule {prior_version or 'unknown'} -> {candidate.rule_version}",
                before, candidate.payload,
            )
            if mode == APPLY:
                store.upsert(candidate, source_raw=envelope)
        else:
            unchanged += 1
            if rule_metadata_changed:
                detail = (
                    f"rule metadata {prior_version or 'unknown'} -> {candidate.rule_version} "
                    "but derived value identical; not recomputed"
                )
            else:
                detail = "derived value identical; not recomputed"
            items.append(
                {
                    "record_key": record_key,
                    "action": ACTION_UNCHANGED,
                    "reason_code": None,
                    "detail": detail,
                    "before": before,
                    "after": candidate.payload,
                }
            )
            store.add_reprocess_item(
                run_id, record_key, ACTION_UNCHANGED, None, detail, before, candidate.payload,
            )

    # Re-evaluate open quarantine rows: new rules may now accept them.
    released = _reevaluate_quarantine(store, catalog, mode, run_id, items)
    summary = {
        "released_from_quarantine": released,
        "rule_version": catalog.version,
    }
    store.finish_reprocess_run(run_id, "completed", affected, unchanged, quarantined, summary)
    return ReprocessOutcome(run_id, mode, affected, unchanged, quarantined, items)


def _reevaluate_quarantine(
    store: DataStore, catalog: RuleCatalog, mode: str, run_id: int, items: list[dict[str, Any]]
) -> int:
    released = 0
    for row in store.list_open_quarantine():
        raw = json.loads(row["raw_json"])
        try:
            candidate = normalize(raw, row["source"] or "quarantine", catalog)
            issues = validate(candidate, catalog)
            if issues:
                raise RuleViolation("OUTSIDE_DOMAIN_RANGE", "; ".join(issues))
        except RuleViolation:
            continue
        released += 1
        items.append(
            {
                "record_key": candidate.record_key,
                "action": ACTION_RELEASE,
                "reason_code": None,
                "detail": f"quarantine {row['quarantine_id']} now passes under {catalog.version}",
                "before": {"reason_code": row["reason_code"], "error": row["error"]},
                "after": candidate.payload,
            }
        )
        store.add_reprocess_item(
            run_id, candidate.record_key, ACTION_RELEASE, None,
            f"quarantine {row['quarantine_id']} now passes",
            {"reason_code": row["reason_code"], "error": row["error"]}, candidate.payload,
        )
        if mode == APPLY:
            store.upsert(candidate, source_raw=raw)
            store.mark_quarantine_status(row["quarantine_id"], "released")
    return released


def preview_affected(store: DataStore, catalog: RuleCatalog) -> ReprocessOutcome:
    """Read-only impact assessment; never writes observations or quarantine."""
    return reprocess_records(store, catalog, PREVIEW)
