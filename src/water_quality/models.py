from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias


@dataclass(frozen=True)
class WaterQualityRecord:
    # --- identity / provenance ---
    source: str
    record_key: str
    observed_at: str
    station_id: str
    sample_id: str
    metric: str
    quality_flag: str
    # --- reported expression, retained for traceability ---
    reported_value_text: str
    reported_value: float | None
    qualifier: str  # canonical vocabulary: "=" | "<" | "ND"
    detection_limit: float | None
    reported_unit: str  # casefolded source spelling, before any conversion
    # --- normalized comparison semantics ---
    value: float | None  # comparable value; for censored results this is the detection limit, never zero
    unit: str  # canonical unit of the metric rule
    comparison_semantics: str  # "exact" | "left_censored"
    conversion_factor: float
    rule_version: str
    rule_fingerprint: str
    # --- bookkeeping ---
    payload: dict[str, Any]
    raw_hash: str
    raw_digest: str | None = None


DOMAIN_RECORD_TYPE: TypeAlias = WaterQualityRecord
