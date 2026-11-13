from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias


@dataclass(frozen=True)
class WaterQualityRecord:
    source: str
    record_key: str
    observed_at: str
    station_id: str
    sample_id: str
    metric: str
    value: float
    unit: str
    quality_flag: str
    payload: dict[str, Any]
    raw_hash: str


DOMAIN_RECORD_TYPE: TypeAlias = WaterQualityRecord
