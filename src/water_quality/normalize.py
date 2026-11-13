from __future__ import annotations

import hashlib
import json
from typing import Any

from .models import WaterQualityRecord
from .identity import record_key


def _text(value: Any) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("numeric field is invalid") from exc


def normalize(raw: Any, source: str) -> WaterQualityRecord:
    if not isinstance(raw, dict):
        raise ValueError("record must be an object")
    payload = raw.get("payload")
    observed_at = _text(raw.get("observed_at"))
    if not isinstance(payload, dict) or not observed_at:
        raise ValueError("observed_at and object payload are required")

    station_id = _text(payload.get("station_id"))
    sample_id = _text(payload.get("sample_id"))
    metric = _text(payload.get("metric")).lower()
    value = _number(payload.get("value"))
    unit = _text(payload.get("unit")).lower()
    quality_flag = _text(payload.get("quality_flag")).lower() or "unknown"
    if not station_id or not sample_id or not observed_at or not metric or value is None or not unit:
        raise ValueError("station_id, sample_id, observed_at, metric, value and unit are required")
    if metric not in {"ph", "dissolved_oxygen", "ammonia_n", "total_phosphorus", "lead"}:
        raise ValueError("unsupported water quality metric")
    if quality_flag not in {"good", "suspect", "bad", "unknown"}:
        raise ValueError("unsupported quality flag")

    normalized = {"station_id": station_id, "sample_id": sample_id, "metric": metric, "value": value, "unit": unit, "quality_flag": quality_flag}
    key = record_key(station_id, sample_id, metric, observed_at)
    canonical = json.dumps({"source": source, "observed_at": observed_at, "payload": normalized}, ensure_ascii=False, sort_keys=True)
    raw_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return WaterQualityRecord(
        source=source,
        record_key=key,
        observed_at=observed_at,
        station_id=normalized["station_id"],
        sample_id=normalized["sample_id"],
        metric=normalized["metric"],
        value=normalized["value"],
        unit=normalized["unit"],
        quality_flag=normalized["quality_flag"],
        payload=normalized,
        raw_hash=raw_hash,
    )
