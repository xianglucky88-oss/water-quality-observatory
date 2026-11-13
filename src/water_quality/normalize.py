from __future__ import annotations

import hashlib
import json
from typing import Any

from .identity import record_key
from .models import WaterQualityRecord, WithdrawalDirective

SUPPORTED_METRICS = {"ph", "dissolved_oxygen", "ammonia_n", "total_phosphorus", "lead"}
SUPPORTED_QUALITY_FLAGS = {"good", "suspect", "bad", "unknown"}
SUPPORTED_ACTIONS = {"report", "withdraw"}


def _text(value: Any) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("numeric field is invalid") from exc


def _canonical_hash(value: dict[str, Any]) -> str:
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize(raw: Any, source: str) -> WaterQualityRecord:
    if not isinstance(raw, dict):
        raise ValueError("record must be an object")
    payload = raw.get("payload")
    observed_at = _text(raw.get("observed_at"))
    if not isinstance(payload, dict) or not observed_at:
        raise ValueError("observed_at and object payload are required")

    # The record's own source drives precedence adjudication; the batch source
    # (e.g. fixture://..., https://...) remains traceable via batch_id/manifest.
    record_source = _text(raw.get("source")) or source

    station_id = _text(payload.get("station_id"))
    sample_id = _text(payload.get("sample_id"))
    metric = _text(payload.get("metric")).lower()
    value = _number(payload.get("value"))
    unit = _text(payload.get("unit")).lower()
    quality_flag = _text(payload.get("quality_flag")).lower() or "unknown"
    if not station_id or not sample_id or not observed_at or not metric or value is None or not unit:
        raise ValueError("station_id, sample_id, observed_at, metric, value and unit are required")
    if metric not in SUPPORTED_METRICS:
        raise ValueError("unsupported water quality metric")
    if quality_flag not in SUPPORTED_QUALITY_FLAGS:
        raise ValueError("unsupported quality flag")

    normalized = {
        "station_id": station_id,
        "sample_id": sample_id,
        "metric": metric,
        "value": value,
        "unit": unit,
        "quality_flag": quality_flag,
    }
    key = record_key(station_id, sample_id, metric, observed_at)
    canonical = {"source": record_source, "observed_at": observed_at, "payload": normalized}
    raw_hash = _canonical_hash(canonical)
    # Content fingerprint is independent of source, observation time and quality flag,
    # so a preliminary report and a confirming retest that carry the same measurement hash equal.
    content = {"station_id": station_id, "sample_id": sample_id, "metric": metric, "value": value, "unit": unit}
    content_hash = _canonical_hash(content)
    return WaterQualityRecord(
        source=record_source,
        record_key=key,
        observed_at=observed_at,
        station_id=station_id,
        sample_id=sample_id,
        metric=metric,
        value=value,
        unit=unit,
        quality_flag=quality_flag,
        payload=normalized,
        raw_hash=raw_hash,
        content_hash=content_hash,
    )


def normalize_withdrawal(raw: Any, source: str) -> WithdrawalDirective:
    """Parse a withdraw envelope. The target is identified by the same natural key fields."""
    if not isinstance(raw, dict):
        raise ValueError("withdrawal directive must be an object")
    payload = raw.get("payload")
    observed_at = _text(raw.get("observed_at"))
    if not isinstance(payload, dict) or not observed_at:
        raise ValueError("observed_at and object payload are required")
    station_id = _text(payload.get("station_id"))
    sample_id = _text(payload.get("sample_id"))
    metric = _text(payload.get("metric")).lower()
    if not station_id or not sample_id or not metric:
        raise ValueError("station_id, sample_id and metric are required for a withdrawal")
    if metric not in SUPPORTED_METRICS:
        raise ValueError("unsupported water quality metric")
    target = record_key(station_id, sample_id, metric, observed_at)
    record_source = _text(raw.get("source")) or source
    canonical = {
        "action": "withdraw",
        "source": record_source,
        "observed_at": observed_at,
        "payload": {"station_id": station_id, "sample_id": sample_id, "metric": metric},
    }
    return WithdrawalDirective(
        source=record_source,
        target_record_key=target,
        observed_at=observed_at,
        station_id=station_id,
        sample_id=sample_id,
        metric=metric,
        directive_hash=_canonical_hash(canonical),
        payload=canonical["payload"],
    )


def normalize_envelope(raw: Any, source: str) -> tuple[str, Any]:
    """Return ('report', WaterQualityRecord) or ('withdraw', WithdrawalDirective)."""
    if isinstance(raw, dict) and _text(raw.get("action")).lower() == "withdraw":
        return "withdraw", normalize_withdrawal(raw, source)
    return "report", normalize(raw, source)
