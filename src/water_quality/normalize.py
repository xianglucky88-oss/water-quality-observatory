"""Field standardization with censored-result and unit-rule awareness.

Every accepted record keeps both the reported expression (text, qualifier,
detection limit, original unit) and the derived comparison semantics.
Ambiguous or contradictory expressions raise :class:`RuleViolation` and are
routed to the explainable quarantine store by the pipeline.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from .models import WaterQualityRecord
from .identity import record_key
from .rules import (
    CENSORED_QUALIFIERS,
    COMPARISON_EXACT,
    COMPARISON_LEFT_CENSORED,
    NON_DETECT_TOKENS,
    QUALIFIER_ALIASES,
    QUALIFIER_EXACT,
    QUALIFIER_NON_DETECT,
    RuleCatalog,
    RuleViolation,
    ReasonCode,
    DEFAULT_RULES,
    canonicalize_unit_text,
)

_NUMBER_RE = re.compile(r"^[+-]?(\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$")


def _text(value: Any) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _optional_number(value: Any, field_name: str) -> float | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, bool):
        raise RuleViolation(
            ReasonCode.CONFLICTING_EXPRESSION, f"{field_name} must be numeric, not boolean"
        )
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise RuleViolation(
            ReasonCode.CONFLICTING_EXPRESSION, f"{field_name} is not numeric: {value!r}"
        ) from exc
    if number != number or number in (float("inf"), float("-inf")):
        raise RuleViolation(
            ReasonCode.CONFLICTING_EXPRESSION, f"{field_name} must be finite: {value!r}"
        )
    return number


def _parse_qualifier(raw_text: str) -> str:
    key = raw_text.casefold()
    try:
        return QUALIFIER_ALIASES[key]
    except KeyError as exc:
        raise RuleViolation(
            ReasonCode.UNSUPPORTED_QUALIFIER, f"unsupported qualifier: {raw_text!r}"
        ) from exc


def _parse_value_expression(
    value_field: Any, qualifier_field: Any
) -> tuple[str, float | None, str]:
    """Parse the reported value/qualifier pair.

    Returns ``(reported_text, reported_numeric, canonical_qualifier)``.
    Qualifiers may arrive in a dedicated field or be embedded in the value
    text (``"<0.01"``, ``"ND"``); the two channels must never contradict.
    """
    explicit_qualifier = None
    qualifier_text = _text(qualifier_field)
    if qualifier_text:
        explicit_qualifier = _parse_qualifier(qualifier_text)

    if isinstance(value_field, bool):
        raise RuleViolation(
            ReasonCode.CONFLICTING_EXPRESSION, "value must be numeric or text, not boolean"
        )
    if isinstance(value_field, (int, float)):
        numeric = float(value_field)
        if numeric != numeric or numeric in (float("inf"), float("-inf")):
            raise RuleViolation(
                ReasonCode.CONFLICTING_EXPRESSION, f"value must be finite: {value_field!r}"
            )
        embedded = QUALIFIER_EXACT
        reported_text = _text(value_field)
    else:
        text = _text(value_field)
        if not text:
            raise RuleViolation(
                ReasonCode.CONFLICTING_EXPRESSION, "value is required"
            )
        folded = text.casefold()
        if folded in NON_DETECT_TOKENS:
            return text, None, explicit_qualifier or QUALIFIER_NON_DETECT
        match = re.match(r"^([^0-9+\-.]*?)\s*([+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?)\s*$", text)
        if not match:
            raise RuleViolation(
                ReasonCode.CONFLICTING_EXPRESSION, f"unparseable value expression: {text!r}"
            )
        symbol, number_text = match.group(1).strip(), match.group(2)
        numeric = float(number_text)
        if symbol:
            embedded = _parse_qualifier(symbol)
        else:
            embedded = QUALIFIER_EXACT
        reported_text = text

    if explicit_qualifier is not None and explicit_qualifier != embedded:
        raise RuleViolation(
            ReasonCode.CONFLICTING_EXPRESSION,
            f"qualifier {qualifier_text!r} contradicts value expression {reported_text!r}",
        )
    return reported_text, numeric, explicit_qualifier or embedded


def _validate_detection_limit(
    qualifier: str,
    reported_numeric: float | None,
    detection_limit: float | None,
    catalog: RuleCatalog,
    metric: str,
) -> float | None:
    rule = catalog.get(metric)
    if qualifier in CENSORED_QUALIFIERS:
        if detection_limit is None:
            if qualifier == QUALIFIER_NON_DETECT or reported_numeric is None:
                raise RuleViolation(
                    ReasonCode.DETECTION_LIMIT_MISSING,
                    f"censored result for {metric!r} requires an explicit detection limit",
                )
            # "<0.01" without a dedicated limit: the reported number itself is the limit.
            detection_limit = reported_numeric
        if not detection_limit > 0:
            raise RuleViolation(
                ReasonCode.DETECTION_LIMIT_INVALID,
                f"detection limit must be positive, got {detection_limit}",
            )
        if detection_limit > rule.detection_limit_max:
            raise RuleViolation(
                ReasonCode.DETECTION_LIMIT_INVALID,
                f"detection limit {detection_limit} is implausible for {metric!r} "
                f"(maximum {rule.detection_limit_max})",
            )
        # An explicit numeric reported alongside "<" must not contradict the limit.
        if reported_numeric is not None and reported_numeric != detection_limit:
            raise RuleViolation(
                ReasonCode.CONFLICTING_EXPRESSION,
                f"reported value {reported_numeric} contradicts detection limit {detection_limit}",
            )
        return detection_limit

    if detection_limit is not None and not detection_limit > 0:
        raise RuleViolation(
            ReasonCode.DETECTION_LIMIT_INVALID,
            f"detection limit must be positive, got {detection_limit}",
        )
    return detection_limit


def normalize(
    raw: Any,
    source: str,
    catalog: RuleCatalog = DEFAULT_RULES,
    raw_digest: str | None = None,
) -> WaterQualityRecord:
    if not isinstance(raw, dict):
        raise RuleViolation(
            ReasonCode.CONFLICTING_EXPRESSION, "record must be an object"
        )
    payload = raw.get("payload")
    observed_at = _text(raw.get("observed_at"))
    if not isinstance(payload, dict) or not observed_at:
        raise RuleViolation(
            ReasonCode.CONFLICTING_EXPRESSION, "observed_at and object payload are required"
        )

    station_id = _text(payload.get("station_id"))
    sample_id = _text(payload.get("sample_id"))
    metric = _text(payload.get("metric")).lower()
    unit_text = _text(payload.get("unit"))
    quality_flag = _text(payload.get("quality_flag")).lower() or "unknown"
    if not station_id or not sample_id or not observed_at or not metric or not unit_text:
        raise RuleViolation(
            ReasonCode.CONFLICTING_EXPRESSION,
            "station_id, sample_id, observed_at, metric and unit are required",
        )
    if quality_flag not in {"good", "suspect", "bad", "unknown"}:
        raise RuleViolation(
            ReasonCode.CONFLICTING_EXPRESSION, f"unsupported quality flag: {quality_flag!r}"
        )

    # Unknown metric is rejected explicitly via the versioned catalog.
    rule = catalog.get(metric)

    reported_text, reported_numeric, qualifier = _parse_value_expression(
        payload.get("value"), payload.get("qualifier")
    )
    detection_limit = _optional_number(payload.get("detection_limit"), "detection_limit")
    detection_limit = _validate_detection_limit(
        qualifier, reported_numeric, detection_limit, catalog, metric
    )

    # Unit resolution: only declared catalog factors are permitted.
    canonical_unit, factor = catalog.resolve_unit(metric, unit_text)
    reported_unit = canonicalize_unit_text(unit_text)

    censored = qualifier in CENSORED_QUALIFIERS
    if censored:
        comparison_semantics = COMPARISON_LEFT_CENSORED
        # Never write zero: the comparable value is the reporting limit. Prefer an
        # explicit reported number when present; otherwise the detection limit,
        # always in the canonical unit via the declared factor.
        basis = reported_numeric if reported_numeric is not None else detection_limit
        value = basis * factor
    else:
        if reported_numeric is None:
            raise RuleViolation(
                ReasonCode.CONFLICTING_EXPRESSION, "exact result requires a numeric value"
            )
        comparison_semantics = COMPARISON_EXACT
        value = reported_numeric * factor
        if detection_limit is not None and reported_numeric < detection_limit:
            raise RuleViolation(
                ReasonCode.CONFLICTING_EXPRESSION,
                f"exact value {reported_numeric} is below its own detection limit {detection_limit}",
            )

    lower, upper = rule.value_range
    if not lower <= value <= upper:
        raise RuleViolation(
            ReasonCode.OUTSIDE_DOMAIN_RANGE,
            f"{metric} comparable value {value} {canonical_unit} outside domain range [{lower}, {upper}]",
        )

    normalized = {
        "station_id": station_id,
        "sample_id": sample_id,
        "metric": metric,
        "reported_value_text": reported_text,
        "reported_value": reported_numeric,
        "qualifier": qualifier,
        "detection_limit": detection_limit,
        "reported_unit": reported_unit,
        "value": value,
        "unit": canonical_unit,
        "comparison_semantics": comparison_semantics,
        "conversion_factor": factor,
        "quality_flag": quality_flag,
        "rule_version": catalog.version,
        "rule_fingerprint": rule.fingerprint(),
    }
    key = record_key(station_id, sample_id, metric, observed_at)
    canonical = json.dumps(
        {"source": source, "observed_at": observed_at, "payload": normalized},
        ensure_ascii=False,
        sort_keys=True,
    )
    raw_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return WaterQualityRecord(
        source=source,
        record_key=key,
        observed_at=observed_at,
        station_id=station_id,
        sample_id=sample_id,
        metric=metric,
        quality_flag=quality_flag,
        reported_value_text=reported_text,
        reported_value=reported_numeric,
        qualifier=qualifier,
        detection_limit=detection_limit,
        reported_unit=reported_unit,
        value=value,
        unit=canonical_unit,
        comparison_semantics=comparison_semantics,
        conversion_factor=factor,
        rule_version=catalog.version,
        rule_fingerprint=rule.fingerprint(),
        payload=normalized,
        raw_hash=raw_hash,
        raw_digest=raw_digest,
    )
