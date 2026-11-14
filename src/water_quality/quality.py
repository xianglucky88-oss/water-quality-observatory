"""Domain quality rules.

Range enforcement is driven by the versioned :mod:`water_quality.rules`
catalog so that rule upgrades flow through one auditable place. The checker
acts as a backstop behind normalization and never rewrites reported values.
"""

from __future__ import annotations

from .models import WaterQualityRecord
from .rules import (
    CENSORED_QUALIFIERS,
    DEFAULT_RULES,
    RuleCatalog,
    RuleViolation,
    ReasonCode,
)


def validate(record: WaterQualityRecord, catalog: RuleCatalog = DEFAULT_RULES) -> list[str]:
    issues: list[str] = []
    rule = catalog.get(record.metric)
    lower, upper = rule.value_range
    if record.value is None or not lower <= record.value <= upper:
        issues.append(f"{record.metric} value outside domain range")

    if record.qualifier in CENSORED_QUALIFIERS and record.detection_limit is None:
        issues.append("censored result must retain a detection limit")
    if record.qualifier in CENSORED_QUALIFIERS and record.comparison_semantics != "left_censored":
        issues.append("censored qualifier must map to left_censored semantics")
    return issues


def ensure_valid(record: WaterQualityRecord, catalog: RuleCatalog = DEFAULT_RULES) -> None:
    issues = validate(record, catalog)
    if issues:
        raise RuleViolation(ReasonCode.OUTSIDE_DOMAIN_RANGE, "; ".join(issues))
