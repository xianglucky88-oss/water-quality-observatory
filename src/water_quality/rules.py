"""Versioned metric and unit rule catalog.

Conversions are only allowed through explicit, verifiable factor relations
declared in a catalog. Unknown units are never guessed or string-replaced;
callers receive a :class:`RuleViolation` and route the record to quarantine.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Mapping


class ReasonCode:
    UNKNOWN_UNIT = "UNKNOWN_UNIT"
    UNSUPPORTED_QUALIFIER = "UNSUPPORTED_QUALIFIER"
    DETECTION_LIMIT_MISSING = "DETECTION_LIMIT_MISSING"
    DETECTION_LIMIT_INVALID = "DETECTION_LIMIT_INVALID"
    CONFLICTING_EXPRESSION = "CONFLICTING_EXPRESSION"
    OUTSIDE_DOMAIN_RANGE = "OUTSIDE_DOMAIN_RANGE"
    UNSUPPORTED_METRIC = "UNSUPPORTED_METRIC"


class RuleViolation(ValueError):
    """A domain rule violation that must produce an explainable quarantine row."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


# Canonical qualifier vocabulary.
QUALIFIER_EXACT = "="
QUALIFIER_BELOW = "<"
QUALIFIER_NON_DETECT = "ND"
CENSORED_QUALIFIERS = frozenset({QUALIFIER_BELOW, QUALIFIER_NON_DETECT})
COMPARISON_EXACT = "exact"
COMPARISON_LEFT_CENSORED = "left_censored"

# Accepted free-text qualifier spellings mapped to the canonical vocabulary.
QUALIFIER_ALIASES = {
    "=": QUALIFIER_EXACT,
    "eq": QUALIFIER_EXACT,
    "<": QUALIFIER_BELOW,
    "<=": QUALIFIER_BELOW,
    "≤": QUALIFIER_BELOW,
    "nd": QUALIFIER_NON_DETECT,
    "n.d.": QUALIFIER_NON_DETECT,
    "n.d": QUALIFIER_NON_DETECT,
    "lod": QUALIFIER_NON_DETECT,
    "未检出": QUALIFIER_NON_DETECT,
}

# Non-detect tokens accepted inside a textual value.
NON_DETECT_TOKENS = frozenset({"nd", "n.d.", "n.d", "lod", "未检出"})

# Unit spelling aliases. Keys are casefolded/whitespace-collapsed source text;
# values are the canonical unit spelling used inside the catalog.
UNIT_ALIASES = {
    "mg/l": "mg/l",
    "mg l-1": "mg/l",
    "mg/l-1": "mg/l",
    "mg·L-1".casefold(): "mg/l",
    "milligram/liter": "mg/l",
    "milligram per liter": "mg/l",
    "mg per l": "mg/l",
    "ug/l": "ug/l",
    "µg/l": "ug/l",
    "μg/l": "ug/l",
    "mcg/l": "ug/l",
    "ug l-1": "ug/l",
    "µg l-1": "ug/l",
    "microgram/liter": "ug/l",
    "microgram per liter": "ug/l",
    "ng/l": "ng/l",
    "ηg/l": "ng/l",
    "ph": "ph",
    "ph unit": "ph",
    "ph units": "ph",
}


@dataclass(frozen=True)
class MetricRule:
    metric: str
    canonical_unit: str
    # Canonical unit spelling -> multiplicative factor to the canonical unit.
    unit_factors: Mapping[str, float]
    value_range: tuple[float, float]
    detection_limit_max: float

    def fingerprint(self) -> str:
        body = json.dumps(
            {
                "metric": self.metric,
                "canonical_unit": self.canonical_unit,
                "unit_factors": dict(sorted(self.unit_factors.items())),
                "value_range": list(self.value_range),
                "detection_limit_max": self.detection_limit_max,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class RuleCatalog:
    version: str
    metrics: Mapping[str, MetricRule] = field(default_factory=dict)

    def get(self, metric: str) -> MetricRule:
        try:
            return self.metrics[metric]
        except KeyError as exc:
            raise RuleViolation(
                ReasonCode.UNSUPPORTED_METRIC, f"unsupported water quality metric: {metric}"
            ) from exc

    def fingerprint(self, metric: str) -> str:
        return self.get(metric).fingerprint()

    def resolve_unit(self, metric: str, unit_text: str) -> tuple[str, float]:
        """Return ``(canonical_unit, factor_to_canonical_unit)`` or raise."""
        rule = self.get(metric)
        canonical_spelling = canonicalize_unit_text(unit_text)
        factor = rule.unit_factors.get(canonical_spelling)
        if factor is None:
            raise RuleViolation(
                ReasonCode.UNKNOWN_UNIT,
                f"unit {unit_text!r} has no declared conversion for metric {metric!r}",
            )
        return rule.canonical_unit, float(factor)


def canonicalize_unit_text(unit_text: str) -> str:
    """Collapse case/whitespace and map spelling variants; never guess units."""
    collapsed = " ".join(str(unit_text).split()).casefold()
    canonical = UNIT_ALIASES.get(collapsed)
    if canonical is None:
        # Keep the collapsed spelling so the catalog can reject it explicitly.
        return collapsed
    return canonical


DEFAULT_RULES = RuleCatalog(
    version="2026.11.0",
    metrics={
        "ph": MetricRule("ph", "ph", {"ph": 1.0}, (0.0, 14.0), 14.0),
        "dissolved_oxygen": MetricRule(
            "dissolved_oxygen", "mg/l", {"mg/l": 1.0}, (0.0, 30.0), 30.0
        ),
        "ammonia_n": MetricRule(
            "ammonia_n", "mg/l", {"mg/l": 1.0, "ug/l": 0.001}, (0.0, 1000.0), 1000.0
        ),
        "total_phosphorus": MetricRule(
            "total_phosphorus", "mg/l", {"mg/l": 1.0, "ug/l": 0.001}, (0.0, 1000.0), 1000.0
        ),
        "lead": MetricRule(
            "lead", "mg/l", {"mg/l": 1.0, "ug/l": 0.001}, (0.0, 100.0), 100.0
        ),
    },
)
