"""Metric/unit vocabulary backed by the versioned rule catalog.

Kept as a thin compatibility facade over :mod:`water_quality.rules`; new code
should resolve units through the catalog directly so the rule version and
conversion factor are retained.
"""

from __future__ import annotations

from .rules import DEFAULT_RULES, RuleCatalog, canonicalize_unit_text


def unit_supported(metric: str, unit: str, catalog: RuleCatalog = DEFAULT_RULES) -> bool:
    try:
        catalog.resolve_unit(metric, unit)
    except Exception:
        return False
    return True


def supported_units(metric: str, catalog: RuleCatalog = DEFAULT_RULES) -> frozenset[str]:
    rule = catalog.get(metric)
    return frozenset(rule.unit_factors)


__all__ = ["unit_supported", "supported_units", "canonicalize_unit_text", "DEFAULT_RULES"]
