METRIC_UNITS = {
    "ph": {"ph"},
    "dissolved_oxygen": {"mg/l"},
    "ammonia_n": {"mg/l"},
    "total_phosphorus": {"mg/l"},
    "lead": {"mg/l"},
}


def unit_supported(metric: str, unit: str) -> bool:
    return unit.casefold() in METRIC_UNITS.get(metric, set())
