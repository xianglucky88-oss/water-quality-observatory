METRIC_UNITS = {
    "ph": {"ph"},
    "dissolved_oxygen": {"mg/l", "mg/l"},
    "ammonia_n": {"mg/l", "mg/l"},
    "total_phosphorus": {"mg/l", "mg/l"},
    "lead": {"mg/l", "mg/l"},
}


def unit_supported(metric: str, unit: str) -> bool:
    return unit.casefold() in METRIC_UNITS.get(metric, set())
