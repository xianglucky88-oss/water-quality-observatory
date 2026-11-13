from __future__ import annotations

from .models import WaterQualityRecord


def validate(record: WaterQualityRecord) -> list[str]:
    issues: list[str] = []

    limits = {"ph": (0, 14), "dissolved_oxygen": (0, 30), "ammonia_n": (0, 1000), "total_phosphorus": (0, 1000), "lead": (0, 100)}
    lower, upper = limits[record.metric]
    if not lower <= record.value <= upper:
        issues.append(f"{record.metric} value outside domain range")

    return issues
