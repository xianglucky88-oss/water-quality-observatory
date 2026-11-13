from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class AdjudicationConfig:
    """Configurable source precedence and quality conditions for revision adjudication.

    Higher ``priority`` value wins. Sources absent from the mapping share ``fallback_priority``.
    Only reports whose quality flag is in ``eligible_quality_flags`` can become a confirmed
    regulatory conclusion; others stay pending and cannot supersede an eligible report.
    """

    source_priority: dict[str, int] = field(default_factory=dict)
    fallback_priority: int = 0
    eligible_quality_flags: frozenset[str] = frozenset({"good", "suspect"})

    def priority_of(self, source: str) -> int:
        return self.source_priority.get(source, self.fallback_priority)

    def quality_eligible(self, quality_flag: str) -> bool:
        return quality_flag in self.eligible_quality_flags

    @classmethod
    def load(cls, path: str | None) -> "AdjudicationConfig":
        if path is None:
            return cls.default()
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("adjudication config must be a JSON object")
        source_priority = data.get("source_priority", {})
        if not isinstance(source_priority, dict) or not all(
            isinstance(key, str) and isinstance(value, int) for key, value in source_priority.items()
        ):
            raise ValueError("source_priority must map source names to integer ranks")
        quality = data.get("eligible_quality_flags", ["good", "suspect"])
        if not isinstance(quality, list) or not all(isinstance(flag, str) for flag in quality):
            raise ValueError("eligible_quality_flags must be a list of strings")
        fallback = data.get("fallback_priority", 0)
        if not isinstance(fallback, int):
            raise ValueError("fallback_priority must be an integer")
        return cls(
            source_priority=dict(source_priority),
            fallback_priority=fallback,
            eligible_quality_flags=frozenset(flag.lower() for flag in quality),
        )

    @classmethod
    def default(cls) -> "AdjudicationConfig":
        # Laboratory re-tests outrank automatic station preliminary reports.
        return cls(
            source_priority={
                "lab-a": 100,
                "lab-b": 100,
                "lab-c": 90,
                "station-network": 10,
            },
            fallback_priority=0,
            eligible_quality_flags=frozenset({"good", "suspect"}),
        )
