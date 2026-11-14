from __future__ import annotations

from collections.abc import Iterable, Callable
from typing import Any

from .identity import record_key
from .models import DOMAIN_RECORD_TYPE
from .quality import validate
from .rules import RuleCatalog, RuleViolation, DEFAULT_RULES
from .storage import DataStore

MODE_INGEST = "ingest"
MODE_PREVIEW = "preview"


def _safe_record_key(raw: Any) -> str | None:
    """Best-effort identity for quarantine rows; never raises inside the loop."""
    try:
        payload = raw["payload"]
        return record_key(
            str(payload["station_id"]),
            str(payload["sample_id"]),
            str(payload["metric"]),
            str(raw["observed_at"]),
        )
    except (KeyError, TypeError):
        return None


class IngestionPipeline:
    """Coordinates normalization, quality checks and persistence only.

    In ``preview`` mode no formal observation or quarantine row is written;
    every per-record decision is returned instead so callers can show the
    effect of a run (or a rule change) before committing anything.
    """

    def __init__(
        self,
        records: Iterable[Any],
        source: str,
        store: DataStore | None,
        batch_id: int | None,
        normalizer: Callable[..., DOMAIN_RECORD_TYPE],
        catalog: RuleCatalog = DEFAULT_RULES,
        raw_digest: str | None = None,
        mode: str = MODE_INGEST,
    ) -> None:
        self.records = records
        self.source = source
        self.store = store
        self.batch_id = batch_id
        self.normalizer = normalizer
        self.catalog = catalog
        self.raw_digest = raw_digest
        self.mode = mode

    def run(self) -> dict[str, Any]:
        processed = accepted = rejected = changed = 0
        decisions: list[dict[str, Any]] = []
        for raw in self.records:
            processed += 1
            try:
                record = self.normalizer(raw, self.source, self.catalog, self.raw_digest)
                issues = validate(record, self.catalog)
                if issues:
                    raise RuleViolation("OUTSIDE_DOMAIN_RANGE", "; ".join(issues))
                accepted += 1
                decision = {
                    "status": "accepted",
                    "record_key": record.record_key,
                    "metric": record.metric,
                    "reported_value_text": record.reported_value_text,
                    "reported_unit": record.reported_unit,
                    "qualifier": record.qualifier,
                    "detection_limit": record.detection_limit,
                    "comparable_value": record.value,
                    "canonical_unit": record.unit,
                    "comparison_semantics": record.comparison_semantics,
                    "conversion_factor": record.conversion_factor,
                    "rule_version": record.rule_version,
                }
                decisions.append(decision)
                if self.mode == MODE_INGEST and self.store is not None:
                    changed += int(self.store.upsert(record, source_raw=raw))
            except RuleViolation as exc:
                rejected += 1
                record_key = _safe_record_key(raw)
                decisions.append(
                    {
                        "status": "quarantined",
                        "reason_code": exc.code,
                        "error": exc.message,
                        "rule_version": self.catalog.version,
                        "record_key": record_key,
                        "raw": raw,
                    }
                )
                if self.mode == MODE_INGEST and self.store is not None:
                    source_name = raw.get("source") if isinstance(raw, dict) else None
                    self.store.quarantine(
                        raw, source_name, exc.code, exc.message,
                        self.catalog.version, batch_id=self.batch_id, record_key=record_key,
                    )
            except (TypeError, ValueError, KeyError) as exc:
                rejected += 1
                decisions.append(
                    {
                        "status": "quarantined",
                        "reason_code": "UNCLASSIFIED",
                        "error": str(exc),
                        "rule_version": self.catalog.version,
                        "record_key": _safe_record_key(raw),
                        "raw": raw,
                    }
                )
                if self.mode == MODE_INGEST and self.store is not None:
                    source_name = raw.get("source") if isinstance(raw, dict) else None
                    self.store.quarantine(
                        raw, source_name, "UNCLASSIFIED", str(exc),
                        self.catalog.version, batch_id=self.batch_id,
                        record_key=_safe_record_key(raw),
                    )

        if self.mode == MODE_INGEST and self.store is not None and self.batch_id is not None:
            self.store.finish_batch(self.batch_id, "completed", processed, accepted, rejected)
        result: dict[str, Any] = {
            "processed": processed,
            "accepted": accepted,
            "rejected": rejected,
            "changed": changed,
            "mode": self.mode,
            "decisions": decisions,
        }
        if self.store is not None:
            result.update(self.store.counts())
        return result
