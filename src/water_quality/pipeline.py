from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .adjudication import AdjudicationConfig
from .models import WaterQualityRecord, WithdrawalDirective
from .normalize import normalize_envelope
from .quality import validate
from .storage import DataStore


class IngestionPipeline:
    """Coordinates normalization, quality checks, persistence and revision adjudication.

    Each input is handled independently: a failure on one record is recorded as a
    rejection and never poisons the batch or leaves a partial logical version,
    because every adjudication write is itself transactional.
    """

    def __init__(
        self,
        records: Iterable[Any],
        source: str,
        store: DataStore,
        batch_id: int,
        config: AdjudicationConfig,
    ) -> None:
        self.records = records
        self.source = source
        self.store = store
        self.batch_id = batch_id
        self.config = config

    def run(self) -> dict[str, int]:
        processed = accepted = rejected = changed = 0
        new_evidence = withdrawals = 0
        for sequence, raw in enumerate(self.records):
            processed += 1
            try:
                kind, domain = normalize_envelope(raw, self.source)
                if kind == "withdraw":
                    accepted += 1
                    outcome = self.store.ingest_withdrawal(
                        self._as_withdrawal(domain), self.batch_id, self.config
                    )
                    withdrawals += int(outcome["evidence_added"])
                    changed += int(outcome["changed"])
                    continue

                record = self._as_record(domain)
                issues = validate(record)
                if issues:
                    raise ValueError("; ".join(issues))
                accepted += 1
                outcome = self.store.ingest_report(record, self.batch_id, sequence, self.config)
                new_evidence += int(outcome["evidence_added"])
                changed += int(outcome["changed"])
            except (TypeError, ValueError, KeyError) as exc:
                rejected += 1
                source = raw.get("source") if isinstance(raw, dict) else None
                self.store.reject(self.batch_id, raw, source, str(exc))
        self.store.finish_batch(self.batch_id, "completed", processed, accepted, rejected)
        result = {
            "processed": processed,
            "accepted": accepted,
            "rejected": rejected,
            "changed": changed,
            "new_evidence": new_evidence,
            "withdrawals": withdrawals,
        }
        result.update(self.store.counts())
        return result

    @staticmethod
    def _as_record(domain: Any) -> WaterQualityRecord:
        if not isinstance(domain, WaterQualityRecord):
            raise ValueError("normalizer produced an unexpected report type")
        return domain

    @staticmethod
    def _as_withdrawal(domain: Any) -> WithdrawalDirective:
        if not isinstance(domain, WithdrawalDirective):
            raise ValueError("normalizer produced an unexpected directive type")
        return domain
