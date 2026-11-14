from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable

from .faults import FaultInjector
from .models import DOMAIN_RECORD_TYPE
from .quality import validate
from .storage import DataStore


class IngestionPipeline:
    """Coordinates normalization, quality checks and persistence only.

    Every item outcome is committed together with its structured row or
    rejection row. A resumed run reads the ledger first and only replays items
    that have no durable outcome, so repeated submissions and repeated
    recoveries converge on the same counts.
    """

    def __init__(
        self,
        records: Iterable[Any],
        source: str,
        store: DataStore,
        batch_id: int,
        normalizer: Callable[[Any, str], DOMAIN_RECORD_TYPE],
        faults: FaultInjector | None = None,
    ) -> None:
        self.records = list(records)
        self.source = source
        self.store = store
        self.batch_id = batch_id
        self.normalizer = normalizer
        self.faults = faults or FaultInjector()

    def run(self) -> dict[str, int]:
        done = {item["item_index"] for item in self.store.ledger(self.batch_id)}
        writes = 0
        for index, raw in enumerate(self.records):
            if index in done:
                continue
            try:
                record = self.normalizer(raw, self.source)
                issues = validate(record)
                if issues:
                    raise ValueError("; ".join(issues))
            except (TypeError, ValueError, KeyError) as exc:
                source = raw.get("source") if isinstance(raw, dict) else None
                self.store.reject_item(self.batch_id, index, raw, source, str(exc))
                self.faults.trigger("after_rejection", {"batch_id": self.batch_id, "item_index": index})
                continue

            self.store.accept_item(self.batch_id, index, record)
            writes += 1
            if writes == 1:
                self.faults.trigger("after_first_write", {"batch_id": self.batch_id, "item_index": index})

        if self.faults.armed():
            self.faults.trigger("before_finalize", {"batch_id": self.batch_id})
        stats = self.store.settle_batch(self.batch_id)
        self.faults.trigger("after_finalize", {"batch_id": self.batch_id})
        result = {
            "processed": stats["items"],
            "accepted": stats["accepted"],
            "rejected": stats["rejected"],
            "changed": stats["changed"],
        }
        result.update(self.store.counts())
        return result

