from __future__ import annotations

from typing import Any

from .normalize import normalize
from .pipeline import IngestionPipeline, MODE_INGEST, MODE_PREVIEW
from .reprocess import APPLY, PREVIEW, ReprocessOutcome, reprocess_records
from .rules import DEFAULT_RULES, RuleCatalog
from .sources import FixtureSource, HttpJsonSource, RawLanding
from .storage import DataStore


def _catalog_snapshot(catalog: RuleCatalog) -> dict[str, Any]:
    return {
        "version": catalog.version,
        "metrics": {
            name: {
                "canonical_unit": rule.canonical_unit,
                "unit_factors": dict(rule.unit_factors),
                "value_range": list(rule.value_range),
                "detection_limit_max": rule.detection_limit_max,
                "fingerprint": rule.fingerprint(),
            }
            for name, rule in sorted(catalog.metrics.items())
        },
    }


def _run(batch, database: str, raw_dir: str | None, mode: str, catalog: RuleCatalog) -> dict[str, Any]:
    landing = RawLanding(raw_dir)
    digest, raw_path = landing.save(batch)
    store = DataStore(database)
    store.register_rule_version(catalog.version, _catalog_snapshot(catalog))
    batch_id = store.start_batch(batch.source, mode) if mode == MODE_INGEST else None
    store.save_manifest(digest, batch.source, batch.content_type, len(batch.raw_body), raw_path)
    try:
        return IngestionPipeline(
            batch.records, batch.source, store, batch_id, normalize,
            catalog=catalog, raw_digest=digest, mode=mode,
        ).run()
    except Exception as exc:
        if batch_id is not None:
            store.finish_batch(batch_id, "failed", 0, 0, 0, str(exc))
        raise
    finally:
        store.close()


def run_fixture(
    fixture: str,
    database: str,
    raw_dir: str | None = None,
    preview: bool = False,
    catalog: RuleCatalog = DEFAULT_RULES,
) -> dict[str, Any]:
    return _run(
        FixtureSource().fetch(fixture), database, raw_dir,
        MODE_PREVIEW if preview else MODE_INGEST, catalog,
    )


def run_url(
    url: str,
    database: str,
    raw_dir: str | None = None,
    timeout: float = 30.0,
    records_key: str | None = None,
    preview: bool = False,
    catalog: RuleCatalog = DEFAULT_RULES,
) -> dict[str, Any]:
    return _run(
        HttpJsonSource(url, timeout=timeout, records_key=records_key).fetch(),
        database, raw_dir, MODE_PREVIEW if preview else MODE_INGEST, catalog,
    )


def preview_fixture(fixture: str, database: str, raw_dir: str | None = None, catalog: RuleCatalog = DEFAULT_RULES) -> dict[str, Any]:
    return run_fixture(fixture, database, raw_dir, preview=True, catalog=catalog)


def reprocess(
    database: str, catalog: RuleCatalog, apply: bool = False
) -> ReprocessOutcome:
    store = DataStore(database)
    try:
        return reprocess_records(store, catalog, APPLY if apply else PREVIEW)
    finally:
        store.close()
