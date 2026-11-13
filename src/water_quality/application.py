from __future__ import annotations

from .adjudication import AdjudicationConfig
from .pipeline import IngestionPipeline
from .reporting import list_subjects, status_summary, subject_detail
from .sources import FixtureSource, HttpJsonSource, RawLanding
from .storage import DataStore


def _load_config(config_path: str | None) -> AdjudicationConfig:
    return AdjudicationConfig.load(config_path)


def _run(batch, database: str, raw_dir: str | None, config_path: str | None = None) -> dict[str, int]:
    landing = RawLanding(raw_dir)
    digest, raw_path = landing.save(batch)
    store = DataStore(database)
    batch_id = store.start_batch(batch.source)
    store.save_manifest(digest, batch.source, batch.content_type, len(batch.raw_body), raw_path)
    store.attach_manifest(batch_id, digest)
    config = _load_config(config_path)
    try:
        return IngestionPipeline(batch.records, batch.source, store, batch_id, config).run()
    except Exception as exc:
        store.finish_batch(batch_id, "failed", 0, 0, 0, str(exc))
        raise
    finally:
        store.close()


def run_fixture(
    fixture: str, database: str, raw_dir: str | None = None, config_path: str | None = None
) -> dict[str, int]:
    return _run(FixtureSource().fetch(fixture), database, raw_dir, config_path)


def run_url(
    url: str,
    database: str,
    raw_dir: str | None = None,
    timeout: float = 30.0,
    records_key: str | None = None,
    config_path: str | None = None,
) -> dict[str, int]:
    batch = HttpJsonSource(url, timeout=timeout, records_key=records_key).fetch()
    return _run(batch, database, raw_dir, config_path)


# --------------------------------------------------------------------- queries
def query_summary(database: str) -> dict:
    store = DataStore(database)
    try:
        return status_summary(store)
    finally:
        store.close()


def query_subjects(database: str, status: str | None = None, final_only: bool = False) -> list[dict]:
    store = DataStore(database)
    try:
        return list_subjects(store, status=status, final_only=final_only)
    finally:
        store.close()


def query_subject(database: str, subject_key_value: str) -> dict:
    store = DataStore(database)
    try:
        return subject_detail(store, subject_key_value)
    finally:
        store.close()
