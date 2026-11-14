"""Window-level evidence completeness assessment.

This module is *read only*. It never writes to the ingestion database and never
decides whether pollution occurred; it states whether the available evidence is
complete enough for an investigator to make that determination.

Five evidence categories are kept strictly separate and never merged:

1. usable observations          - accepted versions inside the window
2. quality-quarantined records  - rejected by domain/normalization rules
3. duplicate conflicts          - same record key, sources repeated with
                                  different content (distinct raw hashes)
4. evidence gaps                - time intervals longer than the metric allows
5. late arrivals                - sampled inside the window but only first seen
                                  after the configured arrival deadline

Missing data, invalid (rejected) data and genuine zero values are reported as
distinct situations: a zero value is a valid numeric observation.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
SCHEMA_VERSION = "1.0"

SUPPORTED_METRICS = (
    "ph",
    "dissolved_oxygen",
    "ammonia_n",
    "total_phosphorus",
    "lead",
)

# Per-metric defaults. Callers override per metric via policy_overrides; the
# defaults only apply when the caller does not configure a metric.
DEFAULT_POLICY: dict[str, dict[str, int]] = {
    "ph": {"min_observations": 3, "max_gap_seconds": 3600},
    "dissolved_oxygen": {"min_observations": 3, "max_gap_seconds": 3600},
    "ammonia_n": {"min_observations": 2, "max_gap_seconds": 7200},
    "total_phosphorus": {"min_observations": 2, "max_gap_seconds": 7200},
    "lead": {"min_observations": 2, "max_gap_seconds": 7200},
}

REASON_TEXT = {
    "MISSING_IN_WINDOW": "窗口内没有任何该指标的可用观测（缺测，区别于无效记录和零值）",
    "BELOW_MIN_OBSERVATIONS": "可用观测数低于该指标最低覆盖要求",
    "GAP_EXCEEDS_MAX_INTERVAL": "存在超过最大可接受间隔的证据空洞",
    "CONFLICTING_VERSIONS": "存在来源重复但内容不同的冲突版本，未经裁决不能采信",
    "ONLY_REJECTED_EVIDENCE": "窗口内仅有被质量规则隔离的记录，无任何可用观测",
}


@dataclass(frozen=True)
class WindowSpec:
    station_id: str
    start: str
    end: str
    metrics: tuple[str, ...]
    arrival_deadline: str | None = None


def parse_iso8601(value: str) -> datetime:
    """Parse an ISO-8601 timestamp (including trailing Z) into aware UTC."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp must be a non-empty ISO-8601 string")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"unparseable ISO-8601 timestamp: {value!r}") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat()


def build_policy(overrides: dict[str, dict[str, int]] | None) -> dict[str, dict[str, int]]:
    """Merge caller overrides onto the default per-metric policy."""
    policy = {metric: dict(DEFAULT_POLICY[metric]) for metric in SUPPORTED_METRICS}
    for raw_metric, config in (overrides or {}).items():
        metric = str(raw_metric).strip().lower()
        if metric not in SUPPORTED_METRICS:
            raise ValueError(f"unsupported metric in policy: {raw_metric!r}")
        if not isinstance(config, dict):
            raise ValueError(f"policy for {metric} must be an object")
        merged = dict(policy[metric])
        for key, value in config.items():
            if key not in ("min_observations", "max_gap_seconds"):
                raise ValueError(f"unsupported policy key: {key!r}")
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"policy {key} for {metric} must be a non-negative integer")
            merged[key] = value
        policy[metric] = merged
    return policy


def _connect_read_only(database: str) -> sqlite3.Connection:
    path = Path(database)
    if not path.exists():
        raise FileNotFoundError(f"assessment database does not exist: {database}")
    conn = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _require_tables(conn: sqlite3.Connection) -> None:
    present = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    missing = sorted(name for name in ("record_versions", "batches", "rejected_records",
                                       "batch_raw_materials") if name not in present)
    if missing:
        raise RuntimeError(
            "assessment database is missing provenance tables; re-ingest with the "
            f"current pipeline. Missing: {', '.join(missing)}"
        )


def _text(value: Any) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _rejection_identity(raw: Any) -> tuple[str | None, str | None, datetime | None]:
    """Best-effort attribution of a quarantined raw record to a window."""
    if not isinstance(raw, dict):
        return None, None, None
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return None, None, None
    station = _text(payload.get("station_id")) or None
    metric = _text(payload.get("metric")).lower() or None
    observed_text = _text(raw.get("observed_at"))
    observed: datetime | None = None
    if observed_text:
        try:
            observed = parse_iso8601(observed_text)
        except ValueError:
            observed = None
    return station, metric, observed


def assess_window(
    database: str,
    station_id: str,
    start: str,
    end: str,
    metrics: list[str] | tuple[str, ...] | None = None,
    policy_overrides: dict[str, dict[str, int]] | None = None,
    arrival_deadline: str | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Produce a reproducible evidence-completeness assessment for one window."""
    window_start = parse_iso8601(start)
    window_end = parse_iso8601(end)
    if window_end <= window_start:
        raise ValueError("window end must be after window start")

    requested = tuple(m.strip().lower() for m in (metrics or SUPPORTED_METRICS))
    if not requested:
        raise ValueError("at least one metric must be requested")
    unknown = sorted(set(requested) - set(SUPPORTED_METRICS))
    if unknown:
        raise ValueError(f"unsupported metrics: {', '.join(unknown)}")
    if len(set(requested)) != len(requested):
        raise ValueError("metrics must not be repeated")

    policy = build_policy(policy_overrides)
    deadline = parse_iso8601(arrival_deadline) if arrival_deadline else window_end
    evaluated_at = parse_iso8601(as_of) if as_of else datetime.now(timezone.utc)

    conn = _connect_read_only(database)
    try:
        _require_tables(conn)
        return _assess(
            conn, station_id.strip(), window_start, window_end, requested,
            policy, deadline, evaluated_at,
        )
    finally:
        conn.close()


def _assess(conn, station_id, window_start, window_end, requested, policy,
            deadline, evaluated_at) -> dict[str, Any]:
    station_token = station_id.upper()
    referenced_batches: set[int] = set()
    batches_registry: dict[int, dict[str, Any]] = {}
    raw_registry: dict[str, dict[str, Any]] = {}

    def register_batch(batch_id: int) -> None:
        if batch_id in batches_registry:
            return
        row = conn.execute(
            "SELECT batch_id, source, status, started_at, finished_at FROM batches WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if row is None:
            return
        batches_registry[batch_id] = {
            "batch_id": batch_id,
            "source": row["source"],
            "status": row["status"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
        }
        referenced_batches.add(batch_id)
        for material in conn.execute(
            """SELECT digest, source, content_type, byte_count, path, captured_at
               FROM batch_raw_materials WHERE batch_id=?""",
            (batch_id,),
        ).fetchall():
            entry = raw_registry.setdefault(material["digest"], {
                "digest": material["digest"],
                "source": material["source"],
                "content_type": material["content_type"],
                "byte_count": material["byte_count"],
                "path": material["path"],
                "captured_at": material["captured_at"],
                "batches": [],
            })
            if batch_id not in entry["batches"]:
                entry["batches"].append(batch_id)

    # --- Group accepted versions by record key --------------------------------
    versions_by_key: dict[str, list[sqlite3.Row]] = {}
    placeholders = ",".join("?" for _ in requested)
    query = f"""
        SELECT record_key, batch_id, source, observed_at, station_id, sample_id,
               metric, value, unit, quality_flag, payload_json, raw_hash, first_seen_at
        FROM record_versions
        WHERE upper(station_id) = ? AND metric IN ({placeholders})
        ORDER BY metric, observed_at, record_key, batch_id, raw_hash
    """
    for row in conn.execute(query, (station_token, *requested)).fetchall():
        try:
            observed = parse_iso8601(row["observed_at"])
        except ValueError:
            continue
        if window_start <= observed < window_end:
            versions_by_key.setdefault(row["record_key"], []).append(row)
            register_batch(row["batch_id"])

    metric_blocks: list[dict[str, Any]] = []
    window_totals = {
        "usable_observations": 0,
        "late_arrivals": 0,
        "zero_value_observations": 0,
        "exact_duplicate_versions": 0,
        "conflicting_record_keys": 0,
        "rejected_records": 0,
        "unattributed_rejections": 0,
        "evidence_gaps": 0,
    }

    for metric in requested:
        usable: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        exact_duplicates = 0

        for record_key in sorted(versions_by_key):
            versions = versions_by_key[record_key]
            if versions[0]["metric"] != metric:
                continue
            distinct_hashes = sorted({row["raw_hash"] for row in versions})
            exact_duplicates += len(versions) - len(distinct_hashes)

            def evidence(row: sqlite3.Row) -> dict[str, Any]:
                return {
                    "batch_id": row["batch_id"],
                    "source": row["source"],
                    "raw_hash": row["raw_hash"],
                    "first_seen_at": row["first_seen_at"],
                    "quality_flag": row["quality_flag"],
                }

            evidence_refs = [evidence(row) for row in versions]
            ordered = sorted(versions, key=lambda r: (r["first_seen_at"], r["batch_id"], r["raw_hash"]))
            latest = ordered[-1]

            if len(distinct_hashes) > 1:
                conflicts.append({
                    "record_key": record_key,
                    "sample_id": latest["sample_id"],
                    "observed_at": latest["observed_at"],
                    "distinct_version_count": len(distinct_hashes),
                    "raw_hashes": distinct_hashes,
                    "values_seen": sorted({row["value"] for row in versions}),
                    "latest_value": latest["value"],
                    "latest_unit": latest["unit"],
                    "evidence": evidence_refs,
                })
                continue

            observed_dt = parse_iso8601(latest["observed_at"])
            first_seen_dt = parse_iso8601(latest["first_seen_at"])
            usable.append({
                "record_key": record_key,
                "sample_id": latest["sample_id"],
                "observed_at": latest["observed_at"],
                "value": latest["value"],
                "unit": latest["unit"],
                "quality_flag": latest["quality_flag"],
                "is_zero_value": latest["value"] == 0.0,
                "late_arrival": first_seen_dt > deadline,
                "first_seen_at": latest["first_seen_at"],
                "duplicate_copies": len(versions) - 1,
                "raw_hash": latest["raw_hash"],
                "evidence": evidence_refs,
                "_observed_dt": observed_dt,
            })

        usable.sort(key=lambda item: (item["_observed_dt"], item["record_key"]))
        conflicts.sort(key=lambda item: (item["observed_at"], item["record_key"]))

        # --- Evidence gaps ----------------------------------------------------
        max_gap = policy[metric]["max_gap_seconds"]
        gaps: list[dict[str, Any]] = []

        def add_gap(kind: str, gap_start: datetime, gap_end: datetime) -> None:
            length = (gap_end - gap_start).total_seconds()
            gaps.append({
                "gap_id": f"gap-{len(gaps) + 1:02d}",
                "kind": kind,
                "start": _iso(gap_start),
                "end": _iso(gap_end),
                "gap_seconds": int(length),
                "max_acceptable_gap_seconds": max_gap,
                "exceeds_threshold": length > max_gap,
            })

        if not usable:
            add_gap("whole_window", window_start, window_end)
        else:
            first_dt = usable[0]["_observed_dt"]
            if first_dt - window_start > timedelta(seconds=max_gap):
                add_gap("leading_edge", window_start, first_dt)
            for earlier, later in zip(usable, usable[1:]):
                if later["_observed_dt"] - earlier["_observed_dt"] > timedelta(seconds=max_gap):
                    add_gap("internal", earlier["_observed_dt"], later["_observed_dt"])
            last_dt = usable[-1]["_observed_dt"]
            if window_end - last_dt > timedelta(seconds=max_gap):
                add_gap("trailing_edge", last_dt, window_end)
        exceeding_gaps = [gap for gap in gaps if gap["exceeds_threshold"]]

        # --- Quality-quarantined records attributable to this metric/window ---
        rejected_entries, unattributed = _collect_rejections(
            conn, station_token, metric, window_start, window_end, register_batch
        )

        # --- Verdict -----------------------------------------------------------
        reasons: list[dict[str, str]] = []
        usable_count = len(usable)
        min_observations = policy[metric]["min_observations"]
        if usable_count == 0:
            code = "ONLY_REJECTED_EVIDENCE" if rejected_entries else "MISSING_IN_WINDOW"
            reasons.append({"code": code, "detail": REASON_TEXT[code]})
        elif usable_count < min_observations:
            reasons.append({
                "code": "BELOW_MIN_OBSERVATIONS",
                "detail": (f"{REASON_TEXT['BELOW_MIN_OBSERVATIONS']} "
                           f"({usable_count} < {min_observations})"),
            })
        if exceeding_gaps:
            reasons.append({
                "code": "GAP_EXCEEDS_MAX_INTERVAL",
                "detail": (f"{REASON_TEXT['GAP_EXCEEDS_MAX_INTERVAL']} "
                           f"({len(exceeding_gaps)} 处，阈值 {max_gap}s)"),
            })
        if conflicts:
            reasons.append({
                "code": "CONFLICTING_VERSIONS",
                "detail": (f"{REASON_TEXT['CONFLICTING_VERSIONS']} "
                           f"({len(conflicts)} 个记录键)"),
            })

        late_count = sum(1 for item in usable if item["late_arrival"])
        zero_count = sum(1 for item in usable if item["is_zero_value"])
        block = {
            "metric": metric,
            "status": "supports_pollution_assessment" if not reasons else "insufficient_evidence",
            "reasons": reasons,
            "policy_applied": dict(policy[metric]),
            "arrival_deadline": _iso(deadline),
            "counts": {
                "usable_observations": usable_count,
                "late_arrivals": late_count,
                "zero_value_observations": zero_count,
                "exact_duplicate_versions": exact_duplicates,
                "conflicting_record_keys": len(conflicts),
                "rejected_records": len(rejected_entries),
                "evidence_gaps": len(exceeding_gaps),
            },
            "usable_observations": [
                {key: value for key, value in item.items() if not key.startswith("_")}
                for item in usable
            ],
            "duplicate_conflicts": conflicts,
            "evidence_gaps": gaps,
            "rejected_records": rejected_entries,
        }
        metric_blocks.append(block)

        window_totals["usable_observations"] += usable_count
        window_totals["late_arrivals"] += late_count
        window_totals["zero_value_observations"] += zero_count
        window_totals["exact_duplicate_versions"] += exact_duplicates
        window_totals["conflicting_record_keys"] += len(conflicts)
        window_totals["rejected_records"] += len(rejected_entries)
        window_totals["evidence_gaps"] += len(exceeding_gaps)
        window_totals["unattributed_rejections"] += unattributed

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "determination_scope": (
            "evidence_completeness_only: 本结论仅评估证据完整度，不判定污染是否发生；"
            "不确定数据一律表述为证据不足，不得表述为确定污染事件"
        ),
        "station_id": station_id,
        "window": {"start": _iso(window_start), "end": _iso(window_end)},
        "arrival_deadline": _iso(deadline),
        "requested_metrics": list(requested),
        "effective_policy": {metric: dict(policy[metric]) for metric in requested},
        "overall_status": (
            "sufficient_evidence_for_all_metrics"
            if all(block["status"] == "supports_pollution_assessment" for block in metric_blocks)
            else "insufficient_evidence"
        ),
        "window_totals": window_totals,
        "metrics": metric_blocks,
        "provenance": {
            "batches": [batches_registry[key] for key in sorted(batches_registry)],
            "raw_materials": [raw_registry[key] for key in sorted(raw_registry)],
        },
        "evaluated_at": _iso(evaluated_at),
    }
    result["assessment_fingerprint"] = assessment_fingerprint(result)
    return result


def _collect_rejections(conn, station_token, metric, window_start, window_end, register_batch):
    entries: list[dict[str, Any]] = []
    unattributed = 0
    rows = conn.execute(
        """SELECT r.rejection_id, r.batch_id, r.source, r.error, r.raw_json, r.created_at
           FROM rejected_records r ORDER BY r.rejection_id"""
    ).fetchall()
    for row in rows:
        try:
            raw = json.loads(row["raw_json"])
        except (TypeError, ValueError):
            raw = None
        station, raw_metric, observed = _rejection_identity(raw)
        in_window = observed is not None and window_start <= observed < window_end
        if station is None or raw_metric is None or observed is None:
            if station is not None and station.upper() == station_token:
                unattributed += 1
                register_batch(row["batch_id"])
            continue
        if station.upper() != station_token or raw_metric != metric or not in_window:
            continue
        register_batch(row["batch_id"])
        entries.append({
            "rejection_id": row["rejection_id"],
            "batch_id": row["batch_id"],
            "source": row["source"],
            "reason": row["error"],
            "observed_at": _iso(observed),
            "station_id": station,
            "metric": raw_metric,
            "raw_summary": _raw_summary(raw),
        })
    return entries, unattributed


def _raw_summary(raw: Any) -> dict[str, Any]:
    """Compact, JSON-safe summary of the quarantined original material."""
    if not isinstance(raw, dict):
        return {"type": type(raw).__name__}
    payload = raw.get("payload")
    summary = {
        "source": raw.get("source"),
        "observed_at": raw.get("observed_at"),
    }
    if isinstance(payload, dict):
        summary["payload_fields"] = sorted(payload.keys())
        for field in ("station_id", "sample_id", "metric", "unit", "quality_flag"):
            if field in payload:
                summary[field] = payload[field]
        if "value" in payload:
            summary["value"] = payload["value"]
    return summary


def assessment_fingerprint(result: dict[str, Any]) -> str:
    """Stable hash of the substantive conclusion.

    Batch ids and first-seen wall-clock bookkeeping are deliberately excluded:
    re-importing identical raw material must not move the fingerprint.
    """
    payload = {
        "station_id": result["station_id"],
        "window": result["window"],
        "arrival_deadline": result["arrival_deadline"],
        "requested_metrics": result["requested_metrics"],
        "effective_policy": result["effective_policy"],
        "metrics": [
            {
                "metric": block["metric"],
                "status": block["status"],
                "reasons": [reason["code"] for reason in block["reasons"]],
                "usable_observations": [
                    {
                        "record_key": item["record_key"],
                        "observed_at": item["observed_at"],
                        "value": item["value"],
                        "unit": item["unit"],
                        "quality_flag": item["quality_flag"],
                        "is_zero_value": item["is_zero_value"],
                        "late_arrival": item["late_arrival"],
                        "raw_hash": item["raw_hash"],
                        "evidence_hashes": sorted({
                            ref["raw_hash"] for ref in item["evidence"]
                        }),
                    }
                    for item in block["usable_observations"]
                ],
                "duplicate_conflicts": [
                    {
                        "record_key": item["record_key"],
                        "observed_at": item["observed_at"],
                        "raw_hashes": item["raw_hashes"],
                        "values_seen": item["values_seen"],
                    }
                    for item in block["duplicate_conflicts"]
                ],
                "evidence_gaps": [
                    {
                        "kind": gap["kind"],
                        "start": gap["start"],
                        "end": gap["end"],
                        "gap_seconds": gap["gap_seconds"],
                    }
                    for gap in block["evidence_gaps"]
                ],
                "rejection_reasons": sorted(
                    item["reason"] for item in block["rejected_records"]
                ),
                "rejected_count": block["counts"]["rejected_records"],
            }
            for block in result["metrics"]
        ],
        "unattributed_rejections": result["window_totals"]["unattributed_rejections"],
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def render_summary(result: dict[str, Any]) -> str:
    """Human-readable CLI summary; the structured document stays machine-consumable."""
    window = result["window"]
    lines = [
        "窗口证据完整度评估（仅评估证据完整度，不判定污染是否发生）",
        f"站点: {result['station_id']}  窗口: {window['start']} ~ {window['end']}",
        f"到达截止: {result['arrival_deadline']}  整体结论: {result['overall_status']}",
        f"评估指纹: sha256:{result['assessment_fingerprint']}",
        "",
    ]
    for block in result["metrics"]:
        counts = block["counts"]
        lines.append(
            f"- {block['metric']}: {block['status']} "
            f"(最低观测 {block['policy_applied']['min_observations']}, "
            f"最大间隔 {block['policy_applied']['max_gap_seconds']}s)"
        )
        lines.append(
            "    可用观测 {usable}（迟到 {late}，零值 {zero}）| 重复冲突 {conflict} | "
            "完全重复副本 {dup} | 隔离记录 {rej} | 证据空洞 {gaps}".format(
                usable=counts["usable_observations"],
                late=counts["late_arrivals"],
                zero=counts["zero_value_observations"],
                conflict=counts["conflicting_record_keys"],
                dup=counts["exact_duplicate_versions"],
                rej=counts["rejected_records"],
                gaps=counts["evidence_gaps"],
            )
        )
        if block["reasons"]:
            for reason in block["reasons"]:
                lines.append(f"    原因 [{reason['code']}]: {reason['detail']}")
        for gap in block["evidence_gaps"]:
            marker = "超阈值" if gap["exceeds_threshold"] else "未超阈值"
            lines.append(
                f"    空洞 {gap['gap_id']} {gap['kind']} {gap['start']} ~ {gap['end']} "
                f"({gap['gap_seconds']}s, {marker})"
            )
        for conflict in block["duplicate_conflicts"]:
            lines.append(
                f"    冲突 {conflict['record_key']}: {conflict['distinct_version_count']} 个内容不同版本，"
                f"值 {conflict['values_seen']}，批次 {sorted(ref['batch_id'] for ref in conflict['evidence'])}"
            )
        for rejected in block["rejected_records"]:
            lines.append(
                f"    隔离 rejection#{rejected['rejection_id']} 批次{rejected['batch_id']} "
                f"{rejected['observed_at']}: {rejected['reason']}"
            )
    totals = result["window_totals"]
    lines.append("")
    lines.append(
        "窗口合计: 可用 {usable} | 迟到 {late} | 零值 {zero} | 冲突 {conflict} | "
        "隔离 {rej}（无法定位 {unattr}）| 空洞 {gaps}".format(
            usable=totals["usable_observations"],
            late=totals["late_arrivals"],
            zero=totals["zero_value_observations"],
            conflict=totals["conflicting_record_keys"],
            rej=totals["rejected_records"],
            unattr=totals["unattributed_rejections"],
            gaps=totals["evidence_gaps"],
        )
    )
    batches = ", ".join(str(item["batch_id"]) for item in result["provenance"]["batches"]) or "无"
    materials = ", ".join(item["digest"][:12] for item in result["provenance"]["raw_materials"]) or "无"
    lines.append(f"溯源批次: {batches}")
    lines.append(f"原始材料摘要(digest): {materials}")
    return "\n".join(lines)
