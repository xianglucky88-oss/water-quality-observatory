"""Operator-facing read models for revision adjudication.

All output is derived from the evidence ledger and the transition history. It never
touches environment variables, headers or any authentication material: operators
see conclusions, supersession reasons and linked evidence only.
"""

from __future__ import annotations

from typing import Any

from .storage import DataStore

STATE_LABELS = {
    "pending": "待裁决",
    "confirmed": "已确认",
    "superseded": "已替代",
    "disputed": "争议",
    "withdrawn": "撤回",
}

REASON_LABELS = {
    "no_eligible_report": "尚无满足质量条件的结果",
    "quality_not_eligible": "质量标记不满足确认条件",
    "highest_priority_earliest_arrival": "来源优先级最高且到达最早",
    "same_content_duplicate_evidence": "相同内容的重复/佐证证据",
    "higher_priority_result_published": "更高优先级结果已发布",
    "conflicting_equal_priority_results": "同优先级结果互相矛盾",
    "underlying_result_in_dispute": "关联结果存在未决争议",
    "awaiting_dispute_resolution": "等待争议裁决",
    "withdrawn_by_source": "来源方撤回",
}

SUBJECT_LABELS = {
    "pending": "待裁决",
    "confirmed": "已确认",
    "disputed": "争议",
}


def _label(mapping: dict[str, str], code: str | None) -> str | None:
    if code is None:
        return None
    return mapping.get(code, code)


def list_subjects(store: DataStore, status: str | None = None, final_only: bool = False) -> list[dict[str, Any]]:
    """List subject-level adjudication rows.

    ``final_only`` returns exactly the rows safe to publish as final regulatory
    data (subject state = confirmed). Disputed subjects are always excluded there.
    """
    conn = store.conn
    sql = "SELECT * FROM subject_states"
    clauses: list[str] = []
    params: list[Any] = []
    if final_only:
        clauses.append("state = 'confirmed'")
    elif status:
        clauses.append("state = ?")
        params.append(status)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY station_id, sample_id, metric"
    rows = conn.execute(sql, params).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["state_label"] = _label(SUBJECT_LABELS, row["state"])
        item["reason_label"] = _label(REASON_LABELS, row["reason"])
        result.append(item)
    return result


def subject_detail(store: DataStore, skey: str) -> dict[str, Any]:
    """Full traceability view for one station/sample/metric subject."""
    subject = store.conn.execute("SELECT * FROM subject_states WHERE subject_key=?", (skey,)).fetchone()
    if subject is None:
        raise KeyError(f"subject not found: {skey}")

    evidence_rows = store.conn.execute(
        """SELECT re.*, rs.state, rs.reason, rs.updated_at AS state_updated_at
           FROM report_evidence re
           LEFT JOIN report_states rs ON rs.report_hash = re.report_hash
           WHERE re.subject_key=?
           ORDER BY re.arrival_order""",
        (skey,),
    ).fetchall()
    reports = []
    for row in evidence_rows:
        item = dict(row)
        item["state_label"] = _label(STATE_LABELS, row["state"])
        item["reason_label"] = _label(REASON_LABELS, row["reason"])
        item.pop("payload_json", None)
        reports.append(item)
    withdrawal_rows = store.conn.execute(
        "SELECT directive_hash, source, target_record_key, observed_at, batch_id, first_seen_at "
        "FROM withdrawal_directives WHERE subject_key=? ORDER BY first_seen_at",
        (skey,),
    ).fetchall()

    transition_rows = store.conn.execute(
        """SELECT transition_id, report_hash, scope, previous_state, new_state, reason, batch_id, created_at
           FROM state_transitions WHERE subject_key=? ORDER BY transition_id""",
        (skey,),
    ).fetchall()
    transitions = []
    for row in transition_rows:
        item = dict(row)
        item["new_state_label"] = _label(STATE_LABELS, row["new_state"]) or _label(SUBJECT_LABELS, row["new_state"])
        item["previous_state_label"] = _label(STATE_LABELS, row["previous_state"]) or _label(
            SUBJECT_LABELS, row["previous_state"]
        )
        item["reason_label"] = _label(REASON_LABELS, row["reason"])
        transitions.append(item)

    confirmed = None
    if subject["confirmed_report_hash"]:
        confirmed_row = store.conn.execute(
            "SELECT * FROM report_evidence WHERE report_hash=?",
            (subject["confirmed_report_hash"],),
        ).fetchone()
        if confirmed_row is not None:
            confirmed = dict(confirmed_row)
            confirmed.pop("payload_json", None)

    subject_item = dict(subject)
    subject_item["state_label"] = _label(SUBJECT_LABELS, subject["state"])
    subject_item["reason_label"] = _label(REASON_LABELS, subject["reason"])
    return {
        "subject": subject_item,
        "confirmed_report": confirmed,
        "reports": reports,
        "withdrawals": [dict(row) for row in withdrawal_rows],
        "transitions": transitions,
    }


def status_summary(store: DataStore) -> dict[str, Any]:
    """Counts per subject/report state plus the publishable regulatory view size."""
    subject_counts = dict(
        store.conn.execute("SELECT state, COUNT(*) FROM subject_states GROUP BY state").fetchall()
    )
    report_counts = dict(
        store.conn.execute("SELECT state, COUNT(*) FROM report_states GROUP BY state").fetchall()
    )
    return {
        "subjects": {state: subject_counts.get(state, 0) for state in ("pending", "confirmed", "disputed")},
        "reports": {
            state: report_counts.get(state, 0)
            for state in ("pending", "confirmed", "superseded", "disputed", "withdrawn")
        },
        "final_regulatory_subjects": subject_counts.get("confirmed", 0),
        "unresolved_disputes": subject_counts.get("disputed", 0),
    }
