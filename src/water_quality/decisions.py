"""Pure, deterministic revision adjudication state machine.

The function in this module takes *all* evidence currently known for one logical
subject (station + sample + metric) and derives the state of every report plus the
subject-level regulatory status. It holds no I/O, no clock and no global state:

* the same evidence set and the same config always produce the same result;
* later evidence never deletes earlier evidence, it only changes derived states;
* subjects with an unresolved dispute can never become final regulatory data.
"""

from __future__ import annotations

from typing import Any, Mapping

from .adjudication import AdjudicationConfig

# Report-level lifecycle states (queryable).
PENDING = "pending"
CONFIRMED = "confirmed"
SUPERSEDED = "superseded"
DISPUTED = "disputed"
WITHDRAWN = "withdrawn"

# Subject-level regulatory status.
SUBJECT_PENDING = "pending"
SUBJECT_CONFIRMED = "confirmed"
SUBJECT_DISPUTED = "disputed"

REPORT_STATES = frozenset({PENDING, CONFIRMED, SUPERSEDED, DISPUTED, WITHDRAWN})
SUBJECT_STATES = frozenset({SUBJECT_PENDING, SUBJECT_CONFIRMED, SUBJECT_DISPUTED})


def _eligible(report: Mapping[str, Any], config: AdjudicationConfig) -> bool:
    return config.quality_eligible(str(report["quality_flag"]))


def adjudicate_subject(
    reports: list[Mapping[str, Any]],
    withdrawn_record_keys: frozenset[str],
    config: AdjudicationConfig,
) -> dict[str, Any]:
    """Recompute every state for one subject from its full evidence set.

    ``reports`` items expose: report_hash, record_key, source, quality_flag,
    content_hash and arrival_order. Decisions are keyed by the unique
    ``report_hash`` so two distinct transmissions sharing a natural key never
    collapse. ``arrival_order`` is a gap-free global sequence assigned at first
    landing, which makes ordering deterministic across replays.
    """
    decisions: dict[str, dict[str, str]] = {}
    active: list[dict[str, Any]] = []

    for report in sorted(reports, key=lambda item: int(item["arrival_order"])):
        report_hash = str(report["report_hash"])
        if str(report["record_key"]) in withdrawn_record_keys:
            # A withdrawal targets the natural observation key; every transmission
            # of that observation is withdrawn while remaining fully traceable.
            decisions[report_hash] = {"state": WITHDRAWN, "reason": "withdrawn_by_source"}
            continue
        active.append(dict(report))

    subject_state = SUBJECT_PENDING
    subject_reason = "no_eligible_report"

    if active:
        for report in active:
            report["priority"] = config.priority_of(str(report["source"]))
            report["eligible"] = _eligible(report, config)

        eligible_reports = [report for report in active if report["eligible"]]
        if not eligible_reports:
            # Ineligible evidence (e.g. quality_flag = bad/unknown) is retained and
            # queryable but can never publish a regulatory conclusion.
            for report in active:
                decisions[report["report_hash"]] = {
                    "state": PENDING,
                    "reason": "quality_not_eligible",
                }
        else:
            top_priority = max(report["priority"] for report in eligible_reports)
            top_reports = [report for report in eligible_reports if report["priority"] == top_priority]
            top_hashes = {report["report_hash"] for report in top_reports}
            distinct_contents = {report["content_hash"] for report in top_reports}

            if len(distinct_contents) > 1:
                # Two equally authoritative results disagree: a human must resolve it.
                # The subject must not leak into final regulatory data.
                earliest_order = min(int(report["arrival_order"]) for report in top_reports)
                for report in active:
                    report_hash = report["report_hash"]
                    if report_hash in top_hashes:
                        state, reason = DISPUTED, "conflicting_equal_priority_results"
                    elif report["eligible"] and int(report["arrival_order"]) < earliest_order:
                        state, reason = DISPUTED, "underlying_result_in_dispute"
                    else:
                        state, reason = PENDING, "awaiting_dispute_resolution"
                    decisions[report_hash] = {"state": state, "reason": reason}
                subject_state = SUBJECT_DISPUTED
                subject_reason = "conflicting_equal_priority_results"
            else:
                # Deterministic winner: highest precedence, earliest arrival on ties.
                winner = min(top_reports, key=lambda report: int(report["arrival_order"]))
                winner_hash = winner["report_hash"]
                winner_content = winner["content_hash"]
                for report in active:
                    report_hash = report["report_hash"]
                    if report_hash == winner_hash:
                        state, reason = CONFIRMED, "highest_priority_earliest_arrival"
                    elif report["content_hash"] == winner_content:
                        state, reason = SUPERSEDED, "same_content_duplicate_evidence"
                    elif not report["eligible"]:
                        state, reason = PENDING, "quality_not_eligible"
                    else:
                        # Eligible but lower precedence than the winner.
                        state, reason = SUPERSEDED, "higher_priority_result_published"
                    decisions[report_hash] = {"state": state, "reason": reason}
                subject_state = SUBJECT_CONFIRMED
                subject_reason = "highest_priority_earliest_arrival"

    return {
        "subject_state": subject_state,
        "subject_reason": subject_reason,
        "reports": decisions,
    }
