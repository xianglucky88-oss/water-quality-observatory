from __future__ import annotations

import argparse
import json
import sys

from .application import recover_batch, run_fixture, run_url, verify_batch
from .errors import IngestionError
from .faults import CHECKPOINTS, FaultInjector
from .reconciliation import list_batches, list_orphan_evidence


def _faults(value: str | None) -> FaultInjector:
    try:
        return FaultInjector(value)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run 水环境监测与污染事件数据平台 ingestion")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--fixture", help="local JSON fixture")
    mode.add_argument("--url", help="HTTP(S) JSON endpoint")
    mode.add_argument("--recover-batch", type=int, help="explicitly resume or settle a batch id")
    mode.add_argument("--recover-digest", help="explicitly resume the batch for a landed evidence digest")
    mode.add_argument("--verify-batch", type=int, help="print the reconciliation report for a batch id")
    mode.add_argument("--list-batches", action="store_true", help="print reconciliation reports for all batches")
    mode.add_argument("--list-orphans", action="store_true", help="list landed raw evidence with no registered batch")
    parser.add_argument("--database", required=True)
    parser.add_argument("--raw-dir")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--records-key")
    parser.add_argument("--fail-at", default=None, metavar="CHECKPOINT[:raise|:crash]",
                        help="comma-separated controlled checkpoints: " + ", ".join(CHECKPOINTS))
    parser.add_argument("--lease-seconds", type=float, default=60.0)
    parser.add_argument("--force", action="store_true", help="override a live lease during explicit recovery")
    parser.add_argument("--legacy-batch", action="store_true", help="always start a new batch instead of resuming an unfinished digest")
    args = parser.parse_args(argv)

    if not any([args.fixture, args.url, args.recover_batch is not None, args.recover_digest,
                args.verify_batch is not None, args.list_batches, args.list_orphans]):
        parser.error("one of --fixture, --url, --recover-batch, --recover-digest, --verify-batch, "
                     "--list-batches or --list-orphans is required")

    faults = _faults(args.fail_at)
    try:
        if args.recover_batch is not None:
            result = recover_batch(args.database, args.recover_batch, raw_dir=args.raw_dir, faults=faults,
                                   lease_seconds=args.lease_seconds, force=args.force)
        elif args.recover_digest:
            result = recover_batch(args.database, digest=args.recover_digest, raw_dir=args.raw_dir, faults=faults,
                                   lease_seconds=args.lease_seconds, force=args.force)
        elif args.verify_batch is not None:
            result = verify_batch(args.database, args.verify_batch, args.raw_dir, args.lease_seconds)
        elif args.list_batches:
            result = list_batches(args.database, args.raw_dir, args.lease_seconds)
        elif args.list_orphans:
            if not args.raw_dir:
                parser.error("--list-orphans requires --raw-dir")
            result = list_orphan_evidence(args.database, args.raw_dir)
        elif args.fixture:
            result = run_fixture(args.fixture, args.database, args.raw_dir, faults=faults,
                                 lease_seconds=args.lease_seconds, resume=not args.legacy_batch, force=args.force)
        else:
            result = run_url(args.url, args.database, args.raw_dir, args.timeout, args.records_key, faults=faults,
                             lease_seconds=args.lease_seconds, resume=not args.legacy_batch, force=args.force)
    except IngestionError as exc:
        print(json.dumps({"error": str(exc), "report": exc.report}, ensure_ascii=False, sort_keys=True, default=str))
        raise SystemExit(1) from exc
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))


if __name__ == "__main__":
    main(sys.argv[1:])
