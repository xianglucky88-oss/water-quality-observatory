from __future__ import annotations

import argparse
import json

from .application import reprocess, run_fixture, run_url
from .rules import DEFAULT_RULES


def _outcome_to_dict(outcome) -> dict:
    return {
        "run_id": outcome.run_id,
        "mode": outcome.mode,
        "affected": outcome.affected,
        "unchanged": outcome.unchanged,
        "quarantined": outcome.quarantined,
        "items": outcome.items,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="水环境监测与污染事件数据平台")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--fixture")
    source.add_argument("--url")
    parser.add_argument("--database")
    parser.add_argument("--raw-dir")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--records-key")
    parser.add_argument(
        "--reprocess", action="store_true",
        help="re-evaluate stored records against current rules (preview unless --apply is given)",
    )
    parser.add_argument("--apply", action="store_true", help="commit reprocessing writes")
    parser.add_argument(
        "--preview", action="store_true",
        help="ingest preview: evaluate only, write no observations or quarantine rows",
    )
    args = parser.parse_args()

    if args.reprocess:
        if not args.database:
            parser.error("--database is required with --reprocess")
        outcome = reprocess(args.database, DEFAULT_RULES, apply=args.apply)
        print(json.dumps(_outcome_to_dict(outcome), ensure_ascii=False, sort_keys=True))
        return

    if not args.database or not (args.fixture or args.url):
        parser.error("--database and one of --fixture/--url are required")

    if args.preview:
        if args.fixture:
            result = run_fixture(args.fixture, args.database, args.raw_dir, preview=True)
        else:
            result = run_url(
                args.url, args.database, args.raw_dir,
                args.timeout, args.records_key, preview=True,
            )
    elif args.fixture:
        result = run_fixture(args.fixture, args.database, args.raw_dir)
    else:
        result = run_url(args.url, args.database, args.raw_dir, args.timeout, args.records_key)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
