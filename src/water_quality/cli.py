from __future__ import annotations

import argparse
import json
import sys

from .application import (
    query_subject,
    query_subjects,
    query_summary,
    run_fixture,
    run_url,
)


def _print(value) -> None:
    # Operator output contains data and evidence only; authentication material is
    # never read here and therefore can never be printed.
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _add_ingest_arguments(parser: argparse.ArgumentParser) -> None:
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture")
    source.add_argument("--url")
    parser.add_argument("--database", required=True)
    parser.add_argument("--raw-dir")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--records-key")
    parser.add_argument("--config", help="JSON file with source_priority and quality conditions")


def _run_ingest(args: argparse.Namespace) -> None:
    if args.fixture:
        result = run_fixture(args.fixture, args.database, args.raw_dir, args.config)
    else:
        result = run_url(
            args.url,
            args.database,
            args.raw_dir,
            args.timeout,
            args.records_key,
            args.config,
        )
    _print(result)


def _run_summary(args: argparse.Namespace) -> None:
    _print(query_summary(args.database))


def _run_subjects(args: argparse.Namespace) -> None:
    _print(query_subjects(args.database, status=args.status, final_only=args.final_only))


def _run_subject_detail(args: argparse.Namespace) -> None:
    _print(query_subject(args.database, args.subject_key))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="水环境监测数据平台：摄取、修订裁决与运营查询（输出不含任何认证信息）"
    )
    subparsers = parser.add_subparsers(dest="command")

    ingest = subparsers.add_parser("ingest", help="摄取一批自动站/实验室结果或撤回指令")
    _add_ingest_arguments(ingest)
    ingest.set_defaults(handler=_run_ingest)

    summary = subparsers.add_parser("summary", help="按状态统计裁决结果与可发布监管数据量")
    summary.add_argument("--database", required=True)
    summary.set_defaults(handler=_run_summary)

    subjects = subparsers.add_parser("subjects", help="列出水样指标的当前裁决结论")
    subjects.add_argument("--database", required=True)
    subjects.add_argument("--status", choices=["pending", "confirmed", "disputed"], help="按主体状态过滤")
    subjects.add_argument("--final-only", action="store_true", help="仅列出可作为最终监管数据的已确认主体")
    subjects.set_defaults(handler=_run_subjects)

    detail = subparsers.add_parser("subject", help="查看单个水样指标的结论、替代原因与关联证据")
    detail.add_argument("--database", required=True)
    detail.add_argument("--subject-key", required=True, help="格式：站点|采样标识|指标（大写）")
    detail.set_defaults(handler=_run_subject_detail)

    # Backwards-compatible flat invocation: `cli --fixture ... --database ...`
    # behaves like the `ingest` subcommand.
    argv = sys.argv[1:]
    if argv and argv[0] not in {"ingest", "summary", "subjects", "subject", "-h", "--help"}:
        legacy = argparse.ArgumentParser(add_help=False)
        _add_ingest_arguments(legacy)
        args = legacy.parse_args(argv)
        _run_ingest(args)
        return

    args = parser.parse_args()
    if not getattr(args, "handler", None):
        parser.print_help()
        return
    args.handler(args)


if __name__ == "__main__":
    main()
