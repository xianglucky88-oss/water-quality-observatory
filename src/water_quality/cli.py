from __future__ import annotations

import argparse
import json
import sys

from .application import run_fixture, run_url
from .assessment import (
    SUPPORTED_METRICS,
    assess_window,
    build_policy,
    render_summary,
)


def _build_ingest_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run 水环境监测与污染事件数据平台 ingestion")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture")
    source.add_argument("--url")
    parser.add_argument("--database", required=True)
    parser.add_argument("--raw-dir")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--records-key")
    return parser


def _build_assess_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="water-quality-observatory assess",
        description="窗口级证据完整度评估（只读，不修改数据库，不判定污染）",
    )
    parser.add_argument("--database", required=True, help="摄取数据库 SQLite 路径")
    parser.add_argument("--station", required=True, help="站点 ID（大小写不敏感）")
    parser.add_argument("--start", required=True, help="窗口起始（含）ISO-8601")
    parser.add_argument("--end", required=True, help="窗口结束（不含）ISO-8601")
    parser.add_argument(
        "--metrics",
        help=f"逗号分隔的指标，默认全部：{','.join(SUPPORTED_METRICS)}",
    )
    parser.add_argument(
        "--policy-file",
        help="JSON 文件：按指标覆盖 min_observations / max_gap_seconds",
    )
    parser.add_argument(
        "--arrival-deadline",
        help="迟到判定到达截止时刻 ISO-8601，默认等于窗口结束",
    )
    parser.add_argument("--as-of", help="评估基准时刻 ISO-8601，默认当前 UTC")
    parser.add_argument(
        "--format", choices=("json", "summary"), default="summary",
        help="输出格式：summary（默认，命令行摘要）或 json（结构化输出）",
    )
    parser.add_argument("--output", help="将结构化 JSON 写入文件（仍同时打印摘要）")
    return parser


def _load_policy(path: str | None) -> dict | None:
    if not path:
        return None
    with open(path, encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("policy file must contain a JSON object keyed by metric")
    # Validate eagerly so configuration errors surface before touching data.
    build_policy(value)
    return value


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "assess":
        args = _build_assess_parser().parse_args(argv[1:])
        metrics = None
        if args.metrics:
            metrics = [item.strip() for item in args.metrics.split(",") if item.strip()]
        result = assess_window(
            database=args.database,
            station_id=args.station,
            start=args.start,
            end=args.end,
            metrics=metrics,
            policy_overrides=_load_policy(args.policy_file),
            arrival_deadline=args.arrival_deadline,
            as_of=args.as_of,
        )
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                json.dump(result, handle, ensure_ascii=False, indent=2, sort_keys=True)
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(render_summary(result))
        return

    args = _build_ingest_parser().parse_args(argv)
    if args.fixture:
        result = run_fixture(args.fixture, args.database, args.raw_dir)
    else:
        result = run_url(args.url, args.database, args.raw_dir, args.timeout, args.records_key)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
