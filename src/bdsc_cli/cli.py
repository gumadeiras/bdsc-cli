from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .core import (
    build_index,
    format_search_results,
    format_stock,
    format_sync_results,
    get_stock,
    live_search,
    resolve_state_dir,
    search_local,
    sync_datasets,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bdsc", description="Sync and query BDSC data")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync", help="download public BDSC CSV datasets")
    sync_parser.add_argument("--state-dir", help="cache/index directory")
    sync_parser.add_argument("--force", action="store_true", help="skip conditional HTTP headers")
    sync_parser.add_argument(
        "--skip-index",
        action="store_true",
        help="download only; do not rebuild the local SQLite index",
    )

    build_parser_cmd = subparsers.add_parser(
        "build-index", help="rebuild the local SQLite index from downloaded CSVs"
    )
    build_parser_cmd.add_argument("--state-dir", help="cache/index directory")

    search_parser = subparsers.add_parser("search", help="query the local SQLite index")
    search_parser.add_argument("query")
    search_parser.add_argument("--state-dir", help="cache/index directory")
    search_parser.add_argument("--limit", type=int, default=10)
    search_parser.add_argument("--json", action="store_true")

    stock_parser = subparsers.add_parser("stock", help="show local details for one stock")
    stock_parser.add_argument("stknum", type=int)
    stock_parser.add_argument("--state-dir", help="cache/index directory")
    stock_parser.add_argument("--json", action="store_true")

    live_parser = subparsers.add_parser(
        "live-search", help="hit BDSC's current live search endpoint directly"
    )
    live_parser.add_argument("query")
    live_parser.add_argument("--limit", type=int, default=10)
    live_parser.add_argument("--json", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "sync":
            state_dir = resolve_state_dir(args.state_dir)
            results = sync_datasets(state_dir, force=args.force)
            print(format_sync_results(results))
            if not args.skip_index:
                counts = build_index(state_dir)
                print(json.dumps({"indexed": counts, "state_dir": str(state_dir)}, indent=2))
            return 0

        if args.command == "build-index":
            state_dir = resolve_state_dir(args.state_dir)
            counts = build_index(state_dir)
            print(json.dumps({"indexed": counts, "state_dir": str(state_dir)}, indent=2))
            return 0

        if args.command == "search":
            results = search_local(resolve_state_dir(args.state_dir), args.query, limit=args.limit)
            if args.json:
                print(json.dumps(results, indent=2))
            else:
                print(format_search_results(results))
            return 0

        if args.command == "stock":
            stock = get_stock(resolve_state_dir(args.state_dir), args.stknum)
            if args.json:
                print(json.dumps(stock, indent=2))
            else:
                print(format_stock(stock))
            return 0 if stock else 1

        if args.command == "live-search":
            results = live_search(args.query, limit=args.limit)
            if args.json:
                print(json.dumps(results, indent=2))
            else:
                print(format_search_results(results))
            return 0

        parser.error(f"unknown command: {args.command}")
        return 2
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

