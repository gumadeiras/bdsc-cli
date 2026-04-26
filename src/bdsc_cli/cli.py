from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from . import __version__
from .core import (
    build_index,
    EXPORT_DATASETS,
    format_component_results,
    format_dataset_results,
    format_gene_results,
    format_lookup_result,
    format_search_results,
    format_stock,
    format_sync_results,
    format_term_results,
    get_status,
    get_stock,
    get_stock_by_rrid,
    iter_export_rows,
    iter_report_rows,
    list_terms,
    live_search,
    LOOKUP_KINDS,
    lookup_query,
    QueryCriterion,
    REPORT_NAMES,
    REPORT_SPECS,
    resolve_state_dir,
    search_component,
    search_fbid,
    search_gene,
    search_local,
    search_property,
    search_relationship,
    sync_datasets,
    TERM_SCOPES,
)


FILTER_ARGUMENTS = (
    ("stock", "match stock number"),
    ("rrid", "match RRID:BDSC_*"),
    ("gene", "match gene symbol or FBgn"),
    ("component", "match component symbol"),
    ("fbid", "match FlyBase component id"),
    ("property", "match component property synonym/description"),
    ("relationship", "match component-gene relationship"),
    ("search", "substring search across stock text"),
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

    export_parser = subparsers.add_parser(
        "export", help="stream normalized rows for stocks/components/genes/properties"
    )
    export_parser.add_argument("dataset", choices=EXPORT_DATASETS)
    export_parser.add_argument("--state-dir", help="cache/index directory")
    export_parser.add_argument("--limit", type=int, help="max rows to emit")
    export_parser.add_argument("--query", help="filter exported rows by a query value")
    export_parser.add_argument(
        "--kind",
        choices=LOOKUP_KINDS,
        default="auto",
        help="interpret --query as this lookup kind",
    )
    add_filter_arguments(export_parser)
    export_parser.add_argument(
        "--format",
        choices=("jsonl", "csv", "tsv"),
        default="jsonl",
        help="output format",
    )
    export_parser.add_argument(
        "--output",
        help="output path; defaults to stdout",
    )

    report_parser = subparsers.add_parser(
        "report",
        help="canned reports for common BDSC retrieval tasks",
    )
    report_parser.add_argument("name", choices=REPORT_NAMES)
    report_parser.add_argument(
        "--dataset",
        choices=EXPORT_DATASETS,
        help="override the report's default dataset",
    )
    report_parser.add_argument("--state-dir", help="cache/index directory")
    report_parser.add_argument("--limit", type=int, default=20)
    report_parser.add_argument("--json", action="store_true")
    report_parser.add_argument("--jsonl", action="store_true")

    filter_parser = subparsers.add_parser(
        "filter",
        help="compound AND filters across normalized datasets",
    )
    filter_parser.add_argument(
        "--dataset",
        choices=EXPORT_DATASETS,
        default="components",
        help="row shape to return",
    )
    filter_parser.add_argument("--state-dir", help="cache/index directory")
    filter_parser.add_argument("--limit", type=int, default=20)
    add_filter_arguments(filter_parser)
    filter_parser.add_argument("--json", action="store_true")
    filter_parser.add_argument("--jsonl", action="store_true")

    terms_parser = subparsers.add_parser(
        "terms",
        help="list available property/relationship vocab with counts",
    )
    terms_parser.add_argument("scope", choices=TERM_SCOPES)
    terms_parser.add_argument("--state-dir", help="cache/index directory")
    terms_parser.add_argument("--query", help="prefix/substring filter for the term list")
    terms_parser.add_argument("--limit", type=int, default=50)
    terms_parser.add_argument("--json", action="store_true")
    terms_parser.add_argument("--jsonl", action="store_true")

    status_parser = subparsers.add_parser(
        "status", help="show local dataset/index status for the current state dir"
    )
    status_parser.add_argument("--state-dir", help="cache/index directory")

    search_parser = subparsers.add_parser("search", help="query the local SQLite index")
    search_parser.add_argument("query")
    search_parser.add_argument("--state-dir", help="cache/index directory")
    search_parser.add_argument("--limit", type=int, default=10)
    search_parser.add_argument("--json", action="store_true")
    search_parser.add_argument("--jsonl", action="store_true")

    gene_parser = subparsers.add_parser(
        "gene", help="query stocks by gene symbol or FBgn identifier"
    )
    gene_parser.add_argument("query")
    gene_parser.add_argument("--state-dir", help="cache/index directory")
    gene_parser.add_argument("--limit", type=int, default=20)
    gene_parser.add_argument("--json", action="store_true")
    gene_parser.add_argument("--jsonl", action="store_true")

    component_parser = subparsers.add_parser(
        "component", help="query stocks by component symbol"
    )
    component_parser.add_argument("query")
    component_parser.add_argument("--state-dir", help="cache/index directory")
    component_parser.add_argument("--limit", type=int, default=20)
    component_parser.add_argument("--json", action="store_true")
    component_parser.add_argument("--jsonl", action="store_true")

    fbid_parser = subparsers.add_parser(
        "fbid", help="query stocks by FlyBase component identifier"
    )
    fbid_parser.add_argument("query")
    fbid_parser.add_argument("--state-dir", help="cache/index directory")
    fbid_parser.add_argument("--limit", type=int, default=20)
    fbid_parser.add_argument("--json", action="store_true")
    fbid_parser.add_argument("--jsonl", action="store_true")

    stock_parser = subparsers.add_parser("stock", help="show local details for one stock")
    stock_parser.add_argument("stknum", type=int)
    stock_parser.add_argument("--state-dir", help="cache/index directory")
    stock_parser.add_argument("--json", action="store_true")

    rrid_parser = subparsers.add_parser("rrid", help="show local details for one RRID:BDSC_*")
    rrid_parser.add_argument("query")
    rrid_parser.add_argument("--state-dir", help="cache/index directory")
    rrid_parser.add_argument("--json", action="store_true")

    property_parser = subparsers.add_parser(
        "property", help="query stocks by component property synonym or description"
    )
    property_parser.add_argument("query")
    property_parser.add_argument("--state-dir", help="cache/index directory")
    property_parser.add_argument("--limit", type=int, default=20)
    property_parser.add_argument("--json", action="store_true")
    property_parser.add_argument("--jsonl", action="store_true")

    relationship_parser = subparsers.add_parser(
        "relationship", help="query stocks by component-gene relationship label"
    )
    relationship_parser.add_argument("query")
    relationship_parser.add_argument("--state-dir", help="cache/index directory")
    relationship_parser.add_argument("--limit", type=int, default=20)
    relationship_parser.add_argument("--json", action="store_true")
    relationship_parser.add_argument("--jsonl", action="store_true")

    lookup_parser = subparsers.add_parser(
        "lookup",
        help="auto-detect query kind; supports batch args or file/stdin input",
    )
    lookup_parser.add_argument("queries", nargs="*")
    lookup_parser.add_argument("--state-dir", help="cache/index directory")
    lookup_parser.add_argument("--kind", choices=LOOKUP_KINDS, default="auto")
    lookup_parser.add_argument("--limit", type=int, default=20)
    lookup_parser.add_argument(
        "--input",
        help="read newline-delimited queries from a file path or '-' for stdin",
    )
    lookup_parser.add_argument("--json", action="store_true")
    lookup_parser.add_argument("--jsonl", action="store_true")

    live_parser = subparsers.add_parser(
        "live-search", help="hit BDSC's current live search endpoint directly"
    )
    live_parser.add_argument("query")
    live_parser.add_argument("--limit", type=int, default=10)
    live_parser.add_argument("--json", action="store_true")
    live_parser.add_argument("--jsonl", action="store_true")

    return parser


def print_jsonl(rows: list[dict]) -> None:
    for row in rows:
        print(json.dumps(row, ensure_ascii=False))


def add_filter_arguments(parser: argparse.ArgumentParser) -> None:
    for kind, help_text in FILTER_ARGUMENTS:
        parser.add_argument(
            f"--{kind}",
            dest=f"{kind}_filters",
            action="append",
            default=[],
            help=help_text,
        )


def build_filter_criteria(args: argparse.Namespace) -> list[QueryCriterion]:
    criteria: list[QueryCriterion] = []
    for kind, _ in FILTER_ARGUMENTS:
        for value in getattr(args, f"{kind}_filters", []):
            if value.strip():
                criteria.append(QueryCriterion(kind=kind, query=value))
    return criteria


def emit_output(
    payload: object,
    *,
    as_json: bool,
    as_jsonl: bool,
    formatter,
) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return
    if as_jsonl:
        if not isinstance(payload, list):
            raise ValueError("jsonl output requires a list payload")
        print_jsonl(payload)
        return
    print(formatter(payload))


def load_queries(positional_queries: list[str], input_path: str | None) -> list[str]:
    queries = [query for query in positional_queries if query.strip()]
    if input_path:
        if input_path == "-":
            source = sys.stdin.read()
        else:
            source = Path(input_path).read_text(encoding="utf-8")
        queries.extend(line.strip() for line in source.splitlines() if line.strip())
    return queries


def emit_export_rows(
    rows,
    *,
    output_format: str,
    output_path: str | None,
) -> None:
    if output_path:
        handle = Path(output_path).open("w", encoding="utf-8", newline="")
    else:
        handle = sys.stdout

    try:
        if output_format == "jsonl":
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            return

        writer = None
        delimiter = "," if output_format == "csv" else "\t"
        for row in rows:
            if writer is None:
                writer = csv.DictWriter(handle, fieldnames=list(row.keys()), delimiter=delimiter)
                writer.writeheader()
            writer.writerow(row)
    finally:
        if output_path:
            handle.close()


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

        if args.command == "export":
            emit_export_rows(
                iter_export_rows(
                    resolve_state_dir(args.state_dir),
                    args.dataset,
                    limit=args.limit,
                    criteria=build_filter_criteria(args),
                    query=args.query,
                    kind=args.kind,
                ),
                output_format=args.format,
                output_path=args.output,
            )
            return 0

        if args.command == "report":
            rows = list(
                iter_report_rows(
                    resolve_state_dir(args.state_dir),
                    args.name,
                    dataset=args.dataset,
                    limit=args.limit,
                )
            )
            report_dataset = args.dataset or REPORT_SPECS.get(args.name, None).default_dataset
            emit_output(
                rows,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=lambda payload: format_dataset_results(report_dataset, payload),
            )
            return 0 if rows else 1

        if args.command == "filter":
            criteria = build_filter_criteria(args)
            if not criteria:
                parser.error("filter requires at least one filter flag")
            rows = list(
                iter_export_rows(
                    resolve_state_dir(args.state_dir),
                    args.dataset,
                    limit=args.limit,
                    criteria=criteria,
                )
            )
            emit_output(
                rows,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=lambda payload: format_dataset_results(args.dataset, payload),
            )
            return 0 if rows else 1

        if args.command == "terms":
            results = list_terms(
                resolve_state_dir(args.state_dir),
                args.scope,
                query=args.query,
                limit=args.limit,
            )
            emit_output(
                results,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=format_term_results,
            )
            return 0

        if args.command == "status":
            print(json.dumps(get_status(resolve_state_dir(args.state_dir)), indent=2))
            return 0

        if args.command == "search":
            results = search_local(resolve_state_dir(args.state_dir), args.query, limit=args.limit)
            emit_output(
                results,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=format_search_results,
            )
            return 0

        if args.command == "gene":
            results = search_gene(resolve_state_dir(args.state_dir), args.query, limit=args.limit)
            emit_output(
                results,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=format_gene_results,
            )
            return 0

        if args.command == "component":
            results = search_component(
                resolve_state_dir(args.state_dir), args.query, limit=args.limit
            )
            emit_output(
                results,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=format_component_results,
            )
            return 0

        if args.command == "fbid":
            results = search_fbid(resolve_state_dir(args.state_dir), args.query, limit=args.limit)
            emit_output(
                results,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=format_component_results,
            )
            return 0

        if args.command == "stock":
            stock = get_stock(resolve_state_dir(args.state_dir), args.stknum)
            emit_output(stock, as_json=args.json, as_jsonl=False, formatter=format_stock)
            return 0 if stock else 1

        if args.command == "rrid":
            stock = get_stock_by_rrid(resolve_state_dir(args.state_dir), args.query)
            emit_output(stock, as_json=args.json, as_jsonl=False, formatter=format_stock)
            return 0 if stock else 1

        if args.command == "property":
            results = search_property(resolve_state_dir(args.state_dir), args.query, limit=args.limit)
            emit_output(
                results,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=format_component_results,
            )
            return 0

        if args.command == "relationship":
            results = search_relationship(
                resolve_state_dir(args.state_dir), args.query, limit=args.limit
            )
            emit_output(
                results,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=format_component_results,
            )
            return 0

        if args.command == "lookup":
            queries = load_queries(args.queries, args.input)
            if not queries:
                parser.error("lookup requires at least one query or --input")
            state_dir = resolve_state_dir(args.state_dir)
            lookup_results = [
                lookup_query(state_dir, query, kind=args.kind, limit=args.limit)
                for query in queries
            ]
            if args.json:
                print(
                    json.dumps(
                        lookup_results[0] if len(lookup_results) == 1 else lookup_results,
                        indent=2,
                        ensure_ascii=False,
                    )
                )
            elif args.jsonl:
                print_jsonl(lookup_results)
            else:
                print("\n\n".join(format_lookup_result(result) for result in lookup_results))
            return 0 if all(result["results"] for result in lookup_results) else 1

        if args.command == "live-search":
            results = live_search(args.query, limit=args.limit)
            emit_output(
                results,
                as_json=args.json,
                as_jsonl=args.jsonl,
                formatter=format_search_results,
            )
            return 0

        parser.error(f"unknown command: {args.command}")
        return 2
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
