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
    search_driver_family,
    search_fbid,
    search_gene,
    search_local,
    search_property,
    search_property_exact,
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
    ("property-exact", "match exact component property synonym/description"),
    ("driver-family", "match true driver family signals like GAL4/LexA/QF/FLP/split"),
    ("relationship", "match component-gene relationship"),
    ("search", "substring search across stock text"),
)

LEGACY_HELP = argparse.SUPPRESS
LEGACY_COMMANDS = {
    "filter",
    "search",
    "gene",
    "component",
    "fbid",
    "rrid",
    "property",
    "property-exact",
    "driver-family",
    "relationship",
    "lookup",
    "live-search",
}
PUBLIC_COMMAND_METAVAR = "{sync,build-index,export,report,terms,status,find,stock}"


class HelpOnErrorArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.print_help(sys.stderr)
        self.exit(2, f"\nerror: {message}\n")


def _filter_dest(kind: str) -> str:
    return f"{kind.replace('-', '_')}_filters"


def add_json_flags(parser: argparse.ArgumentParser, *, jsonl: bool = True) -> None:
    parser.add_argument("--json", action="store_true")
    if jsonl:
        parser.add_argument("--jsonl", action="store_true")


def add_limit_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--limit", type=int, help="max rows to emit")


def add_subcommand(subparsers, name: str, **kwargs):
    parser = subparsers.add_parser(name, **kwargs)
    parser.set_defaults(_command_parser=parser)
    return parser


def command_error(
    root_parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    message: str,
) -> None:
    getattr(args, "_command_parser", root_parser).error(message)


def add_query_parser(
    subparsers,
    name: str,
    help_text: str,
    *,
    jsonl: bool = True,
    hidden: bool = False,
):
    parser = add_subcommand(subparsers, name, help=LEGACY_HELP if hidden else help_text)
    parser.add_argument("query")
    parser.add_argument("--state-dir", help="cache/index directory")
    add_limit_argument(parser)
    add_json_flags(parser, jsonl=jsonl)
    return parser


def hide_legacy_commands(subparsers_action) -> None:
    subparsers_action._choices_actions = [
        action
        for action in subparsers_action._choices_actions
        if action.dest not in LEGACY_COMMANDS
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = HelpOnErrorArgumentParser(prog="bdsc", description="Sync and query BDSC data")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        metavar=PUBLIC_COMMAND_METAVAR,
        parser_class=HelpOnErrorArgumentParser,
    )

    sync_parser = add_subcommand(subparsers, "sync", help="download public BDSC CSV datasets")
    sync_parser.add_argument("--state-dir", help="cache/index directory")
    sync_parser.add_argument("--force", action="store_true", help="skip conditional HTTP headers")
    sync_parser.add_argument(
        "--skip-index",
        action="store_true",
        help="download only; do not rebuild the local SQLite index",
    )

    build_parser_cmd = add_subcommand(
        subparsers,
        "build-index", help="rebuild the local SQLite index from downloaded CSVs"
    )
    build_parser_cmd.add_argument("--state-dir", help="cache/index directory")

    export_parser = add_subcommand(
        subparsers,
        "export", help="stream normalized rows for stocks/components/genes/properties"
    )
    export_parser.add_argument("dataset", choices=EXPORT_DATASETS)
    export_parser.add_argument("--state-dir", help="cache/index directory")
    add_limit_argument(export_parser)
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

    report_parser = add_subcommand(
        subparsers,
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
    add_limit_argument(report_parser)
    report_parser.add_argument("--json", action="store_true")
    report_parser.add_argument("--jsonl", action="store_true")

    filter_parser = add_subcommand(
        subparsers,
        "filter",
        help=LEGACY_HELP,
    )
    filter_parser.add_argument(
        "--dataset",
        choices=EXPORT_DATASETS,
        default="components",
        help="row shape to return",
    )
    filter_parser.add_argument("--state-dir", help="cache/index directory")
    add_limit_argument(filter_parser)
    add_filter_arguments(filter_parser)
    filter_parser.add_argument("--json", action="store_true")
    filter_parser.add_argument("--jsonl", action="store_true")

    terms_parser = add_subcommand(
        subparsers,
        "terms",
        help="list available property/relationship vocab with counts",
    )
    terms_parser.add_argument("scope", choices=TERM_SCOPES)
    terms_parser.add_argument("--state-dir", help="cache/index directory")
    terms_parser.add_argument("--query", help="prefix/substring filter for the term list")
    terms_parser.add_argument("--limit", type=int, default=50)
    terms_parser.add_argument("--json", action="store_true")
    terms_parser.add_argument("--jsonl", action="store_true")

    status_parser = add_subcommand(
        subparsers,
        "status", help="show local dataset/index status for the current state dir"
    )
    status_parser.add_argument("--state-dir", help="cache/index directory")
    add_json_flags(status_parser, jsonl=False)

    find_parser = add_subcommand(
        subparsers,
        "find",
        help="primary query command; free-text lookup or structured compound filters",
    )
    find_parser.add_argument("query", nargs="?")
    find_parser.add_argument("--state-dir", help="cache/index directory")
    add_limit_argument(find_parser)
    find_parser.add_argument(
        "--kind",
        choices=LOOKUP_KINDS,
        default="auto",
        help="interpret the positional query as this lookup kind",
    )
    find_parser.add_argument(
        "--dataset",
        choices=EXPORT_DATASETS,
        help="return normalized rows for this dataset instead of auto-shaped lookup output",
    )
    add_filter_arguments(find_parser)
    add_json_flags(find_parser)

    add_query_parser(
        subparsers,
        "search",
        "query the local SQLite index",
        hidden=True,
    )
    add_query_parser(
        subparsers,
        "gene",
        "query stocks by gene symbol or FBgn identifier",
        hidden=True,
    )
    add_query_parser(
        subparsers,
        "component",
        "query stocks by component symbol",
        hidden=True,
    )
    add_query_parser(
        subparsers,
        "fbid",
        "query stocks by FlyBase component identifier",
        hidden=True,
    )

    stock_parser = add_subcommand(subparsers, "stock", help="show local details for one stock")
    stock_parser.add_argument("stknum", type=int)
    stock_parser.add_argument("--state-dir", help="cache/index directory")
    add_json_flags(stock_parser, jsonl=False)

    rrid_parser = add_subcommand(subparsers, "rrid", help=LEGACY_HELP)
    rrid_parser.add_argument("query")
    rrid_parser.add_argument("--state-dir", help="cache/index directory")
    add_json_flags(rrid_parser, jsonl=False)

    add_query_parser(
        subparsers,
        "property",
        "query stocks by component property synonym or description",
        hidden=True,
    )
    add_query_parser(
        subparsers,
        "property-exact",
        "query stocks by exact component property synonym or description",
        hidden=True,
    )
    add_query_parser(
        subparsers,
        "driver-family",
        "query true driver family lines like GAL4, LexA, QF, FLP, or split drivers",
        hidden=True,
    )
    add_query_parser(
        subparsers,
        "relationship",
        "query stocks by component-gene relationship label",
        hidden=True,
    )

    lookup_parser = add_subcommand(
        subparsers,
        "lookup",
        help=LEGACY_HELP,
    )
    lookup_parser.add_argument("queries", nargs="*")
    lookup_parser.add_argument("--state-dir", help="cache/index directory")
    lookup_parser.add_argument("--kind", choices=LOOKUP_KINDS, default="auto")
    add_limit_argument(lookup_parser)
    lookup_parser.add_argument(
        "--input",
        help="read newline-delimited queries from a file path or '-' for stdin",
    )
    add_json_flags(lookup_parser)

    live_parser = add_subcommand(
        subparsers,
        "live-search", help=LEGACY_HELP
    )
    live_parser.add_argument("query")
    add_limit_argument(live_parser)
    add_json_flags(live_parser)

    hide_legacy_commands(subparsers)
    return parser


def print_jsonl(rows: list[dict]) -> None:
    for row in rows:
        print(json.dumps(row, ensure_ascii=False))


def add_filter_arguments(parser: argparse.ArgumentParser) -> None:
    for kind, help_text in FILTER_ARGUMENTS:
        parser.add_argument(
            f"--{kind}",
            dest=_filter_dest(kind),
            action="append",
            default=[],
            help=help_text,
        )


def build_filter_criteria(args: argparse.Namespace) -> list[QueryCriterion]:
    criteria: list[QueryCriterion] = []
    for kind, _ in FILTER_ARGUMENTS:
        for value in getattr(args, _filter_dest(kind), []):
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


def emit_query_results(
    args: argparse.Namespace,
    results,
    *,
    formatter,
) -> int:
    emit_output(
        results,
        as_json=args.json,
        as_jsonl=args.jsonl,
        formatter=formatter,
    )
    return 0


def emit_stock_result(args: argparse.Namespace, stock: object) -> int:
    emit_output(stock, as_json=args.json, as_jsonl=False, formatter=format_stock)
    return 0 if stock else 1


def emit_lookup_payload(args: argparse.Namespace, results: list[dict]) -> int:
    if args.json:
        print(
            json.dumps(
                results[0] if len(results) == 1 else results,
                indent=2,
                ensure_ascii=False,
            )
        )
    elif args.jsonl:
        print_jsonl(results)
    else:
        print("\n\n".join(format_lookup_result(result) for result in results))
    return 0 if all(result["results"] for result in results) else 1


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


QUERY_COMMAND_SPECS = {
    "search": (search_local, format_search_results),
    "gene": (search_gene, format_gene_results),
    "component": (search_component, format_component_results),
    "fbid": (search_fbid, format_component_results),
    "property": (search_property, format_component_results),
    "property-exact": (search_property_exact, format_component_results),
    "driver-family": (search_driver_family, format_component_results),
    "relationship": (search_relationship, format_component_results),
}


def run_find(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    query = (args.query or "").strip()
    criteria = build_filter_criteria(args)
    if not query and not criteria:
        command_error(parser, args, "find requires a query or at least one filter flag")

    state_dir = resolve_state_dir(args.state_dir)
    if query and not criteria and not args.dataset:
        return emit_lookup_payload(
            args,
            [lookup_query(state_dir, query, kind=args.kind, limit=args.limit)],
        )

    rows = list(
        iter_export_rows(
            state_dir,
            args.dataset or "components",
            limit=args.limit,
            criteria=criteria,
            query=query or None,
            kind=args.kind,
        )
    )
    emit_output(
        rows,
        as_json=args.json,
        as_jsonl=args.jsonl,
        formatter=lambda payload: format_dataset_results(args.dataset or "components", payload),
    )
    return 0 if rows else 1


def run_legacy_lookup(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    queries = load_queries(args.queries, args.input)
    if not queries:
        command_error(parser, args, "lookup requires at least one query or --input")
    state_dir = resolve_state_dir(args.state_dir)
    lookup_results = [
        lookup_query(state_dir, query, kind=args.kind, limit=args.limit)
        for query in queries
    ]
    return emit_lookup_payload(args, lookup_results)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        state_dir = resolve_state_dir(getattr(args, "state_dir", None))

        if args.command == "sync":
            results = sync_datasets(state_dir, force=args.force)
            print(format_sync_results(results))
            if not args.skip_index:
                counts = build_index(state_dir)
                print(json.dumps({"indexed": counts, "state_dir": str(state_dir)}, indent=2))
            return 0

        if args.command == "build-index":
            counts = build_index(state_dir)
            print(json.dumps({"indexed": counts, "state_dir": str(state_dir)}, indent=2))
            return 0

        if args.command == "export":
            emit_export_rows(
                iter_export_rows(
                    state_dir,
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
                    state_dir,
                    args.name,
                    dataset=args.dataset,
                    limit=args.limit,
                )
            )
            report_dataset = args.dataset or REPORT_SPECS[args.name].default_dataset
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
                command_error(parser, args, "filter requires at least one filter flag")
            rows = list(
                iter_export_rows(
                    state_dir,
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

        if args.command == "find":
            return run_find(parser, args)

        if args.command == "terms":
            results = list_terms(
                state_dir,
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
            print(json.dumps(get_status(state_dir), indent=2, ensure_ascii=False))
            return 0

        if args.command in QUERY_COMMAND_SPECS:
            query_fn, formatter = QUERY_COMMAND_SPECS[args.command]
            results = query_fn(state_dir, args.query, limit=args.limit)
            return emit_query_results(args, results, formatter=formatter)

        if args.command == "stock":
            stock = get_stock(state_dir, args.stknum)
            return emit_stock_result(args, stock)

        if args.command == "rrid":
            stock = get_stock_by_rrid(state_dir, args.query)
            return emit_stock_result(args, stock)

        if args.command == "lookup":
            return run_legacy_lookup(parser, args)

        if args.command == "live-search":
            results = live_search(args.query, limit=args.limit)
            return emit_query_results(args, results, formatter=format_search_results)

        parser.error(f"unknown command: {args.command}")
        return 2
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
