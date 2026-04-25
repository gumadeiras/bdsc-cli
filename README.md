# bdsc-cli

Small CLI for syncing public Bloomington Drosophila Stock Center datasets and
querying them locally.

Repo:

- https://github.com/gumadeiras/bdsc-cli

Primary source:

- https://bdsc.indiana.edu/stocks/stockdata.html

What it does:

- syncs BDSC CSV datasets into a local cache
- builds a local SQLite index
- supports local text search and stock lookups
- exposes optional live search against BDSC's current web endpoint

No third-party Python dependencies.

## Install

Preferred: `pipx`

```bash
pipx install git+https://github.com/gumadeiras/bdsc-cli.git
```

Repo-local dev install:

```bash
cd ~/git/bdsc-cli
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
```

Check the CLI:

```bash
bdsc --help
```

## Quickstart

Create a local cache and index:

```bash
bdsc sync
```

Then query it:

```bash
bdsc search Chronos
bdsc gene Chronos
bdsc component 'P{10XUAS-Chronos'
bdsc fbid FBti0195688
bdsc rrid RRID:BDSC_77118
bdsc property VALIUM20
bdsc lookup Chronos
printf 'Chronos\nFBti0195688\n' | bdsc lookup --input - --jsonl
bdsc stock 77118
```

## Usage

Default state directory:

```text
~/.local/share/bdsc-cli
```

Sync datasets and build the local index:

```bash
bdsc sync
```

Search locally:

```bash
bdsc search Chronos
bdsc search FBgn0003996 --json
bdsc search Chronos --jsonl
```

Inspect one stock:

```bash
bdsc stock 77118
bdsc stock 77118 --json
```

Hit the live BDSC search endpoint directly:

```bash
bdsc live-search Chronos
```

Query by gene symbol or FBgn:

```bash
bdsc gene Chronos
bdsc gene FBgn0003996 --json
```

Query by component symbol, FlyBase component id, or RRID:

```bash
bdsc component 'P{10XUAS-Chronos'
bdsc fbid FBti0195688
bdsc rrid RRID:BDSC_77118
```

Query by component property:

```bash
bdsc property VALIUM20
bdsc property 'guide RNA'
```

Inspect cache/index status:

```bash
bdsc status
```

Use a custom cache/index location:

```bash
bdsc sync --state-dir ./data
bdsc search Chronos --state-dir ./data
```

Structured output for scripts or agents:

```bash
bdsc status --json
bdsc search Chronos --jsonl
bdsc gene FBgn0003996 --json
bdsc lookup Chronos FBti0195688 --json
bdsc export components --limit 5 --format jsonl
bdsc stock 77118 --json
```

## Commands

- `bdsc sync`: download the BDSC CSV datasets; builds the index by default
- `bdsc build-index`: rebuild the SQLite index from previously downloaded CSVs
- `bdsc status`: show local dataset freshness and index metadata
- `bdsc search <query>`: local full-text search
- `bdsc gene <query>`: exact/prefix lookup by gene symbol or FBgn
- `bdsc component <query>`: exact/prefix lookup by component symbol
- `bdsc fbid <query>`: exact/prefix lookup by FlyBase component identifier
- `bdsc rrid <query>`: exact lookup by `RRID:BDSC_*`
- `bdsc property <query>`: lookup by component property synonym/description
- `bdsc lookup ...`: auto-detect query kind; supports multiple args or `--input`
- `bdsc export <dataset>`: stream normalized rows as `jsonl`, `csv`, or `tsv`
- `bdsc stock <stknum>`: local stock details
- `bdsc live-search <query>`: direct POST to BDSC's live search endpoint

## Batch Lookup

Use `lookup` when the caller does not want to choose the query command up
front.

Auto-detect rules:

- digits -> `stock`
- `RRID:BDSC_*` or `BDSC_*` -> `rrid`
- `FBgn...` -> `gene`
- `FBti...` / `FBal...` / similar `FB..` ids in the component table -> `fbid`
- transgene/component-like text (`P{...}`, brackets, `attP`, `CyO`) -> `component`
- `--kind property` when you want property-driven lookup explicitly
- everything else -> `gene`, then local full-text `search` fallback if no gene hits

Examples:

```bash
bdsc lookup Chronos
bdsc lookup RRID:BDSC_77118
bdsc lookup --kind component 'P{10XUAS-Chronos'
bdsc lookup --kind property VALIUM20
bdsc lookup --input queries.txt --json
printf 'Chronos\nRRID:BDSC_77118\nFBti0195688\n' | bdsc lookup --input - --jsonl
```

## Export

Use `export` when another tool wants direct normalized rows instead of
search-oriented output.

Datasets:

- `stocks`
- `components`
- `genes`
- `properties`

Examples:

```bash
bdsc export stocks --limit 3
bdsc export components --format tsv --output components.tsv
bdsc export genes --format csv --output genes.csv
bdsc export properties --limit 20 --format jsonl
```

## Notes

- `sync` uses conditional HTTP headers when possible (`ETag`,
  `If-Modified-Since`) to avoid re-downloading unchanged files.
- Local lookup is built from the public CSV dumps, not the private site search
  endpoints.
- The live endpoint is undocumented and may change without notice.
- BDSC data is large enough that the first full sync/index can take a few
  minutes depending on network and disk speed.
