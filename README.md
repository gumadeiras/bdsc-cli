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

Another computer:

```bash
brew tap gumadeiras/tap
brew install bdsc-cli
```

Or install the release wheel directly with `pipx`:

```bash
pipx install 'bdsc-cli @ https://github.com/gumadeiras/bdsc-cli/releases/download/v0.2.1/bdsc_cli-0.2.1-py3-none-any.whl'
```

Or with plain `pip`:

```bash
python3 -m pip install 'bdsc-cli @ https://github.com/gumadeiras/bdsc-cli/releases/download/v0.2.1/bdsc_cli-0.2.1-py3-none-any.whl'
```

Source install:

```bash
git clone https://github.com/gumadeiras/bdsc-cli.git
cd bdsc-cli
python3 -m pip install .
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

Build release artifacts locally:

```bash
python -m pip install -e .[release]
python -m build
python -m twine check dist/*
python scripts/render_homebrew_formula.py dist/bdsc_cli-$(python - <<'PY'
from bdsc_cli import __version__
print(__version__)
PY
).tar.gz
```

PyPI note:

- the GitHub release is live
- PyPI trusted publishing is not configured yet for `bdsc-cli`
- `pip install bdsc-cli` will work after that publisher is added

## Quickstart

Create a local cache and index:

```bash
bdsc sync
```

Then query it:

```bash
bdsc find Chronos
bdsc find 'Or56a Lexa'
bdsc report optogenetics
bdsc find --gene Or56a --property lexA
bdsc find --gene Or42b --driver-family lexA
bdsc find RRID:BDSC_77118
bdsc find FBti0195688
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

Use `find` for nearly all interactive querying:

```bash
bdsc find Chronos
bdsc find Chronis
bdsc find 'Or56a Lexa'
bdsc find FBgn0003996 --json
bdsc find RRID:BDSC_77118
bdsc find FBti0195688
bdsc find --kind property VALIUM20
bdsc find --kind property-exact lexA
bdsc find --kind driver-family QF
bdsc find --kind relationship RNAi
bdsc find --gene Or56a --property lexA
bdsc find --gene Or42b --driver-family lexA
bdsc find --gene Or42b --driver-family qf
bdsc find --dataset genes --property olfactory --relationship coding --jsonl
```

Use canned reports for common retrieval buckets:

```bash
bdsc report olfactory
bdsc report drivers --jsonl
bdsc report optogenetics --limit 50
```

Inspect cache/index status:

```bash
bdsc status
```

Use a custom cache/index location:

```bash
bdsc sync --state-dir ./data
bdsc find Chronos --state-dir ./data
```

Structured output for scripts or agents:

```bash
bdsc status --json
bdsc find Chronos --json
bdsc find FBgn0003996 --dataset genes --json
bdsc find --gene Or56a --property lexA --json
bdsc export components --limit 5 --format jsonl
bdsc stock 77118 --json
```

## Commands

- `bdsc sync`: download the BDSC CSV datasets; builds the index by default
- `bdsc build-index`: rebuild the SQLite index from previously downloaded CSVs
- `bdsc status`: show local dataset freshness and index metadata
- `bdsc find [query]`: primary query command; free-text lookup or compound filters
- `bdsc report <name>`: canned reports for `olfactory`, `drivers`, `optogenetics`
- `bdsc export <dataset>`: stream normalized rows as `jsonl`, `csv`, or `tsv`
- `bdsc terms <scope>`: inspect available property/relationship vocab
- `bdsc stock <stknum>`: local stock details
- legacy compatibility shims still exist for `search`, `gene`, `component`, `fbid`, `rrid`, `property`, `property-exact`, `driver-family`, `relationship`, `lookup`, `filter`, `live-search`

## Find

Use `find` when the caller does not want to choose a dedicated query command up
front.

Auto-detect rules:

- digits -> `stock`
- `RRID:BDSC_*` or `BDSC_*` -> `rrid`
- `FBgn...` -> `gene`
- `FBti...` / `FBal...` / similar `FB..` ids in the component table -> `fbid`
- transgene/component-like text (`P{...}`, brackets, `attP`, `CyO`) -> `component`
- multi-term or dotted construct fragments -> local full-text `search`
- `--kind property` when you want property-driven lookup explicitly
- single bare terms -> `gene`, then local full-text `search` fallback if no gene hits

Examples:

```bash
bdsc find Chronos
bdsc find RRID:BDSC_77118
bdsc find --kind component 'P{10XUAS-Chronos'
bdsc find --kind property VALIUM20
bdsc find --kind property-exact lexA
bdsc find --kind driver-family qf
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
bdsc export genes --query Chronos --kind gene
bdsc export components --query FBti0195688 --kind fbid --format jsonl
bdsc export properties --query VALIUM20 --kind property --format tsv
bdsc export components --gene Or56a --property lexA --format jsonl
bdsc export components --gene Or42b --driver-family qf --format jsonl
bdsc export genes --property olfactory --relationship coding --format csv
bdsc export components --format tsv --output components.tsv
bdsc export genes --format csv --output genes.csv
bdsc export properties --limit 20 --format jsonl
```

`export --query` uses the same lookup kinds as `find --kind`:

- `stock`
- `rrid`
- `gene`
- `fbid`
- `component`
- `property`
- `property-exact`
- `driver-family`
- `relationship`
- `search`
- `auto`

You can also stack explicit filter flags on `export`; multiple flags combine as
AND:

- `--stock`
- `--rrid`
- `--gene`
- `--component`
- `--fbid`
- `--property`
- `--property-exact`
- `--driver-family`
- `--relationship`
- `--search`

## Compound Find

`find` also subsumes compound filters. Default dataset: `components`.

Examples:

```bash
bdsc find --gene Or56a --property lexA
bdsc find --gene Or67d --property qf
bdsc find --gene Or42b --driver-family lexA
bdsc find --gene Or56a --property-exact lexA
bdsc find --dataset stocks --property optogenetic
bdsc find --dataset genes --property olfactory --relationship coding --jsonl
```

## Reports

Use `report` for curated high-level buckets that would otherwise need multiple
queries or OR filters.

Reports:

- `olfactory`: receptor-family genes (`Or*`, `Orco`, `Ir*`, `Obp*`)
- `drivers`: GAL4 / lexA / QF / split-driver / FLP-like driver surfaces
- `optogenetics`: common optogenetic effectors plus optogenetic-tagged properties

Examples:

```bash
bdsc report olfactory
bdsc report olfactory --dataset genes --jsonl
bdsc report drivers --limit 50 --json
bdsc report optogenetics --dataset components --jsonl
```

## Terms

Use `terms` when you need to discover the vocabulary before filtering.

Scopes:

- `properties`
- `property-descriptions`
- `relationships`

Examples:

```bash
bdsc terms properties --limit 20
bdsc terms properties --query VALIUM --json
bdsc terms relationships --limit 20
bdsc terms property-descriptions --query optogenetic --jsonl
```

## Notes

- `sync` uses conditional HTTP headers when possible (`ETag`,
  `If-Modified-Since`) to avoid re-downloading unchanged files.
- Local lookup is built from the public CSV dumps, not the private site search
  endpoints.
- `search` now uses a two-stage index: exact/prefix FTS first, trigram fuzzy
  fallback second. Typos and loose spacing/punctuation usually still find the
  intended stock without having the exact BDSC string.
- `find` is the intended interactive entrypoint; dedicated legacy query
  commands still work but are no longer the main documented path.
- direct lookup paths also rerank fuzzy candidates when exact/prefix matching
  misses.
- use `property-exact` or `driver-family` when `property` is too broad for a
  reliable LexA/QF/GAL4-style answer.
- tag pushes like `vX.Y.Z` run the release workflow: build artifacts, create a
  GitHub release, and publish to PyPI.
- `scripts/render_homebrew_formula.py` renders a Homebrew formula from a built
  sdist; use it when updating a tap after a release.
- The live endpoint is undocumented and may change without notice.
- BDSC data is large enough that the first full sync/index can take a few
  minutes depending on network and disk speed.
