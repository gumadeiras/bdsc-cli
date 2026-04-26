# AGENTS.md

Purpose: fast agent use of `bdsc-cli` data.

## Setup

- repo root: `/Users/gumadeiras/git/bdsc-cli`
- install: `.venv/bin/python -m pip install -e .`
- cli: `.venv/bin/bdsc`
- default local state: `~/.local/share/bdsc-cli`
- custom state: `--state-dir /path/to/state`

## Fetch

- first sync:
  - `.venv/bin/bdsc sync`
- rebuild local index from existing CSVs:
  - `.venv/bin/bdsc build-index`
- check freshness/counts:
  - `.venv/bin/bdsc status --json`

Notes:
- local data comes from public BDSC CSV dumps
- local search/index is the default source of truth
- `live-search` is optional; use only when you need current site behavior

## Best Commands

- primary interactive query:
  - `.venv/bin/bdsc find Chronis --json`
  - `.venv/bin/bdsc find 'Or56a Lexa' --json`
  - `.venv/bin/bdsc find RRID:BDSC_77118 --json`
  - `.venv/bin/bdsc find FBti0195688 --json`
  - `.venv/bin/bdsc find --kind property optogen --json`
  - `.venv/bin/bdsc find --kind property-exact lexA --json`
  - `.venv/bin/bdsc find --kind driver-family qf --json`
  - `.venv/bin/bdsc find --kind relationship codng --json`
- compound filters:
  - `.venv/bin/bdsc find --gene Or56a --property lexA --json`
  - `.venv/bin/bdsc find --gene Or42b --driver-family lexA --json`
  - `.venv/bin/bdsc find --gene Or56a --property-exact lexA --json`
  - `.venv/bin/bdsc find --dataset genes --property olfactory --relationship coding --jsonl`

## Machine Consumption

Prefer:
- `--json` for one query / one payload
- `--jsonl` for lists, batches, streaming, and agent pipelines
- `export` for normalized rows

Examples:
- primary lookup:
  - `.venv/bin/bdsc find Chronos --json`
- export normalized subsets:
  - `.venv/bin/bdsc export components --gene Or56a --property lexA --format jsonl`
  - `.venv/bin/bdsc export components --gene Or42b --driver-family qf --format jsonl`
  - `.venv/bin/bdsc export genes --property olfactory --relationship coding --format jsonl`

Datasets:
- `stocks`
- `components`
- `genes`
- `properties`

## Discovery

Use vocab discovery before narrow filtering:

- `.venv/bin/bdsc terms properties --limit 20 --json`
- `.venv/bin/bdsc terms relationships --limit 20 --json`
- `.venv/bin/bdsc terms property-descriptions --query optogenetic --json`

## Query Strategy

Use this order:

1. `terms` if you do not know the controlled vocabulary
2. `find` for almost all interactive retrieval
3. `property-exact` / `driver-family` when `property` is too noisy
4. `find --gene ... --property ...` for intersections
5. `export ... --format jsonl` for downstream agent processing
6. bare `find <messy term>` when the term is partial or typo-prone
7. `live-search` only for site-current behavior

## Expectations

- `find` and legacy direct query commands are typo-tolerant, not semantic search
- exact matches should rank first; fuzzy fallback broadens recall
- `property` is broad; prefer `property-exact` or `driver-family` for
  trustworthy LexA/QF/GAL4-family answers
- BDSC data is large; keep agent prompts/results scoped with `--limit`
- use `find --json`, `find --jsonl`, or `export` instead of post-filtering raw
  prose output
