# bdsc-cli

Small CLI for syncing public Bloomington Drosophila Stock Center datasets and
querying them locally.

Primary source:

- https://bdsc.indiana.edu/stocks/stockdata.html

What it does:

- syncs BDSC CSV datasets into a local cache
- builds a local SQLite index
- supports local text search and stock lookups
- exposes optional live search against BDSC's current web endpoint

No third-party Python dependencies.

## Install

```bash
cd ~/git/bdsc-cli
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
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

Use a custom cache/index location:

```bash
bdsc sync --state-dir ./data
bdsc search Chronos --state-dir ./data
```

## Commands

- `bdsc sync`: download the BDSC CSV datasets; builds the index by default
- `bdsc build-index`: rebuild the SQLite index from previously downloaded CSVs
- `bdsc search <query>`: local full-text search
- `bdsc stock <stknum>`: local stock details
- `bdsc live-search <query>`: direct POST to BDSC's live search endpoint

## Notes

- `sync` uses conditional HTTP headers when possible (`ETag`,
  `If-Modified-Since`) to avoid re-downloading unchanged files.
- Local lookup is built from the public CSV dumps, not the private site search
  endpoints.
- The live endpoint is undocumented and may change without notice.
