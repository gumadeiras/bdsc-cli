from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib import error, parse, request

USER_AGENT = "bdsc-cli/0.1 (+https://bdsc.indiana.edu/)"
DEFAULT_STATE_DIR = Path(
    os.environ.get("BDSC_CLI_HOME", Path.home() / ".local" / "share" / "bdsc-cli")
)
DB_NAME = "bdsc.sqlite3"
MANIFEST_NAME = "manifest.json"

DATASETS = {
    "bloomington": "https://bdsc.indiana.edu/pdf/bloomington.csv",
    "stockcomps_map_comments": "https://bdsc.indiana.edu/pdf/stockcomps_map_comments.csv",
    "stockgenes": "https://bdsc.indiana.edu/pdf/stockgenes.csv",
    "stockgenes_compgenes": "https://bdsc.indiana.edu/pdf/stockgenes_compgenes.csv",
    "stockgenes_compprops": "https://bdsc.indiana.edu/pdf/stockgenes_compprops.csv",
}


@dataclass
class SyncResult:
    name: str
    path: Path
    status: str
    bytes_downloaded: int
    metadata: dict[str, Any]


@dataclass
class GeneMatch:
    stknum: int
    genotype: str
    component_symbol: str
    gene_symbol: str
    fbgn: str


LOOKUP_KINDS = ("auto", "stock", "rrid", "gene", "fbid", "component", "property", "search")
EXPORT_DATASETS = ("stocks", "components", "genes", "properties")


def resolve_state_dir(value: str | Path | None) -> Path:
    return Path(value).expanduser() if value else DEFAULT_STATE_DIR


def ensure_state_dir(state_dir: Path) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "raw").mkdir(parents=True, exist_ok=True)


def manifest_file(state_dir: Path) -> Path:
    return state_dir / MANIFEST_NAME


def db_file(state_dir: Path) -> Path:
    return state_dir / DB_NAME


def load_manifest(state_dir: Path) -> dict[str, Any]:
    path = manifest_file(state_dir)
    if not path.exists():
        return {"datasets": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def save_manifest(state_dir: Path, manifest: dict[str, Any]) -> None:
    manifest_file(state_dir).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def raw_file(state_dir: Path, name: str) -> Path:
    return state_dir / "raw" / f"{name}.csv"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sync_datasets(state_dir: Path, force: bool = False) -> list[SyncResult]:
    ensure_state_dir(state_dir)
    manifest = load_manifest(state_dir)
    results: list[SyncResult] = []

    for name, url in DATASETS.items():
        path = raw_file(state_dir, name)
        entry = manifest.setdefault("datasets", {}).get(name, {})
        headers = {"User-Agent": USER_AGENT}
        if not force:
            if entry.get("etag"):
                headers["If-None-Match"] = entry["etag"]
            if entry.get("last_modified"):
                headers["If-Modified-Since"] = entry["last_modified"]

        req = request.Request(url, headers=headers)
        try:
            with request.urlopen(req) as response:
                temp_path = path.with_suffix(".csv.tmp")
                size = 0
                digest = hashlib.sha256()
                with temp_path.open("wb") as handle:
                    for chunk in iter(lambda: response.read(1024 * 1024), b""):
                        size += len(chunk)
                        digest.update(chunk)
                        handle.write(chunk)
                temp_path.replace(path)
                metadata = {
                    "url": url,
                    "etag": response.headers.get("ETag"),
                    "last_modified": response.headers.get("Last-Modified"),
                    "content_length": response.headers.get("Content-Length"),
                    "sha256": digest.hexdigest(),
                    "fetched_at": _now_iso(),
                }
                manifest["datasets"][name] = metadata
                results.append(
                    SyncResult(
                        name=name,
                        path=path,
                        status="downloaded",
                        bytes_downloaded=size,
                        metadata=metadata,
                    )
                )
        except error.HTTPError as exc:
            if exc.code == 304 and path.exists():
                metadata = {
                    **entry,
                    "checked_at": _now_iso(),
                    "sha256": entry.get("sha256") or _hash_file(path),
                }
                manifest["datasets"][name] = metadata
                results.append(
                    SyncResult(
                        name=name,
                        path=path,
                        status="not-modified",
                        bytes_downloaded=0,
                        metadata=metadata,
                    )
                )
                continue
            raise RuntimeError(f"failed to download {url}: {exc}") from exc

    manifest["updated_at"] = _now_iso()
    save_manifest(state_dir, manifest)
    return results


def _iter_csv_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for encoding_errors in ("strict", "replace"):
        try:
            with path.open(
                "r",
                encoding="utf-8-sig",
                errors=encoding_errors,
                newline="",
            ) as handle:
                reader = csv.DictReader(handle)
                for raw_row in reader:
                    row = {
                        (key or "").strip(): (value or "").strip()
                        for key, value in raw_row.items()
                    }
                    if any(row.values()):
                        rows.append(row)
            return rows
        except UnicodeDecodeError:
            rows.clear()
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, f"could not decode {path}")
    return rows


def _to_int(value: str) -> int | None:
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _require_files(state_dir: Path) -> None:
    missing = [name for name in DATASETS if not raw_file(state_dir, name).exists()]
    if missing:
        missing_list = ", ".join(missing)
        raise FileNotFoundError(
            f"missing raw datasets: {missing_list}. run `bdsc sync` first"
        )


def build_index(state_dir: Path) -> dict[str, int]:
    ensure_state_dir(state_dir)
    _require_files(state_dir)
    manifest = load_manifest(state_dir)

    bloomington_rows = _iter_csv_rows(raw_file(state_dir, "bloomington"))
    component_rows = _iter_csv_rows(raw_file(state_dir, "stockcomps_map_comments"))
    stockgene_rows = _iter_csv_rows(raw_file(state_dir, "stockgenes"))
    compgene_rows = _iter_csv_rows(raw_file(state_dir, "stockgenes_compgenes"))
    compprop_rows = _iter_csv_rows(raw_file(state_dir, "stockgenes_compprops"))

    db_path = db_file(state_dir)
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(
            """
            CREATE TABLE stocks (
              stknum INTEGER PRIMARY KEY,
              genotype TEXT NOT NULL,
              chromosomes TEXT,
              aka TEXT,
              date_added TEXT,
              donor_info TEXT,
              stock_comments TEXT
            );

            CREATE TABLE component_comments (
              stknum INTEGER NOT NULL,
              genotype TEXT,
              component_symbol TEXT,
              fbid TEXT,
              mapstatement TEXT,
              comment1 TEXT,
              comment2 TEXT,
              comment3 TEXT
            );

            CREATE TABLE stockgenes (
              stknum INTEGER NOT NULL,
              genotype TEXT,
              component_symbol TEXT,
              gene_symbol TEXT,
              fbgn TEXT,
              bdsc_symbol_id INTEGER,
              bdsc_gene_id INTEGER
            );

            CREATE TABLE compgenes (
              bdsc_symbol_id INTEGER,
              bdsc_gene_id INTEGER,
              compgeneprop_id INTEGER,
              prop_syn TEXT
            );

            CREATE TABLE compprops (
              bdsc_symbol_id INTEGER,
              property_id INTEGER,
              property_descrip TEXT,
              prop_syn TEXT
            );

            CREATE TABLE search_documents (
              stknum INTEGER PRIMARY KEY,
              genotype TEXT,
              aka TEXT,
              donor_info TEXT,
              stock_comments TEXT,
              component_symbols TEXT,
              gene_symbols TEXT,
              fbgns TEXT,
              search_text TEXT
            );

            CREATE INDEX idx_component_comments_stknum ON component_comments(stknum);
            CREATE INDEX idx_stockgenes_stknum ON stockgenes(stknum);
            CREATE INDEX idx_stockgenes_gene_symbol ON stockgenes(gene_symbol);
            CREATE INDEX idx_stockgenes_fbgn ON stockgenes(fbgn);
            CREATE INDEX idx_compgenes_symbol_id ON compgenes(bdsc_symbol_id);
            CREATE INDEX idx_compprops_symbol_id ON compprops(bdsc_symbol_id);
            """
        )

        conn.executemany(
            """
            INSERT INTO stocks (
              stknum, genotype, chromosomes, aka, date_added, donor_info, stock_comments
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    _to_int(row["Stk #"]),
                    row["Genotype"],
                    row["Ch # all"],
                    row["A.K.A"],
                    row["Date added"],
                    row["Donor info"],
                    row["Stock comments"],
                )
                for row in bloomington_rows
                if _to_int(row["Stk #"]) is not None
            ],
        )

        conn.executemany(
            """
            INSERT INTO component_comments (
              stknum, genotype, component_symbol, fbid, mapstatement, comment1, comment2, comment3
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    _to_int(row["Stk #"]),
                    row["Genotype"],
                    row["component_symbol"],
                    row["fbid"],
                    row["mapstatement"],
                    row["comment1"],
                    row["comment2"],
                    row["comment3"],
                )
                for row in component_rows
                if _to_int(row["Stk #"]) is not None
            ],
        )

        conn.executemany(
            """
            INSERT INTO stockgenes (
              stknum, genotype, component_symbol, gene_symbol, fbgn, bdsc_symbol_id, bdsc_gene_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    _to_int(row["stknum"]),
                    row["genotype"],
                    row["component_symbol"],
                    row["gene_symbol"],
                    row["fbgn"],
                    _to_int(row["bdsc_symbol_id"]),
                    _to_int(row["bdsc_gene_id"]),
                )
                for row in stockgene_rows
                if _to_int(row["stknum"]) is not None
            ],
        )

        conn.executemany(
            """
            INSERT INTO compgenes (
              bdsc_symbol_id, bdsc_gene_id, compgeneprop_id, prop_syn
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (
                    _to_int(row["bdsc_symbol_id"]),
                    _to_int(row["bdsc_gene_id"]),
                    _to_int(row["compgeneprop_id"]),
                    row["prop_syn"],
                )
                for row in compgene_rows
            ],
        )

        conn.executemany(
            """
            INSERT INTO compprops (
              bdsc_symbol_id, property_id, property_descrip, prop_syn
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (
                    _to_int(row["bdsc_symbol_id"]),
                    _to_int(row["property_id"]),
                    row["property_descrip"],
                    row["prop_syn"],
                )
                for row in compprop_rows
            ],
        )

        conn.execute(
            """
            INSERT INTO search_documents (
              stknum, genotype, aka, donor_info, stock_comments,
              component_symbols, gene_symbols, fbgns, search_text
            )
            SELECT
              s.stknum,
              s.genotype,
              COALESCE(s.aka, ''),
              COALESCE(s.donor_info, ''),
              COALESCE(s.stock_comments, ''),
              COALESCE((
                SELECT group_concat(component_symbol, ' ')
                FROM (
                  SELECT DISTINCT sg.component_symbol AS component_symbol
                  FROM stockgenes sg
                  WHERE sg.stknum = s.stknum AND sg.component_symbol != ''
                  ORDER BY sg.component_symbol
                )
              ), ''),
              COALESCE((
                SELECT group_concat(gene_symbol, ' ')
                FROM (
                  SELECT DISTINCT sg.gene_symbol AS gene_symbol
                  FROM stockgenes sg
                  WHERE sg.stknum = s.stknum AND sg.gene_symbol != ''
                  ORDER BY sg.gene_symbol
                )
              ), ''),
              COALESCE((
                SELECT group_concat(fbgn, ' ')
                FROM (
                  SELECT DISTINCT sg.fbgn AS fbgn
                  FROM stockgenes sg
                  WHERE sg.stknum = s.stknum AND sg.fbgn != ''
                  ORDER BY sg.fbgn
                )
              ), ''),
              trim(
                s.stknum || ' ' ||
                COALESCE(s.genotype, '') || ' ' ||
                COALESCE(s.aka, '') || ' ' ||
                COALESCE(s.donor_info, '') || ' ' ||
                COALESCE(s.stock_comments, '') || ' ' ||
                COALESCE((
                  SELECT group_concat(component_symbol, ' ')
                  FROM (
                    SELECT DISTINCT sg.component_symbol AS component_symbol
                    FROM stockgenes sg
                    WHERE sg.stknum = s.stknum AND sg.component_symbol != ''
                  )
                ), '') || ' ' ||
                COALESCE((
                  SELECT group_concat(gene_symbol, ' ')
                  FROM (
                    SELECT DISTINCT sg.gene_symbol AS gene_symbol
                    FROM stockgenes sg
                    WHERE sg.stknum = s.stknum AND sg.gene_symbol != ''
                  )
                ), '') || ' ' ||
                COALESCE((
                  SELECT group_concat(fbgn, ' ')
                  FROM (
                    SELECT DISTINCT sg.fbgn AS fbgn
                    FROM stockgenes sg
                    WHERE sg.stknum = s.stknum AND sg.fbgn != ''
                  )
                ), '')
              )
            FROM stocks s
            """
        )

        fts_enabled = True
        try:
            conn.execute(
                """
                CREATE VIRTUAL TABLE stock_fts USING fts5(
                  stknum UNINDEXED,
                  genotype,
                  aka,
                  donor_info,
                  stock_comments,
                  component_symbols,
                  gene_symbols,
                  fbgns,
                  tokenize='porter unicode61'
                )
                """
            )
        except sqlite3.OperationalError:
            fts_enabled = False

        if fts_enabled:
            conn.execute(
                """
                INSERT INTO stock_fts (
                  stknum, genotype, aka, donor_info, stock_comments,
                  component_symbols, gene_symbols, fbgns
                )
                SELECT
                  stknum, genotype, aka, donor_info, stock_comments,
                  component_symbols, gene_symbols, fbgns
                FROM search_documents
                """
            )

        conn.commit()
        counts = {
            "stocks": len(bloomington_rows),
            "component_comments": len(component_rows),
            "stockgenes": len(stockgene_rows),
            "compgenes": len(compgene_rows),
            "compprops": len(compprop_rows),
            "fts_enabled": int(fts_enabled),
        }
        manifest["index"] = {
            "db_path": str(db_path),
            "built_at": _now_iso(),
            "counts": counts,
        }
        save_manifest(state_dir, manifest)
        return counts
    finally:
        conn.close()


def _connect(state_dir: Path) -> sqlite3.Connection:
    path = db_file(state_dir)
    if not path.exists():
        raise FileNotFoundError(f"missing index: {path}. run `bdsc sync` or `bdsc build-index`")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def build_fts_query(text: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9]+", text.lower())
    if not tokens:
        escaped = text.replace('"', '""').strip()
        return f'"{escaped}"'
    return " ".join(f"{token}*" for token in tokens)


def search_local(state_dir: Path, query: str, limit: int = 10) -> list[dict[str, Any]]:
    query = query.strip()
    if not query:
        return []

    conn = _connect(state_dir)
    try:
        if query.isdigit():
            stock = get_stock(state_dir, int(query))
            return [stock] if stock else []

        has_fts = bool(
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_fts'"
            ).fetchone()
        )
        rows: list[sqlite3.Row]

        if has_fts:
            rows = conn.execute(
                """
                SELECT
                  s.stknum,
                  s.genotype,
                  sd.gene_symbols,
                  sd.fbgns,
                  sd.component_symbols
                FROM stock_fts f
                JOIN stocks s ON s.stknum = f.stknum
                JOIN search_documents sd ON sd.stknum = s.stknum
                WHERE stock_fts MATCH ?
                ORDER BY bm25(stock_fts), s.stknum
                LIMIT ?
                """,
                (build_fts_query(query), limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                  s.stknum,
                  s.genotype,
                  sd.gene_symbols,
                  sd.fbgns,
                  sd.component_symbols
                FROM search_documents sd
                JOIN stocks s ON s.stknum = sd.stknum
                WHERE sd.search_text LIKE ?
                ORDER BY s.stknum
                LIMIT ?
                """,
                (f"%{query}%", limit),
            ).fetchall()

        return [dict(row) for row in rows]
    finally:
        conn.close()


def search_gene(state_dir: Path, query: str, limit: int = 20) -> list[dict[str, Any]]:
    query = query.strip()
    if not query:
        return []

    conn = _connect(state_dir)
    try:
        if query.upper().startswith("FBGN"):
            rows = conn.execute(
                """
                SELECT DISTINCT
                  sg.stknum,
                  sg.genotype,
                  sg.component_symbol,
                  sg.gene_symbol,
                  sg.fbgn
                FROM stockgenes sg
                WHERE UPPER(sg.fbgn) = UPPER(?)
                ORDER BY sg.stknum, sg.component_symbol, sg.gene_symbol
                LIMIT ?
                """,
                (query, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT DISTINCT
                  sg.stknum,
                  sg.genotype,
                  sg.component_symbol,
                  sg.gene_symbol,
                  sg.fbgn
                FROM stockgenes sg
                WHERE LOWER(sg.gene_symbol) = LOWER(?)
                   OR LOWER(sg.gene_symbol) LIKE LOWER(?)
                ORDER BY
                  CASE WHEN LOWER(sg.gene_symbol) = LOWER(?) THEN 0 ELSE 1 END,
                  sg.stknum,
                  sg.component_symbol,
                  sg.gene_symbol
                LIMIT ?
                """,
                (query, f"{query}%", query, limit),
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _component_metadata_subqueries(
    stock_num_expr: str,
    component_symbol_expr: str,
    symbol_id_expr: str,
) -> str:
    return f"""
      COALESCE((
        SELECT group_concat(gene_symbol, ' ')
        FROM (
          SELECT DISTINCT sg.gene_symbol AS gene_symbol
          FROM stockgenes sg
          WHERE sg.stknum = {stock_num_expr}
            AND sg.component_symbol = {component_symbol_expr}
            AND sg.gene_symbol != ''
          ORDER BY sg.gene_symbol
        )
      ), '') AS gene_symbols,
      COALESCE((
        SELECT group_concat(fbgn, ' ')
        FROM (
          SELECT DISTINCT sg.fbgn AS fbgn
          FROM stockgenes sg
          WHERE sg.stknum = {stock_num_expr}
            AND sg.component_symbol = {component_symbol_expr}
            AND sg.fbgn != ''
          ORDER BY sg.fbgn
        )
      ), '') AS fbgns,
      COALESCE((
        SELECT group_concat(prop_syn, ' | ')
        FROM (
          SELECT DISTINCT cp.prop_syn AS prop_syn
          FROM compprops cp
          WHERE cp.bdsc_symbol_id = {symbol_id_expr}
            AND cp.prop_syn != ''
          ORDER BY cp.prop_syn
        )
      ), '') AS property_syns,
      COALESCE((
        SELECT group_concat(property_descrip, ' | ')
        FROM (
          SELECT DISTINCT cp.property_descrip AS property_descrip
          FROM compprops cp
          WHERE cp.bdsc_symbol_id = {symbol_id_expr}
            AND cp.property_descrip != ''
          ORDER BY cp.property_descrip
        )
      ), '') AS property_descriptions,
      COALESCE((
        SELECT group_concat(prop_syn, ' | ')
        FROM (
          SELECT DISTINCT cg.prop_syn AS prop_syn
          FROM compgenes cg
          WHERE cg.bdsc_symbol_id = {symbol_id_expr}
            AND cg.prop_syn != ''
          ORDER BY cg.prop_syn
        )
      ), '') AS gene_relationships
    """


def _search_component_table(
    state_dir: Path,
    *,
    column: str,
    query: str,
    limit: int,
) -> list[dict[str, Any]]:
    query = query.strip()
    if not query:
        return []

    if column not in {"fbid", "component_symbol"}:
        raise ValueError(f"unsupported component search column: {column}")

    conn = _connect(state_dir)
    try:
        rows = conn.execute(
            f"""
            SELECT
              cc.stknum,
              cc.genotype,
              cc.component_symbol,
              cc.fbid,
              cc.mapstatement,
              {_component_metadata_subqueries(
                  "cc.stknum",
                  "cc.component_symbol",
                  "(SELECT MIN(sg.bdsc_symbol_id) FROM stockgenes sg WHERE sg.stknum = cc.stknum AND sg.component_symbol = cc.component_symbol)",
              )}
            FROM component_comments cc
            WHERE LOWER(cc.{column}) = LOWER(?)
               OR LOWER(cc.{column}) LIKE LOWER(?)
            ORDER BY
              CASE WHEN LOWER(cc.{column}) = LOWER(?) THEN 0 ELSE 1 END,
              cc.stknum,
              cc.component_symbol
            LIMIT ?
            """,
            (query, f"{query}%", query, limit),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def search_property(state_dir: Path, query: str, limit: int = 20) -> list[dict[str, Any]]:
    query = query.strip()
    if not query:
        return []

    conn = _connect(state_dir)
    try:
        rows = conn.execute(
            f"""
            WITH matching_props AS (
              SELECT DISTINCT bdsc_symbol_id
              FROM compprops
              WHERE LOWER(prop_syn) = LOWER(?)
                 OR LOWER(prop_syn) LIKE LOWER(?)
                 OR LOWER(property_descrip) LIKE LOWER(?)
            )
            SELECT
              cc.stknum,
              cc.genotype,
              cc.component_symbol,
              cc.fbid,
              cc.mapstatement,
              {_component_metadata_subqueries("cc.stknum", "cc.component_symbol", "sg0.bdsc_symbol_id")}
            FROM component_comments cc
            JOIN stockgenes sg0
              ON sg0.stknum = cc.stknum
             AND sg0.component_symbol = cc.component_symbol
            JOIN matching_props mp
              ON mp.bdsc_symbol_id = sg0.bdsc_symbol_id
            GROUP BY
              cc.stknum,
              cc.genotype,
              cc.component_symbol,
              cc.fbid,
              cc.mapstatement,
              sg0.bdsc_symbol_id
            ORDER BY cc.stknum, cc.component_symbol
            LIMIT ?
            """,
            (query, f"{query}%", f"%{query}%", limit),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def resolve_rrid_to_stknum(query: str) -> int | None:
    match = re.fullmatch(r"(?:RRID:)?BDSC_(\d+)", query.strip(), flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    if query.strip().isdigit():
        return int(query.strip())
    return None


def get_stock_by_rrid(state_dir: Path, query: str) -> dict[str, Any] | None:
    stknum = resolve_rrid_to_stknum(query)
    if stknum is None:
        return None
    return get_stock(state_dir, stknum)


def search_fbid(state_dir: Path, query: str, limit: int = 20) -> list[dict[str, Any]]:
    return _search_component_table(state_dir, column="fbid", query=query, limit=limit)


def search_component(state_dir: Path, query: str, limit: int = 20) -> list[dict[str, Any]]:
    return _search_component_table(
        state_dir,
        column="component_symbol",
        query=query,
        limit=limit,
    )


def detect_query_kind(query: str) -> str:
    value = query.strip()
    if not value:
        return "search"
    if value.isdigit():
        return "stock"
    if resolve_rrid_to_stknum(value) is not None and not value.isdigit():
        return "rrid"
    if re.fullmatch(r"FBgn\d+", value, flags=re.IGNORECASE):
        return "gene"
    if re.fullmatch(r"FB[a-z]{2}\d+", value, flags=re.IGNORECASE):
        return "fbid"
    if any(token in value for token in ("P{", "}", "[", "]", "attP", "CyO")):
        return "component"
    return "gene"


def lookup_query(
    state_dir: Path,
    query: str,
    *,
    kind: str = "auto",
    limit: int = 20,
) -> dict[str, Any]:
    requested_kind = kind
    resolved_kind = detect_query_kind(query) if kind == "auto" else kind

    if resolved_kind == "stock":
        result = get_stock(state_dir, int(query.strip()))
        results = [result] if result else []
    elif resolved_kind == "rrid":
        result = get_stock_by_rrid(state_dir, query)
        results = [result] if result else []
    elif resolved_kind == "gene":
        results = search_gene(state_dir, query, limit=limit)
        if kind == "auto" and not results:
            resolved_kind = "search"
            results = search_local(state_dir, query, limit=limit)
    elif resolved_kind == "fbid":
        results = search_fbid(state_dir, query, limit=limit)
    elif resolved_kind == "component":
        results = search_component(state_dir, query, limit=limit)
    elif resolved_kind == "property":
        results = search_property(state_dir, query, limit=limit)
    elif resolved_kind == "search":
        results = search_local(state_dir, query, limit=limit)
    else:
        raise ValueError(f"unsupported lookup kind: {kind}")

    return {
        "query": query,
        "requested_kind": requested_kind,
        "kind": resolved_kind,
        "result_count": len(results),
        "results": results,
    }


def get_stock(state_dir: Path, stknum: int) -> dict[str, Any] | None:
    conn = _connect(state_dir)
    try:
        stock_row = conn.execute(
            """
            SELECT
              s.stknum,
              s.genotype,
              s.chromosomes,
              s.aka,
              s.date_added,
              s.donor_info,
              s.stock_comments,
              sd.component_symbols,
              sd.gene_symbols,
              sd.fbgns
            FROM stocks s
            LEFT JOIN search_documents sd ON sd.stknum = s.stknum
            WHERE s.stknum = ?
            """,
            (stknum,),
        ).fetchone()
        if stock_row is None:
            return None

        component_rows = conn.execute(
            f"""
            SELECT
              component_symbol,
              fbid,
              mapstatement,
              comment1,
              comment2,
              comment3,
              {_component_metadata_subqueries(
                  "component_comments.stknum",
                  "component_comments.component_symbol",
                  "(SELECT MIN(sg.bdsc_symbol_id) FROM stockgenes sg WHERE sg.stknum = component_comments.stknum AND sg.component_symbol = component_comments.component_symbol)",
              )}
            FROM component_comments
            WHERE stknum = ?
            ORDER BY component_symbol
            """,
            (stknum,),
        ).fetchall()

        gene_rows = conn.execute(
            """
            SELECT DISTINCT
              component_symbol,
              gene_symbol,
              fbgn
            FROM stockgenes
            WHERE stknum = ?
            ORDER BY component_symbol, gene_symbol, fbgn
            """,
            (stknum,),
        ).fetchall()

        stock = dict(stock_row)
        stock["rrid"] = f"RRID:BDSC_{stknum}"
        stock["components"] = [dict(row) for row in component_rows]
        stock["genes"] = [dict(row) for row in gene_rows]
        return stock
    finally:
        conn.close()


def live_search(query: str, limit: int = 10) -> list[dict[str, Any]]:
    simple_payload = parse.urlencode({"presearch": query, "type": "contains"}).encode("utf-8")
    req = request.Request(
        "https://bdsc.indiana.edu/Home/GetSearchResults",
        data=simple_payload,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        },
        method="POST",
    )
    with request.urlopen(req) as response:
        data = json.loads(response.read().decode("utf-8"))
    rows = data.get("Data") or []
    if rows:
        return rows[:limit]

    advanced_payload = parse.urlencode(
        {
            "selectedGenotypeMatches": "any genotype",
            "selectedGenotypeContains1": "contains",
            "genotype1": query,
            "selectedGenotypeContains2": "contains",
            "genotype2": "",
            "selectedGenotypeContains3": "contains",
            "genotype3": "",
            "selectedCommentContains": "contains",
            "stockComment": "",
            "selectedDonorContains": "contains",
            "donor": "",
            "selectedAffectedChromosomes": "any",
        }
    ).encode("utf-8")
    advanced_req = request.Request(
        "https://bdsc.indiana.edu/Home/GetAdvancedSearchResults",
        data=advanced_payload,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        },
        method="POST",
    )
    with request.urlopen(advanced_req) as response:
        advanced_data = json.loads(response.read().decode("utf-8"))
    return (advanced_data.get("Data") or [])[:limit]


def get_status(state_dir: Path) -> dict[str, Any]:
    state_dir = resolve_state_dir(state_dir)
    manifest = load_manifest(state_dir)
    datasets = manifest.get("datasets", {})
    db_path = db_file(state_dir)
    index_info = manifest.get("index")
    if index_info is None and db_path.exists():
        conn = sqlite3.connect(db_path)
        try:
            has_fts = bool(
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_fts'"
                ).fetchone()
            )
            index_info = {
                "db_path": str(db_path),
                "built_at": None,
                "counts": {
                    "stocks": conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0],
                    "component_comments": conn.execute(
                        "SELECT COUNT(*) FROM component_comments"
                    ).fetchone()[0],
                    "stockgenes": conn.execute("SELECT COUNT(*) FROM stockgenes").fetchone()[0],
                    "compgenes": conn.execute("SELECT COUNT(*) FROM compgenes").fetchone()[0],
                    "compprops": conn.execute("SELECT COUNT(*) FROM compprops").fetchone()[0],
                    "fts_enabled": int(has_fts),
                },
            }
        finally:
            conn.close()
    return {
        "state_dir": str(state_dir),
        "db_path": str(db_path),
        "db_exists": db_path.exists(),
        "dataset_count": len(datasets),
        "datasets": datasets,
        "index": index_info,
        "updated_at": manifest.get("updated_at"),
    }


def iter_export_rows(
    state_dir: Path,
    dataset: str,
    *,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    if dataset not in EXPORT_DATASETS:
        raise ValueError(f"unsupported export dataset: {dataset}")

    conn = _connect(state_dir)
    try:
        if dataset == "stocks":
            sql = """
                SELECT
                  s.stknum,
                  'RRID:BDSC_' || s.stknum AS rrid,
                  s.genotype,
                  s.chromosomes,
                  s.aka,
                  s.date_added,
                  s.donor_info,
                  s.stock_comments,
                  COALESCE(sd.component_symbols, '') AS component_symbols,
                  COALESCE(sd.gene_symbols, '') AS gene_symbols,
                  COALESCE(sd.fbgns, '') AS fbgns
                FROM stocks s
                LEFT JOIN search_documents sd ON sd.stknum = s.stknum
                ORDER BY s.stknum
            """
        elif dataset == "components":
            sql = f"""
                SELECT
                  cc.stknum,
                  cc.genotype,
                  cc.component_symbol,
                  cc.fbid,
                  cc.mapstatement,
                  cc.comment1,
                  cc.comment2,
                  cc.comment3,
                  {_component_metadata_subqueries(
                      "cc.stknum",
                      "cc.component_symbol",
                      "(SELECT MIN(sg.bdsc_symbol_id) FROM stockgenes sg WHERE sg.stknum = cc.stknum AND sg.component_symbol = cc.component_symbol)",
                  )}
                FROM component_comments cc
                ORDER BY cc.stknum, cc.component_symbol
            """
        elif dataset == "genes":
            sql = """
                SELECT DISTINCT
                  sg.stknum,
                  sg.genotype,
                  sg.component_symbol,
                  cc.fbid,
                  sg.gene_symbol,
                  sg.fbgn,
                  sg.bdsc_symbol_id,
                  sg.bdsc_gene_id,
                  COALESCE((
                    SELECT group_concat(prop_syn, ' | ')
                    FROM (
                      SELECT DISTINCT cg.prop_syn AS prop_syn
                      FROM compgenes cg
                      WHERE cg.bdsc_symbol_id = sg.bdsc_symbol_id
                        AND cg.bdsc_gene_id = sg.bdsc_gene_id
                        AND cg.prop_syn != ''
                      ORDER BY cg.prop_syn
                    )
                  ), '') AS gene_relationships
                FROM stockgenes sg
                LEFT JOIN component_comments cc
                  ON cc.stknum = sg.stknum
                 AND cc.component_symbol = sg.component_symbol
                ORDER BY sg.stknum, sg.component_symbol, sg.gene_symbol, sg.fbgn
            """
        else:
            sql = """
                SELECT DISTINCT
                  cc.stknum,
                  cc.genotype,
                  cc.component_symbol,
                  cc.fbid,
                  cp.property_id,
                  cp.prop_syn,
                  cp.property_descrip
                FROM component_comments cc
                JOIN stockgenes sg
                  ON sg.stknum = cc.stknum
                 AND sg.component_symbol = cc.component_symbol
                JOIN compprops cp
                  ON cp.bdsc_symbol_id = sg.bdsc_symbol_id
                ORDER BY cc.stknum, cc.component_symbol, cp.prop_syn, cp.property_id
            """

        if limit is not None:
            sql += "\nLIMIT ?"
            cursor = conn.execute(sql, (limit,))
        else:
            cursor = conn.execute(sql)

        columns = [description[0] for description in cursor.description]
        try:
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(columns, row, strict=False))
        finally:
            cursor.close()
    finally:
        conn.close()


def format_sync_results(results: list[SyncResult]) -> str:
    lines = []
    for result in results:
        lines.append(
            f"{result.name}: {result.status} {result.bytes_downloaded}B {result.path}"
        )
    return "\n".join(lines)


def format_search_results(results: list[dict[str, Any]]) -> str:
    if not results:
        return "no results"
    lines = []
    for row in results:
        stknum = row.get("stknum", row.get("Stknum"))
        genotype = row.get("genotype", row.get("Genotype"))
        bits = [str(stknum), genotype]
        genes = row.get("gene_symbols") or row.get("fbgns") or row.get("SearchText") or ""
        if genes:
            bits.append(f"genes={genes}")
        lines.append(" | ".join(bits))
    return "\n".join(lines)


def format_gene_results(results: list[dict[str, Any]]) -> str:
    if not results:
        return "no results"
    lines = []
    for row in results:
        lines.append(
            " | ".join(
                [
                    str(row["stknum"]),
                    row["gene_symbol"],
                    row["fbgn"],
                    row["component_symbol"],
                    row["genotype"],
                ]
            )
        )
    return "\n".join(lines)


def format_component_results(results: list[dict[str, Any]]) -> str:
    if not results:
        return "no results"
    lines = []
    for row in results:
        bits = [
            str(row["stknum"]),
            row["component_symbol"],
            row["fbid"],
        ]
        genes = row.get("gene_symbols") or row.get("fbgns") or ""
        if genes:
            bits.append(f"genes={genes}")
        properties = row.get("property_syns") or ""
        if properties:
            bits.append(f"props={properties}")
        relationships = row.get("gene_relationships") or ""
        if relationships:
            bits.append(f"rels={relationships}")
        lines.append(" | ".join(bits + [row["genotype"]]))
    return "\n".join(lines)


def format_lookup_result(result: dict[str, Any]) -> str:
    lines = [f"query: {result['query']}", f"kind: {result['kind']}"]
    rows = result["results"]
    kind = result["kind"]
    if kind in {"stock", "rrid"}:
        body = format_stock(rows[0] if rows else None)
    elif kind == "gene":
        body = format_gene_results(rows)
    elif kind in {"component", "fbid", "property"}:
        body = format_component_results(rows)
    else:
        body = format_search_results(rows)
    lines.append(body)
    return "\n".join(lines)


def format_stock(stock: dict[str, Any] | None) -> str:
    if stock is None:
        return "not found"

    lines = [
        f"stknum: {stock['stknum']}",
        f"rrid: {stock['rrid']}",
        f"genotype: {stock['genotype']}",
        f"chromosomes: {stock['chromosomes'] or '-'}",
        f"aka: {stock['aka'] or '-'}",
        f"date_added: {stock['date_added'] or '-'}",
        f"donor_info: {stock['donor_info'] or '-'}",
        f"stock_comments: {stock['stock_comments'] or '-'}",
        f"component_symbols: {stock['component_symbols'] or '-'}",
        f"gene_symbols: {stock['gene_symbols'] or '-'}",
        f"fbgns: {stock['fbgns'] or '-'}",
    ]

    if stock["components"]:
        lines.append("components:")
        for row in stock["components"][:20]:
            detail = "; ".join(
                part
                for part in [
                    row["fbid"],
                    row["mapstatement"],
                    row["comment1"],
                    row["comment2"],
                    row["comment3"],
                ]
                if part
            )
            if detail:
                lines.append(f"  - {row['component_symbol']}: {detail}")
            else:
                lines.append(f"  - {row['component_symbol']}")
            if row.get("property_syns"):
                lines.append(f"    properties: {row['property_syns']}")
            if row.get("gene_relationships"):
                lines.append(f"    gene_relationships: {row['gene_relationships']}")

    if stock["genes"]:
        lines.append("genes:")
        for row in stock["genes"][:40]:
            bits = [row["component_symbol"], row["gene_symbol"], row["fbgn"]]
            lines.append(f"  - {' | '.join(bit for bit in bits if bit)}")

    return "\n".join(lines)
