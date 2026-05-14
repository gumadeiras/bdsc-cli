from __future__ import annotations

import csv
import difflib
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

USER_AGENT = "bdsc/0.1 (+https://bdsc.indiana.edu/)"
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
class QueryCriterion:
    kind: str
    query: str


@dataclass(frozen=True)
class ReportSpec:
    name: str
    description: str
    default_dataset: str
    groups: tuple[tuple[QueryCriterion, ...], ...] = ()


LOOKUP_KINDS = (
    "auto",
    "stock",
    "rrid",
    "gene",
    "fbid",
    "component",
    "property",
    "property-exact",
    "driver-family",
    "relationship",
    "search",
)
EXPORT_DATASETS = ("stocks", "components", "genes", "properties")
TERM_SCOPES = ("properties", "property-descriptions", "relationships")
REPORT_NAMES = ("olfactory", "drivers", "optogenetics")

REPORT_SPECS = {
    "olfactory": ReportSpec(
        name="olfactory",
        description="olfactory receptor and odorant-binding gene families",
        default_dataset="components",
    ),
    "drivers": ReportSpec(
        name="drivers",
        description="expression-driver and recombinase components",
        default_dataset="components",
        groups=(
            (QueryCriterion(kind="driver-family", query="GAL4"),),
            (QueryCriterion(kind="driver-family", query="lexA"),),
            (QueryCriterion(kind="driver-family", query="QF"),),
            (QueryCriterion(kind="driver-family", query="split"),),
            (QueryCriterion(kind="driver-family", query="FLP"),),
        ),
    ),
    "optogenetics": ReportSpec(
        name="optogenetics",
        description="common optogenetic effectors and optogenetic-tagged components",
        default_dataset="components",
        groups=(
            (QueryCriterion(kind="gene", query="Chronos"),),
            (QueryCriterion(kind="gene", query="CsChrimson"),),
            (QueryCriterion(kind="gene", query="Chrimson"),),
            (QueryCriterion(kind="gene", query="GtACR"),),
            (QueryCriterion(kind="gene", query="ReaChR"),),
            (QueryCriterion(kind="gene", query="ChR2"),),
            (QueryCriterion(kind="gene", query="eNpHR"),),
            (QueryCriterion(kind="property", query="optogen"),),
        ),
    ),
}

REPORT_DATASET_SYMBOLS = {
    "stocks": "s",
    "components": "cc",
    "genes": "sg",
    "properties": "cc",
}

DRIVER_FAMILY_ALIASES = {
    "gal4": ("gal4", "gawb"),
    "lexa": ("lexa",),
    "qf": ("qf",),
    "flp": ("flp", "flpo", "flp recombinase"),
    "split": ("split zip hemi driver", "split intein hemi driver"),
}


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
              fbids TEXT,
              gene_symbols TEXT,
              fbgns TEXT,
              property_terms TEXT,
              relationship_terms TEXT,
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
              component_symbols, fbids, gene_symbols, fbgns,
              property_terms, relationship_terms, search_text
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
                SELECT group_concat(fbid, ' ')
                FROM (
                  SELECT DISTINCT cc.fbid AS fbid
                  FROM component_comments cc
                  WHERE cc.stknum = s.stknum AND cc.fbid != ''
                  ORDER BY cc.fbid
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
              COALESCE((
                SELECT group_concat(prop_syn, ' ')
                FROM (
                  SELECT DISTINCT cp.prop_syn AS prop_syn
                  FROM stockgenes sg
                  JOIN compprops cp ON cp.bdsc_symbol_id = sg.bdsc_symbol_id
                  WHERE sg.stknum = s.stknum AND cp.prop_syn != ''
                  ORDER BY cp.prop_syn
                )
              ), ''),
              COALESCE((
                SELECT group_concat(prop_syn, ' ')
                FROM (
                  SELECT DISTINCT cg.prop_syn AS prop_syn
                  FROM stockgenes sg
                  JOIN compgenes cg
                    ON cg.bdsc_symbol_id = sg.bdsc_symbol_id
                   AND cg.bdsc_gene_id = sg.bdsc_gene_id
                  WHERE sg.stknum = s.stknum AND cg.prop_syn != ''
                  ORDER BY cg.prop_syn
                )
              ), ''),
              trim(
                s.stknum || ' ' ||
                COALESCE(s.genotype, '') || ' ' ||
                COALESCE(s.aka, '') || ' ' ||
                COALESCE(s.donor_info, '') || ' ' ||
                COALESCE(s.stock_comments, '') || ' ' ||
                COALESCE((
                  SELECT group_concat(fbid, ' ')
                  FROM (
                    SELECT DISTINCT cc.fbid AS fbid
                    FROM component_comments cc
                    WHERE cc.stknum = s.stknum AND cc.fbid != ''
                  )
                ), '') || ' ' ||
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
                ), '') || ' ' ||
                COALESCE((
                  SELECT group_concat(prop_syn, ' ')
                  FROM (
                    SELECT DISTINCT cp.prop_syn AS prop_syn
                    FROM stockgenes sg
                    JOIN compprops cp ON cp.bdsc_symbol_id = sg.bdsc_symbol_id
                    WHERE sg.stknum = s.stknum AND cp.prop_syn != ''
                  )
                ), '') || ' ' ||
                COALESCE((
                  SELECT group_concat(property_descrip, ' ')
                  FROM (
                    SELECT DISTINCT cp.property_descrip AS property_descrip
                    FROM stockgenes sg
                    JOIN compprops cp ON cp.bdsc_symbol_id = sg.bdsc_symbol_id
                    WHERE sg.stknum = s.stknum AND cp.property_descrip != ''
                  )
                ), '') || ' ' ||
                COALESCE((
                  SELECT group_concat(prop_syn, ' ')
                  FROM (
                    SELECT DISTINCT cg.prop_syn AS prop_syn
                    FROM stockgenes sg
                    JOIN compgenes cg
                      ON cg.bdsc_symbol_id = sg.bdsc_symbol_id
                     AND cg.bdsc_gene_id = sg.bdsc_gene_id
                    WHERE sg.stknum = s.stknum AND cg.prop_syn != ''
                  )
                ), '') || ' ' ||
                COALESCE((
                  SELECT group_concat(comment_text, ' ')
                  FROM (
                    SELECT DISTINCT cc.comment1 AS comment_text
                    FROM component_comments cc
                    WHERE cc.stknum = s.stknum AND cc.comment1 != ''
                    UNION
                    SELECT DISTINCT cc.comment2 AS comment_text
                    FROM component_comments cc
                    WHERE cc.stknum = s.stknum AND cc.comment2 != ''
                    UNION
                    SELECT DISTINCT cc.comment3 AS comment_text
                    FROM component_comments cc
                    WHERE cc.stknum = s.stknum AND cc.comment3 != ''
                    UNION
                    SELECT DISTINCT cc.mapstatement AS comment_text
                    FROM component_comments cc
                    WHERE cc.stknum = s.stknum AND cc.mapstatement != ''
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
                  fbids,
                  gene_symbols,
                  fbgns,
                  property_terms,
                  relationship_terms,
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
                  component_symbols, fbids, gene_symbols, fbgns,
                  property_terms, relationship_terms
                )
                SELECT
                  stknum, genotype, aka, donor_info, stock_comments,
                  component_symbols, fbids, gene_symbols, fbgns,
                  property_terms, relationship_terms
                FROM search_documents
                """
            )

        trigram_enabled = True
        try:
            conn.execute(
                """
                CREATE VIRTUAL TABLE stock_trigram USING fts5(
                  stknum UNINDEXED,
                  search_text,
                  tokenize='trigram'
                )
                """
            )
        except sqlite3.OperationalError:
            trigram_enabled = False

        if trigram_enabled:
            conn.execute(
                """
                INSERT INTO stock_trigram (stknum, search_text)
                SELECT stknum, search_text
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
            "trigram_enabled": int(trigram_enabled),
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


def _query_tokens(text: str) -> list[str]:
    return re.findall(r"[A-Za-z0-9]+", text.lower())


def _is_free_text_query(text: str) -> bool:
    return len(_query_tokens(text)) > 1


def _compact_text(text: str) -> str:
    return "".join(_query_tokens(text))


def _trigrams(text: str) -> list[str]:
    if len(text) < 3:
        return []
    return [text[index : index + 3] for index in range(len(text) - 2)]


def build_trigram_query(text: str) -> str | None:
    tokens = _query_tokens(text)
    grams: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        for gram in _trigrams(token):
            if gram not in seen:
                seen.add(gram)
                grams.append(gram)
    compact = _compact_text(text)
    for gram in _trigrams(compact):
        if gram not in seen:
            seen.add(gram)
            grams.append(gram)
    if not grams:
        return None
    return " OR ".join(f'"{gram}"' for gram in grams)


def _trigram_overlap_ratio(query: str, text: str) -> float:
    query_grams = set(_trigrams(_compact_text(query)))
    text_grams = set(_trigrams(_compact_text(text)))
    if not query_grams or not text_grams:
        return 0.0
    return len(query_grams & text_grams) / len(query_grams)


def _best_term_similarity(query: str, text: str) -> float:
    query_compact = _compact_text(query)
    if not query_compact:
        return 0.0

    best = 0.0
    for term in _query_tokens(text):
        if len(term) < 3:
            continue
        similarity = difflib.SequenceMatcher(None, query_compact, term).ratio()
        similarity += _trigram_overlap_ratio(query, term)
        if similarity > best:
            best = similarity
    return best


def _score_search_document(query: str, row: sqlite3.Row | dict[str, Any]) -> float:
    query_value = query.strip().lower()
    query_tokens = _query_tokens(query)
    query_compact = _compact_text(query)
    search_text = row["search_text"]
    haystack = search_text.lower()
    compact_haystack = _compact_text(search_text)
    document_tokens = set(_query_tokens(search_text))

    score = 0.0
    if query_value and query_value in haystack:
        score += 8.0
    if query_compact and query_compact in compact_haystack:
        score += 10.0

    exact_matches = sum(1 for token in query_tokens if token in document_tokens)
    prefix_matches = sum(
        1
        for token in query_tokens
        if token not in document_tokens and any(doc.startswith(token) for doc in document_tokens)
    )
    score += exact_matches * 3.0
    score += prefix_matches * 1.5

    overlap = _trigram_overlap_ratio(query, search_text)
    score += overlap * 4.0

    gene_symbols = row["gene_symbols"] or ""
    component_symbols = row["component_symbols"] or ""
    primary_fields = f"{gene_symbols} {component_symbols}".strip()
    if primary_fields:
        score += _trigram_overlap_ratio(query, primary_fields) * 8.0
        score += _best_term_similarity(query, primary_fields) * 12.0

    return score


def _search_result_payload(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    return {
        "stknum": row["stknum"],
        "genotype": row["genotype"],
        "gene_symbols": row["gene_symbols"],
        "fbgns": row["fbgns"],
        "component_symbols": row["component_symbols"],
    }


def _merge_ranked_matches(
    matches: list[dict[str, Any]],
    key_fn,
) -> list[dict[str, Any]]:
    merged: dict[Any, dict[str, Any]] = {}
    for match in matches:
        key = key_fn(match["row"])
        existing = merged.get(key)
        if existing is None or match["score"] > existing["score"]:
            merged[key] = match
    return sorted(
        merged.values(),
        key=lambda item: (-item["score"], item["row"]["stknum"]),
    )


def _limit_sql(limit: int | None) -> tuple[str, list[int]]:
    if limit is None:
        return "", []
    return "LIMIT ?", [limit]


def _scaled_limit(limit: int | None, multiplier: int, floor: int) -> int | None:
    if limit is None:
        return None
    return max(limit * multiplier, floor)


def _limit_rows(rows: list[Any], limit: int | None) -> list[Any]:
    if limit is None:
        return rows
    return rows[:limit]


def _search_candidates_from_prefix_fts(
    conn: sqlite3.Connection,
    query: str,
    limit: int | None,
) -> list[dict[str, Any]]:
    limit_clause, limit_params = _limit_sql(limit)
    has_fts = bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_fts'"
        ).fetchone()
    )
    if not has_fts:
        rows = conn.execute(
            f"""
            SELECT
              s.stknum,
              s.genotype,
              sd.gene_symbols,
              sd.fbgns,
              sd.component_symbols,
              sd.search_text
            FROM search_documents sd
            JOIN stocks s ON s.stknum = sd.stknum
            WHERE sd.search_text LIKE ?
            ORDER BY s.stknum
            {limit_clause}
            """,
            (f"%{query}%", *limit_params),
        ).fetchall()
        return [{"row": row, "score": _score_search_document(query, row) + 20.0} for row in rows]

    rows = conn.execute(
        f"""
        SELECT
          s.stknum,
          s.genotype,
          sd.gene_symbols,
          sd.fbgns,
          sd.component_symbols,
          sd.search_text,
          bm25(stock_fts) AS rank
        FROM stock_fts f
        JOIN stocks s ON s.stknum = f.stknum
        JOIN search_documents sd ON sd.stknum = s.stknum
        WHERE stock_fts MATCH ?
        ORDER BY bm25(stock_fts), s.stknum
        {limit_clause}
        """,
        (build_fts_query(query), *limit_params),
    ).fetchall()
    return [
        {
            "row": row,
            "score": _score_search_document(query, row) + 40.0 + min(10.0, abs(row["rank"]) * 1000000.0),
        }
        for row in rows
    ]


def _search_candidates_from_trigram_fts(
    conn: sqlite3.Connection,
    query: str,
    limit: int | None,
) -> list[dict[str, Any]]:
    trigram_query = build_trigram_query(query)
    if not trigram_query:
        return []
    limit_clause, limit_params = _limit_sql(limit)

    has_trigram = bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_trigram'"
        ).fetchone()
    )
    if not has_trigram:
        return []

    rows = conn.execute(
        f"""
        SELECT
          s.stknum,
          s.genotype,
          sd.gene_symbols,
          sd.fbgns,
          sd.component_symbols,
          sd.search_text,
          bm25(stock_trigram) AS rank
        FROM stock_trigram t
        JOIN stocks s ON s.stknum = t.stknum
        JOIN search_documents sd ON sd.stknum = s.stknum
        WHERE stock_trigram MATCH ?
        ORDER BY bm25(stock_trigram), s.stknum
        {limit_clause}
        """,
        (trigram_query, *limit_params),
    ).fetchall()

    matches: list[dict[str, Any]] = []
    for row in rows:
        score = _score_search_document(query, row) + min(6.0, abs(row["rank"]) * 1000000.0)
        if score >= 4.5:
            matches.append({"row": row, "score": score})
    return matches


def _candidate_stock_ids_for_query(
    conn: sqlite3.Connection,
    query: str,
    limit: int | None,
) -> list[int]:
    candidates: dict[int, float] = {}
    for match in _search_candidates_from_prefix_fts(conn, query, _scaled_limit(limit, 2, 20)):
        candidates[match["row"]["stknum"]] = max(
            match["score"],
            candidates.get(match["row"]["stknum"], float("-inf")),
        )
    for match in _search_candidates_from_trigram_fts(conn, query, _scaled_limit(limit, 6, 60)):
        candidates[match["row"]["stknum"]] = max(
            match["score"],
            candidates.get(match["row"]["stknum"], float("-inf")),
        )
    ranked = sorted(candidates.items(), key=lambda item: (-item[1], item[0]))
    return _limit_rows([stknum for stknum, _ in ranked], limit)


def _score_field_match(query: str, text: str) -> float:
    if not text:
        return 0.0
    lowered_query = query.strip().lower()
    compact_query = _compact_text(query)
    lowered_text = text.lower()
    compact_text = _compact_text(text)
    text_tokens = set(_query_tokens(text))
    query_tokens = _query_tokens(query)

    score = 0.0
    if lowered_query and lowered_query == lowered_text:
        score += 12.0
    elif lowered_query and lowered_query in lowered_text:
        score += 8.0
    if compact_query and compact_query == compact_text:
        score += 14.0
    elif compact_query and compact_query in compact_text:
        score += 10.0
    score += _trigram_overlap_ratio(query, text) * 6.0
    score += _best_term_similarity(query, text) * 8.0
    score += sum(1 for token in query_tokens if token in text_tokens) * 1.5
    score += sum(
        1
        for token in query_tokens
        if token not in text_tokens and any(text_token.startswith(token) for text_token in text_tokens)
    )
    return score


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _default_row_key(row: sqlite3.Row) -> tuple[Any, ...]:
    return tuple(row[key] for key in row.keys())


def _component_result_key(row: sqlite3.Row | dict[str, Any]) -> tuple[Any, ...]:
    return (row["stknum"], row["component_symbol"], row["fbid"])


def _gene_result_key(row: sqlite3.Row | dict[str, Any]) -> tuple[Any, ...]:
    return (row["stknum"], row["component_symbol"], row["gene_symbol"], row["fbgn"])


def _rank_direct_rows(
    query: str,
    rows: list[sqlite3.Row],
    *,
    field_names: list[str],
    limit: int | None,
    min_score: float = 5.0,
    key_fn=None,
) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for row in rows:
        row_score = max(_score_field_match(query, row[field_name] or "") for field_name in field_names)
        if row_score >= min_score:
            scored.append({"row": row, "score": row_score})

    if key_fn is None:
        key_fn = _default_row_key
    ranked = _merge_ranked_matches(scored, key_fn)
    return [dict(item["row"]) for item in _limit_rows(ranked, limit)]


def search_local(state_dir: Path, query: str, limit: int | None = None) -> list[dict[str, Any]]:
    query = query.strip()
    if not query:
        return []

    conn = _connect(state_dir)
    try:
        if query.isdigit():
            stock = get_stock(state_dir, int(query))
            return [stock] if stock else []

        candidates: dict[int, dict[str, Any]] = {}
        for match in _search_candidates_from_prefix_fts(conn, query, _scaled_limit(limit, 3, 20)):
            stknum = match["row"]["stknum"]
            existing = candidates.get(stknum)
            if existing is None or match["score"] > existing["score"]:
                candidates[stknum] = match

        if not candidates:
            for match in _search_candidates_from_trigram_fts(conn, query, _scaled_limit(limit, 12, 60)):
                stknum = match["row"]["stknum"]
                existing = candidates.get(stknum)
                if existing is None or match["score"] > existing["score"]:
                    candidates[stknum] = match

        ranked = sorted(
            candidates.values(),
            key=lambda item: (-item["score"], item["row"]["stknum"]),
        )
        return [_search_result_payload(item["row"]) for item in _limit_rows(ranked, limit)]
    finally:
        conn.close()


def search_gene(state_dir: Path, query: str, limit: int | None = None) -> list[dict[str, Any]]:
    query = query.strip()
    if not query:
        return []

    conn = _connect(state_dir)
    try:
        limit_clause, limit_params = _limit_sql(limit)
        if query.upper().startswith("FBGN"):
            rows = conn.execute(
                f"""
                SELECT DISTINCT
                  sg.stknum,
                  sg.genotype,
                  sg.component_symbol,
                  sg.gene_symbol,
                  sg.fbgn
                FROM stockgenes sg
                WHERE UPPER(sg.fbgn) = UPPER(?)
                ORDER BY sg.stknum, sg.component_symbol, sg.gene_symbol
                {limit_clause}
                """,
                (query, *limit_params),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
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
                {limit_clause}
                """,
                (query, f"{query}%", query, *limit_params),
            ).fetchall()
        if rows:
            return _rows_to_dicts(rows)

        stock_ids = _candidate_stock_ids_for_query(conn, query, _scaled_limit(limit, 4, 40))
        if not stock_ids:
            return []
        placeholders = ", ".join("?" for _ in stock_ids)
        fuzzy_rows = conn.execute(
            f"""
            SELECT DISTINCT
              sg.stknum,
              sg.genotype,
              sg.component_symbol,
              sg.gene_symbol,
              sg.fbgn
            FROM stockgenes sg
            WHERE sg.stknum IN ({placeholders})
            """,
            stock_ids,
        ).fetchall()
        return _rank_direct_rows(
            query,
            fuzzy_rows,
            field_names=["gene_symbol", "fbgn"],
            limit=limit,
            key_fn=_gene_result_key,
        )
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
    conn: sqlite3.Connection | None = None,
    column: str,
    query: str,
    limit: int | None,
) -> list[dict[str, Any]]:
    query = query.strip()
    if not query:
        return []

    if column not in {"fbid", "component_symbol"}:
        raise ValueError(f"unsupported component search column: {column}")

    close_conn = conn is None
    conn = conn or _connect(state_dir)
    try:
        limit_clause, limit_params = _limit_sql(limit)
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
            {limit_clause}
            """,
            (query, f"{query}%", query, *limit_params),
        ).fetchall()
        if rows:
            return _rows_to_dicts(rows)

        stock_ids = _candidate_stock_ids_for_query(conn, query, _scaled_limit(limit, 4, 40))
        if not stock_ids:
            return []
        placeholders = ", ".join("?" for _ in stock_ids)
        fuzzy_rows = conn.execute(
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
            WHERE cc.stknum IN ({placeholders})
            """,
            stock_ids,
        ).fetchall()
        field_names = ["fbid", "component_symbol", "gene_symbols", "genotype", "property_syns"]
        if column == "component_symbol":
            field_names = ["component_symbol", "gene_symbols", "fbid", "property_syns", "genotype"]
        return _rank_direct_rows(
            query,
            fuzzy_rows,
            field_names=field_names,
            limit=limit,
            key_fn=_component_result_key,
        )
    finally:
        if close_conn:
            conn.close()


def _fetch_component_domain_rows(
    conn: sqlite3.Connection,
    query: str,
    limit: int | None,
    *,
    cte_sql: str,
    cte_params: list[Any],
) -> list[sqlite3.Row]:
    limit_clause, limit_params = _limit_sql(limit)
    rows = conn.execute(
        f"""
        {cte_sql}
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
        JOIN matching_rows mr
          ON mr.bdsc_symbol_id = sg0.bdsc_symbol_id
        GROUP BY
          cc.stknum,
          cc.genotype,
          cc.component_symbol,
          cc.fbid,
          cc.mapstatement,
          sg0.bdsc_symbol_id
        ORDER BY cc.stknum, cc.component_symbol
        {limit_clause}
        """,
        (*cte_params, *limit_params),
    ).fetchall()
    if rows:
        return rows

    stock_ids = _candidate_stock_ids_for_query(conn, query, _scaled_limit(limit, 4, 40))
    if not stock_ids:
        return []
    placeholders = ", ".join("?" for _ in stock_ids)
    return conn.execute(
        f"""
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
        WHERE cc.stknum IN ({placeholders})
        GROUP BY
          cc.stknum,
          cc.genotype,
          cc.component_symbol,
          cc.fbid,
          cc.mapstatement,
          sg0.bdsc_symbol_id
        """,
        stock_ids,
    ).fetchall()

def _search_component_domain(
    state_dir: Path,
    query: str,
    limit: int | None,
    *,
    cte_sql: str,
    cte_params: list[Any],
    field_names: list[str],
) -> list[dict[str, Any]]:
    query = query.strip()
    if not query:
        return []

    conn = _connect(state_dir)
    try:
        rows = _fetch_component_domain_rows(
            conn,
            query,
            limit,
            cte_sql=cte_sql,
            cte_params=cte_params,
        )
        return _rank_direct_rows(
            query,
            rows,
            field_names=field_names,
            limit=limit,
            key_fn=_component_result_key,
        )
    finally:
        conn.close()


def search_property(state_dir: Path, query: str, limit: int | None = None) -> list[dict[str, Any]]:
    query = query.strip()
    return _search_component_domain(
        state_dir,
        query,
        limit,
        cte_sql="""
        WITH matching_rows AS (
          SELECT DISTINCT bdsc_symbol_id
          FROM compprops
          WHERE LOWER(prop_syn) = LOWER(?)
             OR LOWER(prop_syn) LIKE LOWER(?)
             OR LOWER(property_descrip) LIKE LOWER(?)
        )
        """,
        cte_params=[query, f"{query}%", f"%{query}%"],
        field_names=["property_syns", "property_descriptions", "component_symbol", "gene_symbols"],
    )


def search_property_exact(state_dir: Path, query: str, limit: int | None = None) -> list[dict[str, Any]]:
    query = query.strip()
    return _search_component_domain(
        state_dir,
        query,
        limit,
        cte_sql="""
        WITH matching_rows AS (
          SELECT DISTINCT bdsc_symbol_id
          FROM compprops
          WHERE LOWER(prop_syn) = LOWER(?)
             OR LOWER(property_descrip) = LOWER(?)
        )
        """,
        cte_params=[query, query],
        field_names=["property_syns", "property_descriptions", "component_symbol", "gene_symbols"],
    )


def search_driver_family(state_dir: Path, query: str, limit: int | None = None) -> list[dict[str, Any]]:
    query = query.strip()
    _, tokens = normalize_driver_family(query)
    clause, params = _driver_family_clause(
        tokens,
        "cc.component_symbol",
        "sg.gene_symbol",
        "cp.prop_syn",
    )
    return _search_component_domain(
        state_dir,
        query,
        limit,
        cte_sql=f"""
        WITH matching_rows AS (
          SELECT DISTINCT sg.bdsc_symbol_id
          FROM stockgenes sg
          JOIN component_comments cc
            ON cc.stknum = sg.stknum
           AND cc.component_symbol = sg.component_symbol
          LEFT JOIN compprops cp
            ON cp.bdsc_symbol_id = sg.bdsc_symbol_id
          WHERE {clause}
        )
        """,
        cte_params=params,
        field_names=["component_symbol", "property_syns", "gene_symbols"],
    )


def search_relationship(state_dir: Path, query: str, limit: int | None = None) -> list[dict[str, Any]]:
    query = query.strip()
    return _search_component_domain(
        state_dir,
        query,
        limit,
        cte_sql="""
        WITH matching_rows AS (
          SELECT DISTINCT bdsc_symbol_id
          FROM compgenes
          WHERE LOWER(prop_syn) = LOWER(?)
             OR LOWER(prop_syn) LIKE LOWER(?)
        )
        """,
        cte_params=[query, f"{query}%"],
        field_names=["gene_relationships", "gene_symbols", "component_symbol", "property_syns"],
    )


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


def search_fbid(state_dir: Path, query: str, limit: int | None = None) -> list[dict[str, Any]]:
    return _search_component_table(state_dir, column="fbid", query=query, limit=limit)


def search_component(state_dir: Path, query: str, limit: int | None = None) -> list[dict[str, Any]]:
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
    if _is_free_text_query(value):
        return "search"
    return "gene"


def _prefix_match_clause(expr: str, query: str) -> tuple[str, list[Any]]:
    return f"(LOWER({expr}) = LOWER(?) OR LOWER({expr}) LIKE LOWER(?))", [query, f"{query}%"]


def _contains_match_clause(expr: str, query: str) -> tuple[str, list[Any]]:
    return f"LOWER({expr}) LIKE LOWER(?)", [f"%{query}%"]


def _search_text_match_clause(expr: str, query: str) -> tuple[str, list[Any]]:
    tokens = _query_tokens(query)
    if len(tokens) <= 1:
        return _contains_match_clause(expr, query)
    return (
        " AND ".join(f"LOWER({expr}) LIKE LOWER(?)" for _ in tokens),
        [f"%{token}%" for token in tokens],
    )


def _exact_match_clause(expr: str, query: str) -> tuple[str, list[Any]]:
    return f"LOWER({expr}) = LOWER(?)", [query]


def _property_match_clause(query: str, *, exact: bool) -> tuple[str, list[Any]]:
    synonym_clause, synonym_params = (
        _exact_match_clause("cp.prop_syn", query)
        if exact
        else _prefix_match_clause("cp.prop_syn", query)
    )
    description_clause, description_params = (
        _exact_match_clause("cp.property_descrip", query)
        if exact
        else _contains_match_clause("cp.property_descrip", query)
    )
    return (
        f"({synonym_clause} OR {description_clause})",
        synonym_params + description_params,
    )


def _gene_match_clause(fbgn_expr: str, gene_expr: str, query: str) -> tuple[str, list[Any]]:
    if query.upper().startswith("FBGN"):
        return f"UPPER({fbgn_expr}) = UPPER(?)", [query]
    clause, params = _prefix_match_clause(gene_expr, query)
    return clause, params


def normalize_driver_family(query: str) -> tuple[str, tuple[str, ...]]:
    normalized = query.strip().lower()
    for family, aliases in DRIVER_FAMILY_ALIASES.items():
        if normalized == family or normalized in aliases:
            return family, aliases
    return normalized, (normalized,)


def _driver_family_clause(tokens: tuple[str, ...], *exprs: str) -> tuple[str, list[Any]]:
    predicates: list[str] = []
    params: list[Any] = []
    for expr in exprs:
        for token in tokens:
            if token == "lexa":
                lowered_expr = f"LOWER({expr})"
                predicates.append(
                    f"(({lowered_expr} GLOB ? OR {lowered_expr} GLOB ?) "
                    f"AND NOT ({lowered_expr} GLOB ? OR {lowered_expr} GLOB ?))"
                )
                params.extend(
                    ("*[^a-z0-9]lexa*", "lexa*", "*[^a-z0-9]lexaop*", "lexaop*")
                )
                continue
            predicates.append(f"LOWER({expr}) LIKE LOWER(?)")
            params.append(f"%{token}%")
    return "(" + " OR ".join(predicates) + ")", params


def _driver_family_criterion(dataset: str, query: str) -> tuple[str, list[Any], str]:
    _, tokens = normalize_driver_family(query)

    if dataset == "stocks":
        clause, params = _driver_family_clause(
            tokens,
            "cc.component_symbol",
            "cc.genotype",
            "sg.gene_symbol",
            "cp.prop_syn",
        )
        return (
            "EXISTS ("
            "SELECT 1 FROM component_comments cc "
            "LEFT JOIN stockgenes sg ON sg.stknum = cc.stknum AND sg.component_symbol = cc.component_symbol "
            "LEFT JOIN compprops cp ON cp.bdsc_symbol_id = sg.bdsc_symbol_id "
            f"WHERE cc.stknum = s.stknum AND {clause}"
            ")",
            params,
            "driver-family",
        )

    if dataset == "components":
        component_clause, component_params = _driver_family_clause(
            tokens,
            "cc.component_symbol",
        )
        gene_clause, gene_params = _driver_family_clause(tokens, "sg.gene_symbol")
        property_clause, property_params = _driver_family_clause(tokens, "cp.prop_syn")
        return (
            "("
            f"{component_clause} OR EXISTS ("
            "SELECT 1 FROM stockgenes sg "
            "LEFT JOIN compprops cp ON cp.bdsc_symbol_id = sg.bdsc_symbol_id "
            "WHERE sg.stknum = cc.stknum AND sg.component_symbol = cc.component_symbol "
            f"AND ({gene_clause} OR {property_clause})"
            ")"
            ")",
            component_params + gene_params + property_params,
            "driver-family",
        )

    if dataset == "genes":
        component_clause, component_params = _driver_family_clause(
            tokens,
            "sg.component_symbol",
            "sg.gene_symbol",
        )
        property_clause, property_params = _driver_family_clause(tokens, "cp.prop_syn")
        return (
            "("
            f"{component_clause} OR EXISTS ("
            "SELECT 1 FROM compprops cp "
            "WHERE cp.bdsc_symbol_id = sg.bdsc_symbol_id "
            f"AND {property_clause}"
            ")"
            ")",
            component_params + property_params,
            "driver-family",
        )

    component_clause, component_params = _driver_family_clause(
        tokens,
        "cc.component_symbol",
        "cp.prop_syn",
    )
    gene_clause, gene_params = _driver_family_clause(tokens, "sg2.gene_symbol")
    return (
        "("
        f"{component_clause} OR EXISTS ("
        "SELECT 1 FROM stockgenes sg2 "
        "WHERE sg2.stknum = cc.stknum AND sg2.component_symbol = cc.component_symbol "
        f"AND {gene_clause}"
        ")"
        ")",
        component_params + gene_params,
        "driver-family",
    )


def _single_criterion(
    dataset: str,
    query: str,
    kind: str,
) -> tuple[str, list[Any], str | None]:
    resolved_kind = detect_query_kind(query) if kind == "auto" else kind
    params: list[Any] = []

    if resolved_kind == "stock":
        clause = {
            "stocks": "s.stknum = ?",
            "components": "cc.stknum = ?",
            "genes": "sg.stknum = ?",
            "properties": "cc.stknum = ?",
        }[dataset]
        params.append(int(query.strip()))
        return clause, params, resolved_kind

    if resolved_kind == "rrid":
        stknum = resolve_rrid_to_stknum(query)
        if stknum is None:
            return "0", [], resolved_kind
        clause = {
            "stocks": "s.stknum = ?",
            "components": "cc.stknum = ?",
            "genes": "sg.stknum = ?",
            "properties": "cc.stknum = ?",
        }[dataset]
        params.append(stknum)
        return clause, params, resolved_kind

    if resolved_kind == "gene":
        if dataset == "stocks":
            clause, params = _gene_match_clause("sg.fbgn", "sg.gene_symbol", query)
            return (
                f"EXISTS (SELECT 1 FROM stockgenes sg WHERE sg.stknum = s.stknum AND {clause})",
                params,
                resolved_kind,
            )
        if dataset == "components":
            clause, params = _gene_match_clause("sg.fbgn", "sg.gene_symbol", query)
            return (
                f"EXISTS (SELECT 1 FROM stockgenes sg WHERE sg.stknum = cc.stknum AND sg.component_symbol = cc.component_symbol AND {clause})",
                params,
                resolved_kind,
            )
        if dataset == "genes":
            clause, params = _gene_match_clause("sg.fbgn", "sg.gene_symbol", query)
            return clause, params, resolved_kind
        clause, params = _gene_match_clause("sg2.fbgn", "sg2.gene_symbol", query)
        return (
            f"EXISTS (SELECT 1 FROM stockgenes sg2 WHERE sg2.stknum = cc.stknum AND sg2.component_symbol = cc.component_symbol AND {clause})",
            params,
            resolved_kind,
        )

    if resolved_kind == "component":
        clause, params = _prefix_match_clause(
            {"stocks": "sg.component_symbol", "components": "cc.component_symbol", "genes": "sg.component_symbol", "properties": "cc.component_symbol"}[dataset],
            query,
        )
        if dataset == "stocks":
            return (
                f"EXISTS (SELECT 1 FROM stockgenes sg WHERE sg.stknum = s.stknum AND {clause})",
                params,
                resolved_kind,
            )
        return clause, params, resolved_kind

    if resolved_kind == "fbid":
        clause, params = _prefix_match_clause(
            {"components": "cc.fbid", "properties": "cc.fbid"}[dataset]
            if dataset in {"components", "properties"}
            else "cc.fbid",
            query,
        )
        if dataset == "stocks":
            return (
                f"EXISTS (SELECT 1 FROM component_comments cc WHERE cc.stknum = s.stknum AND {clause})",
                params,
                resolved_kind,
            )
        if dataset == "genes":
            return (
                f"EXISTS (SELECT 1 FROM component_comments cc WHERE cc.stknum = sg.stknum AND cc.component_symbol = sg.component_symbol AND {clause})",
                params,
                resolved_kind,
            )
        return clause, params, resolved_kind

    if resolved_kind == "property":
        clause, params = _property_match_clause(query, exact=False)
        if dataset == "stocks":
            return (
                "EXISTS ("
                "SELECT 1 FROM stockgenes sg "
                "JOIN compprops cp ON cp.bdsc_symbol_id = sg.bdsc_symbol_id "
                f"WHERE sg.stknum = s.stknum AND {clause}"
                ")",
                params,
                resolved_kind,
            )
        if dataset == "components":
            return (
                "EXISTS ("
                "SELECT 1 FROM stockgenes sg "
                "JOIN compprops cp ON cp.bdsc_symbol_id = sg.bdsc_symbol_id "
                "WHERE sg.stknum = cc.stknum AND sg.component_symbol = cc.component_symbol "
                f"AND {clause}"
                ")",
                params,
                resolved_kind,
            )
        if dataset == "genes":
            return (
                f"EXISTS (SELECT 1 FROM compprops cp WHERE cp.bdsc_symbol_id = sg.bdsc_symbol_id AND {clause})",
                params,
                resolved_kind,
            )
        return clause, params, resolved_kind

    if resolved_kind == "property-exact":
        clause, params = _property_match_clause(query, exact=True)
        if dataset == "stocks":
            return (
                "EXISTS ("
                "SELECT 1 FROM stockgenes sg "
                "JOIN compprops cp ON cp.bdsc_symbol_id = sg.bdsc_symbol_id "
                f"WHERE sg.stknum = s.stknum AND {clause}"
                ")",
                params,
                resolved_kind,
            )
        if dataset == "components":
            return (
                "EXISTS ("
                "SELECT 1 FROM stockgenes sg "
                "JOIN compprops cp ON cp.bdsc_symbol_id = sg.bdsc_symbol_id "
                "WHERE sg.stknum = cc.stknum AND sg.component_symbol = cc.component_symbol "
                f"AND {clause}"
                ")",
                params,
                resolved_kind,
            )
        if dataset == "genes":
            return (
                f"EXISTS (SELECT 1 FROM compprops cp WHERE cp.bdsc_symbol_id = sg.bdsc_symbol_id AND {clause})",
                params,
                resolved_kind,
            )
        return clause, params, resolved_kind

    if resolved_kind == "driver-family":
        return _driver_family_criterion(dataset, query)

    if resolved_kind == "relationship":
        if dataset == "stocks":
            clause, params = _prefix_match_clause("cg.prop_syn", query)
            return (
                "EXISTS ("
                "SELECT 1 FROM stockgenes sg "
                "JOIN compgenes cg ON cg.bdsc_symbol_id = sg.bdsc_symbol_id "
                f"WHERE sg.stknum = s.stknum AND {clause}"
                ")",
                params,
                resolved_kind,
            )
        if dataset == "components":
            clause, params = _prefix_match_clause("cg.prop_syn", query)
            return (
                "EXISTS ("
                "SELECT 1 FROM stockgenes sg "
                "JOIN compgenes cg ON cg.bdsc_symbol_id = sg.bdsc_symbol_id "
                "WHERE sg.stknum = cc.stknum AND sg.component_symbol = cc.component_symbol "
                f"AND {clause}"
                ")",
                params,
                resolved_kind,
            )
        if dataset == "genes":
            clause, params = _prefix_match_clause("cg.prop_syn", query)
            return (
                f"EXISTS (SELECT 1 FROM compgenes cg WHERE cg.bdsc_symbol_id = sg.bdsc_symbol_id AND cg.bdsc_gene_id = sg.bdsc_gene_id AND {clause})",
                params,
                resolved_kind,
            )
        clause, params = _prefix_match_clause("cg.prop_syn", query)
        return (
            "EXISTS ("
            "SELECT 1 FROM stockgenes sg2 "
            "JOIN compgenes cg ON cg.bdsc_symbol_id = sg2.bdsc_symbol_id "
            "WHERE sg2.stknum = cc.stknum AND sg2.component_symbol = cc.component_symbol "
            f"AND {clause}"
            ")",
            params,
            resolved_kind,
        )

    if resolved_kind == "search":
        if dataset == "stocks":
            clause, params = _search_text_match_clause("sd.search_text", query)
            return clause, params, resolved_kind
        if dataset == "components":
            clause, params = _search_text_match_clause("sd.search_text", query)
            return (
                "EXISTS (SELECT 1 FROM search_documents sd "
                f"WHERE sd.stknum = cc.stknum AND {clause})",
                params,
                resolved_kind,
            )
        if dataset == "genes":
            clause, params = _search_text_match_clause("sd.search_text", query)
            return (
                "EXISTS (SELECT 1 FROM search_documents sd "
                f"WHERE sd.stknum = sg.stknum AND {clause})",
                params,
                resolved_kind,
            )
        clause, params = _search_text_match_clause("sd.search_text", query)
        return (
            "EXISTS (SELECT 1 FROM search_documents sd "
            f"WHERE sd.stknum = cc.stknum AND {clause})",
            params,
            resolved_kind,
        )

    raise ValueError(f"unsupported export filter kind: {kind}")


def _normalize_criteria(
    criteria: list[QueryCriterion] | None,
    query: str | None,
    kind: str,
) -> list[QueryCriterion]:
    normalized = [
        QueryCriterion(kind=item.kind, query=item.query.strip())
        for item in (criteria or [])
        if item.query.strip()
    ]
    if query and query.strip():
        normalized.append(QueryCriterion(kind=kind, query=query.strip()))
    return normalized


def _compose_where_clause(
    dataset: str,
    criteria: list[QueryCriterion] | None,
    *,
    query: str | None = None,
    kind: str = "auto",
) -> tuple[str, list[Any]]:
    normalized = _normalize_criteria(criteria, query, kind)
    if not normalized:
        return "", []

    predicates: list[str] = []
    params: list[Any] = []
    for criterion in normalized:
        predicate, predicate_params, _ = _single_criterion(
            dataset,
            criterion.query,
            criterion.kind,
        )
        predicates.append(f"({predicate})")
        params.extend(predicate_params)
    return "WHERE " + " AND ".join(predicates), params


def lookup_query(
    state_dir: Path,
    query: str,
    *,
    kind: str = "auto",
    limit: int | None = None,
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
    elif resolved_kind == "property-exact":
        results = search_property_exact(state_dir, query, limit=limit)
    elif resolved_kind == "driver-family":
        results = search_driver_family(state_dir, query, limit=limit)
    elif resolved_kind == "relationship":
        results = search_relationship(state_dir, query, limit=limit)
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


def live_search(query: str, limit: int | None = None) -> list[dict[str, Any]]:
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
        return _limit_rows(rows, limit)

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
    return _limit_rows(advanced_data.get("Data") or [], limit)


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
            has_trigram = bool(
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_trigram'"
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
                    "trigram_enabled": int(has_trigram),
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


def _dataset_sort_clause(dataset: str) -> str:
    if dataset == "stocks":
        return "ORDER BY s.stknum"
    if dataset == "components":
        return "ORDER BY cc.stknum, cc.component_symbol"
    if dataset == "genes":
        return "ORDER BY sg.stknum, sg.component_symbol, sg.gene_symbol, sg.fbgn"
    if dataset == "properties":
        return "ORDER BY cc.stknum, cc.component_symbol, cp.prop_syn, cp.property_id"
    raise ValueError(f"unsupported export dataset: {dataset}")


def _dataset_select_sql(dataset: str) -> str:
    if dataset == "stocks":
        return """
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
        """
    if dataset == "components":
        return f"""
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
        """
    if dataset == "genes":
        return """
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
        """
    if dataset == "properties":
        return """
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
        """
    raise ValueError(f"unsupported export dataset: {dataset}")


def iter_dataset_rows(
    state_dir: Path,
    dataset: str,
    *,
    where_clause: str = "",
    params: tuple[Any, ...] = (),
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    if dataset not in EXPORT_DATASETS:
        raise ValueError(f"unsupported export dataset: {dataset}")

    conn = _connect(state_dir)
    try:
        sql = _dataset_select_sql(dataset)
        if where_clause:
            sql += f"\n{where_clause}"
        sql += f"\n{_dataset_sort_clause(dataset)}"

        limit_clause, limit_params = _limit_sql(limit)
        if limit_clause:
            sql += f"\n{limit_clause}"
        cursor = conn.execute(sql, (*params, *limit_params))

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


def iter_export_rows(
    state_dir: Path,
    dataset: str,
    *,
    limit: int | None = None,
    criteria: list[QueryCriterion] | None = None,
    query: str | None = None,
    kind: str = "auto",
) -> Iterator[dict[str, Any]]:
    where_clause, params = _compose_where_clause(
        dataset,
        criteria,
        query=query,
        kind=kind,
    )
    yield from iter_dataset_rows(
        state_dir,
        dataset,
        where_clause=where_clause,
        params=tuple(params),
        limit=limit,
    )


def _report_olfactory_where(dataset: str) -> str:
    component_clause = (
        "component_symbol GLOB '*Or[0-9]*' "
        "OR component_symbol GLOB '*Orco*' "
        "OR component_symbol GLOB '*Ir[0-9]*' "
        "OR component_symbol GLOB '*Obp[0-9]*'"
    )
    if dataset == "stocks":
        return (
            "WHERE EXISTS (SELECT 1 FROM component_comments cc "
            f"WHERE cc.stknum = s.stknum AND ({component_clause}))"
        )
    symbol = REPORT_DATASET_SYMBOLS.get(dataset)
    if symbol is None:
        raise ValueError(f"unsupported report dataset: {dataset}")
    return f"WHERE {component_clause.replace('component_symbol', f'{symbol}.component_symbol')}"


def _report_row_key(dataset: str, row: dict[str, Any]) -> tuple[Any, ...]:
    if dataset == "stocks":
        return (row["stknum"],)
    if dataset == "components":
        return _component_result_key(row)
    if dataset == "genes":
        return _gene_result_key(row)
    if dataset == "properties":
        return (row["stknum"], row["component_symbol"], row["property_id"], row["prop_syn"])
    raise ValueError(f"unsupported report dataset: {dataset}")


def _merge_report_rows(dataset: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = _report_row_key(dataset, row)
        deduped.setdefault(key, row)
    return list(deduped.values())


def iter_report_rows(
    state_dir: Path,
    report_name: str,
    *,
    dataset: str | None = None,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    if report_name not in REPORT_NAMES:
        raise ValueError(f"unsupported report: {report_name}")
    spec = REPORT_SPECS[report_name]
    resolved_dataset = dataset or spec.default_dataset

    if report_name == "olfactory":
        yield from iter_dataset_rows(
            state_dir,
            resolved_dataset,
            where_clause=_report_olfactory_where(resolved_dataset),
            limit=limit,
        )
        return

    merged_rows: list[dict[str, Any]] = []
    for group in spec.groups:
        rows = list(
            iter_export_rows(
                state_dir,
                resolved_dataset,
                criteria=list(group),
                limit=limit,
            )
        )
        merged_rows.extend(rows)
        if limit is not None and len(_merge_report_rows(resolved_dataset, merged_rows)) >= limit:
            break

    deduped = _merge_report_rows(resolved_dataset, merged_rows)
    for row in _limit_rows(deduped, limit):
        yield row


def list_terms(
    state_dir: Path,
    scope: str,
    *,
    query: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if scope not in TERM_SCOPES:
        raise ValueError(f"unsupported term scope: {scope}")

    query = (query or "").strip()
    conn = _connect(state_dir)
    try:
        if scope == "properties":
            sql = """
                SELECT
                  prop_syn AS term,
                  MIN(property_descrip) AS description,
                  COUNT(*) AS count
                FROM compprops
                WHERE prop_syn != ''
            """
            params: list[Any] = []
            if query:
                sql += " AND LOWER(prop_syn) LIKE LOWER(?)"
                params.append(f"{query}%")
            sql += """
                GROUP BY prop_syn
                ORDER BY count DESC, term
                LIMIT ?
            """
        elif scope == "property-descriptions":
            sql = """
                SELECT
                  property_descrip AS term,
                  MIN(prop_syn) AS synonym,
                  COUNT(*) AS count
                FROM compprops
                WHERE property_descrip != ''
            """
            params = []
            if query:
                sql += " AND LOWER(property_descrip) LIKE LOWER(?)"
                params.append(f"%{query}%")
            sql += """
                GROUP BY property_descrip
                ORDER BY count DESC, term
                LIMIT ?
            """
        else:
            sql = """
                SELECT
                  prop_syn AS term,
                  COUNT(*) AS count
                FROM compgenes
                WHERE prop_syn != ''
            """
            params = []
            if query:
                sql += " AND LOWER(prop_syn) LIKE LOWER(?)"
                params.append(f"{query}%")
            sql += """
                GROUP BY prop_syn
                ORDER BY count DESC, term
                LIMIT ?
            """

        rows = conn.execute(sql, (*params, limit)).fetchall()
        return [dict(row) for row in rows]
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


def format_property_results(results: list[dict[str, Any]]) -> str:
    if not results:
        return "no results"
    lines = []
    for row in results:
        bits = [
            str(row["stknum"]),
            row["component_symbol"],
            row["fbid"],
            row["prop_syn"],
        ]
        if row.get("property_descrip"):
            bits.append(row["property_descrip"])
        lines.append(" | ".join(bits + [row["genotype"]]))
    return "\n".join(lines)


def format_dataset_results(dataset: str, results: list[dict[str, Any]]) -> str:
    if dataset == "stocks":
        return format_search_results(results)
    if dataset == "components":
        return format_component_results(results)
    if dataset == "genes":
        return format_gene_results(results)
    if dataset == "properties":
        return format_property_results(results)
    raise ValueError(f"unsupported dataset formatter: {dataset}")


def format_term_results(results: list[dict[str, Any]]) -> str:
    if not results:
        return "no results"
    lines = []
    for row in results:
        bits = [row["term"], f"count={row['count']}"]
        if row.get("description"):
            bits.append(row["description"])
        if row.get("synonym"):
            bits.append(f"synonym={row['synonym']}")
        lines.append(" | ".join(bits))
    return "\n".join(lines)


def format_lookup_result(result: dict[str, Any]) -> str:
    lines = [f"query: {result['query']}", f"kind: {result['kind']}"]
    rows = result["results"]
    kind = result["kind"]
    if kind in {"stock", "rrid"}:
        body = format_stock(rows[0] if rows else None)
    elif kind == "gene":
        body = format_gene_results(rows)
    elif kind in {
        "component",
        "fbid",
        "property",
        "property-exact",
        "driver-family",
        "relationship",
    }:
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
