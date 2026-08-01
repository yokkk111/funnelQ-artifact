#!/usr/bin/env python3
"""Build the formal DeFi-style compact FunnelQ store for the NFT workload.

The old NFT builder materialized several overlapping representations of the
same event and, most importantly, persisted global pair/pathlet structures and
per-anchor path summaries.  Those structures made the NFT index much larger
than the DeFi index and made construction scale with event combinations.

This builder keeps only the structures required by the formal runtime:

* one dictionary-encoded role-transition row per valid canonical NFT event;
* one compact forward-window summary per possible root event;
* small value/type dictionaries and one collection/order access index.

No closure payload, atom posting copy, pair posting, pathlet table, wide event
copy, or per-query/per-path synopsis is persisted.  Base summaries are built in
one sliding-window pass per collection.  Refined role/path feasibility is
computed by the Rust candidate executor from the compact event stream and its
query-time in-memory postings, matching the DeFi execution principle.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import os
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from contextlib import closing
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import pandas as pd
import psutil

ZERO = "0x0000000000000000000000000000000000000000"
REQUIRED_COLUMNS = (
    "event_id",
    "event_type",
    "block_time",
    "block_number",
    "tx_hash",
    "local_order",
    "nft",
    "seller",
    "buyer",
    "from_address",
    "to_address",
)
LAYOUT = "compact_full_binding_v4_defi_runtime"
SCHEMA_VERSION = 4


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build compact full-binding NFT FunnelQ store.")
    ap.add_argument("--event-atoms-csv", required=True, type=Path)
    ap.add_argument("--sqlite-out", required=True, type=Path)
    ap.add_argument("--outdir", default="", type=Path)
    ap.add_argument("--report-json", default="", type=Path)
    ap.add_argument("--max-delta", type=int, default=86400)
    ap.add_argument("--chunk-rows", type=int, default=100_000)
    ap.add_argument("--insert-batch", type=int, default=50_000)
    ap.add_argument("--progress-every", type=int, default=500_000)
    ap.add_argument("--max-rss-mb", type=float, default=18_000.0)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--skip-vacuum", action="store_true")
    return ap.parse_args()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            block = f.read(16 * 1024 * 1024)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def atomic_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def start_memory_guard(max_rss_mb: float) -> None:
    if max_rss_mb <= 0:
        return
    proc = psutil.Process(os.getpid())
    limit = int(max_rss_mb * 1024 * 1024)

    def guard() -> None:
        while True:
            try:
                rss = proc.memory_info().rss
            except psutil.Error:
                return
            if rss > limit:
                print(
                    f"[funnelq-compact][guard] RSS={rss/1024/1024:.1f} MiB "
                    f"> limit={max_rss_mb:.1f} MiB; abort",
                    flush=True,
                )
                os._exit(137)
            time.sleep(1.0)

    threading.Thread(target=guard, daemon=True).start()


def configure(conn: sqlite3.Connection, *, build: bool = True) -> None:
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=FILE")
    conn.execute("PRAGMA cache_size=-262144")
    conn.execute("PRAGMA mmap_size=1073741824")
    if build:
        conn.execute("PRAGMA locking_mode=EXCLUSIVE")


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE nft_build_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE nft_event_type_dict (
            event_type_id INTEGER PRIMARY KEY,
            event_type TEXT NOT NULL UNIQUE
        );

        CREATE TABLE nft_role_value_dict (
            value_kind TEXT NOT NULL,
            value_id INTEGER NOT NULL,
            value_text TEXT NOT NULL,
            PRIMARY KEY(value_kind, value_id)
        ) WITHOUT ROWID;

        CREATE TABLE nft_role_transition_edges (
            order_seq INTEGER PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            event_type_id INTEGER NOT NULL,
            block_ts INTEGER NOT NULL,
            src_addr_id INTEGER NOT NULL,
            dst_addr_id INTEGER NOT NULL,
            nft_id INTEGER NOT NULL,
            collection_id INTEGER NOT NULL
        );

        CREATE TABLE nft_closure_summary (
            closure_id INTEGER PRIMARY KEY,
            anchor_order_seq INTEGER NOT NULL,
            start_ts INTEGER NOT NULL,
            end_ts INTEGER NOT NULL,
            end_order_seq INTEGER NOT NULL,
            collection_id INTEGER NOT NULL,
            transition_count INTEGER NOT NULL,
            addr_count INTEGER NOT NULL,
            nft_count INTEGER NOT NULL,
            nft_ge2_count INTEGER NOT NULL,
            b_out_event_count INTEGER NOT NULL
        );

        CREATE TABLE nft_anchor_directory (
            closure_id INTEGER PRIMARY KEY,
            anchor_order_seq INTEGER NOT NULL
        ) WITHOUT ROWID;
        """
    )
    conn.executemany(
        "INSERT INTO nft_event_type_dict(event_type_id,event_type) VALUES (?,?)",
        [(1, "NFTTrade"), (2, "NFTTransfer")],
    )
    conn.commit()


def set_meta(conn: sqlite3.Connection, key: str, value: object) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO nft_build_meta(key,value) VALUES (?,?)",
        (key, json.dumps(value, ensure_ascii=False, sort_keys=True)),
    )


def get_meta(conn: sqlite3.Connection, key: str, default: object = None) -> object:
    row = conn.execute("SELECT value FROM nft_build_meta WHERE key=?", (key,)).fetchone()
    if row is None:
        return default
    return json.loads(str(row[0]))


def clean_series(chunk: pd.DataFrame, name: str, *, lower: bool = False) -> pd.Series:
    if name not in chunk.columns:
        out = pd.Series([""] * len(chunk), index=chunk.index, dtype="string")
    else:
        out = chunk[name].astype("string").fillna("").str.strip()
        out = out.mask(out.str.lower().isin(["nan", "none", "null", "nat", "<nil>"]), "")
    return out.str.lower() if lower else out


def load_maps(conn: sqlite3.Connection) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    maps: dict[str, dict[str, int]] = {"address": {}, "nft": {}, "collection": {}}
    for kind, value_id, value_text in conn.execute(
        "SELECT value_kind,value_id,value_text FROM nft_role_value_dict"
    ):
        maps[str(kind)][str(value_text)] = int(value_id)
    return maps["address"], maps["nft"], maps["collection"]


def assign_ids(
    values: pd.Series,
    mapping: dict[str, int],
) -> tuple[pd.Series, list[tuple[int, str]]]:
    new_rows: list[tuple[int, str]] = []
    next_id = len(mapping) + 1
    for value in pd.unique(values):
        text = str(value)
        if not text or text in mapping:
            continue
        mapping[text] = next_id
        new_rows.append((next_id, text))
        next_id += 1
    ids = values.map(mapping)
    if ids.isna().any():
        raise RuntimeError("internal dictionary projection failure")
    return ids.astype("int64"), new_rows


def insert_dictionary_rows(
    conn: sqlite3.Connection,
    kind: str,
    rows: Sequence[tuple[int, str]],
) -> None:
    if rows:
        conn.executemany(
            "INSERT INTO nft_role_value_dict(value_kind,value_id,value_text) VALUES (?,?,?)",
            ((kind, value_id, text) for value_id, text in rows),
        )


def ingest_events(
    conn: sqlite3.Connection,
    events: Path,
    *,
    chunk_rows: int,
    insert_batch: int,
    progress_every: int,
    resume: bool,
) -> dict[str, int | float]:
    start_source_row = int(get_meta(conn, "source_rows_seen", 0) or 0) if resume else 0
    valid_before = int(conn.execute("SELECT COUNT(*) FROM nft_role_transition_edges").fetchone()[0])
    address_map, nft_map, collection_map = load_maps(conn)
    t0 = time.perf_counter()
    source_seen = 0
    valid_seen = valid_before
    skipped_invalid = int(get_meta(conn, "invalid_rows", 0) or 0) if resume else 0
    last_canonical_key: tuple[int, int, str, int, str] | None = None

    reader = pd.read_csv(
        events,
        usecols=lambda c: c in REQUIRED_COLUMNS,
        dtype="string",
        chunksize=chunk_rows,
        low_memory=False,
    )
    for chunk in reader:
        chunk_start = source_seen
        chunk_end = source_seen + len(chunk)
        source_seen = chunk_end
        missing = [c for c in REQUIRED_COLUMNS if c not in chunk.columns]
        if missing:
            raise ValueError(f"canonical NFT CSV missing columns: {missing}")

        # All other NFT baselines derive order_seq by sorting on this exact key
        # (see common/nft_semantics.py).  The compact builder deliberately
        # streams instead of holding the 3-month frame in memory, so it requires
        # the canonical input to already be in that order and rejects a mismatch
        # rather than silently changing query semantics.
        full_event_id = clean_series(chunk, "event_id")
        full_parsed = pd.to_datetime(clean_series(chunk, "block_time"), errors="coerce", utc=True)
        if full_parsed.isna().any():
            raise ValueError(f"cannot parse block_time for {int(full_parsed.isna().sum())} rows")
        # Use an explicit resolution: pandas/Windows commonly parses this as
        # datetime64[us], while older code assumed datetime64[ns].
        full_block_ts = full_parsed.dt.as_unit("s").astype("int64")
        full_block_number = pd.to_numeric(clean_series(chunk, "block_number"), errors="coerce").fillna(-1).astype("int64")
        full_tx_hash = clean_series(chunk, "tx_hash")
        full_local_order = pd.to_numeric(clean_series(chunk, "local_order"), errors="coerce").fillna(-1).astype("int64")
        keys = zip(full_block_ts, full_block_number, full_tx_hash, full_local_order, full_event_id)
        for offset, raw_key in enumerate(keys):
            key = (int(raw_key[0]), int(raw_key[1]), str(raw_key[2]), int(raw_key[3]), str(raw_key[4]))
            if last_canonical_key is not None and key < last_canonical_key:
                raise ValueError(
                    "canonical NFT CSV is not ordered by "
                    "(block_ts,block_number,tx_hash,local_order,event_id) at source row "
                    f"{chunk_start + offset}; compact streaming order would disagree with SQL/HGMatch"
                )
            last_canonical_key = key

        if chunk_end <= start_source_row:
            continue
        if chunk_start < start_source_row:
            cut = start_source_row - chunk_start
            chunk = chunk.iloc[cut:].copy()
            full_event_id = full_event_id.iloc[cut:]
            full_block_ts = full_block_ts.iloc[cut:]
            chunk_start = start_source_row

        event_id = full_event_id
        event_type = clean_series(chunk, "event_type")
        nft = clean_series(chunk, "nft", lower=True)
        collection = nft.str.split(":", n=1).str[0].fillna("")
        seller = clean_series(chunk, "seller", lower=True)
        buyer = clean_series(chunk, "buyer", lower=True)
        from_addr = clean_series(chunk, "from_address", lower=True)
        to_addr = clean_series(chunk, "to_address", lower=True)
        src = pd.Series("", index=chunk.index, dtype="string")
        dst = pd.Series("", index=chunk.index, dtype="string")
        trade_mask = event_type.eq("NFTTrade")
        transfer_mask = event_type.eq("NFTTransfer")
        src.loc[trade_mask] = seller.loc[trade_mask]
        dst.loc[trade_mask] = buyer.loc[trade_mask]
        src.loc[transfer_mask] = from_addr.loc[transfer_mask]
        dst.loc[transfer_mask] = to_addr.loc[transfer_mask]
        block_ts = full_block_ts

        valid = (
            event_id.ne("")
            & event_type.isin(["NFTTrade", "NFTTransfer"])
            & nft.ne("")
            & collection.ne("")
            & src.ne("")
            & dst.ne("")
            & src.ne(dst)
            & src.str.lower().ne(ZERO)
            & dst.str.lower().ne(ZERO)
        )
        skipped_invalid += int((~valid).sum())
        if not valid.any():
            set_meta(conn, "source_rows_seen", chunk_end)
            set_meta(conn, "invalid_rows", skipped_invalid)
            conn.commit()
            continue

        selected = pd.DataFrame(
            {
                "order_seq": pd.Series(range(chunk_start, chunk_end), index=chunk.index, dtype="int64")[valid],
                "event_id": event_id[valid],
                "event_type": event_type[valid],
                "block_ts": block_ts[valid],
                "src": src[valid],
                "dst": dst[valid],
                "nft": nft[valid],
                "collection": collection[valid],
            }
        )
        addr_values = pd.concat([selected["src"], selected["dst"]], ignore_index=True)
        _addr_ids, new_addr = assign_ids(addr_values, address_map)
        insert_dictionary_rows(conn, "address", new_addr)
        src_ids = selected["src"].map(address_map).astype("int64")
        dst_ids = selected["dst"].map(address_map).astype("int64")
        nft_ids, new_nft = assign_ids(selected["nft"], nft_map)
        coll_ids, new_coll = assign_ids(selected["collection"], collection_map)
        insert_dictionary_rows(conn, "nft", new_nft)
        insert_dictionary_rows(conn, "collection", new_coll)
        type_ids = selected["event_type"].map({"NFTTrade": 1, "NFTTransfer": 2}).astype("int64")

        rows = zip(
            selected["order_seq"].astype("int64"),
            selected["event_id"].astype(str),
            type_ids,
            selected["block_ts"].astype("int64"),
            src_ids,
            dst_ids,
            nft_ids,
            coll_ids,
        )
        batch: list[tuple[object, ...]] = []
        for row in rows:
            batch.append(tuple(row))
            if len(batch) >= insert_batch:
                conn.executemany(
                    "INSERT INTO nft_role_transition_edges VALUES (?,?,?,?,?,?,?,?)",
                    batch,
                )
                valid_seen += len(batch)
                batch.clear()
        if batch:
            conn.executemany(
                "INSERT INTO nft_role_transition_edges VALUES (?,?,?,?,?,?,?,?)",
                batch,
            )
            valid_seen += len(batch)
        set_meta(conn, "source_rows_seen", chunk_end)
        set_meta(conn, "invalid_rows", skipped_invalid)
        conn.commit()
        if source_seen % progress_every < len(chunk):
            rss = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            print(
                f"[funnelq-compact][ingest] source={source_seen:,} valid={valid_seen:,} "
                f"invalid={skipped_invalid:,} rss={rss:.1f} MiB elapsed={time.perf_counter()-t0:.1f}s",
                flush=True,
            )

    set_meta(conn, "ingest_complete", True)
    set_meta(conn, "source_rows_seen", source_seen)
    set_meta(conn, "invalid_rows", skipped_invalid)
    set_meta(conn, "canonical_input_order_validated", True)
    conn.commit()
    return {
        "source_rows": source_seen,
        "valid_events": valid_seen,
        "invalid_rows": skipped_invalid,
        "elapsed_s": time.perf_counter() - t0,
    }


def iter_collection_groups(
    conn: sqlite3.Connection,
    *,
    after_collection_id: int = 0,
) -> Iterator[tuple[int, list[tuple[int, int, int, int, int]]]]:
    cur = conn.execute(
        """
        SELECT collection_id,order_seq,block_ts,src_addr_id,dst_addr_id,nft_id
        FROM nft_role_transition_edges
        WHERE collection_id>?
        ORDER BY collection_id,order_seq
        """,
        (after_collection_id,),
    )
    current: int | None = None
    rows: list[tuple[int, int, int, int, int]] = []
    for collection_id, order_seq, block_ts, src, dst, nft_id in cur:
        cid = int(collection_id)
        if current is None:
            current = cid
        if cid != current:
            yield current, rows
            current = cid
            rows = []
        rows.append((int(order_seq), int(block_ts), int(src), int(dst), int(nft_id)))
    if current is not None:
        yield current, rows


def build_window_summaries(
    conn: sqlite3.Connection,
    *,
    max_delta: int,
    insert_batch: int,
    progress_every: int,
    resume: bool,
) -> dict[str, int | float]:
    if not get_meta(conn, "ingest_complete", False):
        raise RuntimeError("event ingestion is incomplete")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fq_edges_collection_order "
        "ON nft_role_transition_edges(collection_id,order_seq)"
    )
    conn.commit()
    if not resume:
        conn.execute("DELETE FROM nft_closure_summary")
        set_meta(conn, "summary_last_collection_id", 0)
        conn.commit()
    after_collection = int(get_meta(conn, "summary_last_collection_id", 0) or 0)
    # Batches may have been committed inside the next collection before an
    # interrupted run recorded that collection as complete. Remove only those
    # partial rows; completed collection ids at or below the marker are kept.
    if resume:
        conn.execute("DELETE FROM nft_closure_summary WHERE collection_id>?", (after_collection,))
        conn.commit()
    t0 = time.perf_counter()
    total = int(conn.execute("SELECT COUNT(*) FROM nft_closure_summary").fetchone()[0])
    collections = 0
    pending: list[tuple[int, ...]] = []

    def flush() -> None:
        nonlocal pending
        if not pending:
            return
        conn.executemany(
            """
            INSERT INTO nft_closure_summary(
                closure_id,anchor_order_seq,start_ts,end_ts,end_order_seq,collection_id,
                transition_count,addr_count,nft_count,nft_ge2_count,b_out_event_count
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            pending,
        )
        pending = []

    for collection_id, rows in iter_collection_groups(conn, after_collection_id=after_collection):
        n = len(rows)
        src_positions: dict[int, list[int]] = defaultdict(list)
        for pos, row in enumerate(rows):
            src_positions[row[2]].append(pos)
        addr_counts: dict[int, int] = {}
        nft_counts: dict[int, int] = {}
        distinct_addr = 0
        distinct_nft = 0
        nft_ge2 = 0
        j = 0

        def add_addr(value: int) -> None:
            nonlocal distinct_addr
            old = addr_counts.get(value, 0)
            if old == 0:
                distinct_addr += 1
            addr_counts[value] = old + 1

        def remove_addr(value: int) -> None:
            nonlocal distinct_addr
            old = addr_counts[value]
            if old == 1:
                distinct_addr -= 1
                del addr_counts[value]
            else:
                addr_counts[value] = old - 1

        def add_nft(value: int) -> None:
            nonlocal distinct_nft, nft_ge2
            old = nft_counts.get(value, 0)
            if old == 0:
                distinct_nft += 1
            if old == 1:
                nft_ge2 += 1
            nft_counts[value] = old + 1

        def remove_nft(value: int) -> None:
            nonlocal distinct_nft, nft_ge2
            old = nft_counts[value]
            if old == 2:
                nft_ge2 -= 1
            if old == 1:
                distinct_nft -= 1
                del nft_counts[value]
            else:
                nft_counts[value] = old - 1

        for i, (order_seq, block_ts, src, dst, nft_id) in enumerate(rows):
            end_ts = block_ts + max_delta
            while j < n and rows[j][1] <= end_ts:
                _o, _ts, s, d, nft = rows[j]
                add_addr(s)
                add_addr(d)
                add_nft(nft)
                j += 1
            positions = src_positions.get(dst, [])
            lo = bisect.bisect_right(positions, i)
            hi = bisect.bisect_left(positions, j)
            b_out_count = max(0, hi - lo)
            end_order = rows[j - 1][0] if j > i else order_seq
            pending.append(
                (
                    order_seq + 1,
                    order_seq,
                    block_ts,
                    end_ts,
                    end_order,
                    collection_id,
                    j - i,
                    distinct_addr,
                    distinct_nft,
                    nft_ge2,
                    b_out_count,
                )
            )
            total += 1
            remove_addr(src)
            remove_addr(dst)
            remove_nft(nft_id)
            if len(pending) >= insert_batch:
                flush()
                conn.commit()
            if total % progress_every == 0:
                rss = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                print(
                    f"[funnelq-compact][summary] closures={total:,} collections={collections:,} "
                    f"rss={rss:.1f} MiB elapsed={time.perf_counter()-t0:.1f}s",
                    flush=True,
                )
        flush()
        set_meta(conn, "summary_last_collection_id", collection_id)
        conn.commit()
        collections += 1

    set_meta(conn, "summary_complete", True)
    conn.execute("DELETE FROM nft_anchor_directory")
    conn.execute(
        """
        INSERT INTO nft_anchor_directory(closure_id,anchor_order_seq)
        SELECT closure_id,anchor_order_seq
        FROM nft_closure_summary
        WHERE b_out_event_count>0
        ORDER BY anchor_order_seq
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_nft_anchor_directory_order "
        "ON nft_anchor_directory(anchor_order_seq)"
    )
    set_meta(
        conn,
        "anchor_directory_rows",
        int(conn.execute("SELECT COUNT(*) FROM nft_anchor_directory").fetchone()[0]),
    )
    conn.commit()
    return {
        "closures": total,
        "collections_processed": collections,
        "elapsed_s": time.perf_counter() - t0,
    }


def validate_store(conn: sqlite3.Connection) -> dict[str, object]:
    events = int(conn.execute("SELECT COUNT(*) FROM nft_role_transition_edges").fetchone()[0])
    closures = int(conn.execute("SELECT COUNT(*) FROM nft_closure_summary").fetchone()[0])
    anchor_rows = int(conn.execute("SELECT COUNT(*) FROM nft_anchor_directory").fetchone()[0])
    if events != closures:
        raise RuntimeError(f"compact store coverage mismatch: events={events} closures={closures}")
    missing = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM nft_role_transition_edges e
            LEFT JOIN nft_closure_summary s ON s.closure_id=e.order_seq+1
            WHERE s.closure_id IS NULL OR s.anchor_order_seq<>e.order_seq
            """
        ).fetchone()[0]
    )
    if missing:
        raise RuntimeError(f"{missing} compact events have no matching closure summary")
    extra = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM nft_closure_summary s
            LEFT JOIN nft_role_transition_edges e ON e.order_seq=s.anchor_order_seq
            WHERE e.order_seq IS NULL OR s.closure_id<>e.order_seq+1
            """
        ).fetchone()[0]
    )
    if extra:
        raise RuntimeError(f"{extra} closure summaries have no matching compact event")
    invalid = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM nft_closure_summary
            WHERE closure_id<>anchor_order_seq+1
               OR end_order_seq<anchor_order_seq
               OR end_ts<start_ts
               OR transition_count<1
               OR addr_count<2
               OR nft_count<1
            """
        ).fetchone()[0]
    )
    if invalid:
        raise RuntimeError(f"{invalid} malformed compact closure summaries")
    expected_anchor_rows = int(
        conn.execute(
            "SELECT COUNT(*) FROM nft_closure_summary WHERE b_out_event_count>0"
        ).fetchone()[0]
    )
    if anchor_rows != expected_anchor_rows:
        raise RuntimeError(
            "compact anchor directory coverage mismatch: "
            f"directory={anchor_rows} expected={expected_anchor_rows}"
        )
    tables = [
        str(r[0])
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
    ]
    forbidden = {
        "nft_events",
        "nft_closure_records",
        "nft_closure_payload",
        "nft_integer_hyperedges",
        "nft_role_postings",
        "nft_atom_postings",
        "nft_atom_pair_postings",
        "nft_pathlet_index",
        "nft_anchor_role_aggregate_synopsis",
        "nft_root_anchor_refined_synopsis",
        "nft_anchor_pathlet_refined_synopsis",
    }
    present_forbidden = sorted(forbidden.intersection(tables))
    if present_forbidden:
        raise RuntimeError(f"forbidden bulky tables remain: {present_forbidden}")
    return {
        "events": events,
        "closures": closures,
        "missing_event_summaries": missing,
        "extra_summaries": extra,
        "tables": tables,
        "forbidden_tables_present": present_forbidden,
        "dictionary_counts": {
            str(kind): int(count)
            for kind, count in conn.execute(
                "SELECT value_kind,COUNT(*) FROM nft_role_value_dict GROUP BY value_kind"
            )
        },
    }


def main() -> int:
    args = parse_args()
    if args.chunk_rows <= 0 or args.insert_batch <= 0 or args.progress_every <= 0:
        raise ValueError("chunk/batch/progress values must be positive")
    if args.max_delta <= 0:
        raise ValueError("--max-delta must be positive")
    start_memory_guard(float(args.max_rss_mb))

    events = args.event_atoms_csv.resolve()
    db = args.sqlite_out.resolve()
    outdir = (
        args.outdir.resolve()
        if str(args.outdir).strip() not in {"", "."}
        else db.parent.resolve()
    )
    report_path = (
        args.report_json.resolve()
        if str(args.report_json).strip() not in {"", "."}
        else outdir / "compact_build_report.json"
    )
    if not events.exists():
        raise FileNotFoundError(events)
    outdir.mkdir(parents=True, exist_ok=True)
    db.parent.mkdir(parents=True, exist_ok=True)
    if args.force and db.exists():
        db.unlink()
    if db.exists() and not args.resume:
        raise FileExistsError(f"store already exists: {db}; use --resume or --force")

    input_fingerprint = {
        "path": str(events),
        "size_bytes": events.stat().st_size,
        "sha256": sha256_file(events),
        "max_delta": int(args.max_delta),
        "layout": LAYOUT,
        "schema_version": SCHEMA_VERSION,
    }
    t0 = time.perf_counter()
    new_store = not db.exists()
    with closing(sqlite3.connect(db)) as conn:
        configure(conn)
        if new_store:
            create_schema(conn)
            set_meta(conn, "input_fingerprint", input_fingerprint)
            set_meta(conn, "layout", LAYOUT)
            set_meta(conn, "schema_version", SCHEMA_VERSION)
            conn.commit()
        else:
            previous = get_meta(conn, "input_fingerprint")
            if previous != input_fingerprint:
                raise ValueError("existing compact store uses a different input/configuration; use --force")

        ingest_report = (
            {"resumed_completed": True}
            if get_meta(conn, "ingest_complete", False)
            else ingest_events(
                conn,
                events,
                chunk_rows=int(args.chunk_rows),
                insert_batch=int(args.insert_batch),
                progress_every=int(args.progress_every),
                resume=bool(args.resume),
            )
        )
        summary_report = (
            {"resumed_completed": True}
            if get_meta(conn, "summary_complete", False)
            else build_window_summaries(
                conn,
                max_delta=int(args.max_delta),
                insert_batch=int(args.insert_batch),
                progress_every=int(args.progress_every),
                resume=bool(args.resume),
            )
        )
        print("[funnelq-compact] ANALYZE", flush=True)
        duplicate_event_ids = int(conn.execute(
            "SELECT COUNT(*) FROM ("
            "SELECT event_id FROM nft_role_transition_edges "
            "GROUP BY event_id HAVING COUNT(*)>1)"
        ).fetchone()[0])
        if duplicate_event_ids:
            raise ValueError(
                "compact NFT store contains duplicate event_id values: "
                f"{duplicate_event_ids}"
            )
        conn.execute("ANALYZE")
        conn.commit()
        validation = validate_store(conn)
        set_meta(conn, "build_complete", True)
        conn.commit()

    if not args.skip_vacuum:
        print("[funnelq-compact] VACUUM", flush=True)
        with closing(sqlite3.connect(db)) as conn:
            configure(conn)
            conn.execute("VACUUM")
    final_size = db.stat().st_size
    report = {
        "system": "FunnelQ",
        "layout": LAYOUT,
        "schema_version": SCHEMA_VERSION,
        "event_atoms_csv": str(events),
        "sqlite_out": str(db),
        "sqlite_size_bytes": final_size,
        "input_fingerprint": input_fingerprint,
        "ingest": ingest_report,
        "window_summary": summary_report,
        "validation": validation,
        "total_build_s": time.perf_counter() - t0,
        "persistent_rules": {
            "one_integer_event_row_per_valid_event": True,
            "one_sliding_window_summary_per_root": True,
            "wide_event_copy": False,
            "closure_payload_membership": False,
            "global_atom_posting_copy": False,
            "global_pair_postings": False,
            "global_pathlet_index": False,
            "per_anchor_role_aggregate_table": False,
            "per_anchor_path_synopsis_table": False,
            "refined_filter_source": "query-time Rust postings over compact event stream",
            "full_binding_source": "compact event stream + value dictionaries",
        },
    }
    atomic_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
