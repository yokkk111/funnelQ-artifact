#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from bisect import bisect_right, bisect_right as _bisect_right
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Set

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from systems.nft.common.nft_semantics import RUNTIME_COLUMNS, configure_sqlite, load_event_atoms

ROLE_POSTING_FIELDS = ("nft", "seller", "buyer", "from_address", "to_address", "move_from", "move_to")
GENERIC_VALUE_FIELDS = tuple(dict.fromkeys((*ROLE_POSTING_FIELDS, "collection_contract")))
INTERFACE_SOURCE_FIELDS = ("seller", "buyer")
INTERFACE_TARGET_FIELDS = INTERFACE_SOURCE_FIELDS
INTERFACE_EVENT_TYPES = ("NFTTrade", "NFTTransfer")


def interface_bit(event_type: str, source_field: str, target_field: str) -> int:
    return (
        INTERFACE_EVENT_TYPES.index(event_type) * len(INTERFACE_SOURCE_FIELDS) * len(INTERFACE_TARGET_FIELDS)
        + INTERFACE_SOURCE_FIELDS.index(source_field) * len(INTERFACE_TARGET_FIELDS)
        + INTERFACE_TARGET_FIELDS.index(target_field)
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build paper-aligned compact v13 NFT HVCEQ sidecar/store.")
    ap.add_argument("--event-atoms-csv", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--sqlite-out", required=True)
    ap.add_argument("--report-json", default="")
    ap.add_argument("--max-delta", type=int, default=86400)
    ap.add_argument(
        "--compact",
        action="store_true",
        help="Build a space-optimized generic HVCEQ store: keep generic sidecars, omit SQL-style event indexes, and compact role-transition edges to integer ids.",
    )
    ap.add_argument(
        "--omit-closure-payload",
        action="store_true",
        help="Do not persist nft_closure_payload; local exact reconstructs closure-local edges from closure descriptors and role-transition edges.",
    )
    ap.add_argument(
        "--build-closure-local",
        action="store_true",
        help="Enable v20 QuerySpec metadata for query-local closure edge arrays; does not persist full closure-edge membership.",
    )
    ap.add_argument(
        "--pathlet-insert-batch",
        type=int,
        default=50000,
        help="Rows per SQLite executemany commit while materializing 2-hop pathlets.",
    )
    ap.add_argument(
        "--pathlet-progress-every",
        type=int,
        default=50000,
        help="Print pathlet progress after this many processed role-transition edges.",
    )
    ap.add_argument(
        "--persist-pathlets",
        action="store_true",
        help=(
            "Persist the exact 2-hop nft_pathlet_index. The compact formal "
            "runtime does not need this table: it keeps only closure-level "
            "summaries and later builds the root-relative refined synopsis."
        ),
    )
    ap.add_argument(
        "--skip-vacuum",
        action="store_true",
        help="Skip the final VACUUM. Useful for development only; formal storage measurements should keep VACUUM enabled.",
    )
    return ap.parse_args()


def _exec_many(conn: sqlite3.Connection, sql: str, rows: Iterable[Tuple[Any, ...]], batch_size: int = 50000) -> int:
    cur = conn.cursor()
    batch: List[Tuple[Any, ...]] = []
    total = 0
    for r in rows:
        batch.append(r)
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            total += len(batch)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)
        total += len(batch)
    return total


def build_event_type_dict(df: pd.DataFrame) -> Dict[str, int]:
    types = sorted(str(x) for x in df["event_type"].dropna().unique())
    return {typ: i + 1 for i, typ in enumerate(types)}


def build_anchor_stats(df: pd.DataFrame, type_dict: Dict[str, int]) -> List[Tuple[int, int, int]]:


    counts = df["event_type"].value_counts(dropna=False).to_dict()
    return [
        (int(type_dict[event_type]), int(counts.get(event_type, 0)), 0)
        for event_type in sorted(type_dict, key=type_dict.get)
    ]


def iter_event_rows(df: pd.DataFrame) -> Iterable[Tuple[Any, ...]]:
    cols = list(RUNTIME_COLUMNS)
    for r in df[cols].itertuples(index=False, name=None):
        yield tuple(r)


def iter_closure_record_rows(df: pd.DataFrame, type_dict: Dict[str, int], max_delta: int = 86400) -> Iterable[Tuple[int, int, int, str, str, str, int, int, int]]:


    cols = ["order_seq", "event_type", "nft", "collection_contract", "block_ts", "nft_pos"]
    for order_seq, event_type, nft, collection_contract, block_ts, nft_pos in df[cols].itertuples(index=False, name=None):
        anchor_order_seq = int(order_seq)
        start_ts = int(block_ts)
        yield (
            anchor_order_seq + 1,
            anchor_order_seq,
            int(type_dict[str(event_type)]),
            str(event_type),
            str(nft),
            str(collection_contract),
            start_ts,
            start_ts + int(max_delta),
            int(nft_pos),
        )


def iter_same_nft_closure_payload_rows(df: pd.DataFrame, max_delta: int = 86400) -> Iterable[Tuple[int, int]]:


    non_empty = df[df["nft"].ne("")].copy()
    if non_empty.empty:
        return
    for _nft, g in non_empty.sort_values(["nft", "block_ts", "order_seq"], kind="mergesort").groupby("nft", sort=False):
        records = list(g[["order_seq", "block_ts"]].itertuples(index=False, name=None))
        n = len(records)
        j = 0
        for i, rec in enumerate(records):
            anchor_order_seq = int(rec[0])
            start_ts = int(rec[1])
            if j < i:
                j = i
            end_ts = start_ts + int(max_delta)
            while j < n and int(records[j][1]) <= end_ts:
                j += 1
            closure_id = anchor_order_seq + 1
            for k in range(i, j):
                yield (closure_id, int(records[k][0]))


def iter_base_count_rows(df: pd.DataFrame, max_delta: int = 86400) -> Iterable[Tuple[int, int, int, int, int]]:

    non_empty = df[df["nft"].ne("")].copy()
    if non_empty.empty:
        return
    for _nft, g in non_empty.sort_values(["nft", "order_seq"], kind="mergesort").groupby("nft", sort=False):
        records = g[["order_seq", "block_ts", "event_type"]].to_records(index=False)
        n = len(records)
        trade_prefix = [0] * (n + 1)
        transfer_prefix = [0] * (n + 1)
        for i, rec in enumerate(records):
            et = str(rec[2])
            trade_prefix[i + 1] = trade_prefix[i] + (1 if et == "NFTTrade" else 0)
            transfer_prefix[i + 1] = transfer_prefix[i] + (1 if et == "NFTTransfer" else 0)
        j = 0
        for i, rec in enumerate(records):
            order_seq = int(rec[0])
            start_ts = int(rec[1])
            if j < i:
                j = i
            end_ts = start_ts + int(max_delta)
            while j < n and int(records[j][1]) <= end_ts:
                j += 1
            trade_count = int(trade_prefix[j] - trade_prefix[i])
            transfer_count = int(transfer_prefix[j] - transfer_prefix[i])
            yield (order_seq + 1, order_seq, trade_count, transfer_count, int(j - i))


def build_base_count_rows(df: pd.DataFrame, max_delta: int = 86400) -> List[Tuple[int, int, int, int, int]]:
    return list(iter_base_count_rows(df, max_delta=max_delta))

def iter_refined_interface_rows(df: pd.DataFrame, max_delta: int = 86400) -> Iterable[Tuple[int, int, int, int]]:


    non_empty = df[df["nft"].ne("")].copy()
    cols = ["order_seq", "block_ts", "event_type", *INTERFACE_SOURCE_FIELDS]
    for _nft, group in non_empty.sort_values(["nft", "block_ts", "order_seq"], kind="mergesort").groupby("nft", sort=False):
        records = list(group[cols].itertuples(index=False, name=None))
        postings: Dict[Tuple[str, str, str], List[Tuple[int, int]]] = {}
        for rec in records:
            order_seq, block_ts, event_type = int(rec[0]), int(rec[1]), str(rec[2])
            if event_type not in INTERFACE_EVENT_TYPES:
                continue
            for offset, target_field in enumerate(INTERFACE_TARGET_FIELDS):
                value = str(rec[3 + offset])
                if value:
                    postings.setdefault((event_type, target_field, value), []).append((block_ts, order_seq))
        for rec in records:
            order_seq, block_ts = int(rec[0]), int(rec[1])
            mask_lo = 0
            mask_hi = 0
            for source_offset, source_field in enumerate(INTERFACE_SOURCE_FIELDS):
                value = str(rec[3 + source_offset])
                if not value:
                    continue
                for event_type in INTERFACE_EVENT_TYPES:
                    for target_field in INTERFACE_TARGET_FIELDS:
                        events = postings.get((event_type, target_field, value))
                        if not events:
                            continue
                        pos = bisect_right(events, (block_ts, order_seq))
                        if pos >= len(events) or events[pos][0] > block_ts + int(max_delta):
                            continue
                        bit = interface_bit(event_type, source_field, target_field)
                        if bit < 64:
                            mask_lo |= 1 << bit
                        else:
                            mask_hi |= 1 << (bit - 64)
            yield (order_seq + 1, order_seq, mask_lo, mask_hi)


def build_refined_interface_rows(df: pd.DataFrame, max_delta: int = 86400) -> List[Tuple[int, int, int, int]]:
    return list(iter_refined_interface_rows(df, max_delta=max_delta))


ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _is_zero_addr(value: object) -> bool:
    text = str(value or "").strip().lower()
    return (not text) or text == ZERO_ADDRESS


def _iter_after_records(records: List[Tuple[int, int, str, str, str]], lower_order: int, max_ts: int):


    for rec in records:
        order_seq = int(rec[0])
        if order_seq <= int(lower_order):
            continue
        if int(rec[1]) > int(max_ts):
            break
        yield rec


def build_role_value_dict(df: pd.DataFrame) -> Tuple[List[Tuple[int, str, str]], Dict[Tuple[str, str], int]]:
    rows: List[Tuple[int, str, str]] = []
    ids: Dict[Tuple[str, str], int] = {}
    next_id = 1
    for field in GENERIC_VALUE_FIELDS:
        for value in sorted(str(v) for v in df[field].dropna().unique() if str(v)):
            ids[(field, value)] = next_id
            rows.append((next_id, field, value))
            next_id += 1
    return rows, ids


def iter_integer_hyperedges(df: pd.DataFrame, type_dict: Dict[str, int], role_ids: Dict[Tuple[str, str], int]) -> Iterable[Tuple[int, int, int, int, int, int, int, int]]:
    for row in df[["order_seq", "event_type", "nft", "seller", "buyer", "from_address", "to_address", "block_ts"]].itertuples(index=False, name=None):
        order_seq, event_type, nft, seller, buyer, from_address, to_address, block_ts = row
        yield (int(order_seq), int(type_dict[str(event_type)]), int(role_ids.get(("nft", str(nft)), 0)), int(role_ids.get(("seller", str(seller)), 0)), int(role_ids.get(("buyer", str(buyer)), 0)), int(role_ids.get(("from_address", str(from_address)), 0)), int(role_ids.get(("to_address", str(to_address)), 0)), int(block_ts))


def iter_role_postings(df: pd.DataFrame, role_ids: Dict[Tuple[str, str], int]) -> Iterable[Tuple[str, int, int]]:
    for field in ROLE_POSTING_FIELDS:
        for order_seq, value in df[["order_seq", field]].itertuples(index=False, name=None):
            value = str(value)
            if value:
                yield (field, int(role_ids[(field, value)]), int(order_seq))


def _build_pathlet_index_streaming(
    conn: sqlite3.Connection,
    *,
    max_delta: int,
    insert_batch: int,
    progress_every: int,
    persist_rows: bool,
) -> int:


    if insert_batch <= 0:
        raise ValueError("pathlet_insert_batch must be positive")
    if progress_every <= 0:
        raise ValueError("pathlet_progress_every must be positive")

    print("[funnelq:pathlet] creating compact collection-ordered edge stream ...", flush=True)
    conn.execute("DROP TABLE IF EXISTS temp_pathlet_edges")
    conn.execute(
        """
        CREATE TEMP TABLE temp_pathlet_edges (
            collection_id INTEGER NOT NULL,
            order_seq INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            block_ts INTEGER NOT NULL,
            src_role_id INTEGER NOT NULL,
            dst_role_id INTEGER NOT NULL,
            src_canon_id INTEGER NOT NULL,
            dst_canon_id INTEGER NOT NULL,
            nft_id INTEGER NOT NULL,
            PRIMARY KEY(collection_id, order_seq)
        ) WITHOUT ROWID
        """
    )
    conn.execute(
        """
        INSERT INTO temp_pathlet_edges(
            collection_id, order_seq, event_type, block_ts,
            src_role_id, dst_role_id, src_canon_id, dst_canon_id, nft_id
        )
        SELECT
            e.collection_id, e.order_seq, e.event_type, e.block_ts,
            e.src_addr_id, e.dst_addr_id,
            COALESCE(src_c.addr_id, 0), COALESCE(dst_c.addr_id, 0), e.nft_id
        FROM nft_role_transition_edges e
        LEFT JOIN nft_address_canon_dict src_c ON src_c.value_text=e.src_addr
        LEFT JOIN nft_address_canon_dict dst_c ON dst_c.value_text=e.dst_addr
        WHERE e.src_addr_id<>0 AND e.dst_addr_id<>0
          AND e.nft_id<>0 AND e.collection_id<>0
          AND COALESCE(src_c.addr_id,0)<>0
          AND COALESCE(dst_c.addr_id,0)<>0
        """
    )
    edge_total = int(conn.execute("SELECT COUNT(*) FROM temp_pathlet_edges").fetchone()[0])
    collection_total = int(conn.execute("SELECT COUNT(DISTINCT collection_id) FROM temp_pathlet_edges").fetchone()[0])
    conn.execute("DROP TABLE IF EXISTS temp_pathlet_closure_stats")
    conn.execute(
        """
        CREATE TEMP TABLE temp_pathlet_closure_stats (
            closure_id INTEGER PRIMARY KEY,
            pathlet_count INTEGER NOT NULL,
            nft_count INTEGER NOT NULL,
            addr_count INTEGER NOT NULL,
            has_cycle INTEGER NOT NULL,
            has_cross_asset INTEGER NOT NULL
        ) WITHOUT ROWID
        """
    )
    conn.commit()
    print(
        f"[funnelq:pathlet] edges={edge_total:,} collections={collection_total:,} "
        f"batch={insert_batch:,} max_delta={max_delta}",
        flush=True,
    )

    pathlet_sql = """
        INSERT INTO nft_pathlet_index(
            closure_id, path_len, start_addr_id, mid_addr1_id, mid_addr2_id, end_addr_id,
            event_seq1, event_seq2, event_seq3, nft1_id, nft2_id, nft3_id, collection_id,
            edge_type_seq, start_ts, end_ts, time_span,
            asset_relation_flag, is_cycle_flag, is_cross_asset_flag
        ) VALUES (?, 2, ?, ?, 0, ?, ?, ?, 0, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    stats_sql = """
        INSERT INTO temp_pathlet_closure_stats(
            closure_id, pathlet_count, nft_count, addr_count, has_cycle, has_cross_asset
        ) VALUES (?, ?, ?, ?, ?, ?)
    """

    pathlet_batch: List[Tuple[Any, ...]] = []
    stats_batch: List[Tuple[Any, ...]] = []
    inserted = 0
    enumerated = 0
    processed = 0
    collections_done = 0
    next_progress = progress_every
    started = time.perf_counter()

    def flush_pathlets() -> None:
        nonlocal inserted
        if not pathlet_batch:
            return
        if persist_rows:
            conn.executemany(pathlet_sql, pathlet_batch)
            inserted += len(pathlet_batch)
        pathlet_batch.clear()
        if persist_rows:
            conn.commit()

    def flush_stats() -> None:
        if not stats_batch:
            return
        conn.executemany(stats_sql, stats_batch)
        stats_batch.clear()
        conn.commit()

    cursor = conn.execute(
        """
        SELECT collection_id, order_seq, event_type, block_ts,
               src_role_id, dst_role_id, src_canon_id, dst_canon_id, nft_id
        FROM temp_pathlet_edges
        ORDER BY collection_id, order_seq
        """
    )

    current_collection: int | None = None
    collection_rows: List[Tuple[int, str, int, int, int, int, int]] = []

    def process_collection(collection_id: int, rows: List[Tuple[int, str, int, int, int, int, int]]) -> None:
        nonlocal processed, collections_done, next_progress, enumerated


        source_postings: Dict[int, List[Tuple[int, int, str, int, int, int, int, int]]] = defaultdict(list)
        for order_seq, event_type, block_ts, src_role_id, dst_role_id, src_canon_id, dst_canon_id, nft_id in rows:
            source_postings[int(src_canon_id)].append(
                (
                    int(order_seq), int(block_ts), str(event_type),
                    int(src_role_id), int(dst_role_id),
                    int(src_canon_id), int(dst_canon_id), int(nft_id),
                )
            )
        source_orders: Dict[int, List[int]] = {
            key: [int(rec[0]) for rec in values]
            for key, values in source_postings.items()
        }

        for order_seq, event_type, block_ts, src_role_id, dst_role_id, src_canon_id, dst_canon_id, nft_id in rows:
            candidates = source_postings.get(int(dst_canon_id))
            pathlet_count = 0
            nft_ids: Set[int] = set()
            addr_ids: Set[int] = set()
            has_cycle = 0
            has_cross = 0
            if candidates:
                orders = source_orders[int(dst_canon_id)]
                pos = _bisect_right(orders, int(order_seq))
                end_ts = int(block_ts) + int(max_delta)
                for candidate_pos in range(pos, len(candidates)):
                    e2 = candidates[candidate_pos]
                    e2_order, e2_ts, e2_type, _e2_src_role, e2_dst_role, _e2_src_canon, e2_dst_canon, e2_nft = e2
                    if int(e2_ts) > end_ts:
                        break
                    cross = 1 if int(nft_id) != int(e2_nft) else 0
                    cycle = 1 if int(src_canon_id) == int(e2_dst_canon) else 0
                    if persist_rows:
                        pathlet_batch.append(
                            (
                                int(order_seq) + 1,
                                int(src_role_id), int(dst_role_id), int(e2_dst_role),
                                int(order_seq), int(e2_order),
                                int(nft_id), int(e2_nft), int(collection_id),
                                f"{event_type}>{e2_type}",
                                int(block_ts), int(e2_ts), int(e2_ts) - int(block_ts),
                                cross, cycle, cross,
                            )
                        )
                    enumerated += 1
                    pathlet_count += 1
                    nft_ids.add(int(nft_id))
                    nft_ids.add(int(e2_nft))
                    addr_ids.add(int(src_role_id))
                    addr_ids.add(int(dst_role_id))
                    addr_ids.add(int(e2_dst_role))
                    has_cycle |= cycle
                    has_cross |= cross
                    if persist_rows and len(pathlet_batch) >= insert_batch:
                        flush_pathlets()
            if pathlet_count:
                stats_batch.append(
                    (
                        int(order_seq) + 1, pathlet_count * 2,
                        len(nft_ids), len(addr_ids), has_cycle, has_cross,
                    )
                )
                if len(stats_batch) >= insert_batch:
                    flush_stats()
            processed += 1
            if processed >= next_progress:
                elapsed = time.perf_counter() - started
                rate = processed / elapsed if elapsed > 0 else 0.0
                pct = (100.0 * processed / edge_total) if edge_total else 100.0
                print(
                    f"[funnelq:pathlet] processed={processed:,}/{edge_total:,} "
                    f"({pct:.2f}%) pathlets={enumerated:,} "
                    f"collections={collections_done:,}/{collection_total:,} "
                    f"rate={rate:,.0f} edges/s elapsed={elapsed:.1f}s",
                    flush=True,
                )
                next_progress += progress_every
        collections_done += 1

    for rec in cursor:
        collection_id = int(rec[0])
        payload = (
            int(rec[1]), str(rec[2]), int(rec[3]), int(rec[4]),
            int(rec[5]), int(rec[6]), int(rec[7]), int(rec[8]),
        )
        if current_collection is None:
            current_collection = collection_id
        if collection_id != current_collection:
            process_collection(current_collection, collection_rows)
            collection_rows = []
            current_collection = collection_id
        collection_rows.append(payload)
    if current_collection is not None:
        process_collection(current_collection, collection_rows)

    flush_pathlets()
    flush_stats()
    elapsed = time.perf_counter() - started
    print(
        f"[funnelq:pathlet] materialization complete: processed={processed:,} "
        f"pathlets={enumerated:,} persisted={inserted:,} "
        f"collections={collections_done:,} elapsed={elapsed:.1f}s",
        flush=True,
    )

    print("[funnelq:pathlet] applying exact closure-summary updates ...", flush=True)
    conn.execute(
        """
        UPDATE nft_closure_summary
        SET transition_count=MAX(transition_count, COALESCE((
                SELECT pathlet_count * 2 FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), 0)),
            event_count=MAX(event_count, COALESCE((
                SELECT pathlet_count * 2 FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), 0)),
            addr_count=COALESCE((
                SELECT addr_count FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), addr_count),
            distinct_addr_count=COALESCE((
                SELECT addr_count FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), distinct_addr_count),
            nft_count=MAX(nft_count, COALESCE((
                SELECT nft_count FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), 0)),
            distinct_nft_count=MAX(distinct_nft_count, COALESCE((
                SELECT nft_count FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), 0)),
            has_2hop_pathlet=1,
            has_cycle_flag=CASE WHEN COALESCE((
                SELECT has_cycle FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), 0)=1 THEN 1 ELSE has_cycle_flag END,
            has_cycle_like=CASE WHEN COALESCE((
                SELECT has_cycle FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), 0)=1 THEN 1 ELSE has_cycle_like END,
            has_cross_asset_flag=CASE WHEN COALESCE((
                SELECT has_cross_asset FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), 0)=1 THEN 1 ELSE has_cross_asset_flag END,
            has_cross_asset=CASE WHEN COALESCE((
                SELECT has_cross_asset FROM temp_pathlet_closure_stats p
                WHERE p.closure_id=nft_closure_summary.closure_id), 0)=1 THEN 1 ELSE has_cross_asset END
        WHERE closure_id IN (SELECT closure_id FROM temp_pathlet_closure_stats)
        """
    )
    conn.execute("DROP TABLE IF EXISTS temp_pathlet_closure_stats")
    conn.execute("DROP TABLE IF EXISTS temp_pathlet_edges")
    conn.commit()
    return enumerated


def create_generic_hvceq_indexes(
    conn: sqlite3.Connection,
    role_value_ids: Dict[Tuple[str, str], int],
    *,
    compact: bool = False,
    max_delta: int = 86400,
    pathlet_insert_batch: int = 50000,
    pathlet_progress_every: int = 50000,
    persist_pathlets: bool = False,
) -> Dict[str, int]:

    print("[funnelq:generic] creating role-transition edges ...", flush=True)
    conn.execute(
        """
        CREATE TABLE nft_role_transition_edges (
            edge_id INTEGER PRIMARY KEY,
            order_seq INTEGER NOT NULL UNIQUE,
            event_id TEXT NOT NULL,
            closure_id INTEGER NOT NULL DEFAULT 0,
            event_type_id INTEGER NOT NULL DEFAULT 0,
            event_type TEXT NOT NULL,
            block_ts INTEGER NOT NULL,
            src_addr_id INTEGER NOT NULL DEFAULT 0,
            dst_addr_id INTEGER NOT NULL DEFAULT 0,
            nft_id INTEGER NOT NULL DEFAULT 0,
            collection_id INTEGER NOT NULL DEFAULT 0,
            src_addr TEXT NOT NULL,
            dst_addr TEXT NOT NULL,
            nft TEXT NOT NULL,
            collection TEXT NOT NULL,
            tx_hash TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO nft_role_transition_edges(
            edge_id, order_seq, event_id, closure_id, event_type_id, event_type, block_ts,
            src_addr_id, dst_addr_id, nft_id, collection_id,
            src_addr, dst_addr, nft, collection, tx_hash
        )
        SELECT e.order_seq, e.order_seq, e.event_id, 0, COALESCE(t.event_type_id, 0), e.event_type, e.block_ts,
               0, 0, 0, 0,
               move_from, move_to, nft, collection_contract, tx_hash
        FROM nft_events e
        LEFT JOIN nft_event_type_dict t ON t.event_type=e.event_type
        WHERE e.event_type IN ('NFTTrade','NFTTransfer')
          AND e.collection_contract<>'' AND e.nft<>''
          AND e.move_from<>'' AND e.move_to<>'' AND e.move_from<>e.move_to
          AND lower(e.move_from)<>lower('0x0000000000000000000000000000000000000000')
          AND lower(e.move_to)<>lower('0x0000000000000000000000000000000000000000')
        """
    )
    role_edge_count = int(conn.execute("SELECT COUNT(*) FROM nft_role_transition_edges").fetchone()[0])
    print(f"[funnelq:generic] role-transition edges={role_edge_count:,}", flush=True)

    conn.execute(
        """
        CREATE TEMP TABLE temp_generic_value_ids(
            role_name TEXT NOT NULL,
            value_text TEXT NOT NULL,
            value_id INTEGER NOT NULL,
            PRIMARY KEY(role_name, value_text)
        ) WITHOUT ROWID
        """
    )
    conn.executemany(
        "INSERT INTO temp_generic_value_ids(role_name, value_text, value_id) VALUES (?, ?, ?)",
        ((role, value, int(vid)) for (role, value), vid in role_value_ids.items()),
    )
    print("[funnelq:generic] assigning compact integer ids ...", flush=True)
    conn.execute(
        """
        UPDATE nft_role_transition_edges
        SET src_addr_id=COALESCE((SELECT value_id FROM temp_generic_value_ids v WHERE v.role_name='move_from' AND v.value_text=src_addr), 0),
            dst_addr_id=COALESCE((SELECT value_id FROM temp_generic_value_ids v WHERE v.role_name='move_to' AND v.value_text=dst_addr), 0),
            nft_id=COALESCE((SELECT value_id FROM temp_generic_value_ids v WHERE v.role_name='nft' AND v.value_text=nft), 0),
            collection_id=COALESCE((SELECT value_id FROM temp_generic_value_ids v WHERE v.role_name='collection_contract' AND v.value_text=collection), 0)
        """
    )

    print("[funnelq:generic] creating closure summary ...", flush=True)
    conn.execute(
        """
        CREATE TABLE nft_closure_summary (
            closure_id INTEGER PRIMARY KEY,
            anchor_order_seq INTEGER NOT NULL,
            anchor_block_ts INTEGER NOT NULL,
            start_ts INTEGER NOT NULL,
            end_ts INTEGER NOT NULL,
            anchor_nft TEXT NOT NULL,
            anchor_collection TEXT NOT NULL,
            collection_id INTEGER NOT NULL,
            transition_count INTEGER NOT NULL,
            event_count INTEGER NOT NULL,
            trade_count INTEGER NOT NULL,
            transfer_count INTEGER NOT NULL,
            addr_count INTEGER NOT NULL,
            nft_count INTEGER NOT NULL,
            distinct_addr_count INTEGER NOT NULL,
            distinct_nft_count INTEGER NOT NULL,
            distinct_collection_count INTEGER NOT NULL,
            max_in_degree INTEGER NOT NULL,
            max_out_degree INTEGER NOT NULL,
            has_2hop_pathlet INTEGER NOT NULL,
            has_cycle_flag INTEGER NOT NULL,
            has_cycle_like INTEGER NOT NULL,
            has_cross_asset_flag INTEGER NOT NULL,
            has_cross_asset INTEGER NOT NULL,
            time_span INTEGER NOT NULL
        ) WITHOUT ROWID
        """
    )
    conn.execute(
        """
        INSERT INTO nft_closure_summary
        SELECT
          r.closure_id,
          r.anchor_order_seq,
          r.start_block_ts,
          r.start_block_ts,
          r.start_block_ts + ?,
          r.anchor_nft,
          r.anchor_collection_contract,
          COALESCE(vc.value_id, 0),
          COALESCE(b.num_events, 0) AS transition_count,
          COALESCE(b.num_events, 0) AS event_count,
          COALESCE(b.nft_trade_count, 0) AS trade_count,
          COALESCE(b.nft_transfer_count, 0) AS transfer_count,
          0 AS addr_count,
          CASE WHEN COALESCE(b.num_events, 0)>0 THEN 1 ELSE 0 END AS nft_count,
          0 AS distinct_addr_count,
          CASE WHEN COALESCE(b.num_events, 0)>0 THEN 1 ELSE 0 END AS distinct_nft_count,
          CASE WHEN r.anchor_collection_contract<>'' THEN 1 ELSE 0 END AS distinct_collection_count,
          0 AS max_in_degree,
          0 AS max_out_degree,
          0 AS has_2hop_pathlet,
          0 AS has_cycle_flag,
          0 AS has_cycle_like,
          0 AS has_cross_asset_flag,
          0 AS has_cross_asset,
          r.end_block_ts-r.start_block_ts AS time_span
        FROM nft_closure_records r
        LEFT JOIN nft_base_counts b ON b.closure_id=r.closure_id
        LEFT JOIN temp_generic_value_ids vc
          ON vc.role_name='collection_contract'
         AND vc.value_text=r.anchor_collection_contract
        """,
        (int(max_delta),),
    )

    if persist_pathlets:
        conn.execute(
            """
            CREATE TABLE nft_pathlet_index (
            pathlet_id INTEGER PRIMARY KEY,
            closure_id INTEGER NOT NULL,
            path_len INTEGER NOT NULL,
            start_addr_id INTEGER NOT NULL,
            mid_addr1_id INTEGER NOT NULL,
            mid_addr2_id INTEGER NOT NULL,
            end_addr_id INTEGER NOT NULL,
            event_seq1 INTEGER NOT NULL,
            event_seq2 INTEGER NOT NULL,
            event_seq3 INTEGER NOT NULL,
            nft1_id INTEGER NOT NULL,
            nft2_id INTEGER NOT NULL,
            nft3_id INTEGER NOT NULL,
            collection_id INTEGER NOT NULL,
            edge_type_seq TEXT NOT NULL,
            start_ts INTEGER NOT NULL,
            end_ts INTEGER NOT NULL,
            time_span INTEGER NOT NULL,
            asset_relation_flag INTEGER NOT NULL,
            is_cycle_flag INTEGER NOT NULL,
            is_cross_asset_flag INTEGER NOT NULL
            )
            """
        )
    conn.execute(
        """
        CREATE TABLE nft_address_canon_dict AS
        SELECT value_text, MIN(value_id) AS addr_id
        FROM temp_generic_value_ids
        WHERE role_name IN ('move_from','move_to','seller','buyer','from_address','to_address')
          AND value_text<>''
        GROUP BY value_text
        """
    )
    conn.execute("CREATE UNIQUE INDEX idx_hv_addr_canon_text ON nft_address_canon_dict(value_text)")
    conn.execute("CREATE INDEX idx_hv_addr_canon_id ON nft_address_canon_dict(addr_id)")

    atom_posting_count = 0
    if not compact:
        print("[funnelq:generic] creating atom postings ...", flush=True)
        conn.execute(
            """
            CREATE TABLE nft_atom_postings (
            atom_posting_id INTEGER PRIMARY KEY,
            atom_kind_id INTEGER NOT NULL,
            order_seq INTEGER NOT NULL,
            event_type_id INTEGER NOT NULL,
            block_ts INTEGER NOT NULL,
            src_addr_id INTEGER NOT NULL,
            dst_addr_id INTEGER NOT NULL,
            nft_id INTEGER NOT NULL,
            collection_id INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO nft_atom_postings(
            atom_posting_id, atom_kind_id, order_seq, event_type_id, block_ts,
            src_addr_id, dst_addr_id, nft_id, collection_id
        )
        SELECT
            edge_id, 1, order_seq, event_type_id, block_ts,
            COALESCE(src_c.addr_id,0), COALESCE(dst_c.addr_id,0), nft_id, collection_id
        FROM nft_role_transition_edges
        LEFT JOIN nft_address_canon_dict src_c ON src_c.value_text=nft_role_transition_edges.src_addr
        LEFT JOIN nft_address_canon_dict dst_c ON dst_c.value_text=nft_role_transition_edges.dst_addr
        WHERE src_addr_id<>0 AND dst_addr_id<>0 AND nft_id<>0 AND collection_id<>0
          AND COALESCE(src_c.addr_id,0)<>0
          AND COALESCE(dst_c.addr_id,0)<>0
            """
        )
        atom_posting_count = int(conn.execute("SELECT COUNT(*) FROM nft_atom_postings").fetchone()[0])
    conn.commit()

    pathlet_count = _build_pathlet_index_streaming(
        conn,
        max_delta=int(max_delta),
        insert_batch=int(pathlet_insert_batch),
        progress_every=int(pathlet_progress_every),
        persist_rows=bool(persist_pathlets),
    )

    conn.execute("DROP TABLE IF EXISTS temp_generic_value_ids")
    if compact:
        print("[funnelq:generic] compacting role-transition edge table ...", flush=True)
        conn.execute("ALTER TABLE nft_role_transition_edges RENAME TO nft_role_transition_edges_wide")
        conn.execute(
            """
            CREATE TABLE nft_role_transition_edges (
                edge_id INTEGER PRIMARY KEY,
                order_seq INTEGER NOT NULL UNIQUE,
                event_id TEXT NOT NULL,
                closure_id INTEGER NOT NULL DEFAULT 0,
                event_type_id INTEGER NOT NULL DEFAULT 0,
                block_ts INTEGER NOT NULL,
                src_addr_id INTEGER NOT NULL DEFAULT 0,
                dst_addr_id INTEGER NOT NULL DEFAULT 0,
                nft_id INTEGER NOT NULL DEFAULT 0,
                collection_id INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO nft_role_transition_edges(
                edge_id, order_seq, event_id, closure_id, event_type_id, block_ts,
                src_addr_id, dst_addr_id, nft_id, collection_id
            )
            SELECT edge_id, order_seq, event_id, closure_id, event_type_id, block_ts,
                   src_addr_id, dst_addr_id, nft_id, collection_id
            FROM nft_role_transition_edges_wide
            """
        )
        conn.execute("DROP TABLE nft_role_transition_edges_wide")
    conn.commit()
    return {
        "nft_role_transition_edges_rows": role_edge_count,
        "nft_closure_summary_rows": int(conn.execute("SELECT COUNT(*) FROM nft_closure_summary").fetchone()[0]),
        "nft_pathlet_index_rows": pathlet_count,
        "nft_atom_postings_rows": atom_posting_count,
        "nft_anchor_pathlet_refined_synopsis_rows": 0,
    }

def create_closure_local_role_edges(conn: sqlite3.Connection) -> int:


    conn.execute("DROP TABLE IF EXISTS nft_closure_role_edges")
    conn.execute(
        """
        CREATE TABLE nft_closure_role_edges (
            closure_id INTEGER NOT NULL,
            order_seq INTEGER NOT NULL,
            event_type_id INTEGER NOT NULL,
            block_ts INTEGER NOT NULL,
            src_addr_id INTEGER NOT NULL,
            dst_addr_id INTEGER NOT NULL,
            nft_id INTEGER NOT NULL,
            collection_id INTEGER NOT NULL,
            PRIMARY KEY(closure_id, order_seq)
        ) WITHOUT ROWID
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tmp_role_edges_collection_order_int "
        "ON nft_role_transition_edges(collection_id, order_seq, block_ts)"
    )
    conn.execute(
        """
        INSERT INTO nft_closure_role_edges(
            closure_id, order_seq, event_type_id, block_ts,
            src_addr_id, dst_addr_id, nft_id, collection_id
        )
        SELECT
            c.closure_id, e.order_seq, e.event_type_id, e.block_ts,
            e.src_addr_id, e.dst_addr_id, e.nft_id, e.collection_id
        FROM nft_closure_summary c
        JOIN nft_role_transition_edges e
          ON e.collection_id=c.collection_id
         AND e.order_seq>=c.anchor_order_seq
         AND e.block_ts<=c.end_ts
        WHERE c.collection_id<>0
          AND e.src_addr_id<>0 AND e.dst_addr_id<>0
          AND e.nft_id<>0 AND e.collection_id<>0
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hv_final_closure_role_edges_closure ON nft_closure_role_edges(closure_id, order_seq)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hv_final_closure_role_edges_src ON nft_closure_role_edges(closure_id, src_addr_id, order_seq)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hv_final_closure_role_edges_dst ON nft_closure_role_edges(closure_id, dst_addr_id, order_seq)")
    return int(conn.execute("SELECT COUNT(*) FROM nft_closure_role_edges").fetchone()[0])


def main() -> int:
    args = parse_args()
    outdir = Path(args.outdir)
    db = Path(args.sqlite_out)
    report_path = Path(args.report_json) if args.report_json else outdir / "build_report.json"
    outdir.mkdir(parents=True, exist_ok=True)
    db.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if db.exists():
        db.unlink()

    t0 = time.perf_counter()
    print(f"[funnelq:load] reading {args.event_atoms_csv} ...", flush=True)
    df = load_event_atoms(Path(args.event_atoms_csv))
    t1 = time.perf_counter()
    print(f"[funnelq:load] events={len(df):,} elapsed={t1 - t0:.1f}s", flush=True)
    type_dict = build_event_type_dict(df)
    anchor_stats = build_anchor_stats(df, type_dict)
    closure_record_rows = None
    base_count_row_count = 0
    refined_interface_row_count = 0
    role_value_rows, role_value_ids = build_role_value_dict(df)
    role_value_row_count = len(role_value_rows)
    t2 = time.perf_counter()

    with sqlite3.connect(db) as conn:
        configure_sqlite(conn)
        conn.execute("PRAGMA temp_store=FILE")
        conn.execute("PRAGMA cache_size=-65536")
        conn.execute("PRAGMA page_size=4096")
        conn.execute("PRAGMA locking_mode=EXCLUSIVE")
        conn.execute("BEGIN")
        conn.execute(
            "CREATE TABLE nft_events ("
            "event_id TEXT NOT NULL,"
            "event_type TEXT NOT NULL,"
            "block_time TEXT NOT NULL,"
            "block_number TEXT NOT NULL,"
            "tx_hash TEXT NOT NULL,"
            "local_order TEXT NOT NULL,"
            "nft TEXT NOT NULL,"
            "collection_contract TEXT NOT NULL,"
            "seller TEXT NOT NULL,"
            "buyer TEXT NOT NULL,"
            "from_address TEXT NOT NULL,"
            "to_address TEXT NOT NULL,"
            "move_from TEXT NOT NULL,"
            "move_to TEXT NOT NULL,"
            "block_ts INTEGER NOT NULL,"
            "order_seq INTEGER PRIMARY KEY,"
            "nft_pos INTEGER NOT NULL"
            ")"
        )
        placeholders = ",".join("?" for _ in RUNTIME_COLUMNS)
        print("[funnelq:base] writing nft_events ...", flush=True)
        event_write_count = _exec_many(conn, f"INSERT INTO nft_events({', '.join(RUNTIME_COLUMNS)}) VALUES ({placeholders})", iter_event_rows(df))
        print(f"[funnelq:base] nft_events={event_write_count:,}", flush=True)

        conn.execute(
            "CREATE TABLE nft_event_type_dict ("
            "event_type TEXT PRIMARY KEY,"
            "event_type_id INTEGER NOT NULL UNIQUE"
            ") WITHOUT ROWID"
        )
        conn.executemany(
            "INSERT INTO nft_event_type_dict(event_type, event_type_id) VALUES (?, ?)",
            [(typ, tid) for typ, tid in sorted(type_dict.items(), key=lambda x: x[1])],
        )
        conn.execute("CREATE TABLE nft_role_value_dict(value_id INTEGER PRIMARY KEY, role_name TEXT NOT NULL, value_text TEXT NOT NULL, UNIQUE(role_name, value_text)) WITHOUT ROWID")
        print(f"[funnelq:base] writing role-value dictionary rows={role_value_row_count:,} ...", flush=True)
        _exec_many(conn, "INSERT INTO nft_role_value_dict(value_id, role_name, value_text) VALUES (?, ?, ?)", role_value_rows)
        role_value_rows = []
        integer_edge_count = 0
        posting_count = 0
        if not args.compact:
            conn.execute("CREATE TABLE nft_integer_hyperedges(order_seq INTEGER PRIMARY KEY, event_type_id INTEGER NOT NULL, nft_id INTEGER NOT NULL, seller_id INTEGER NOT NULL, buyer_id INTEGER NOT NULL, from_id INTEGER NOT NULL, to_id INTEGER NOT NULL, block_ts INTEGER NOT NULL) WITHOUT ROWID")
            print("[funnelq:base] writing integer hyperedges ...", flush=True)
            integer_edge_count = _exec_many(conn, "INSERT INTO nft_integer_hyperedges(order_seq, event_type_id, nft_id, seller_id, buyer_id, from_id, to_id, block_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", iter_integer_hyperedges(df, type_dict, role_value_ids))
            print(f"[funnelq:base] integer_hyperedges={integer_edge_count:,}", flush=True)
            conn.execute("CREATE TABLE nft_role_postings(role_name TEXT NOT NULL, value_id INTEGER NOT NULL, order_seq INTEGER NOT NULL, PRIMARY KEY(role_name, value_id, order_seq)) WITHOUT ROWID")
            print("[funnelq:base] writing role postings ...", flush=True)
            posting_count = _exec_many(conn, "INSERT INTO nft_role_postings(role_name, value_id, order_seq) VALUES (?, ?, ?)", iter_role_postings(df, role_value_ids))
            print(f"[funnelq:base] role_postings={posting_count:,}", flush=True)
            conn.execute(
                "CREATE TABLE nft_anchor_stats ("
                "event_type_id INTEGER PRIMARY KEY,"
                "closure_hit_count INTEGER NOT NULL,"
                "binding_strength INTEGER NOT NULL"
                ") WITHOUT ROWID"
            )
            conn.executemany(
                "INSERT INTO nft_anchor_stats(event_type_id, closure_hit_count, binding_strength) VALUES (?, ?, ?)",
                anchor_stats,
            )
        conn.execute(
            "CREATE TABLE nft_runtime_meta (meta_key TEXT PRIMARY KEY, meta_value TEXT NOT NULL) WITHOUT ROWID"
        )
        conn.executemany(
            "INSERT INTO nft_runtime_meta(meta_key, meta_value) VALUES (?, ?)",
            [
                ("runtime_version", "paper_aligned_v17_generic_closure_pathlet_local"),
                ("anchor_selection", "LexAnchor(type_frequency,-binding_strength,pattern_id)"),
                ("base_synopsis", "same_nft_type_multiplicity"),
                ("refined_filter", "same_nft_interface_masks_plus_generic_role_transition_pathlet_index"),
                ("local_runtime", "closure_records_generic_pathlet_anchor_seeded_local_matching"),
            ],
        )
        conn.execute(
            "CREATE TABLE nft_closure_records ("
            "closure_id INTEGER PRIMARY KEY,"
            "anchor_order_seq INTEGER NOT NULL UNIQUE,"
            "anchor_event_type_id INTEGER NOT NULL,"
            "anchor_event_type TEXT NOT NULL,"
            "anchor_nft TEXT NOT NULL,"
            "anchor_collection_contract TEXT NOT NULL,"
            "start_block_ts INTEGER NOT NULL,"
            "end_block_ts INTEGER NOT NULL,"
            "anchor_nft_pos INTEGER NOT NULL"
            ") WITHOUT ROWID"
        )
        print("[funnelq:base] writing closure descriptors ...", flush=True)
        closure_count = _exec_many(
            conn,
            "INSERT INTO nft_closure_records(closure_id, anchor_order_seq, anchor_event_type_id, anchor_event_type, anchor_nft, anchor_collection_contract, start_block_ts, end_block_ts, anchor_nft_pos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            iter_closure_record_rows(df, type_dict, max_delta=int(args.max_delta)),
        )
        print(f"[funnelq:base] closure_records={closure_count:,}", flush=True)
        if args.omit_closure_payload:
            closure_payload_count = 0
        else:
            conn.execute(
                "CREATE TABLE nft_closure_payload ("
                "closure_id INTEGER NOT NULL,"
                "member_order_seq INTEGER NOT NULL,"
                "PRIMARY KEY(closure_id, member_order_seq)"
                ") WITHOUT ROWID"
            )
            closure_payload_count = _exec_many(conn, "INSERT INTO nft_closure_payload(closure_id, member_order_seq) VALUES (?, ?)", iter_same_nft_closure_payload_rows(df, max_delta=int(args.max_delta)))

        anchor_count = 0
        collection_anchor_count = 0
        if not args.compact:
            conn.execute(
                "CREATE TABLE nft_anchor_event_type_candidates ("
                "event_type_id INTEGER NOT NULL,"
                "closure_id INTEGER NOT NULL,"
                "PRIMARY KEY(event_type_id, closure_id)"
                ") WITHOUT ROWID"
            )
            anchor_rows = ((int(type_dict[str(et)]), int(order_seq) + 1) for et, order_seq in df[["event_type", "order_seq"]].itertuples(index=False, name=None))
            anchor_count = _exec_many(conn, "INSERT INTO nft_anchor_event_type_candidates(event_type_id, closure_id) VALUES (?, ?)", anchor_rows)
            conn.execute("CREATE TABLE nft_anchor_event_collection_candidates(event_type_id INTEGER NOT NULL, collection_contract TEXT NOT NULL, closure_id INTEGER NOT NULL, PRIMARY KEY(event_type_id, collection_contract, closure_id)) WITHOUT ROWID")
            collection_anchor_count = _exec_many(conn, "INSERT INTO nft_anchor_event_collection_candidates(event_type_id, collection_contract, closure_id) VALUES (?, ?, ?)", ((int(type_dict[str(t)]), str(c), int(o) + 1) for t, c, o in df[["event_type", "collection_contract", "order_seq"]].itertuples(index=False, name=None) if str(c)))

        conn.execute(
            "CREATE TABLE nft_base_counts ("
            "closure_id INTEGER PRIMARY KEY,"
            "anchor_order_seq INTEGER NOT NULL UNIQUE,"
            "nft_trade_count INTEGER NOT NULL,"
            "nft_transfer_count INTEGER NOT NULL,"
            "num_events INTEGER NOT NULL"
            ") WITHOUT ROWID"
        )
        print("[funnelq:base] computing same-NFT base counts ...", flush=True)
        base_count_row_count = _exec_many(
            conn,
            "INSERT INTO nft_base_counts(closure_id, anchor_order_seq, nft_trade_count, nft_transfer_count, num_events) VALUES (?, ?, ?, ?, ?)",
            iter_base_count_rows(df, max_delta=int(args.max_delta)),
        )
        print(f"[funnelq:base] base_count_rows={base_count_row_count:,}", flush=True)
        if not args.compact:
            conn.execute(
                "CREATE TABLE nft_refined_interface_masks ("
                "closure_id INTEGER PRIMARY KEY, anchor_order_seq INTEGER NOT NULL UNIQUE, mask_lo INTEGER NOT NULL, mask_hi INTEGER NOT NULL"
                ") WITHOUT ROWID"
            )
            print("[funnelq:base] computing refined interface masks ...", flush=True)
            refined_interface_row_count = _exec_many(
                conn,
                "INSERT INTO nft_refined_interface_masks(closure_id, anchor_order_seq, mask_lo, mask_hi) VALUES (?, ?, ?, ?)",
                iter_refined_interface_rows(df, max_delta=int(args.max_delta)),
            )
            print(f"[funnelq:base] refined_interface_rows={refined_interface_row_count:,}", flush=True)
        print("[funnelq] building generic role-transition/pathlet substrate ...", flush=True)
        generic_counts = create_generic_hvceq_indexes(
            conn,
            role_value_ids,
            compact=bool(args.compact),
            max_delta=int(args.max_delta),
            pathlet_insert_batch=int(args.pathlet_insert_batch),
            pathlet_progress_every=int(args.pathlet_progress_every),
            persist_pathlets=bool(args.persist_pathlets),
        )


        closure_role_edges_count = 0

        compact_indexes = [
            "CREATE INDEX idx_hv_v14_closure_anchor_order ON nft_closure_records(anchor_order_seq)",
            "CREATE INDEX idx_hv_final_role_edges_order ON nft_role_transition_edges(order_seq)",
            "CREATE INDEX idx_hv_final_role_edges_collection_order_int ON nft_role_transition_edges(collection_id, order_seq, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_src_time_int ON nft_role_transition_edges(src_addr_id, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_dst_time_int ON nft_role_transition_edges(dst_addr_id, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_src_dst_time_int ON nft_role_transition_edges(src_addr_id, dst_addr_id, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_dst_src_time_int ON nft_role_transition_edges(dst_addr_id, src_addr_id, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_nft_time_int ON nft_role_transition_edges(nft_id, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_collection_time_int ON nft_role_transition_edges(collection_id, block_ts)",
            "CREATE INDEX idx_hv_final_closure_summary_filter ON nft_closure_summary(anchor_collection, distinct_addr_count, distinct_nft_count, has_cycle_flag, closure_id)",
            "CREATE INDEX idx_hv_final_closure_summary_cand1 ON nft_closure_summary(collection_id, trade_count, transfer_count, addr_count, nft_count, has_2hop_pathlet, has_cross_asset, has_cycle_like, closure_id)",
        ]
        if not args.compact:
            compact_indexes.extend(
                [
                    "CREATE INDEX idx_hv_final_atom_postings_order ON nft_atom_postings(order_seq)",
                    "CREATE INDEX idx_hv_final_atom_postings_collection_order ON nft_atom_postings(collection_id, order_seq, block_ts)",
                    "CREATE INDEX idx_hv_final_atom_postings_src_collection_order ON nft_atom_postings(src_addr_id, collection_id, order_seq, block_ts)",
                    "CREATE INDEX idx_hv_final_atom_postings_dst_collection_order ON nft_atom_postings(dst_addr_id, collection_id, order_seq, block_ts)",
                ]
            )
        if args.persist_pathlets:
            compact_indexes.extend(
                [
                    "CREATE INDEX idx_hv_final_pathlet_closure_len ON nft_pathlet_index(closure_id, path_len)",
                    "CREATE INDEX idx_hv_final_pathlet_start_end ON nft_pathlet_index(start_addr_id, end_addr_id, path_len, closure_id)",
                    "CREATE INDEX idx_hv_final_pathlet_cycle ON nft_pathlet_index(is_cycle_flag, is_cross_asset_flag, path_len, closure_id)",
                    "CREATE INDEX idx_hv_final_pathlet_len_collection_window ON nft_pathlet_index(path_len, collection_id, closure_id, end_ts)",
                    "CREATE INDEX idx_hv_final_pathlet_len_collection_start_window ON nft_pathlet_index(path_len, collection_id, start_addr_id, closure_id, end_ts)",
                ]
            )
        if not args.omit_closure_payload:
            compact_indexes.insert(1, "CREATE INDEX idx_hv_v14_closure_payload_member ON nft_closure_payload(member_order_seq, closure_id)")
        wide_indexes = [


            "CREATE INDEX idx_hv_v12_events_type_order ON nft_events(event_type, order_seq, block_ts)",
            "CREATE INDEX idx_hv_v14_events_type_collection_order ON nft_events(event_type, collection_contract, order_seq, block_ts)",
            "CREATE INDEX idx_hv_v12_events_type_nft_order ON nft_events(event_type, nft, order_seq, block_ts)",
            "CREATE INDEX idx_hv_v12_events_trade_seller_buyer_order ON nft_events(event_type, seller, buyer, order_seq, block_ts)",
            "CREATE INDEX idx_hv_v12_events_trade_buyer_seller_order ON nft_events(event_type, buyer, seller, order_seq, block_ts)",
            "CREATE INDEX idx_hv_v12_events_nft_order ON nft_events(nft, order_seq)",
            "CREATE INDEX idx_hv_v14_events_order_ts ON nft_events(order_seq, block_ts)",
            "CREATE INDEX idx_hv_v14_events_type_ts_order ON nft_events(event_type, block_ts, order_seq)",
            "CREATE INDEX idx_hv_v14_closure_anchor_order ON nft_closure_records(anchor_order_seq)",
            "CREATE INDEX idx_hv_final_role_edges_src_time ON nft_role_transition_edges(src_addr, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_dst_time ON nft_role_transition_edges(dst_addr, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_src_dst_time ON nft_role_transition_edges(src_addr, dst_addr, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_dst_src_time ON nft_role_transition_edges(dst_addr, src_addr, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_nft_time ON nft_role_transition_edges(nft, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_collection_time ON nft_role_transition_edges(collection, block_ts)",
            "CREATE INDEX idx_hv_final_role_edges_order ON nft_role_transition_edges(order_seq)",
            "CREATE INDEX idx_hv_final_closure_summary_filter ON nft_closure_summary(anchor_collection, distinct_addr_count, distinct_nft_count, has_cycle_flag, closure_id)",
            "CREATE INDEX idx_hv_final_closure_summary_cand1 ON nft_closure_summary(collection_id, trade_count, transfer_count, addr_count, nft_count, has_2hop_pathlet, has_cross_asset, has_cycle_like, closure_id)",
            "CREATE INDEX idx_hv_final_pathlet_closure_len ON nft_pathlet_index(closure_id, path_len)",
            "CREATE INDEX idx_hv_final_pathlet_start_end ON nft_pathlet_index(start_addr_id, end_addr_id, path_len, closure_id)",
            "CREATE INDEX idx_hv_final_pathlet_cycle ON nft_pathlet_index(is_cycle_flag, is_cross_asset_flag, path_len, closure_id)",
            "CREATE INDEX idx_hv_final_pathlet_len_collection_window ON nft_pathlet_index(path_len, collection_id, closure_id, end_ts)",
            "CREATE INDEX idx_hv_final_pathlet_len_collection_start_window ON nft_pathlet_index(path_len, collection_id, start_addr_id, closure_id, end_ts)",


            "CREATE INDEX idx_hv_v13_events_type_nft_seller_buyer_order ON nft_events(event_type, nft, seller, buyer, order_seq, block_ts)",
            "CREATE INDEX idx_hv_v13_events_type_nft_buyer_seller_order ON nft_events(event_type, nft, buyer, seller, order_seq, block_ts)",
            "CREATE INDEX idx_hv_v13_events_type_seller_buyer_nft_order ON nft_events(event_type, seller, buyer, nft, order_seq, block_ts)",
            "CREATE INDEX idx_hv_v13_events_type_nft_from_to_order ON nft_events(event_type, nft, from_address, to_address, order_seq, block_ts)",
        ]
        if not args.omit_closure_payload:
            wide_indexes.insert(9, "CREATE INDEX idx_hv_v14_closure_payload_member ON nft_closure_payload(member_order_seq, closure_id)")
        indexes = compact_indexes if args.compact else wide_indexes
        for stmt in indexes:
            conn.execute(stmt)
        print("[funnelq:index] creating final access indexes ...", flush=True)
        conn.execute("ANALYZE")
        conn.commit()
        conn.execute("PRAGMA optimize")
        if args.skip_vacuum:
            print("[funnelq:index] final VACUUM skipped by request", flush=True)
        else:
            print("[funnelq:index] running final VACUUM ...", flush=True)
            conn.execute("VACUUM")
        conn.commit()
    t3 = time.perf_counter()

    report = {
        "system": "HVCEQ",
        "storage_layout": (
            "paper_aligned_v20_compact_querylocal_closure_arrays_queryspec_hvceq_store"
            if args.build_closure_local
            else ("paper_aligned_v19_compact_nopayload_generic_closure_pathlet_nft_episode_hvceq_store" if args.omit_closure_payload else ("paper_aligned_v18_compact_generic_closure_pathlet_nft_episode_hvceq_store" if args.compact else "paper_aligned_v17_generic_closure_pathlet_nft_episode_hvceq_store"))
        ),
        "compact": bool(args.compact),
        "omit_closure_payload": bool(args.omit_closure_payload),
        "build_closure_local": bool(args.build_closure_local),
        "persist_pathlets": bool(args.persist_pathlets),
        "pathlet_builder": "collection_stream_v2",
        "pathlet_insert_batch": int(args.pathlet_insert_batch),
        "pathlet_progress_every": int(args.pathlet_progress_every),
        "skip_vacuum": bool(args.skip_vacuum),
        "event_atoms_csv": str(Path(args.event_atoms_csv)),
        "sqlite_out": str(db),
        "event_rows": int(len(df)),
        "closure_record_rows": int(closure_count),
        "closure_payload_rows": int(closure_payload_count),
        "anchor_event_type_candidate_rows": int(anchor_count),
        "anchor_event_collection_candidate_rows": int(collection_anchor_count),
        "anchor_stat_rows": int(len(anchor_stats)),
        "role_value_rows": int(role_value_row_count),
        "role_posting_rows": int(posting_count),
        "base_count_rows": int(base_count_row_count),
        "refined_interface_rows": int(refined_interface_row_count),
        "nft_role_transition_edges_rows": int(generic_counts["nft_role_transition_edges_rows"]),
        "nft_closure_summary_rows": int(generic_counts["nft_closure_summary_rows"]),
        "nft_pathlet_index_rows": int(generic_counts["nft_pathlet_index_rows"]),
        "nft_atom_postings_rows": int(generic_counts.get("nft_atom_postings_rows", 0)),
        "nft_closure_role_edges_rows": int(closure_role_edges_count),
        "omitted_legacy_tables": ["nft_anchor_postings", "nft_episode_synopsis", "nft_base_feature_postings"],
        "sqlite_size_bytes": int(db.stat().st_size),
        "hvceq_build_time_s": t3 - t0,
        "hvceq_sidecar_size_mb": int(db.stat().st_size) / (1024 * 1024),
        "timing_s": {
            "load_event_atoms_s": t1 - t0,
            "build_closure_and_synopsis_s": t2 - t1,
            "write_sqlite_and_indexes_s": t3 - t2,
            "total_s": t3 - t0,
        },
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
