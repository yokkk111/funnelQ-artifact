from __future__ import annotations

import json
import sqlite3
import time
from bisect import bisect_left, bisect_right
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from .memory import peak_rss_mb
from .protocol import INF_DELTA, file_sha256

BUILDER_VERSION = "generic_descriptor_builder_v3"
VOCABULARY = "generic_role_nft_relation_min_delta_v1"


def _metrics_path(path: Path) -> Path:
    return Path(path).with_suffix(".metrics.json")


def _descriptor_schema() -> str:
    common = """
      closure_id INTEGER NOT NULL UNIQUE,
      anchor_order_seq INTEGER NOT NULL UNIQUE,
      start_ts INTEGER NOT NULL,end_ts INTEGER NOT NULL,end_order_seq INTEGER NOT NULL,
      collection_id INTEGER NOT NULL,
      transition_count INTEGER NOT NULL,addr_count INTEGER NOT NULL,nft_count INTEGER NOT NULL,
      nft_ge2_count INTEGER NOT NULL,b_out_event_count INTEGER NOT NULL,
      root_event_type_id INTEGER NOT NULL,
      l2_ba_nft_equal_min_delta INTEGER NOT NULL,
      l2_ba_nft_different_min_delta INTEGER NOT NULL,
      l3_bcca_all_equal_min_delta INTEGER NOT NULL,
      l3_bcca_all_distinct_min_delta INTEGER NOT NULL,
      l3_bcca_root_back_equal_min_delta INTEGER NOT NULL
    """
    return f"""
    PRAGMA journal_mode=OFF;
    PRAGMA synchronous=OFF;
    CREATE TABLE formal_closure_descriptor(
      closure_position INTEGER PRIMARY KEY,
      is_anchor INTEGER NOT NULL CHECK(is_anchor IN (0,1)),
      {common}
    );
    CREATE TABLE formal_anchor_descriptor(
      position INTEGER PRIMARY KEY,
      {common}
    );
    CREATE TABLE descriptor_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
    """


def build_descriptors_python(
    source_db: Path,
    out_db: Path,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Portable reference builder for the complete closure and Anchor domains.

    The three-month path should use the bundled Rust builder.  This implementation
    is intentionally straightforward and is used by the automated demo tests and
    by environments without Cargo.
    """
    started = time.perf_counter()
    source_db, out_db = Path(source_db), Path(out_db)
    if out_db.exists():
        if not force:
            raise FileExistsError(f"{out_db} exists; pass force=True")
        out_db.unlink()
    out_db.parent.mkdir(parents=True, exist_ok=True)
    source_hash = file_sha256(source_db)
    source = sqlite3.connect(f"file:{source_db.as_posix()}?mode=ro", uri=True)
    trade_row = source.execute(
        "SELECT event_type_id FROM nft_event_type_dict WHERE event_type='NFTTrade'"
    ).fetchone()
    if trade_row is None:
        raise ValueError("source database has no NFTTrade event type")
    trade = int(trade_row[0])

    edges: list[tuple[int, int, int, int, int, int, int]] = []
    order_to_index: dict[int, int] = {}
    by_src: dict[tuple[int, int], list[int]] = defaultdict(list)
    by_src_orders: dict[tuple[int, int], list[int]] = defaultdict(list)
    for row in source.execute(
        "SELECT order_seq,event_type_id,block_ts,src_addr_id,dst_addr_id,nft_id,collection_id "
        "FROM nft_role_transition_edges ORDER BY collection_id,order_seq"
    ):
        edge = tuple(int(value) for value in row)
        index = len(edges)
        if edge[0] in order_to_index:
            raise ValueError("duplicate order_seq in compact event stream")
        order_to_index[edge[0]] = index
        key = (edge[6], edge[3])
        by_src[key].append(index)
        by_src_orders[key].append(edge[0])
        edges.append(edge)

    anchor_ids = {
        int(row[0]) for row in source.execute("SELECT closure_id FROM nft_anchor_directory")
    }
    output = sqlite3.connect(out_db)
    output.executescript(_descriptor_schema())

    def source_range(collection: int, src: int, lo: int, hi: int) -> Iterable[tuple[int, ...]]:
        key = (collection, src)
        orders = by_src_orders.get(key, [])
        posting = by_src.get(key, [])
        left = bisect_left(orders, lo)
        right = bisect_right(orders, hi)
        for index in posting[left:right]:
            yield edges[index]

    rows = source.execute(
        "SELECT closure_id,anchor_order_seq,start_ts,end_ts,end_order_seq,"
        "collection_id,transition_count,addr_count,nft_count,nft_ge2_count,"
        "b_out_event_count FROM nft_closure_summary "
        "ORDER BY anchor_order_seq,closure_id"
    )
    closure_insert = output.cursor()
    anchor_insert = output.cursor()
    closure_count = 0
    anchor_count = 0
    for summary in rows:
        (
            closure_id,
            anchor_order,
            start_ts,
            end_ts,
            end_order,
            collection,
            transition_count,
            addr_count,
            nft_count,
            nft_ge2_count,
            b_out_event_count,
        ) = (int(value) for value in summary)
        root = edges[order_to_index[anchor_order]] if anchor_order in order_to_index else None
        signatures = [INF_DELTA] * 5
        root_type = 0 if root is None else root[1]
        if (
            root is not None
            and root[6] == collection
            and root[2] == start_ts
            and root[1] == trade
        ):
            _, _, root_ts, root_src, root_dst, root_nft, _ = root
            for e1 in source_range(collection, root_dst, anchor_order, end_order):
                _, e1_type, e1_ts, _e1_src, e1_dst, e1_nft, _ = e1
                if e1_type != trade or e1_ts <= root_ts or e1_ts > end_ts:
                    continue
                delta1 = e1_ts - root_ts
                if e1_dst == root_src:
                    slot = 0 if e1_nft == root_nft else 1
                    signatures[slot] = min(signatures[slot], delta1)
                    continue
                for e2 in source_range(collection, e1_dst, e1[0], end_order):
                    _, e2_type, e2_ts, _e2_src, e2_dst, e2_nft, _ = e2
                    if (
                        e2_type != trade
                        or e2_ts <= e1_ts
                        or e2_ts > end_ts
                        or e2_dst != root_src
                    ):
                        continue
                    delta2 = e2_ts - root_ts
                    if e1_nft == root_nft and e2_nft == root_nft:
                        signatures[2] = min(signatures[2], delta2)
                    if e1_nft != root_nft and e2_nft != root_nft and e2_nft != e1_nft:
                        signatures[3] = min(signatures[3], delta2)
                    if e1_nft != root_nft and e2_nft == root_nft:
                        signatures[4] = min(signatures[4], delta2)
        common = (
            closure_id,
            anchor_order,
            start_ts,
            end_ts,
            end_order,
            collection,
            transition_count,
            addr_count,
            nft_count,
            nft_ge2_count,
            b_out_event_count,
            root_type,
            *signatures,
        )
        is_anchor = 1 if closure_id in anchor_ids else 0
        closure_insert.execute(
            "INSERT INTO formal_closure_descriptor VALUES("
            + ",".join("?" for _ in range(19))
            + ")",
            (closure_count, is_anchor, *common),
        )
        if is_anchor:
            anchor_insert.execute(
                "INSERT INTO formal_anchor_descriptor VALUES("
                + ",".join("?" for _ in range(18))
                + ")",
                (anchor_count, *common),
            )
            anchor_count += 1
        closure_count += 1

    if anchor_count != len(anchor_ids):
        raise ValueError("Anchor directory contains closure ids absent from closure summaries")
    meta = {
        "complete": "true",
        "descriptor_vocabulary": VOCABULARY,
        "anchor_count": str(anchor_count),
        "closure_count": str(closure_count),
        "source_db_sha256": source_hash,
        "source_db_bytes": str(source_db.stat().st_size),
        "builder_version": BUILDER_VERSION,
    }
    output.executemany("INSERT INTO descriptor_meta VALUES(?,?)", meta.items())
    output.executescript(
        "CREATE INDEX idx_formal_anchor_order ON formal_anchor_descriptor(anchor_order_seq);"
        "CREATE INDEX idx_formal_anchor_collection_order ON formal_anchor_descriptor(collection_id,anchor_order_seq);"
        "CREATE INDEX idx_formal_closure_order ON formal_closure_descriptor(anchor_order_seq);"
        "CREATE INDEX idx_formal_closure_collection_order ON formal_closure_descriptor(collection_id,anchor_order_seq);"
        "ANALYZE;"
    )
    output.commit()
    output.close()
    source.close()
    metrics = {
        "status": "BUILT",
        "builder": "python_reference",
        "builderVersion": BUILDER_VERSION,
        "descriptorBuildSeconds": time.perf_counter() - started,
        "descriptorBytes": out_db.stat().st_size,
        "anchorCount": anchor_count,
        "closureCount": closure_count,
        "sourceDbSha256": source_hash,
        "descriptorPeakRssMiB": peak_rss_mb(),
    }
    _metrics_path(out_db).write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    return metrics
