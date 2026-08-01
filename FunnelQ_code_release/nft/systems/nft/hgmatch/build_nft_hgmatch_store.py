#!/usr/bin/env python3
"""Build the persistent integer hypergraph/posting store used by NFT HGMatch."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from systems.nft.common.nft_semantics import configure_sqlite, load_event_atoms

ZERO = "0x0000000000000000000000000000000000000000"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build NFT HGMatch integer hyperedge and vertex-posting store.")
    ap.add_argument("--event-atoms-csv", required=True)
    ap.add_argument("--sqlite-out", required=True)
    ap.add_argument("--report-json", required=True)
    return ap.parse_args()


def factor_dict(values: pd.Series, name: str) -> tuple[pd.Series, pd.DataFrame]:
    codes, uniques = pd.factorize(values.astype(str), sort=True)
    ids = pd.Series(codes + 1, index=values.index, dtype="int64")
    dictionary = pd.DataFrame({f"{name}_id": range(1, len(uniques) + 1), "value_text": uniques.astype(str)})
    return ids, dictionary


def main() -> int:
    args = parse_args()
    out = Path(args.sqlite_out)
    report_path = Path(args.report_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()
    t0 = time.perf_counter()
    df = load_event_atoms(Path(args.event_atoms_csv))
    t1 = time.perf_counter()
    valid = df[
        df["event_type"].isin(["NFTTrade", "NFTTransfer"])
        & df["collection_contract"].ne("")
        & df["nft"].ne("")
        & df["move_from"].ne("")
        & df["move_to"].ne("")
        & df["move_from"].str.lower().ne(ZERO)
        & df["move_to"].str.lower().ne(ZERO)
        & df["move_from"].ne(df["move_to"])
    ].copy()
    address_values = pd.concat([valid["move_from"], valid["move_to"]], ignore_index=True)
    address_codes, address_uniques = pd.factorize(address_values.astype(str), sort=True)
    n = len(valid)
    valid["src_id"] = address_codes[:n] + 1
    valid["dst_id"] = address_codes[n:] + 1
    address_dict = pd.DataFrame({"address_id": range(1, len(address_uniques) + 1), "value_text": address_uniques.astype(str)})
    valid["nft_id"], nft_dict = factor_dict(valid["nft"], "nft")
    valid["collection_id"], collection_dict = factor_dict(valid["collection_contract"], "collection")
    valid["event_type_id"] = valid["event_type"].map({"NFTTrade": 1, "NFTTransfer": 2}).astype("int64")
    valid["edge_id"] = range(1, len(valid) + 1)
    hyperedges = valid[[
        "edge_id", "event_id", "event_type_id", "event_type", "collection_id", "nft_id",
        "src_id", "dst_id", "block_ts", "order_seq", "tx_hash",
    ]].copy()
    t2 = time.perf_counter()
    with sqlite3.connect(out) as conn:
        configure_sqlite(conn)
        # Keep the canonical text table for audit/debug only; the query engine uses
        # the integer hyperedge and posting tables below.
        df.to_sql("hgmatch_events", conn, index=False, if_exists="replace", chunksize=50000)
        address_dict.to_sql("hg_address_dict", conn, index=False, if_exists="replace", chunksize=50000)
        nft_dict.to_sql("hg_nft_dict", conn, index=False, if_exists="replace", chunksize=50000)
        collection_dict.to_sql("hg_collection_dict", conn, index=False, if_exists="replace", chunksize=50000)
        pd.DataFrame({"event_type_id": [1, 2], "value_text": ["NFTTrade", "NFTTransfer"]}).to_sql(
            "hg_event_type_dict", conn, index=False, if_exists="replace"
        )
        hyperedges.to_sql("hg_hyperedges", conn, index=False, if_exists="replace", chunksize=50000)
        hyperedges[["src_id", "edge_id", "order_seq"]].to_sql("hg_out_postings", conn, index=False, if_exists="replace", chunksize=50000)
        hyperedges[["dst_id", "edge_id", "order_seq"]].to_sql("hg_in_postings", conn, index=False, if_exists="replace", chunksize=50000)
        hyperedges[["nft_id", "edge_id", "order_seq"]].to_sql("hg_nft_postings", conn, index=False, if_exists="replace", chunksize=50000)
        hyperedges[["collection_id", "edge_id", "order_seq", "block_ts"]].to_sql("hg_collection_postings", conn, index=False, if_exists="replace", chunksize=50000)
        conn.executescript(
            """
            CREATE UNIQUE INDEX idx_hg_edge_id ON hg_hyperedges(edge_id);
            CREATE UNIQUE INDEX idx_hg_edge_order ON hg_hyperedges(order_seq);
            CREATE INDEX idx_hg_edge_type_order ON hg_hyperedges(event_type_id, order_seq);
            CREATE INDEX idx_hg_edge_collection_order ON hg_hyperedges(collection_id, order_seq, block_ts);
            CREATE INDEX idx_hg_edge_src_order ON hg_hyperedges(src_id, order_seq, block_ts);
            CREATE INDEX idx_hg_edge_dst_order ON hg_hyperedges(dst_id, order_seq, block_ts);
            CREATE INDEX idx_hg_edge_nft_order ON hg_hyperedges(nft_id, order_seq, block_ts);
            CREATE INDEX idx_hg_out_vertex ON hg_out_postings(src_id, order_seq, edge_id);
            CREATE INDEX idx_hg_in_vertex ON hg_in_postings(dst_id, order_seq, edge_id);
            CREATE INDEX idx_hg_nft_vertex ON hg_nft_postings(nft_id, order_seq, edge_id);
            CREATE INDEX idx_hg_collection_vertex ON hg_collection_postings(collection_id, order_seq, block_ts, edge_id);
            CREATE UNIQUE INDEX idx_hg_addr_value ON hg_address_dict(value_text);
            CREATE UNIQUE INDEX idx_hg_nft_value ON hg_nft_dict(value_text);
            CREATE UNIQUE INDEX idx_hg_collection_value ON hg_collection_dict(value_text);
            ANALYZE;
            """
        )
        conn.commit()
    t3 = time.perf_counter()
    report = {
        "system": "HGMatch",
        "storage_layout": "integer_hyperedges_plus_role_vertex_postings_v2",
        "event_atoms_csv": str(Path(args.event_atoms_csv)),
        "sqlite_out": str(out),
        "canonical_event_rows": int(len(df)),
        "hyperedge_rows": int(len(hyperedges)),
        "posting_rows": int(len(hyperedges) * 4),
        "sqlite_size_bytes": int(out.stat().st_size),
        "threads_supported": True,
        "timing_s": {
            "load_event_atoms_s": t1 - t0,
            "integer_projection_s": t2 - t1,
            "write_hypergraph_and_postings_s": t3 - t2,
            "total_s": t3 - t0,
        },
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
