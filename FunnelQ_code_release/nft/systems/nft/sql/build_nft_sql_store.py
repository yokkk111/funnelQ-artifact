#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from systems.nft.common.nft_semantics import configure_sqlite, load_event_atoms


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build NFT Generic SQL relational store.")
    ap.add_argument("--event-atoms-csv", required=True)
    ap.add_argument("--sqlite-out", required=True)
    ap.add_argument("--report-json", required=True)
    return ap.parse_args()


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
    with sqlite3.connect(out) as conn:
        configure_sqlite(conn)
        df.to_sql("nft_events", conn, index=False, if_exists="replace")
        conn.execute(
            """
            CREATE TABLE nft_role_transition_edges AS
            SELECT order_seq AS edge_id,
                   order_seq,
                   event_id,
                   event_type,
                   block_ts,
                   move_from AS src_addr,
                   move_to AS dst_addr,
                   nft,
                   collection_contract AS collection,
                   tx_hash
            FROM nft_events
            WHERE event_type IN ('NFTTrade','NFTTransfer')
              AND collection_contract<>'' AND nft<>''
              AND move_from<>'' AND move_to<>'' AND move_from<>move_to
              AND lower(move_from)<>lower('0x0000000000000000000000000000000000000000')
              AND lower(move_to)<>lower('0x0000000000000000000000000000000000000000')
            """
        )
        indexes = [
            "CREATE INDEX idx_nft_events_event_type ON nft_events(event_type)",
            "CREATE INDEX idx_nft_events_nft ON nft_events(nft)",
            "CREATE INDEX idx_nft_events_block_ts ON nft_events(block_ts)",
            "CREATE INDEX idx_nft_events_order_seq ON nft_events(order_seq)",
            "CREATE INDEX idx_nft_events_nft_order_seq ON nft_events(nft, order_seq)",
            "CREATE INDEX idx_nft_events_type_nft_order ON nft_events(event_type, nft, order_seq)",
            "CREATE INDEX idx_nft_events_trade_buyer_order ON nft_events(event_type, nft, buyer, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_trade_seller_order ON nft_events(event_type, nft, seller, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_transfer_from_order ON nft_events(event_type, nft, from_address, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_transfer_to_order ON nft_events(event_type, nft, to_address, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_move_pair_order ON nft_events(nft, move_from, move_to, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_move_from_order ON nft_events(nft, move_from, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_move_to_order ON nft_events(nft, move_to, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_move_from_global ON nft_events(event_type, move_from, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_move_to_global ON nft_events(event_type, move_to, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_move_pair_global ON nft_events(event_type, move_from, move_to, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_buyer_global ON nft_events(event_type, buyer, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_seller_global ON nft_events(event_type, seller, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_from_global ON nft_events(event_type, from_address, order_seq, block_ts)",
            "CREATE INDEX idx_nft_events_to_global ON nft_events(event_type, to_address, order_seq, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_type_time ON nft_events(event_type, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_order ON nft_events(order_seq)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_nft_time ON nft_events(nft, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_collection_time ON nft_events(collection_contract, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_seller_time ON nft_events(seller, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_buyer_time ON nft_events(buyer, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_seller_buyer_time ON nft_events(seller, buyer, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_buyer_seller_time ON nft_events(buyer, seller, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_from_time ON nft_events(from_address, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_to_time ON nft_events(to_address, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_from_to_time ON nft_events(from_address, to_address, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_nft_events_to_from_time ON nft_events(to_address, from_address, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_role_edges_src_time ON nft_role_transition_edges(src_addr, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_role_edges_dst_time ON nft_role_transition_edges(dst_addr, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_role_edges_src_dst_time ON nft_role_transition_edges(src_addr, dst_addr, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_role_edges_dst_src_time ON nft_role_transition_edges(dst_addr, src_addr, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_role_edges_nft_time ON nft_role_transition_edges(nft, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_role_edges_collection_time ON nft_role_transition_edges(collection, block_ts)",
            "CREATE INDEX IF NOT EXISTS idx_role_edges_order ON nft_role_transition_edges(order_seq)",
        ]
        for stmt in indexes:
            conn.execute(stmt)
        conn.execute("ANALYZE")
        role_edge_rows = int(conn.execute("SELECT COUNT(*) FROM nft_role_transition_edges").fetchone()[0])
        conn.commit()
    t2 = time.perf_counter()
    report = {
        "system": "Generic SQL",
        "storage_layout": "nft_generic_relational_store",
        "event_atoms_csv": str(Path(args.event_atoms_csv)),
        "sqlite_out": str(out),
        "event_rows": int(len(df)),
        "event_type_counts": {str(k): int(v) for k, v in df["event_type"].value_counts().to_dict().items()},
        "sqlite_size_bytes": int(out.stat().st_size),
        "sql_build_time_s": t2 - t0,
        "sql_index_size_mb": int(out.stat().st_size) / (1024 * 1024),
        "nft_role_transition_edges_rows": int(role_edge_rows),
        "timing_s": {
            "load_event_atoms_s": t1 - t0,
            "write_sqlite_and_indexes_s": t2 - t1,
            "total_s": t2 - t0,
        },
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
