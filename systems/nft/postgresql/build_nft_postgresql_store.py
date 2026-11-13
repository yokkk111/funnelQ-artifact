#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

import psycopg2

from systems.nft.common.nft_semantics import RUNTIME_COLUMNS, load_event_atoms


def connection_args(args: argparse.Namespace) -> dict[str, object]:
    return {
        "host": args.host,
        "port": args.port,
        "dbname": args.database,
        "user": args.user,
        "password": args.password,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the NFT PostgreSQL relational baseline.")
    ap.add_argument("--event-atoms-csv", required=True, type=Path)
    ap.add_argument("--report-json", required=True, type=Path)
    ap.add_argument("--host", default=os.getenv("PGHOST", "postgres"))
    ap.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")))
    ap.add_argument("--database", default=os.getenv("PGDATABASE", "funnelq"))
    ap.add_argument("--user", default=os.getenv("PGUSER", "funnelq"))
    ap.add_argument("--password", default=os.getenv("PGPASSWORD", "funnelq"))
    args = ap.parse_args()

    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    wall0 = time.perf_counter()
    dataframe = load_event_atoms(args.event_atoms_csv)
    loaded_at = time.perf_counter()

    text_columns = [column for column in RUNTIME_COLUMNS if column not in {"block_ts", "order_seq", "nft_pos"}]
    definitions = [f'"{column}" TEXT NOT NULL' for column in text_columns]
    definitions += ['"block_ts" BIGINT NOT NULL', '"order_seq" BIGINT NOT NULL', '"nft_pos" BIGINT NOT NULL']

    conn = psycopg2.connect(**connection_args(args))
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS nft_role_transition_edges")
            cur.execute("DROP TABLE IF EXISTS nft_events")
            cur.execute(f"CREATE TABLE nft_events ({', '.join(definitions)})")
        ordered = dataframe[RUNTIME_COLUMNS]
        with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", suffix=".csv", delete=False) as tmp:
            temp_path = Path(tmp.name)
            ordered.to_csv(tmp, index=False)
        try:
            columns_sql = ", ".join(f'"{column}"' for column in RUNTIME_COLUMNS)
            force_not_null = ", ".join(f'"{column}"' for column in text_columns)
            with conn.cursor() as cur, temp_path.open("r", newline="", encoding="utf-8") as src:
                cur.copy_expert(
                    f"COPY nft_events ({columns_sql}) FROM STDIN WITH "
                    f"(FORMAT CSV, HEADER TRUE, NULL '\\N', FORCE_NOT_NULL ({force_not_null}))",
                    src,
                )
        finally:
            temp_path.unlink(missing_ok=True)

        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE nft_role_transition_edges AS
                SELECT order_seq AS edge_id, order_seq, event_id, event_type,
                       block_ts, move_from AS src_addr, move_to AS dst_addr,
                       nft, collection_contract AS collection, tx_hash
                FROM nft_events
                WHERE event_type IN ('NFTTrade','NFTTransfer')
                  AND collection_contract<>'' AND nft<>''
                  AND move_from<>'' AND move_to<>'' AND move_from<>move_to
                  AND lower(move_from)<>lower('0x0000000000000000000000000000000000000000')
                  AND lower(move_to)<>lower('0x0000000000000000000000000000000000000000')
                """
            )
            indexes = [
                "CREATE INDEX idx_pg_event_type ON nft_events(event_type)",
                "CREATE INDEX idx_pg_nft ON nft_events(nft)",
                "CREATE INDEX idx_pg_block_ts ON nft_events(block_ts)",
                "CREATE INDEX idx_pg_order_seq ON nft_events(order_seq)",
                "CREATE INDEX idx_pg_nft_order ON nft_events(nft, order_seq)",
                "CREATE INDEX idx_pg_type_nft_order ON nft_events(event_type, nft, order_seq)",
                "CREATE INDEX idx_pg_trade_buyer_order ON nft_events(event_type, nft, buyer, order_seq, block_ts)",
                "CREATE INDEX idx_pg_trade_seller_order ON nft_events(event_type, nft, seller, order_seq, block_ts)",
                "CREATE INDEX idx_pg_transfer_from_order ON nft_events(event_type, nft, from_address, order_seq, block_ts)",
                "CREATE INDEX idx_pg_transfer_to_order ON nft_events(event_type, nft, to_address, order_seq, block_ts)",
                "CREATE INDEX idx_pg_move_pair_order ON nft_events(nft, move_from, move_to, order_seq, block_ts)",
                "CREATE INDEX idx_pg_type_nft_move_pair_order ON nft_events(event_type, nft, move_from, move_to, order_seq, block_ts) INCLUDE (event_id, tx_hash, collection_contract)",
                "CREATE INDEX idx_pg_move_from_order ON nft_events(nft, move_from, order_seq, block_ts)",
                "CREATE INDEX idx_pg_move_to_order ON nft_events(nft, move_to, order_seq, block_ts)",
                "CREATE INDEX idx_pg_move_from_global ON nft_events(event_type, move_from, order_seq, block_ts)",
                "CREATE INDEX idx_pg_move_to_global ON nft_events(event_type, move_to, order_seq, block_ts)",
                "CREATE INDEX idx_pg_type_nft_move_from_order ON nft_events(event_type, nft, move_from, order_seq, block_ts) INCLUDE (move_to, event_id, tx_hash, collection_contract)",
                "CREATE INDEX idx_pg_type_nft_move_to_order ON nft_events(event_type, nft, move_to, order_seq, block_ts) INCLUDE (move_from, event_id, tx_hash, collection_contract)",
                "CREATE INDEX idx_pg_type_move_from_order ON nft_events(event_type, move_from, order_seq, block_ts) INCLUDE (move_to, nft, event_id, tx_hash, collection_contract)",
                "CREATE INDEX idx_pg_type_move_to_order ON nft_events(event_type, move_to, order_seq, block_ts) INCLUDE (move_from, nft, event_id, tx_hash, collection_contract)",
                "CREATE INDEX idx_pg_move_pair_global ON nft_events(event_type, move_from, move_to, order_seq, block_ts)",
                "CREATE INDEX idx_pg_type_move_pair_order ON nft_events(event_type, move_from, move_to, order_seq, block_ts) INCLUDE (nft, event_id, tx_hash, collection_contract)",
                "CREATE INDEX idx_pg_buyer_global ON nft_events(event_type, buyer, order_seq, block_ts)",
                "CREATE INDEX idx_pg_seller_global ON nft_events(event_type, seller, order_seq, block_ts)",
                "CREATE INDEX idx_pg_from_global ON nft_events(event_type, from_address, order_seq, block_ts)",
                "CREATE INDEX idx_pg_to_global ON nft_events(event_type, to_address, order_seq, block_ts)",
                "CREATE INDEX idx_pg_collection_time ON nft_events(collection_contract, block_ts)",
                "CREATE INDEX idx_pg_seller_buyer_time ON nft_events(seller, buyer, block_ts)",
                "CREATE INDEX idx_pg_buyer_seller_time ON nft_events(buyer, seller, block_ts)",
                "CREATE INDEX idx_pg_from_to_time ON nft_events(from_address, to_address, block_ts)",
                "CREATE INDEX idx_pg_to_from_time ON nft_events(to_address, from_address, block_ts)",
                "CREATE INDEX idx_pg_role_src_time ON nft_role_transition_edges(src_addr, block_ts)",
                "CREATE INDEX idx_pg_role_dst_time ON nft_role_transition_edges(dst_addr, block_ts)",
                "CREATE INDEX idx_pg_role_src_dst_time ON nft_role_transition_edges(src_addr, dst_addr, block_ts)",
                "CREATE INDEX idx_pg_role_dst_src_time ON nft_role_transition_edges(dst_addr, src_addr, block_ts)",
                "CREATE INDEX idx_pg_role_nft_time ON nft_role_transition_edges(nft, block_ts)",
                "CREATE INDEX idx_pg_role_collection_time ON nft_role_transition_edges(collection, block_ts)",
                "CREATE INDEX idx_pg_role_order ON nft_role_transition_edges(order_seq)",
            ]
            for statement in indexes:
                cur.execute(statement)
            cur.execute("ANALYZE nft_events")
            cur.execute("ANALYZE nft_role_transition_edges")
            cur.execute("SELECT COUNT(*) FROM nft_role_transition_edges")
            role_rows = int(cur.fetchone()[0])
        conn.commit()
        built_at = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            database_size = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)),0) "
                "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='public' AND c.relkind IN ('r','m')"
            )
            relation_size = int(cur.fetchone()[0])
    finally:
        conn.close()

    report = {
        "system": "PostgreSQL",
        "engine": "PostgreSQL relational full-instance baseline",
        "event_atoms_csv": str(args.event_atoms_csv),
        "event_rows": int(len(dataframe)),
        "event_type_counts": {str(k): int(v) for k, v in dataframe["event_type"].value_counts().to_dict().items()},
        "nft_role_transition_edges_rows": role_rows,
        "database_size_bytes": database_size,
        "public_relations_size_bytes": relation_size,
        "timing_s": {
            "load_event_atoms_s": loaded_at - wall0,
            "copy_and_index_s": built_at - loaded_at,
            "total_s": built_at - wall0,
        },
    }
    args.report_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
