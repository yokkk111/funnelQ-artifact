#!/usr/bin/env python3


from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path


def rss_mb() -> float:
    try:
        import psutil

        return float(psutil.Process(os.getpid()).memory_info().rss) / (1024.0 * 1024.0)
    except Exception:
        return 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description="Build generic NFT anchor directory.")
    ap.add_argument("--db", required=True)
    ap.add_argument("--max-rss-mb", type=float, default=18000.0)
    args = ap.parse_args()

    db = Path(args.db)
    t0 = time.perf_counter()
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA temp_store=FILE")
        conn.execute("PRAGMA cache_size=-65536")
        conn.execute("DROP TABLE IF EXISTS nft_anchor_directory")
        conn.execute("DROP TABLE IF EXISTS nft_anchor_signature_stats")
        conn.execute(
            """
            CREATE TABLE nft_anchor_directory (
                anchor_sig_id INTEGER NOT NULL,
                closure_id INTEGER NOT NULL,
                root_order_seq INTEGER NOT NULL,
                support_count INTEGER NOT NULL,
                PRIMARY KEY(anchor_sig_id, closure_id, root_order_seq)
            ) WITHOUT ROWID
            """
        )

        conn.execute(
            """
            INSERT INTO nft_anchor_directory(anchor_sig_id, closure_id, root_order_seq, support_count)
            SELECT 1, closure_id, root_order_seq, 1
            FROM nft_root_anchor_postings
            """
        )


        conn.execute(
            """
            INSERT INTO nft_anchor_directory(anchor_sig_id, closure_id, root_order_seq, support_count)
            SELECT 2, r.closure_id, r.root_order_seq, a.c2_out_dst_count
            FROM nft_root_anchor_postings r
            JOIN nft_anchor_role_aggregate_synopsis a USING(closure_id)
            WHERE a.c2_out_dst_count>0
            """
        )
        if rss_mb() > args.max_rss_mb:
            raise MemoryError(f"RSS exceeded after sig2: {rss_mb():.1f} MB")


        conn.execute(
            """
            INSERT INTO nft_anchor_directory(anchor_sig_id, closure_id, root_order_seq, support_count)
            SELECT 3, r.closure_id, r.root_order_seq,
                   p.b_to_a_nonanchor_pathlet2_count
            FROM nft_root_anchor_postings r
            JOIN nft_root_anchor_refined_synopsis p USING(closure_id)
            WHERE p.b_to_a_nonanchor_pathlet2_count>0
            """
        )
        if rss_mb() > args.max_rss_mb:
            raise MemoryError(f"RSS exceeded after sig3: {rss_mb():.1f} MB")
        conn.execute("CREATE INDEX idx_hv_anchor_directory_closure ON nft_anchor_directory(closure_id, anchor_sig_id)")
        conn.execute("CREATE INDEX idx_hv_anchor_directory_sig_support ON nft_anchor_directory(anchor_sig_id, support_count)")
        conn.execute(
            """
            CREATE TABLE nft_anchor_signature_stats AS
            SELECT anchor_sig_id,
                   COUNT(*) AS closure_hit_count,
                   SUM(support_count) AS total_support,
                   AVG(support_count) AS avg_support,
                   CASE anchor_sig_id
                     WHEN 1 THEN 1
                     WHEN 2 THEN 2
                     WHEN 3 THEN 3
                     ELSE 0
                   END AS bind_strength
            FROM nft_anchor_directory
            GROUP BY anchor_sig_id
            """
        )
        conn.execute("CREATE UNIQUE INDEX idx_hv_anchor_signature_stats_sig ON nft_anchor_signature_stats(anchor_sig_id)")
        rows = int(conn.execute("SELECT COUNT(*) FROM nft_anchor_directory").fetchone()[0])
        stats = list(conn.execute("SELECT anchor_sig_id, closure_hit_count, total_support FROM nft_anchor_signature_stats ORDER BY anchor_sig_id"))
        conn.commit()
    print(f"[anchor-directory] rows={rows} stats={stats} elapsed_s={time.perf_counter()-t0:.3f} rss_mb={rss_mb():.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
