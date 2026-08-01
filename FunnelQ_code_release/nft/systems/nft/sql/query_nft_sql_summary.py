#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from systems.nft.common.nft_semantics import configure_sqlite, load_query, materialize_sql_instances


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run generic NFT SQL instance-materialization query.")
    ap.add_argument("--db", required=True)
    ap.add_argument("--query-json", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--reuse-prebuilt-indexes", action="store_true")
    ap.add_argument("--sample-limit", type=int, default=0)
    ap.add_argument("--write-bindings", action="store_true", help="Kept for compatibility; instances are materialized by default.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    query = load_query(Path(args.query_json))
    runs = max(1, int(args.runs))
    times = []
    last = {}
    with sqlite3.connect(args.db) as conn:
        conn.row_factory = sqlite3.Row
        configure_sqlite(conn)
        for i in range(runs):
            csv_path = outdir / ("matched_instances.csv" if runs == 1 else f"matched_instances_run_{i+1:02d}.csv")
            last = materialize_sql_instances(conn, query, "nft_events", csv_path)
            times.append(float(last["materialize_s"]))
    report = {
        "query_name": query["query_name"],
        "scope": "nft_episode",
        "system": "Generic SQL",
        "baseline_variant": "generic_json_to_sql_instance_materialization",
        "storage_layout": "nft_relational_store",
        "rebuild_query_indexes_each_run": False,
        "reuse_prebuilt_indexes": True,
        "full_result_materialized": True,
        "matched_instances_csv_written": True,
        "summary_only": False,
        "matched_instances_csv": last.get("matched_instances_csv", ""),
        "sample_limit": int(args.sample_limit),
        "sample_rows": [],
        "delta_seconds": int(query.get("delta_seconds", 86400)),
        "num_matches": int(last.get("num_matches", 0)),
        "num_distinct_nfts": int(last.get("num_distinct_nfts", 0)),
        "num_patterns": int(last.get("num_patterns", 0)),
        "timing_s": {
            "mean_index_build_s": 0.0,
            "mean_main_query_s": statistics.mean(times),
            "median_main_query_s": statistics.median(times),
            "mean_total_s": statistics.mean(times),
            "median_total_s": statistics.median(times),
            "all_main_query_runs_s": times,
            "all_total_runs_s": times,
        },
    }
    (outdir / "execution_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
