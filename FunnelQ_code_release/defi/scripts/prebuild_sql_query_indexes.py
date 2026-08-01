#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAME_TX_HELPER = PROJECT_ROOT / "systems/sql/same_tx/query_same_tx_summary_reuse_indexes.py"
EPISODE_HELPER = (
    PROJECT_ROOT / "systems/sql/episode/query_episode_summary_reuse_indexes.py"
)

SAME_TX_QUERIES = [
    "flashloan_swap_borrow",
    "flashloan_swap_borrow_followup",
    "flashloan_2swaps_distinct_followup",
    "flashloan_3swaps_chain_reuse",
    "flashloan_3swaps_distinct_parallel",
]
EPISODE_QUERIES = [
    "flashloan_swap_borrow_episode_v1",
    "flashloan_swap_borrow_followup_episode_v1",
    "flashloan_2swaps_distinct_followup_episode_v1",
    "flashloan_3swaps_chain_reuse_episode_v1",
    "flashloan_3swaps_distinct_parallel_episode_v1",
]


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module: {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def query_names_for_mode(mode: str) -> List[str]:
    return SAME_TX_QUERIES if mode == "same_tx" else EPISODE_QUERIES


def query_path_for_mode(root: Path, mode: str, qname: str) -> Path:
    if mode == "same_tx":
        return root / "queries/same_tx" / f"{qname}.json"
    return root / "queries/episode/hvceq_sql" / f"{qname}.json"


def ddl_for_query(helper: Any, mode: str, query_json: Path) -> Tuple[List[str], List[str], Dict[str, Any]]:
    query = helper.load_query(str(query_json))
    helper.validate_query(query)
    specs = helper.choose_event_order_query_order(query)
    ddl, alias_to_index, index_names = helper.build_index_signatures(query, specs)
    return ddl, index_names, {
        "query_name": query.get("query_name"),
        "query_json": str(query_json),
        "index_mode": "generic",
        "index_names": index_names,
        "ddl": ddl,
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Prebuild fixed generic SQL indexes once for the two-week fair-cost runs.")
    ap.add_argument("--db", required=True)
    ap.add_argument("--mode", required=True, choices=["same_tx", "episode"])
    ap.add_argument("--report-json", required=True)
    ap.add_argument("--queries", default="")
    ap.add_argument("--project-root", default=str(PROJECT_ROOT))
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.project_root).resolve()
    helper_path = SAME_TX_HELPER if args.mode == "same_tx" else EPISODE_HELPER
    helper = load_module(helper_path, f"{args.mode}_sql_prebuild_helper")
    qnames = [x.strip() for x in args.queries.split(",") if x.strip()] or query_names_for_mode(args.mode)

    all_ddl: List[str] = []
    all_index_names: List[str] = []
    per_query: List[Dict[str, Any]] = []
    for qname in qnames:
        ddl, index_names, row = ddl_for_query(helper, args.mode, query_path_for_mode(root, args.mode, qname))
        all_ddl.extend(ddl)
        all_index_names.extend(index_names)
        per_query.append(row)

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db)
    try:
        helper.configure_pragmas(conn, "MEMORY", 200)
        t0 = time.perf_counter()
        stale_query_specific = [
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND (name LIKE 'idx_q_%' OR name LIKE 'idx_qae_%')"
            ).fetchall()
        ]
        helper.drop_indexes(conn, sorted(set(all_index_names) | set(stale_query_specific)))
        t_drop = time.perf_counter()
        helper.create_indexes(conn, all_ddl)
        t_create = time.perf_counter()
        conn.execute("ANALYZE")
        conn.commit()
        t_analyze = time.perf_counter()
    finally:
        conn.close()

    report = {
        "mode": args.mode,
        "db": str(args.db),
        "helper": str(helper_path),
        "queries": qnames,
        "index_mode": "generic",
        "num_index_statements": len(set(all_ddl)),
        "num_unique_index_names": len(set(all_index_names)),
        "dropped_stale_query_specific_indexes": stale_query_specific,
        "timing_s": {
            "drop_indexes_s": t_drop - t0,
            "create_indexes_s": t_create - t_drop,
            "analyze_s": t_analyze - t_create,
            "total_s": t_analyze - t0,
        },
        "per_query": per_query,
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

