#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

EXPECTED_LAYOUT = "compact_full_binding_v4_defi_runtime"
REQUIRED = {
    "nft_build_meta",
    "nft_event_type_dict",
    "nft_role_value_dict",
    "nft_role_transition_edges",
    "nft_closure_summary",
}
FORBIDDEN = {
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


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate compact NFT FunnelQ v4 store.")
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--out-json", default="", type=Path)
    args = ap.parse_args()
    db = args.db.resolve()
    if not db.is_file():
        raise FileNotFoundError(db)
    with sqlite3.connect(db) as conn:
        tables = {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        missing_required = sorted(REQUIRED - tables)
        forbidden = sorted(FORBIDDEN & tables)
        layout_row = conn.execute("SELECT value FROM nft_build_meta WHERE key='layout'").fetchone()
        layout = json.loads(layout_row[0]) if layout_row else None
        events = int(conn.execute("SELECT COUNT(*) FROM nft_role_transition_edges").fetchone()[0])
        distinct_event_ids = int(conn.execute(
            "SELECT COUNT(DISTINCT event_id) FROM nft_role_transition_edges"
        ).fetchone()[0])
        duplicate_event_id_rows = events - distinct_event_ids
        closures = int(conn.execute("SELECT COUNT(*) FROM nft_closure_summary").fetchone()[0])
        malformed = int(conn.execute(
            "SELECT COUNT(*) FROM nft_closure_summary "
            "WHERE closure_id<>anchor_order_seq+1 OR end_order_seq<anchor_order_seq "
            "OR transition_count<1 OR addr_count<2 OR nft_count<1"
        ).fetchone()[0])
        missing = int(conn.execute(
            "SELECT COUNT(*) FROM nft_role_transition_edges e "
            "LEFT JOIN nft_closure_summary s ON s.closure_id=e.order_seq+1 "
            "WHERE s.closure_id IS NULL OR s.anchor_order_seq<>e.order_seq"
        ).fetchone()[0])
        extra = int(conn.execute(
            "SELECT COUNT(*) FROM nft_closure_summary s "
            "LEFT JOIN nft_role_transition_edges e ON e.order_seq=s.anchor_order_seq "
            "WHERE e.order_seq IS NULL OR s.closure_id<>e.order_seq+1"
        ).fetchone()[0])
        dictionary_counts = {
            str(k): int(v)
            for k, v in conn.execute(
                "SELECT value_kind,COUNT(*) FROM nft_role_value_dict GROUP BY value_kind"
            )
        }
    passed = (
        not missing_required and not forbidden and layout == EXPECTED_LAYOUT
        and events == closures and malformed == 0 and missing == 0 and extra == 0
        and duplicate_event_id_rows == 0
    )
    report = {
        "passed": passed,
        "db": str(db),
        "size_bytes": db.stat().st_size,
        "layout": layout,
        "expected_layout": EXPECTED_LAYOUT,
        "event_rows": events,
        "distinct_event_ids": distinct_event_ids,
        "duplicate_event_id_rows": duplicate_event_id_rows,
        "event_id_unique": duplicate_event_id_rows == 0,
        "closure_rows": closures,
        "malformed_closures": malformed,
        "missing_event_summaries": missing,
        "extra_summaries": extra,
        "dictionary_counts": dictionary_counts,
        "missing_required_tables": missing_required,
        "forbidden_tables_present": forbidden,
        "tables": sorted(tables),
    }
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if str(args.out_json).strip() not in {"", "."}:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(text, encoding="utf-8")
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
