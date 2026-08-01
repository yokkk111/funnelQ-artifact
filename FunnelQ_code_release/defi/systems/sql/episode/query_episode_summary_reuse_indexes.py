#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

ALLOWED_FIELDS = {
    "event_id", "tx_hash", "block_number", "block_time", "block_time_unix",
    "evt_index", "seq_index", "global_tx_ord", "global_event_ord",
    "event_type", "protocol", "version", "contract",
    "recipient", "borrower", "taker", "maker", "asset",
    "token_sold", "token_bought", "pool", "amount", "amount_in", "amount_out",
}
ALLOWED_EVENT_TYPES = {"flashloan", "swap", "borrow", "repay", "liquidation"}
ALLOWED_ATTR_OPS = {">=", ">", "<=", "<", "="}


@dataclass
class EventSpec:
    qid: str
    alias: str
    event_type: str
    roles: Dict[str, str]


def ensure_dir(path: str | os.PathLike[str]) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def sanitize_name(s: str) -> str:
    s2 = re.sub(r"[^A-Za-z0-9_]+", "_", str(s)).strip("_")
    return s2 or "q"


def qlit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def load_query(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        q = json.load(f)
    if q.get("scope") != "anchored_episode":
        raise ValueError("generic episode SQL baseline supports only scope='anchored_episode'")
    q.setdefault("events", [])
    q.setdefault("order", [])
    q.setdefault("neq_constraints", [])
    q.setdefault("attr_filters", [])
    q.setdefault("scope_params", {})
    if not q["events"]:
        raise ValueError("query must contain at least one event")
    if not q["scope_params"].get("semantic_anchor_event"):
        raise ValueError("anchored_episode query requires scope_params.semantic_anchor_event")
    if "forward_window_sec" not in q["scope_params"]:
        raise ValueError("anchored_episode query requires scope_params.forward_window_sec")
    return q


def validate_query(query: dict) -> None:
    ids = set()
    for e in query["events"]:
        qid = e.get("id")
        if not qid:
            raise ValueError("every event must have an id")
        if qid in ids:
            raise ValueError(f"duplicate event id: {qid}")
        ids.add(qid)
        if e.get("type") not in ALLOWED_EVENT_TYPES:
            raise ValueError(f"unsupported event type: {e.get('type')}")
        roles = e.get("roles", {}) or {}
        if not isinstance(roles, dict):
            raise ValueError(f"roles for {qid} must be an object")
        for field in roles:
            if field not in ALLOWED_FIELDS or field in {"tx_hash", "block_number", "block_time", "block_time_unix", "seq_index", "global_tx_ord", "global_event_ord", "event_type"}:
                raise ValueError(f"unsupported role field in query: {field}")
    anchor = query["scope_params"]["semantic_anchor_event"]
    if anchor not in ids:
        raise ValueError("semantic_anchor_event must reference a known event id")
    if int(query["scope_params"]["forward_window_sec"]) < 0:
        raise ValueError("forward_window_sec must be non-negative")
    for a, b in query.get("order", []):
        if a not in ids or b not in ids:
            raise ValueError(f"order references unknown event ids: {(a, b)}")
    for a, lf, b, rf in query.get("neq_constraints", []):
        if a not in ids or b not in ids:
            raise ValueError(f"neq references unknown event ids: {(a, b)}")
        if lf not in ALLOWED_FIELDS or rf not in ALLOWED_FIELDS:
            raise ValueError(f"unsupported neq fields: {(lf, rf)}")
    for flt in query.get("attr_filters", []):
        if flt.get("event") not in ids:
            raise ValueError("attr_filter references unknown event")
        if flt.get("field") not in ALLOWED_FIELDS:
            raise ValueError("unsupported attr_filter field")
        if flt.get("op") not in ALLOWED_ATTR_OPS:
            raise ValueError("unsupported attr_filter op")


def choose_event_order_query_order(query: dict) -> List[EventSpec]:
    return [
        EventSpec(qid=e["id"], alias=f"t{i}", event_type=e["type"], roles=dict(e.get("roles", {}) or {}))
        for i, e in enumerate(query["events"])
    ]


def build_index_signatures(query: dict, specs: List[EventSpec]) -> Tuple[List[str], Dict[str, str], List[str]]:
    role_fields = ["recipient", "borrower", "taker", "asset", "token_sold", "token_bought", "pool", "protocol"]
    ddl: List[str] = [
        "CREATE INDEX IF NOT EXISTS idx_generic_episode_evt_type_time_ord ON event_atoms_flat(event_type, block_time_unix, global_event_ord)",
        "CREATE INDEX IF NOT EXISTS idx_generic_episode_time_ord ON event_atoms_flat(block_time_unix, global_event_ord)",
        "CREATE INDEX IF NOT EXISTS idx_generic_episode_tx_global ON event_atoms_flat(tx_hash, global_event_ord)",
        "CREATE INDEX IF NOT EXISTS idx_generic_episode_event_id ON event_atoms_flat(event_id)",
    ]
    for field in role_fields:
        ddl.append(
            f"CREATE INDEX IF NOT EXISTS idx_generic_episode_{field}_time_ord "
            f"ON event_atoms_flat(event_type, {field}, block_time_unix, global_event_ord)"
        )
    alias_to_index = {spec.alias: "generic_episode_index_family" for spec in specs}
    index_names = [stmt.split()[5] for stmt in ddl]
    return ddl, alias_to_index, index_names


def build_conditions(query: dict, specs: List[EventSpec]) -> Tuple[List[str], EventSpec]:
    qid_to_spec = {s.qid: s for s in specs}
    anchor = qid_to_spec[query["scope_params"]["semantic_anchor_event"]]
    window = int(query["scope_params"]["forward_window_sec"])
    parts: List[str] = []
    for spec in specs:
        parts.append(f"{spec.alias}.event_type = {qlit(spec.event_type)}")
        for field, raw in spec.roles.items():
            if field == "event_id":
                continue
            parts.append(f"{spec.alias}.{field} IS NOT NULL AND {spec.alias}.{field} <> ''")
            if not str(raw).startswith("$"):
                parts.append(f"{spec.alias}.{field} = {qlit(raw)}")
    for spec in specs:
        if spec.alias == anchor.alias:
            continue
        parts.append(f"{spec.alias}.block_time_unix >= {anchor.alias}.block_time_unix")
        parts.append(f"{spec.alias}.block_time_unix <= {anchor.alias}.block_time_unix + {window}")
        parts.append(f"{spec.alias}.global_event_ord >= {anchor.alias}.global_event_ord")
    for i in range(len(specs)):
        for j in range(i + 1, len(specs)):
            parts.append(f"{specs[i].alias}.event_id <> {specs[j].alias}.event_id")
    var_occ: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for spec in specs:
        for field, raw in spec.roles.items():
            if str(raw).startswith("$"):
                var_occ[str(raw)].append((spec.alias, field))
    for occs in var_occ.values():
        base_a, base_f = occs[0]
        for a, f in occs[1:]:
            parts.append(f"{a}.{f} = {base_a}.{base_f}")
    for left, right in query.get("order", []):
        parts.append(f"{qid_to_spec[left].alias}.global_event_ord < {qid_to_spec[right].alias}.global_event_ord")
    for le, lf, re_, rf in query.get("neq_constraints", []):
        parts.append(f"{qid_to_spec[le].alias}.{lf} <> {qid_to_spec[re_].alias}.{rf}")
    for flt in query.get("attr_filters", []):
        spec = qid_to_spec[flt["event"]]
        field, op, value = flt["field"], flt["op"], flt["value"]
        if op == "=":
            parts.append(f"CAST({spec.alias}.{field} AS REAL) = {float(value)}")
        else:
            parts.append(f"CAST({spec.alias}.{field} AS REAL) {op} {float(value)}")
    return parts, anchor


def build_select_sql(query: dict, specs: List[EventSpec], alias_to_index: Dict[str, str]) -> str:
    parts, anchor = build_conditions(query, specs)
    select_cols = [
        f"{anchor.alias}.event_id AS episode_id",
        f"{anchor.alias}.tx_hash AS anchor_tx_hash",
        f"{anchor.alias}.block_time_unix AS anchor_block_time_unix",
    ]
    for spec in specs:
        select_cols.append(f"{spec.alias}.event_id AS {sanitize_name(spec.qid)}_event_id")
        select_cols.append(f"{spec.alias}.tx_hash AS {sanitize_name(spec.qid)}_tx_hash")
        select_cols.append(f"{spec.alias}.global_event_ord AS {sanitize_name(spec.qid)}_global_event_ord")
    from_sql = "\nJOIN ".join(f"event_atoms_flat AS {spec.alias}" for spec in specs)
    return (
        "SELECT\n    "
        + ",\n    ".join(select_cols)
        + "\nFROM "
        + from_sql
        + "\nWHERE\n    "
        + "\n    AND ".join(parts)
    )


def strip_order(sql: str) -> str:
    return sql


def configure_pragmas(conn: sqlite3.Connection, temp_store: str, cache_size_mb: int) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA temp_store = {temp_store}")
    cur.execute(f"PRAGMA cache_size = {-1024 * int(cache_size_mb)}")
    cur.execute("PRAGMA mmap_size = 268435456")
    cur.execute("PRAGMA automatic_index = ON")
    conn.commit()


def drop_indexes(conn: sqlite3.Connection, names: List[str]) -> None:
    for name in names:
        conn.execute(f"DROP INDEX IF EXISTS {name}")
    conn.commit()


def create_indexes(conn: sqlite3.Connection, ddl: List[str]) -> float:
    t0 = time.perf_counter()
    for stmt in ddl:
        conn.execute(stmt)
    conn.commit()
    conn.execute("ANALYZE")
    conn.commit()
    return time.perf_counter() - t0


def run_count(conn: sqlite3.Connection, sql: str) -> Tuple[int, int, float]:
    count_sql = "SELECT COUNT(*) AS num_matches, COUNT(DISTINCT episode_id) AS num_result_episodes FROM (\n" + strip_order(sql) + "\n) q"
    t0 = time.perf_counter()
    row = conn.execute(count_sql).fetchone()
    t1 = time.perf_counter()
    if row is None:
        return 0, 0, t1 - t0
    return int(row[0] or 0), int(row[1] or 0), t1 - t0


def write_bindings(conn: sqlite3.Connection, sql: str, outdir: Path, query: dict) -> Tuple[int, int]:
    inst_path = outdir / "matched_instances.csv"
    bind_path = outdir / "matched_bindings.csv"
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    event_cols = [(c, cols.index(c)) for c in cols if c.endswith("_event_id")]
    rows = 0
    bindings = 0
    with inst_path.open("w", newline="", encoding="utf-8") as inst_f, bind_path.open("w", newline="", encoding="utf-8") as bind_f:
        inst_w = csv.writer(inst_f)
        bind_w = csv.writer(bind_f)
        inst_w.writerow(["match_id", "episode_id", "query_name", "num_events"])
        bind_w.writerow(["match_id", "episode_id", "query_event_id", "event_id"])
        for row in cur:
            episode_id = row[0]
            match_id = f"{query['query_name']}::{rows}"
            inst_w.writerow([match_id, episode_id, query["query_name"], len(event_cols)])
            for col, idx in event_cols:
                qid = col[:-len("_event_id")]
                bind_w.writerow([match_id, episode_id, qid, row[idx]])
                bindings += 1
            rows += 1
    return rows, bindings


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generic JSON-to-SQL anchored-episode baseline with fixed generic indexes, summary/count-only by default.")
    ap.add_argument("--db", required=True)
    ap.add_argument("--query-json", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--warmup-runs", type=int, default=0)
    ap.add_argument("--phase-marker")
    ap.add_argument("--reuse-prebuilt-indexes", action="store_true", default=True)
    ap.add_argument("--sample-limit", type=int, default=0)
    ap.add_argument("--temp-store", default="MEMORY", choices=["MEMORY", "FILE"])
    ap.add_argument("--cache-size-mb", type=int, default=200)
    ap.add_argument("--write-bindings", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    outdir = Path(args.outdir)
    ensure_dir(outdir)
    query = load_query(args.query_json)
    validate_query(query)
    specs = choose_event_order_query_order(query)
    ddl, alias_to_index, index_names = build_index_signatures(query, specs)
    sql = build_select_sql(query, specs, alias_to_index)
    (outdir / f"{query['query_name']}_main.sql").write_text(sql + "\n", encoding="utf-8")
    (outdir / f"{query['query_name']}_indexes.sql").write_text("\n".join(ddl) + "\n", encoding="utf-8")
    conn = sqlite3.connect(args.db)
    try:
        configure_pragmas(conn, args.temp_store, args.cache_size_mb)
        runs = max(1, int(args.runs))
        warmup_runs = max(0, int(args.warmup_runs))
        for _ in range(warmup_runs):
            run_count(conn, sql)
        query_times: List[float] = []
        matches = 0
        episodes = 0
        phase_marker = Path(args.phase_marker) if args.phase_marker else None
        if phase_marker is not None:
            phase_marker.parent.mkdir(parents=True, exist_ok=True)
            phase_marker.write_text("query\n", encoding="utf-8")
        try:
            for _ in range(runs):
                matches, episodes, elapsed = run_count(conn, sql)
                query_times.append(elapsed)
        finally:
            if phase_marker is not None:
                phase_marker.unlink(missing_ok=True)
        written_rows = 0
        written_bindings = 0
        if args.write_bindings:
            written_rows, written_bindings = write_bindings(conn, sql, outdir, query)
        else:
            (outdir / "matched_instances.csv").write_text("match_id,episode_id,query_name,num_events\n", encoding="utf-8")
            (outdir / "matched_bindings.csv").write_text("match_id,episode_id,query_event_id,event_id\n", encoding="utf-8")
    finally:
        conn.close()
    report = {
        "query_name": query["query_name"],
        "scope": query.get("scope"),
        "baseline_variant": "generic_query_json_compiled_relational_anchored_episode_summary_count_only_reuse_generic_indexes",
        "reuse_prebuilt_indexes": True,
        "index_mode": "generic",
        "query_specific_indexes": [],
        "generic_indexes": index_names,
        "generated_generic_index_ddl": ddl,
        "summary_count_only": not bool(args.write_bindings),
        "matched_instances_csv_written": bool(args.write_bindings),
        "matched_bindings_csv_written": bool(args.write_bindings),
        "num_matches": int(matches),
        "num_result_episodes": int(episodes),
        "num_bindings": int(written_bindings),
        "timing_s": {
            "mean_index_build_s": 0.0,
            "mean_main_query_s": statistics.mean(query_times) if query_times else 0.0,
            "median_main_query_s": statistics.median(query_times) if query_times else 0.0,
            "mean_residual_count_s": 0.0,
            "mean_sample_fetch_s": 0.0,
            "mean_total_s": statistics.mean(query_times) if query_times else 0.0,
            "median_total_s": statistics.median(query_times) if query_times else 0.0,
            "all_main_query_runs_s": query_times,
            "all_total_runs_s": query_times,
            "warmup_runs": warmup_runs,
        },
        "matched_instances_path": str(outdir / "matched_instances.csv"),
        "matched_bindings_path": str(outdir / "matched_bindings.csv"),
    }
    (outdir / "execution_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
