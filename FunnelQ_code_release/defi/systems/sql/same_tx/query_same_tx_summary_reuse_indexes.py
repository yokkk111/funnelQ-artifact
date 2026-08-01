#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


ALLOWED_FIELDS = {
    "event_id",
    "tx_hash",
    "block_number",
    "block_time",
    "evt_index",
    "seq_index",
    "event_type",
    "protocol",
    "version",
    "contract",
    "recipient",
    "borrower",
    "taker",
    "maker",
    "asset",
    "token_sold",
    "token_bought",
    "pool",
    "amount",
    "amount_in",
    "amount_out",
}
ALLOWED_EVENT_TYPES = {"flashloan", "swap", "borrow"}
ALLOWED_ATTR_OPS = {">=", ">", "<=", "<", "="}


@dataclass
class EventSpec:
    qid: str
    alias: str
    event_type: str
    roles: Dict[str, str]


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def sanitize_name(s: str) -> str:
    s2 = re.sub(r"[^A-Za-z0-9_]+", "_", s)
    s2 = s2.strip("_")
    return s2 or "q"


def qlit(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def load_query(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        q = json.load(f)
    if "query_name" not in q:
        raise ValueError("query JSON must contain 'query_name'")
    if q.get("scope") != "same_tx":
        raise ValueError("generic baseline currently supports only scope='same_tx'")
    q.setdefault("events", [])
    q.setdefault("order", [])
    q.setdefault("neq_constraints", [])
    q.setdefault("attr_filters", [])
    if not q["events"]:
        raise ValueError("query must contain at least one event")
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
        et = e.get("type")
        if et not in ALLOWED_EVENT_TYPES:
            raise ValueError(f"unsupported event type: {et}")
        roles = e.get("roles", {})
        if not isinstance(roles, dict):
            raise ValueError(f"roles for {qid} must be an object")
        for field in roles:
            if field not in ALLOWED_FIELDS or field in {"tx_hash", "seq_index", "event_type", "block_number", "block_time", "evt_index"}:
                raise ValueError(f"unsupported role field in query: {field}")
    for a, b in query.get("order", []):
        if a not in ids or b not in ids:
            raise ValueError(f"order references unknown event ids: {(a, b)}")
    for a, lf, b, rf in query.get("neq_constraints", []):
        if a not in ids or b not in ids:
            raise ValueError(f"neq constraint references unknown event ids: {(a, b)}")
        for f in (lf, rf):
            if f not in ALLOWED_FIELDS:
                raise ValueError(f"unsupported neq field: {f}")
    for flt in query.get("attr_filters", []):
        ev = flt.get("event")
        field = flt.get("field")
        op = flt.get("op")
        if ev not in ids:
            raise ValueError(f"attr_filter references unknown event id: {ev}")
        if field not in ALLOWED_FIELDS:
            raise ValueError(f"unsupported attr_filter field: {field}")
        if op not in ALLOWED_ATTR_OPS:
            raise ValueError(f"unsupported attr_filter op: {op}")


def read_event_type_counts(conn: sqlite3.Connection, event_types: List[str]) -> Dict[str, int]:
    uniq = sorted(set(event_types))
    placeholders = ",".join("?" for _ in uniq)
    sql = f"SELECT event_type, COUNT(*) AS c FROM event_atoms_flat WHERE event_type IN ({placeholders}) GROUP BY event_type"
    rows = conn.execute(sql, uniq).fetchall()
    out = {et: 0 for et in uniq}
    for et, c in rows:
        out[str(et)] = int(c)
    return out


def choose_event_order_query_order(query: dict) -> List[EventSpec]:
    # Alias assignment follows the query JSON, but the generated SQL no longer
    # forces this order. SQLite may choose the physical join order after ANALYZE.
    specs: List[EventSpec] = []
    for idx, e in enumerate(query["events"]):
        specs.append(EventSpec(qid=e["id"], alias=f"t{idx}", event_type=e["type"], roles=dict(e.get("roles", {}))))
    return specs


def build_index_signatures(query: dict, specs: List[EventSpec]) -> Tuple[List[str], Dict[str, str], List[str]]:
    role_fields = ["recipient", "borrower", "taker", "asset", "token_sold", "token_bought", "pool", "protocol"]
    ddl: List[str] = [
        "CREATE INDEX IF NOT EXISTS idx_generic_same_tx_evt_type_tx_seq ON event_atoms_flat(event_type, tx_hash, seq_index)",
        "CREATE INDEX IF NOT EXISTS idx_generic_same_tx_tx_seq ON event_atoms_flat(tx_hash, seq_index)",
        "CREATE INDEX IF NOT EXISTS idx_generic_same_tx_event_id ON event_atoms_flat(event_id)",
    ]
    for field in role_fields:
        ddl.append(
            f"CREATE INDEX IF NOT EXISTS idx_generic_same_tx_{field}_tx_seq "
            f"ON event_atoms_flat(event_type, {field}, tx_hash, seq_index)"
        )
    alias_to_index = {spec.alias: "generic_same_tx_index_family" for spec in specs}
    index_names = [stmt.split()[5] for stmt in ddl]
    return ddl, alias_to_index, index_names


def build_conditions(query: dict, specs: List[EventSpec]) -> Dict[str, List[str]]:
    qid_to_spec = {s.qid: s for s in specs}
    anchor = specs[0]

    event_type_and_notnull: List[str] = []
    same_tx: List[str] = []
    distinct_event_ids: List[str] = []
    var_eqs: List[str] = []
    order_clauses: List[str] = []
    attr_filters: List[str] = []
    neq_constraints: List[str] = []

    for spec in specs:
        event_type_and_notnull.append(f"{spec.alias}.event_type = {qlit(spec.event_type)}")
        if spec.alias != anchor.alias:
            same_tx.append(f"{spec.alias}.tx_hash = {anchor.alias}.tx_hash")
        for field, raw_val in spec.roles.items():
            if field == "event_id":
                continue
            event_type_and_notnull.append(f"{spec.alias}.{field} IS NOT NULL AND {spec.alias}.{field} <> ''")
            if not str(raw_val).startswith("$"):
                event_type_and_notnull.append(f"{spec.alias}.{field} = {qlit(raw_val)}")

    for i in range(len(specs)):
        for j in range(i + 1, len(specs)):
            distinct_event_ids.append(f"{specs[i].alias}.event_id <> {specs[j].alias}.event_id")

    var_occ: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for spec in specs:
        for field, raw_val in spec.roles.items():
            if str(raw_val).startswith("$"):
                var_occ[str(raw_val)].append((spec.alias, field))
    for occs in var_occ.values():
        base_alias, base_field = occs[0]
        for alias, field in occs[1:]:
            var_eqs.append(f"{alias}.{field} = {base_alias}.{base_field}")

    for left_id, right_id in query.get("order", []):
        lspec = qid_to_spec[left_id]
        rspec = qid_to_spec[right_id]
        order_clauses.append(f"{lspec.alias}.seq_index < {rspec.alias}.seq_index")

    for flt in query.get("attr_filters", []):
        spec = qid_to_spec[flt["event"]]
        field = flt["field"]
        op = flt["op"]
        value = flt["value"]
        if op == "=":
            attr_filters.append(f"CAST({spec.alias}.{field} AS REAL) = {float(value)}")
        else:
            attr_filters.append(f"CAST({spec.alias}.{field} AS REAL) {op} {float(value)}")

    for left_eid, left_field, right_eid, right_field in query.get("neq_constraints", []):
        lspec = qid_to_spec[left_eid]
        rspec = qid_to_spec[right_eid]
        neq_constraints.append(f"{lspec.alias}.{left_field} <> {rspec.alias}.{right_field}")

    return {
        "event_type_and_notnull": event_type_and_notnull,
        "same_tx": same_tx,
        "distinct_event_ids": distinct_event_ids,
        "var_eqs": var_eqs,
        "order_clauses": order_clauses,
        "attr_filters": attr_filters,
        "neq_constraints": neq_constraints,
    }


def clause_aliases(clause: str) -> set[str]:
    return set(re.findall(r"\b(t\d+)\.", clause))


def build_from_optimizer_free(specs_prefix: List[EventSpec], alias_to_index: Dict[str, str]) -> str:
    # Do not force CROSS JOIN or INDEXED BY. Query-specific indexes may exist,
    # but SQLite is allowed to choose the physical join order and access path.
    return "\nJOIN ".join(f"event_atoms_flat AS {spec.alias}" for spec in specs_prefix)


def build_select_sql(query: dict, specs: List[EventSpec], alias_to_index: Dict[str, str]) -> Tuple[str, str, Dict[str, str], Dict[str, List[str]]]:
    anchor = specs[0]
    conditions = build_conditions(query, specs)

    select_cols = [f"{anchor.alias}.tx_hash AS tx_hash"]
    output_alias_map: Dict[str, str] = {}
    for spec in specs:
        out_name = f"{sanitize_name(spec.qid)}_event_id"
        output_alias_map[spec.qid] = out_name
        select_cols.append(f"{spec.alias}.event_id AS {out_name}")
        for field in spec.roles.keys():
            if field == "event_id":
                continue
            select_cols.append(f"{spec.alias}.{field} AS {sanitize_name(spec.qid)}_{field}")

    all_where_parts = (
        conditions["event_type_and_notnull"]
        + conditions["same_tx"]
        + conditions["distinct_event_ids"]
        + conditions["var_eqs"]
        + conditions["order_clauses"]
        + conditions["attr_filters"]
        + conditions["neq_constraints"]
    )

    order_cols = [f"{anchor.alias}.tx_hash"] + [f"{spec.alias}.event_id" for spec in specs]

    sql_main = (
        "SELECT\n    "
        + ",\n    ".join(select_cols)
        + "\nFROM "
        + build_from_optimizer_free(specs, alias_to_index)
        + "\nWHERE\n    "
        + "\n    AND ".join(all_where_parts)
        + "\nORDER BY\n    "
        + ",\n    ".join(order_cols)
    )

    sql_count = "SELECT COUNT(DISTINCT tx_hash) AS num_residual_closures\nFROM (\n" + sql_main + "\n)"
    return sql_main, sql_count, output_alias_map, conditions


def configure_pragmas(conn: sqlite3.Connection, temp_store: str, cache_size_mb: int) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA temp_store = {temp_store}")
    cur.execute(f"PRAGMA cache_size = {-1024 * int(cache_size_mb)}")
    cur.execute("PRAGMA mmap_size = 268435456")
    cur.execute("PRAGMA automatic_index = ON")
    conn.commit()


def drop_indexes(conn: sqlite3.Connection, names: List[str]) -> None:
    cur = conn.cursor()
    for name in names:
        cur.execute(f"DROP INDEX IF EXISTS {name}")
    conn.commit()


def create_indexes(conn: sqlite3.Connection, ddl: List[str]) -> float:
    t0 = time.perf_counter()
    cur = conn.cursor()
    for stmt in ddl:
        cur.execute(stmt)
    conn.commit()
    cur.execute("ANALYZE")
    conn.commit()
    return time.perf_counter() - t0



def timed_count_sql(conn: sqlite3.Connection, sql: str) -> Tuple[int, float]:
    t0 = time.perf_counter()
    row = conn.execute(sql).fetchone()
    t1 = time.perf_counter()
    return (0 if row is None else int(row[0])), (t1 - t0)


def timed_summary_sql(conn: sqlite3.Connection, sql: str) -> Tuple[int, int, float]:
    """Return COUNT(*) and COUNT(DISTINCT tx_hash) in one SQLite pass.

    The older same-tx SQL baseline executed two independent aggregate
    queries over the same self-join.  Besides doing duplicate work, that
    path kept temporary SQLite structures alive long enough to inflate
    peak RSS.  This summary query preserves the count-only semantics while
    matching the episode SQL runner's single-aggregate style.
    """
    t0 = time.perf_counter()
    row = conn.execute(sql).fetchone()
    t1 = time.perf_counter()
    if row is None:
        return 0, 0, (t1 - t0)
    return int(row[0] or 0), int(row[1] or 0), (t1 - t0)


def fetch_sample_rows(conn: sqlite3.Connection, sql: str, limit: int = 20) -> Tuple[List[dict], float]:
    t0 = time.perf_counter()
    cur = conn.execute(f"SELECT * FROM (\n{sql}\n) LIMIT {int(limit)}")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    t1 = time.perf_counter()
    return rows, (t1 - t0)


def strip_final_order_by(sql: str) -> str:
    marker = "\nORDER BY\n    "
    if marker in sql:
        return sql.rsplit(marker, 1)[0]
    return sql


def read_scalar_sql(conn: sqlite3.Connection, sql: str) -> Tuple[int, float]:
    t0 = time.perf_counter()
    row = conn.execute(sql).fetchone()
    t1 = time.perf_counter()
    return (0 if row is None else int(row[0])), (t1 - t0)


def parse_args():
    ap = argparse.ArgumentParser(description="Summary-only generic relational baseline for same-tx CEH query JSON, reusing fixed generic indexes.")
    ap.add_argument("--db", required=True)
    ap.add_argument("--query-json", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--warmup-runs", type=int, default=0)
    ap.add_argument("--phase-marker")
    ap.add_argument("--rebuild-query-indexes-each-run", action="store_true", help="Ignored in this reuse-index variant.")
    ap.add_argument("--reuse-prebuilt-indexes", action="store_true", default=True)
    ap.add_argument("--temp-store", default="MEMORY", choices=["MEMORY", "FILE"])
    ap.add_argument("--cache-size-mb", type=int, default=200)
    ap.add_argument("--sample-limit", type=int, default=0, help="Number of sample rows to fetch. Default 0 keeps the run strictly summary/count-only.")
    return ap.parse_args()


def main():
    args = parse_args()
    ensure_dir(args.outdir)
    query = load_query(args.query_json)
    validate_query(query)

    print("[baseline] opening db ...", flush=True)
    conn = sqlite3.connect(args.db)
    try:
        configure_pragmas(conn, args.temp_store, args.cache_size_mb)
        specs = choose_event_order_query_order(query)
        ddl, alias_to_index, index_names = build_index_signatures(query, specs)
        sql_main, sql_count, output_alias_map, conditions = build_select_sql(query, specs, alias_to_index)

        with open(os.path.join(args.outdir, f"{query['query_name']}_main.sql"), "w", encoding="utf-8") as f:
            f.write(sql_main + "\n")
        with open(os.path.join(args.outdir, f"{query['query_name']}_indexes.sql"), "w", encoding="utf-8") as f:
            for stmt in ddl:
                f.write(stmt + ";\n")
        print("[baseline] reusing fixed generic indexes; no query-specific index build is performed in this query process.", flush=True)

        runs = max(1, int(args.runs))
        warmup_runs = max(0, int(args.warmup_runs))
        final_matches = 0
        sample_rows: List[dict] = []
        num_residual_closures = 0
        t_build_runs: List[float] = []
        t_query_runs: List[float] = []
        t_residual_runs: List[float] = []
        t_sample_runs: List[float] = []
        t_total_runs: List[float] = []
        sql_main_unsorted = strip_final_order_by(sql_main)
        sql_summary = (
            "SELECT COUNT(*) AS num_matches, COUNT(DISTINCT tx_hash) AS num_residual_closures\n"
            "FROM (\n" + sql_main_unsorted + "\n)"
        )

        for _ in range(warmup_runs):
            timed_summary_sql(conn, sql_summary)
        phase_marker = Path(args.phase_marker) if args.phase_marker else None
        if phase_marker is not None:
            phase_marker.parent.mkdir(parents=True, exist_ok=True)
            phase_marker.write_text("query\n", encoding="utf-8")
        try:
            for i in range(runs):
                print(f"[baseline] run {i+1}/{runs} ...", flush=True)
                t_build = 0.0

                nm, nrc, t_query = timed_summary_sql(conn, sql_summary)
                t_residual = 0.0
                if i == runs - 1 and int(args.sample_limit) > 0:
                    sample_rows, t_sample = fetch_sample_rows(conn, sql_main, limit=int(args.sample_limit))
                else:
                    t_sample = 0.0
                t_total = t_build + t_query + t_sample

                print(
                    f"[baseline] run {i+1} done: index_build={t_build:.6f}s, summary={t_query:.6f}s, residual=0.000000s, sample={t_sample:.6f}s, total={t_total:.6f}s, matches={nm}",
                    flush=True,
                )
                final_matches = int(nm)
                num_residual_closures = nrc
                t_build_runs.append(t_build)
                t_query_runs.append(t_query)
                t_residual_runs.append(t_residual)
                t_sample_runs.append(t_sample)
                t_total_runs.append(t_total)
        finally:
            if phase_marker is not None:
                phase_marker.unlink(missing_ok=True)
    finally:
        conn.close()

    report = {
        "query_name": query["query_name"],
        "table_used": "event_atoms_flat",
        "baseline_variant": "generic_query_json_compiled_relational_optimizer_free_single_summary_count_only_reuse_generic_indexes",
        "scope": query.get("scope"),
        "rebuild_query_indexes_each_run": False,
        "reuse_prebuilt_indexes": True,
        "index_mode": "generic",
        "query_specific_indexes": [],
        "generic_indexes": index_names,
        "generated_generic_index_ddl": ddl,
        "event_order_used": [
            {"query_event_id": s.qid, "alias": s.alias, "event_type": s.event_type, "roles": s.roles}
            for s in specs
        ],
        "ceh_alignment": {
            "query_order_preserved": False,
            "optimizer_join_order_allowed": True
        },
        "timing_s": {
            "mean_index_build_s": statistics.mean(t_build_runs) if t_build_runs else 0.0,
            "median_index_build_s": statistics.median(t_build_runs) if t_build_runs else 0.0,
            "mean_main_query_s": statistics.mean(t_query_runs) if t_query_runs else 0.0,
            "median_main_query_s": statistics.median(t_query_runs) if t_query_runs else 0.0,
            "mean_residual_count_s": statistics.mean(t_residual_runs) if t_residual_runs else 0.0,
            "median_residual_count_s": statistics.median(t_residual_runs) if t_residual_runs else 0.0,
            "mean_sample_fetch_s": statistics.mean(t_sample_runs) if t_sample_runs else 0.0,
            "mean_total_s": statistics.mean(t_total_runs) if t_total_runs else 0.0,
            "median_total_s": statistics.median(t_total_runs) if t_total_runs else 0.0,
            "all_index_build_runs_s": t_build_runs,
            "all_main_query_runs_s": t_query_runs,
            "all_residual_count_runs_s": t_residual_runs,
            "all_sample_fetch_runs_s": t_sample_runs,
            "all_total_runs_s": t_total_runs,
            "warmup_runs": warmup_runs,
        },
        "num_matches": final_matches,
        "num_residual_closures": int(num_residual_closures),
        "sample_rows": sample_rows,
        "sample_limit": int(args.sample_limit),
    }
    with open(os.path.join(args.outdir, "execution_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("[baseline] all done.", flush=True)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
