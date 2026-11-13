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
from typing import Dict, List, Tuple

import pandas as pd

ALLOWED_FIELDS = {
    "event_id",
    "tx_hash",
    "block_number",
    "block_time",
    "block_time_unix",
    "evt_index",
    "seq_index",
    "global_tx_ord",
    "global_event_ord",
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
ALLOWED_EVENT_TYPES = {"flashloan", "swap", "borrow", "repay", "liquidation"}
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
    if q.get("scope") != "anchored_episode":
        raise ValueError("generic episode baseline currently supports only scope='anchored_episode'")
    q.setdefault("events", [])
    q.setdefault("order", [])
    q.setdefault("neq_constraints", [])
    q.setdefault("attr_filters", [])
    q.setdefault("scope_params", {})
    if not q["events"]:
        raise ValueError("query must contain at least one event")
    if "semantic_anchor_event" not in q["scope_params"]:
        raise ValueError("anchored_episode query must contain scope_params.semantic_anchor_event")
    if "forward_window_sec" not in q["scope_params"]:
        raise ValueError("anchored_episode query must contain scope_params.forward_window_sec")
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
            if field not in ALLOWED_FIELDS or field in {
                "tx_hash",
                "block_number",
                "block_time",
                "block_time_unix",
                "seq_index",
                "global_tx_ord",
                "global_event_ord",
                "event_type",
            }:
                raise ValueError(f"unsupported role field in query: {field}")
    anchor_qid = query["scope_params"]["semantic_anchor_event"]
    if anchor_qid not in ids:
        raise ValueError("semantic_anchor_event must reference a known event id")
    win = query["scope_params"]["forward_window_sec"]
    if not isinstance(win, int) or win < 0:
        raise ValueError("forward_window_sec must be a non-negative integer")
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


def choose_event_order_query_order(query: dict) -> List[EventSpec]:
    specs: List[EventSpec] = []
    for idx, e in enumerate(query["events"]):
        specs.append(EventSpec(qid=e["id"], alias=f"t{idx}", event_type=e["type"], roles=dict(e.get("roles", {}))))
    return specs


def build_index_signatures(query: dict, specs: List[EventSpec]) -> Tuple[List[str], Dict[str, str], List[str]]:
    attr_fields_by_event: Dict[str, List[str]] = defaultdict(list)
    for flt in query.get("attr_filters", []):
        attr_fields_by_event[flt["event"]].append(flt["field"])

    sig_to_name: Dict[Tuple[str, ...], str] = {}
    alias_to_index: Dict[str, str] = {}
    ddl: List[str] = []
    qslug = sanitize_name(query["query_name"])[:24]

    for i, spec in enumerate(specs):
        cols: List[str] = ["event_type"]
        for field in spec.roles.keys():
            if field != "event_id" and field not in cols:
                cols.append(field)
        for field in attr_fields_by_event.get(spec.qid, []):
            if field not in {"event_id", "tx_hash", "seq_index"} and field not in cols:
                cols.append(field)
        for tail in ["block_time_unix", "global_event_ord"]:
            if tail not in cols:
                cols.append(tail)
        sig = tuple(cols)
        if sig not in sig_to_name:
            idx_name = f"idx_qae_{qslug}_{i}"
            sig_to_name[sig] = idx_name
            ddl.append(f"CREATE INDEX {idx_name} ON event_atoms_flat({', '.join(cols)})")
        alias_to_index[spec.alias] = sig_to_name[sig]
    return ddl, alias_to_index, list(sig_to_name.values())


def build_conditions(query: dict, specs: List[EventSpec]):
    qid_to_spec = {s.qid: s for s in specs}
    anchor_qid = query["scope_params"]["semantic_anchor_event"]
    anchor = qid_to_spec[anchor_qid]
    forward_window_sec = int(query["scope_params"]["forward_window_sec"])

    event_type_and_notnull: List[str] = []
    episode_scope: List[str] = []
    distinct_event_ids: List[str] = []
    var_eqs: List[str] = []
    order_clauses: List[str] = []
    attr_filters: List[str] = []
    neq_constraints: List[str] = []

    for spec in specs:
        event_type_and_notnull.append(f"{spec.alias}.event_type = {qlit(spec.event_type)}")
        for field, raw_val in spec.roles.items():
            if field == "event_id":
                continue
            event_type_and_notnull.append(f"{spec.alias}.{field} IS NOT NULL AND {spec.alias}.{field} <> ''")
            if not str(raw_val).startswith("$"):
                event_type_and_notnull.append(f"{spec.alias}.{field} = {qlit(raw_val)}")

    for spec in specs:
        if spec.alias == anchor.alias:
            continue
        episode_scope.append(f"{spec.alias}.block_time_unix >= {anchor.alias}.block_time_unix")
        episode_scope.append(f"{spec.alias}.block_time_unix <= {anchor.alias}.block_time_unix + {forward_window_sec}")
        episode_scope.append(f"{spec.alias}.global_event_ord >= {anchor.alias}.global_event_ord")

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
        order_clauses.append(f"{lspec.alias}.global_event_ord < {rspec.alias}.global_event_ord")

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
        "episode_scope": episode_scope,
        "distinct_event_ids": distinct_event_ids,
        "var_eqs": var_eqs,
        "order_clauses": order_clauses,
        "attr_filters": attr_filters,
        "neq_constraints": neq_constraints,
    }, anchor


def build_from_optimizer_free(specs_prefix: List[EventSpec], alias_to_index: Dict[str, str]) -> str:


    return "\nJOIN ".join(f"event_atoms_flat AS {spec.alias}" for spec in specs_prefix)


def build_select_sql(query: dict, specs: List[EventSpec], alias_to_index: Dict[str, str]):
    conditions, anchor = build_conditions(query, specs)

    select_cols = [
        f"{anchor.alias}.event_id AS episode_id",
        f"{anchor.alias}.tx_hash AS anchor_tx_hash",
        f"{anchor.alias}.block_time_unix AS anchor_block_time_unix",
    ]
    output_alias_map: Dict[str, str] = {}
    for spec in specs:
        out_name = f"{sanitize_name(spec.qid)}_event_id"
        output_alias_map[spec.qid] = out_name
        select_cols.append(f"{spec.alias}.event_id AS {out_name}")
        select_cols.append(f"{spec.alias}.tx_hash AS {sanitize_name(spec.qid)}_tx_hash")
        select_cols.append(f"{spec.alias}.global_event_ord AS {sanitize_name(spec.qid)}_global_event_ord")
        for field in spec.roles.keys():
            if field == "event_id":
                continue
            select_cols.append(f"{spec.alias}.{field} AS {sanitize_name(spec.qid)}_{field}")

    all_where_parts = (
        conditions["event_type_and_notnull"]
        + conditions["episode_scope"]
        + conditions["distinct_event_ids"]
        + conditions["var_eqs"]
        + conditions["order_clauses"]
        + conditions["attr_filters"]
        + conditions["neq_constraints"]
    )

    order_cols = [f"{anchor.alias}.event_id"] + [f"{spec.alias}.event_id" for spec in specs]

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
    return sql_main, output_alias_map, conditions, anchor.qid


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
        stmt2 = stmt.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
        cur.execute(stmt2)
    conn.commit()
    cur.execute("ANALYZE")
    conn.commit()
    return time.perf_counter() - t0


def timed_read_sql(conn: sqlite3.Connection, sql: str):
    t0 = time.perf_counter()
    df = pd.read_sql_query(sql, conn)
    t1 = time.perf_counter()
    return df, (t1 - t0)


def timed_stream_sql_full_materialization(
    conn: sqlite3.Connection,
    sql: str,
    outdir: str,
    query_name: str,
    batch_size: int,
):
    t0 = time.perf_counter()
    cur = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    episode_idx = columns.index("episode_id") if "episode_id" in columns else None
    event_id_cols = [col for col in columns if col.endswith("_event_id")]
    event_id_indexes = [(col, columns.index(col)) for col in event_id_cols]

    instance_path = os.path.join(outdir, "matched_instances.csv")
    binding_path = os.path.join(outdir, "matched_bindings.csv")
    instance_fields = columns + ["query_name", "match_id"]
    binding_fields = ["match_id", "episode_id", "query_event_id", "event_id"]

    num_rows = 0
    distinct_episodes = set()
    num_bindings = 0
    t_first_row = None

    with open(instance_path, "w", newline="", encoding="utf-8") as inst_f, open(
        binding_path, "w", newline="", encoding="utf-8"
    ) as bind_f:
        inst_w = csv.writer(inst_f)
        bind_w = csv.writer(bind_f)
        inst_w.writerow(instance_fields)
        bind_w.writerow(binding_fields)

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            if t_first_row is None:
                t_first_row = time.perf_counter() - t0
            for row in rows:
                row_values = list(row)
                episode_id = row_values[episode_idx] if episode_idx is not None else ""
                if episode_idx is not None:
                    distinct_episodes.add(episode_id)
                event_values = [str(row_values[idx]) for _, idx in event_id_indexes]
                match_id = "::".join(event_values) if event_values else str(episode_id)
                inst_w.writerow(row_values + [query_name, match_id])
                for col, idx in event_id_indexes:
                    qid = col[: -len("_event_id")]
                    bind_w.writerow([match_id, episode_id, qid, row_values[idx]])
                    num_bindings += 1
                num_rows += 1

    t1 = time.perf_counter()
    return {
        "num_rows": int(num_rows),
        "num_result_episodes": int(len(distinct_episodes)),
        "num_bindings": int(num_bindings),
        "main_query_stream_s": t1 - t0,
        "time_to_first_batch_s": t_first_row if t_first_row is not None else 0.0,
        "batch_size": int(batch_size),
        "matched_instances_path": instance_path,
        "matched_bindings_path": binding_path,
    }


def parse_args():
    ap = argparse.ArgumentParser(
        description="Generic/query-order relational baseline for anchored-episode CEH query JSON. Distinct episode counting is done in-memory from the main query result to avoid rerunning an equivalent heavy SQL count."
    )
    ap.add_argument("--db", required=True)
    ap.add_argument("--query-json", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--rebuild-query-indexes-each-run", action="store_true")
    ap.add_argument("--temp-store", default="MEMORY", choices=["MEMORY", "FILE"])
    ap.add_argument("--cache-size-mb", type=int, default=200)
    ap.add_argument("--stream-batch-size", type=int, default=10000)
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
        sql_main, output_alias_map, conditions, anchor_qid = build_select_sql(query, specs, alias_to_index)

        with open(os.path.join(args.outdir, f"{query['query_name']}_main.sql"), "w", encoding="utf-8") as f:
            f.write(sql_main + "\n")
        with open(os.path.join(args.outdir, f"{query['query_name']}_indexes.sql"), "w", encoding="utf-8") as f:
            for stmt in ddl:
                f.write(stmt + ";\n")
        if args.rebuild_query_indexes_each_run:
            print("[baseline] dropping existing query-specific indexes once before timed runs ...", flush=True)
            drop_indexes(conn, index_names)
        else:
            print("[baseline] ensuring query-specific indexes exist once before query timing ...", flush=True)
            create_indexes(conn, ddl)

        runs = max(1, int(args.runs))
        num_result_episodes = 0
        final_matches = 0
        final_bindings = 0
        stream_info = {}
        t_build_runs: List[float] = []
        t_query_runs: List[float] = []
        t_episode_count_runs: List[float] = []
        t_total_runs: List[float] = []

        for i in range(runs):
            print(f"[baseline] run {i+1}/{runs} ...", flush=True)
            if args.rebuild_query_indexes_each_run:
                drop_indexes(conn, index_names)
                t_build = create_indexes(conn, ddl)
            else:
                t_build = 0.0

            stream_info = timed_stream_sql_full_materialization(
                conn,
                sql_main,
                args.outdir,
                query["query_name"],
                max(1, int(args.stream_batch_size)),
            )
            t_query = float(stream_info["main_query_stream_s"])
            num_result_episodes = int(stream_info["num_result_episodes"])
            final_matches = int(stream_info["num_rows"])
            final_bindings = int(stream_info["num_bindings"])
            t_episode_count = 0.0

            t_total = t_build + t_query + t_episode_count

            print(
                f"[baseline] run {i+1} done: index_build={t_build:.6f}s, stream_query_write={t_query:.6f}s, episodes_streamed={num_result_episodes}, total={t_total:.6f}s, rows={final_matches}, bindings={final_bindings}",
                flush=True,
            )
            t_build_runs.append(t_build)
            t_query_runs.append(t_query)
            t_episode_count_runs.append(t_episode_count)
            t_total_runs.append(t_total)
    finally:
        conn.close()

    report = {
        "query_name": query["query_name"],
        "table_used": "event_atoms_flat",
        "baseline_variant": "generic_query_json_compiled_relational_anchored_episode_streaming_full_materialization",
        "scope": query.get("scope"),
        "semantic_anchor_event": query["scope_params"]["semantic_anchor_event"],
        "forward_window_sec": int(query["scope_params"]["forward_window_sec"]),
        "rebuild_query_indexes_each_run": bool(args.rebuild_query_indexes_each_run),
        "query_specific_indexes": index_names,
        "generated_index_ddl": ddl,
        "event_order_used": [
            {"query_event_id": s.qid, "alias": s.alias, "event_type": s.event_type, "roles": s.roles}
            for s in specs
        ],
        "timing_s": {
            "mean_index_build_s": statistics.mean(t_build_runs) if t_build_runs else 0.0,
            "median_index_build_s": statistics.median(t_build_runs) if t_build_runs else 0.0,
            "mean_main_query_s": statistics.mean(t_query_runs) if t_query_runs else 0.0,
            "median_main_query_s": statistics.median(t_query_runs) if t_query_runs else 0.0,
            "mean_result_episode_count_s": statistics.mean(t_episode_count_runs) if t_episode_count_runs else 0.0,
            "median_result_episode_count_s": statistics.median(t_episode_count_runs) if t_episode_count_runs else 0.0,
            "mean_total_s": statistics.mean(t_total_runs) if t_total_runs else 0.0,
            "median_total_s": statistics.median(t_total_runs) if t_total_runs else 0.0,
            "all_index_build_runs_s": t_build_runs,
            "all_main_query_runs_s": t_query_runs,
            "all_result_episode_count_runs_s": t_episode_count_runs,
            "all_total_runs_s": t_total_runs,
        },
        "num_matches": final_matches,
        "num_result_episodes": int(num_result_episodes),
        "num_residual_episodes": int(num_result_episodes),
        "num_bindings": int(final_bindings),
        "streaming_full_materialization": {
            "enabled": True,
            "stream_batch_size": int(args.stream_batch_size),
            "no_pandas_full_result_dataframe": True,
            "no_fetchall": True,
            "matched_instances_path": stream_info.get("matched_instances_path", os.path.join(args.outdir, "matched_instances.csv")),
            "matched_bindings_path": stream_info.get("matched_bindings_path", os.path.join(args.outdir, "matched_bindings.csv")),
            "time_to_first_batch_s": stream_info.get("time_to_first_batch_s", 0.0),
        },
        "notes": {
            "episode_count_method": "streaming_distinct_episode_id_set_during_cursor_fetchmany",
            "order_quality": "inherits_from_flat_store_input",
        },
    }
    with open(os.path.join(args.outdir, "execution_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("[baseline] all done.", flush=True)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
