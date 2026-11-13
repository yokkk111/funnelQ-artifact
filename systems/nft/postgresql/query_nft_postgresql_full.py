#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import psycopg2

from systems.nft.common.canonical_results import canonical_row, canonicalize_rows, write_normalized_csv
from systems.nft.common.full_binding_spec import FullBindingPlan, load_full_binding_plan
from systems.nft.sql.query_nft_sql_full import compile_branch_sql


def connection_args(args: argparse.Namespace) -> dict[str, object]:
    return {
        "host": args.host,
        "port": args.port,
        "dbname": args.database,
        "user": args.user,
        "password": args.password,
        "options": "-c max_parallel_workers_per_gather=8 -c work_mem=64MB -c jit=off",
    }


def postgres_sql(sql: str) -> str:
    return sql.replace(") WHERE v<>''", ") AS distinct_values WHERE v<>''")


def execute_compiled(
    conn: Any,
    plan: FullBindingPlan,
    compiled: list[tuple[str, list[str], list[str]]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    engine_s = 0.0
    projection_s = 0.0
    for branch_index, (branch, (sql, pattern_ids, variable_names)) in enumerate(zip(plan.branches, compiled)):
        cursor_name = f"nft_{branch_index}_{time.time_ns()}"
        tq = time.perf_counter()
        with conn.cursor(name=cursor_name) as cursor:
            cursor.itersize = 4096
            cursor.execute(sql)
            for result in cursor:
                project0 = time.perf_counter()
                events = {pid: str(result[2 * index]) for index, pid in enumerate(pattern_ids)}
                orders = {pid: int(result[2 * index + 1]) for index, pid in enumerate(pattern_ids)}
                variable_offset = 2 * len(pattern_ids)
                bindings = {
                    name: str(result[variable_offset + index])
                    for index, name in enumerate(variable_names)
                }
                rows.append(canonical_row(plan.query_id, branch.branch_id, events, orders, bindings))
                projection_s += time.perf_counter() - project0
        engine_s += time.perf_counter() - tq
    canonical0 = time.perf_counter()
    normalized, material = canonicalize_rows(rows)
    return normalized, {
        "sql_execute_fetch_s": engine_s,
        "row_projection_s": projection_s,
        "result_canonicalize_s": time.perf_counter() - canonical0,
        **material,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run PostgreSQL NFT full-binding query.")
    ap.add_argument("--query-json", required=True, type=Path)
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--phase-marker", default="", type=Path)
    ap.add_argument("--host", default=os.getenv("PGHOST", "postgres"))
    ap.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")))
    ap.add_argument("--database", default=os.getenv("PGDATABASE", "funnelq"))
    ap.add_argument("--user", default=os.getenv("PGUSER", "funnelq"))
    ap.add_argument("--password", default=os.getenv("PGPASSWORD", "funnelq"))
    args = ap.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    _, plan = load_full_binding_plan(args.query_json)
    compiled: list[tuple[str, list[str], list[str]]] = []
    compiled_sql: dict[str, str] = {}
    compile0 = time.perf_counter()
    for branch in plan.branches:
        sql, pattern_ids, variable_names = compile_branch_sql(branch)
        sql = postgres_sql(sql)
        compiled.append((sql, pattern_ids, variable_names))
        compiled_sql[branch.branch_id] = sql
    compile_s = time.perf_counter() - compile0

    wall0 = time.perf_counter()
    open0 = time.perf_counter()
    conn = psycopg2.connect(**connection_args(args))
    store_open_s = time.perf_counter() - open0
    try:
        conn.autocommit = False
        warmup_s = 0.0
        for _ in range(max(0, args.warmup_runs)):
            warm0 = time.perf_counter()
            execute_compiled(conn, plan, compiled)
            conn.rollback()
            warmup_s += time.perf_counter() - warm0
        marker = args.phase_marker if str(args.phase_marker).strip() not in {"", "."} else None
        if marker is not None:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("query\n", encoding="utf-8")
        try:
            normalized, measured = execute_compiled(conn, plan, compiled)
        finally:
            if marker is not None:
                marker.unlink(missing_ok=True)
        conn.rollback()
    finally:
        conn.close()

    write0 = time.perf_counter()
    instances_path = args.outdir / "instances.csv"
    write_normalized_csv(instances_path, normalized)
    csv_write_s = time.perf_counter() - write0
    query_latency_s = float(measured["sql_execute_fetch_s"]) + float(measured["result_canonicalize_s"])
    report = {
        "system": "PostgreSQL",
        "engine": "PostgreSQL full-instance materialization",
        "query": plan.query_id,
        "result_semantics": plan.result_semantics,
        "instances_csv": str(instances_path),
        **{key: measured[key] for key in ("result_count", "duplicate_count", "result_digest_sha256")},
        "timing": {
            "store_open_s": store_open_s,
            "compile_s": compile_s,
            "warmup_runs": max(0, args.warmup_runs),
            "warmup_s": warmup_s,
            "sql_execute_fetch_s": measured["sql_execute_fetch_s"],
            "row_projection_s": measured["row_projection_s"],
            "result_canonicalize_s": measured["result_canonicalize_s"],
            "csv_write_s": csv_write_s,
            "query_latency_s": query_latency_s,
            "wall_s": time.perf_counter() - wall0,
            "runtime_load_excluded_from_query_latency": True,
            "same_connection_warmup": True,
            "offline_index_build_excluded": True,
            "result_file_write_excluded_from_query_latency": True,
            "complete_result_materialization_included": True,
        },
        "session_settings": {
            "max_parallel_workers_per_gather": 8,
            "work_mem": "64MB",
            "jit": "off",
        },
    }
    (args.outdir / "compiled_sql.json").write_text(json.dumps(compiled_sql, indent=2), encoding="utf-8")
    (args.outdir / "execution_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
