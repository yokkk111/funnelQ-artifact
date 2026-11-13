#!/usr/bin/env python3


from __future__ import annotations

import argparse
import itertools
import json
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from systems.nft.common.canonical_results import canonical_row, canonicalize_rows, write_normalized_csv
from systems.nft.common.full_binding_spec import BranchPlan, FullBindingPlan, is_variable, load_full_binding_plan, variable_name
from systems.nft.common.nft_semantics import configure_sqlite

ZERO = "0x0000000000000000000000000000000000000000"


def qlit(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def qcol(alias: str, field: str) -> str:
    allowed = {
        "event_id", "event_type", "move_from", "move_to", "nft",
        "collection_contract", "block_ts", "order_seq", "tx_hash",
    }
    if field not in allowed:
        raise ValueError(f"unsupported SQL field: {field}")
    return f'{alias}."{field}"'


def distinct_value_count(exprs: list[str]) -> str:
    union = " UNION ALL ".join(f"SELECT {expr} AS v" for expr in exprs)
    return f"(SELECT COUNT(DISTINCT v) FROM ({union}) WHERE v<>'')"


def compile_branch_sql(branch: BranchPlan, table: str = "nft_events") -> tuple[str, list[str], list[str]]:
    aliases = {p.pattern_id: f"e{i}" for i, p in enumerate(branch.event_patterns)}
    variables: dict[str, list[str]] = defaultdict(list)
    wheres: list[str] = []
    for p in branch.event_patterns:
        alias = aliases[p.pattern_id]
        wheres.extend([
            f"{qcol(alias, 'event_type')} IN ({','.join(qlit(x) for x in p.event_types)})",
            f"{qcol(alias, 'move_from')}<>''",
            f"{qcol(alias, 'move_to')}<>''",
            f"lower({qcol(alias, 'move_from')})<>lower({qlit(ZERO)})",
            f"lower({qcol(alias, 'move_to')})<>lower({qlit(ZERO)})",
            f"{qcol(alias, 'move_from')}<>{qcol(alias, 'move_to')}",
            f"{qcol(alias, 'nft')}<>''",
            f"{qcol(alias, 'collection_contract')}<>''",
        ])
        for field, raw in p.roles:
            expr = qcol(alias, field)
            if is_variable(raw):
                variables[variable_name(raw)].append(expr)
                wheres.append(f"{expr}<>''")
            else:
                wheres.append(f"{expr}={qlit(raw)}")
    for exprs in variables.values():
        for left, right in zip(exprs, exprs[1:]):
            wheres.append(f"{left}={right}")

    for left, right in itertools.combinations(branch.event_patterns, 2):
        wheres.append(f"{qcol(aliases[left.pattern_id], 'event_id')}<>{qcol(aliases[right.pattern_id], 'event_id')}")

    def resolve(raw: Any) -> str:
        if not isinstance(raw, str):
            return qlit(raw)
        if raw.startswith("$"):
            vals = variables.get(variable_name(raw), [])
            if not vals:
                raise ValueError(f"unbound SQL variable: {raw}")
            return vals[0]
        if "." in raw:
            pid, field = raw.split(".", 1)
            return qcol(aliases[pid], field)
        return qlit(raw)

    for c in branch.constraints:
        typ = str(c.get("type") or "")
        if typ == "order":
            seq = [str(x) for x in c.get("sequence", []) or []]
            for left, right in zip(seq, seq[1:]):
                wheres.append(f"{qcol(aliases[right], 'order_seq')}>{qcol(aliases[left], 'order_seq')}")
        elif typ == "strict_time_order":
            seq = [str(x) for x in c.get("sequence", []) or []]
            for left, right in zip(seq, seq[1:]):
                wheres.append(f"{qcol(aliases[right], 'block_ts')}>{qcol(aliases[left], 'block_ts')}")
        elif typ == "within":
            earlier = aliases[str(c["earlier"])]
            later = aliases[str(c["later"])]
            delta = int(c.get("delta_seconds") or 86400)
            wheres.append(f"{qcol(later, 'order_seq')}>{qcol(earlier, 'order_seq')}")
            wheres.append(f"{qcol(later, 'block_ts')}<={qcol(earlier, 'block_ts')}+{delta}")
        elif typ == "not_equal":
            wheres.append(f"{resolve(c.get('left'))}<>{resolve(c.get('right'))}")
        elif typ == "less_than":
            wheres.append(f"{resolve(c.get('left'))}<{resolve(c.get('right'))}")
        elif typ == "min_distinct":
            exprs = [resolve(v) for v in c.get("values", []) or []]
            wheres.append(f"{distinct_value_count(exprs)}>={int(c.get('min_count') or 0)}")
        elif typ == "distinct_count_between":
            exprs = [resolve(v) for v in c.get("values", []) or []]
            count = distinct_value_count(exprs)
            wheres.append(f"{count}>={int(c.get('min_count') or 0)}")
            wheres.append(f"{count}<={int(c.get('max_count') or 2**31-1)}")
        elif typ == "distinct_event_ids":
            ids = [str(x) for x in c.get("events", []) or []]
            for left, right in itertools.combinations(ids, 2):
                wheres.append(f"{qcol(aliases[left], 'event_id')}<>{qcol(aliases[right], 'event_id')}")
        elif typ == "distinct_transactions":
            ids = [str(x) for x in c.get("events", []) or []]
            for name in ids:
                wheres.append(f"{qcol(aliases[name], 'tx_hash')}<>''")
            for left, right in itertools.combinations(ids, 2):
                wheres.append(f"{qcol(aliases[left], 'tx_hash')}<>{qcol(aliases[right], 'tx_hash')}")
        elif typ == "no_event":
            anchor = aliases[str(c.get("after") or "root")]
            neg = "neg"
            terms = [
                f"{qcol(neg, 'order_seq')}>{qcol(anchor, 'order_seq')}",
                f"{qcol(neg, 'block_ts')}<={qcol(anchor, 'block_ts')}+{int(c.get('within_seconds') or 86400)}",
            ]
            types = [str(x) for x in c.get("event_type", []) or []]
            if types:
                terms.append(f"{qcol(neg, 'event_type')} IN ({','.join(qlit(x) for x in types)})")
            for key, field in (("src", "move_from"), ("dst", "move_to"), ("nft", "nft"), ("collection", "collection_contract")):
                if c.get(key) is not None:
                    terms.append(f"{qcol(neg, field)}={resolve(c.get(key))}")
            wheres.append(f"NOT EXISTS (SELECT 1 FROM {table} {neg} WHERE {' AND '.join(terms)})")
        else:
            raise ValueError(f"unsupported SQL constraint: {typ}")

    select: list[str] = []
    pattern_ids = [p.pattern_id for p in branch.event_patterns]
    variable_names = sorted(variables)
    for pid in pattern_ids:
        select.append(f"{qcol(aliases[pid], 'event_id')} AS ev_{pid}")
        select.append(f"{qcol(aliases[pid], 'order_seq')} AS ord_{pid}")
    for name in variable_names:
        select.append(f"{variables[name][0]} AS var_{name}")
    from_sql = ", ".join(f"{table} {aliases[p.pattern_id]}" for p in branch.event_patterns)
    sql = f"SELECT {', '.join(select)} FROM {from_sql} WHERE {' AND '.join(wheres)}"
    return sql, pattern_ids, variable_names


def _execute_compiled(
    conn: sqlite3.Connection,
    plan: FullBindingPlan,
    compiled: list[tuple[str, list[str], list[str]]],
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    rows: list[dict[str, Any]] = []
    engine_s = 0.0
    projection_s = 0.0
    for branch, (sql, pattern_ids, variable_names) in zip(plan.branches, compiled):
        tq = time.perf_counter()
        cursor = conn.execute(sql)
        for result in cursor:
            tproject = time.perf_counter()
            events = {pid: str(result[f"ev_{pid}"]) for pid in pattern_ids}
            orders = {pid: int(result[f"ord_{pid}"]) for pid in pattern_ids}
            bindings = {name: str(result[f"var_{name}"]) for name in variable_names}
            rows.append(canonical_row(plan.query_id, branch.branch_id, events, orders, bindings))
            projection_s += time.perf_counter() - tproject
        engine_s += time.perf_counter() - tq
    tcanon = time.perf_counter()
    normalized, material = canonicalize_rows(rows)
    return normalized, {
        "sql_execute_fetch_s": engine_s,
        "row_projection_s": projection_s,
        "result_canonicalize_s": time.perf_counter() - tcanon,
        **material,
    }


def run_query(
    db: Path,
    query_json: Path,
    outdir: Path,
    emit_sql: bool = True,
    *,
    warmup_runs: int = 1,
    phase_marker: Path | None = None,
) -> dict[str, Any]:
    outdir.mkdir(parents=True, exist_ok=True)
    _, plan = load_full_binding_plan(query_json)
    t_all = time.perf_counter()
    t0 = time.perf_counter()
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    configure_sqlite(conn)
    store_open_s = time.perf_counter() - t0
    compiled_sql: dict[str, str] = {}
    compiled: list[tuple[str, list[str], list[str]]] = []
    compile_s = 0.0
    try:
        for branch in plan.branches:
            tc = time.perf_counter()
            sql, pattern_ids, variable_names = compile_branch_sql(branch)
            compile_s += time.perf_counter() - tc
            compiled_sql[branch.branch_id] = sql
            compiled.append((sql, pattern_ids, variable_names))

        warmup_s = 0.0
        for _ in range(max(0, warmup_runs)):
            twarm = time.perf_counter()
            _execute_compiled(conn, plan, compiled)
            warmup_s += time.perf_counter() - twarm

        if phase_marker is not None:
            phase_marker.parent.mkdir(parents=True, exist_ok=True)
            phase_marker.write_text("query\n", encoding="utf-8")
        try:
            normalized, measured = _execute_compiled(conn, plan, compiled)
        finally:
            if phase_marker is not None:
                phase_marker.unlink(missing_ok=True)
    finally:
        conn.close()
    tw = time.perf_counter()
    instances_path = outdir / "instances.csv"
    write_normalized_csv(instances_path, normalized)
    csv_write_s = time.perf_counter() - tw
    material = {
        "instances_csv": str(instances_path),
        **{k: measured[k] for k in ("result_count", "duplicate_count", "result_digest_sha256")},
    }
    query_latency_s = (
        float(measured["sql_execute_fetch_s"])
        + float(measured["result_canonicalize_s"])
    )
    report = {
        "system": "SQL",
        "engine": "SQLite JSON-to-SQL full-instance materialization",
        "query": plan.query_id,
        "result_semantics": plan.result_semantics,
        **material,
        "timing": {
            "store_open_s": store_open_s,
            "compile_s": compile_s,
            "warmup_runs": max(0, warmup_runs),
            "warmup_s": warmup_s,
            "sql_execute_fetch_s": measured["sql_execute_fetch_s"],
            "row_projection_s": measured["row_projection_s"],
            "result_canonicalize_s": measured["result_canonicalize_s"],
            "csv_write_s": csv_write_s,
            "query_latency_s": query_latency_s,
            "wall_s": time.perf_counter() - t_all,
            "runtime_load_excluded_from_query_latency": True,
            "same_connection_warmup": True,
            "offline_index_build_excluded": True,
            "result_file_write_excluded_from_query_latency": True,
        },
    }
    if emit_sql:
        (outdir / "compiled_sql.json").write_text(json.dumps(compiled_sql, indent=2), encoding="utf-8")
    (outdir / "execution_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description="Run true SQL NFT full-binding query.")
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--query-json", required=True, type=Path)
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--no-emit-sql", action="store_true")
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--phase-marker", default="", type=Path)
    args = ap.parse_args()
    report = run_query(
        args.db,
        args.query_json,
        args.outdir,
        emit_sql=not args.no_emit_sql,
        warmup_runs=max(0, args.warmup_runs),
        phase_marker=args.phase_marker if str(args.phase_marker).strip() not in {"", "."} else None,
    )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
