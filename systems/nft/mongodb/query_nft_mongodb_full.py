#!/usr/bin/env python3

from __future__ import annotations

import argparse
import itertools
import json
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from pymongo import MongoClient

from systems.nft.common.canonical_results import canonical_row, canonicalize_rows, write_normalized_csv
from systems.nft.common.full_binding_spec import BranchPlan, FullBindingPlan, is_variable, load_full_binding_plan, variable_name


ZERO = "0x0000000000000000000000000000000000000000"
EVENT_FIELDS = (
    "event_id", "event_type", "move_from", "move_to", "nft",
    "collection_contract", "block_ts", "order_seq", "tx_hash",
)


def event_ref(pattern_id: str, field: str) -> str:
    return f"${field}" if pattern_id == "root" else f"$j_{pattern_id}.{field}"


def lookup_name(pattern_id: str, field: str) -> str:
    return f"v_{pattern_id}_{field}"


def mandatory_expr(field_prefix: str = "$") -> list[dict[str, Any]]:
    return [
        {"$ne": [f"{field_prefix}move_from", ""]},
        {"$ne": [f"{field_prefix}move_to", ""]},
        {"$ne": [{"$toLower": f"{field_prefix}move_from"}, ZERO]},
        {"$ne": [{"$toLower": f"{field_prefix}move_to"}, ZERO]},
        {"$ne": [f"{field_prefix}move_from", f"{field_prefix}move_to"]},
        {"$ne": [f"{field_prefix}nft", ""]},
        {"$ne": [f"{field_prefix}collection_contract", ""]},
    ]


def variable_occurrences(branch: BranchPlan) -> dict[str, list[tuple[str, str]]]:
    occurrences: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for pattern in branch.event_patterns:
        for field, raw in pattern.roles:
            if is_variable(raw):
                occurrences[variable_name(raw)].append((pattern.pattern_id, field))
    return occurrences


def final_value(
    raw: Any,
    aliases: dict[str, str],
    variables: dict[str, list[tuple[str, str]]],
) -> Any:
    if not isinstance(raw, str):
        return {"$literal": raw}
    if is_variable(raw):
        occurrences = variables.get(variable_name(raw), [])
        if not occurrences:
            raise ValueError(f"unbound MongoDB variable: {raw}")
        pattern_id, field = occurrences[0]
        return event_ref(pattern_id, field)
    if "." in raw:
        pattern_id, field = raw.split(".", 1)
        if pattern_id not in aliases or field not in EVENT_FIELDS:
            raise ValueError(f"unsupported MongoDB field reference: {raw}")
        return event_ref(pattern_id, field)
    return {"$literal": raw}


def distinct_count(expressions: list[Any]) -> dict[str, Any]:
    return {"$size": {"$setUnion": [expressions, []]}}


def final_constraints(
    branch: BranchPlan,
    variables: dict[str, list[tuple[str, str]]],
) -> list[dict[str, Any]]:
    aliases = {pattern.pattern_id: f"j_{pattern.pattern_id}" for pattern in branch.event_patterns}
    conditions: list[dict[str, Any]] = []
    for left, right in itertools.combinations(branch.event_patterns, 2):
        conditions.append({"$ne": [event_ref(left.pattern_id, "event_id"), event_ref(right.pattern_id, "event_id")]})

    resolve = lambda raw: final_value(raw, aliases, variables)
    for constraint in branch.constraints:
        kind = str(constraint.get("type") or "")
        if kind == "order":
            sequence = [str(value) for value in constraint.get("sequence", []) or []]
            for left, right in zip(sequence, sequence[1:]):
                conditions.append({"$gt": [event_ref(right, "order_seq"), event_ref(left, "order_seq")]})
        elif kind == "strict_time_order":
            sequence = [str(value) for value in constraint.get("sequence", []) or []]
            for left, right in zip(sequence, sequence[1:]):
                conditions.append({"$gt": [event_ref(right, "block_ts"), event_ref(left, "block_ts")]})
        elif kind == "within":
            earlier = str(constraint["earlier"])
            later = str(constraint["later"])
            delta = int(constraint.get("delta_seconds") or 86400)
            conditions.extend([
                {"$gt": [event_ref(later, "order_seq"), event_ref(earlier, "order_seq")]},
                {"$lte": [event_ref(later, "block_ts"), {"$add": [event_ref(earlier, "block_ts"), delta]}]},
            ])
        elif kind == "not_equal":
            conditions.append({"$ne": [resolve(constraint.get("left")), resolve(constraint.get("right"))]})
        elif kind == "less_than":
            conditions.append({"$lt": [resolve(constraint.get("left")), resolve(constraint.get("right"))]})
        elif kind in {"min_distinct", "distinct_count_between"}:
            values = [resolve(value) for value in constraint.get("values", []) or []]
            count = distinct_count(values)
            conditions.append({"$gte": [count, int(constraint.get("min_count") or 0)]})
            if kind == "distinct_count_between":
                conditions.append({"$lte": [count, int(constraint.get("max_count") or 2**31 - 1)]})
        elif kind == "distinct_event_ids":
            ids = [str(value) for value in constraint.get("events", []) or []]
            for left, right in itertools.combinations(ids, 2):
                conditions.append({"$ne": [event_ref(left, "event_id"), event_ref(right, "event_id")]})
        elif kind == "distinct_transactions":
            ids = [str(value) for value in constraint.get("events", []) or []]
            for pattern_id in ids:
                conditions.append({"$ne": [event_ref(pattern_id, "tx_hash"), ""]})
            for left, right in itertools.combinations(ids, 2):
                conditions.append({"$ne": [event_ref(left, "tx_hash"), event_ref(right, "tx_hash")]})
        elif kind == "no_event":
            raise ValueError("no_event is not used by the selected NFT workload and requires an anti-lookup stage")
        else:
            raise ValueError(f"unsupported MongoDB constraint: {kind}")
    return conditions


def compile_branch_pipeline(branch: BranchPlan) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    patterns = list(branch.event_patterns)
    variables = variable_occurrences(branch)
    root = patterns[0]
    root_expr = mandatory_expr()
    root_match: dict[str, Any] = {"event_type": {"$in": list(root.event_types)}}
    for field, raw in root.roles:
        if is_variable(raw):
            root_expr.append({"$ne": [f"${field}", ""]})
        else:
            root_match[field] = raw
    pipeline: list[dict[str, Any]] = [{"$match": {**root_match, "$expr": {"$and": root_expr}}}]

    assigned: list[str] = [root.pattern_id]
    pattern_by_id = {pattern.pattern_id: pattern for pattern in patterns}
    for pattern in patterns[1:]:
        let_values = {
            lookup_name(pattern_id, field): event_ref(pattern_id, field)
            for pattern_id in assigned
            for field in EVENT_FIELDS
        }
        static_match: dict[str, Any] = {"event_type": {"$in": list(pattern.event_types)}}
        expressions = mandatory_expr()

        for field, raw in pattern.roles:
            if is_variable(raw):
                name = variable_name(raw)
                earlier = next((item for item in variables[name] if item[0] in assigned), None)
                if earlier is None:
                    expressions.append({"$ne": [f"${field}", ""]})
                else:
                    expressions.append({"$eq": [f"${field}", f"$${lookup_name(*earlier)}"]})
            else:
                static_match[field] = raw

        for prior in assigned:
            expressions.append({"$ne": ["$event_id", f"$${lookup_name(prior, 'event_id')}"]})

        for constraint in branch.constraints:
            kind = str(constraint.get("type") or "")
            if kind in {"order", "strict_time_order"}:
                sequence = [str(value) for value in constraint.get("sequence", []) or []]
                field = "order_seq" if kind == "order" else "block_ts"
                for left, right in zip(sequence, sequence[1:]):
                    if right == pattern.pattern_id and left in assigned:
                        expressions.append({"$gt": [f"${field}", f"$${lookup_name(left, field)}"]})
            elif kind == "within" and str(constraint.get("later")) == pattern.pattern_id:
                earlier = str(constraint.get("earlier"))
                if earlier in assigned:
                    delta = int(constraint.get("delta_seconds") or 86400)
                    expressions.extend([
                        {"$gt": ["$order_seq", f"$${lookup_name(earlier, 'order_seq')}"]},
                        {"$lte": ["$block_ts", {"$add": [f"$${lookup_name(earlier, 'block_ts')}", delta]}]},
                    ])
            elif kind == "distinct_transactions" and pattern.pattern_id in {
                str(value) for value in constraint.get("events", []) or []
            }:
                expressions.append({"$ne": ["$tx_hash", ""]})
                for prior in assigned:
                    if prior in {str(value) for value in constraint.get("events", []) or []}:
                        expressions.append({"$ne": ["$tx_hash", f"$${lookup_name(prior, 'tx_hash')}"]})


        def localize(value):
            if isinstance(value, list):
                return [localize(v) for v in value]
            if isinstance(value, dict):
                if "$literal" in value:
                    return value
                return {k: localize(v) for k, v in value.items()}
            if isinstance(value, str) and value.startswith("$"):
                if value.startswith("$j_"):
                    owner, field = value[3:].split(".", 1)
                else:
                    owner, field = "root", value[1:]
                if owner == pattern.pattern_id:
                    return "$" + field
                if owner not in assigned:
                    raise LookupError(owner)
                return "$$" + lookup_name(owner, field)
            return value

        for condition in final_constraints(branch, variables):
            try:
                expressions.append(localize(condition))
            except LookupError:
                pass


        reaches = {(str(x), str(y)) for c in branch.constraints
                   if c.get("type") == "strict_time_order"
                   for x, y in zip(c.get("sequence", []), c.get("sequence", [])[1:])}
        while True:
            expanded = reaches | {(a, d) for a, b in reaches for c, d in reaches if b == c}
            if expanded == reaches:
                break
            reaches = expanded
        for c in branch.constraints:
            if (c.get("type") == "within" and str(c.get("earlier")) in assigned
                    and (pattern.pattern_id, str(c.get("later"))) in reaches):
                expressions.append({"$lt": ["$block_ts", {"$add": [
                    "$$" + lookup_name(str(c["earlier"]), "block_ts"),
                    int(c.get("delta_seconds") or 86400)]}]})
        expressions = list({json.dumps(e, sort_keys=True): e for e in expressions}.values())

        encoded = json.dumps(expressions)
        let_values = {k: v for k, v in let_values.items() if ('"$$' + k + '"') in encoded}
        project = {field: 1 for field in EVENT_FIELDS}
        project["_id"] = 0
        pipeline.extend([
            {
                "$lookup": {
                    "from": "nft_events",
                    "let": let_values,
                    "pipeline": [
                        {"$match": static_match},
                        {"$match": {"$expr": {"$and": expressions}}},
                        {"$project": project},
                    ],
                    "as": f"j_{pattern.pattern_id}",
                }
            },
            {"$unwind": f"$j_{pattern.pattern_id}"},
        ])
        assigned.append(pattern.pattern_id)

    conditions = final_constraints(branch, variables)
    if conditions:
        pipeline.append({"$match": {"$expr": {"$and": conditions}}})

    pattern_ids = [pattern.pattern_id for pattern in patterns]
    variable_names = sorted(variables)
    pipeline.append({
        "$project": {
            "_id": 0,
            "event_ids": {pattern_id: event_ref(pattern_id, "event_id") for pattern_id in pattern_ids},
            "orders": {pattern_id: event_ref(pattern_id, "order_seq") for pattern_id in pattern_ids},
            "variables": {
                name: event_ref(variables[name][0][0], variables[name][0][1]) for name in variable_names
            },
        }
    })
    return pipeline, pattern_ids, variable_names


def execute_compiled(
    collection: Any,
    plan: FullBindingPlan,
    compiled: list[tuple[list[dict[str, Any]], list[str], list[str]]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    aggregate_fetch_s = 0.0
    projection_s = 0.0
    for branch, (pipeline, pattern_ids, variable_names) in zip(plan.branches, compiled):
        started = time.perf_counter()
        cursor = collection.aggregate(pipeline, allowDiskUse=True, batchSize=4096)
        for result in cursor:
            project0 = time.perf_counter()
            events = {pattern_id: str(result["event_ids"][pattern_id]) for pattern_id in pattern_ids}
            orders = {pattern_id: int(result["orders"][pattern_id]) for pattern_id in pattern_ids}
            bindings = {name: str(result["variables"][name]) for name in variable_names}
            rows.append(canonical_row(plan.query_id, branch.branch_id, events, orders, bindings))
            projection_s += time.perf_counter() - project0
        aggregate_fetch_s += time.perf_counter() - started
    canonical0 = time.perf_counter()
    normalized, material = canonicalize_rows(rows)
    return normalized, {
        "aggregate_execute_fetch_s": aggregate_fetch_s,
        "row_projection_s": projection_s,
        "result_canonicalize_s": time.perf_counter() - canonical0,
        **material,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run MongoDB NFT full-binding query.")
    parser.add_argument("--query-json", required=True, type=Path)
    parser.add_argument("--outdir", required=True, type=Path)
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--phase-marker", default="", type=Path)
    parser.add_argument("--uri", default=os.getenv("MONGODB_URI", "mongodb://mongo:27017"))
    parser.add_argument("--database", default=os.getenv("MONGODB_DATABASE", "funnelq"))
    args = parser.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    _, plan = load_full_binding_plan(args.query_json)
    compile0 = time.perf_counter()
    compiled = [compile_branch_pipeline(branch) for branch in plan.branches]
    compile_s = time.perf_counter() - compile0
    compiled_json = {branch.branch_id: pipeline for branch, (pipeline, _, _) in zip(plan.branches, compiled)}

    wall0 = time.perf_counter()
    open0 = time.perf_counter()
    client = MongoClient(args.uri, connectTimeoutMS=30_000, serverSelectionTimeoutMS=30_000)
    collection = client[args.database]["nft_events"]
    collection.database.command("ping")
    store_open_s = time.perf_counter() - open0
    try:
        warmup_s = 0.0
        for _ in range(max(0, args.warmup_runs)):
            warm0 = time.perf_counter()
            execute_compiled(collection, plan, compiled)
            warmup_s += time.perf_counter() - warm0
        marker = args.phase_marker if str(args.phase_marker).strip() not in {"", "."} else None
        if marker is not None:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("query\n", encoding="utf-8")
        try:
            normalized, measured = execute_compiled(collection, plan, compiled)
        finally:
            if marker is not None:
                marker.unlink(missing_ok=True)
    finally:
        client.close()

    write0 = time.perf_counter()
    instances_path = args.outdir / "instances.csv"
    write_normalized_csv(instances_path, normalized)
    csv_write_s = time.perf_counter() - write0
    query_latency_s = float(measured["aggregate_execute_fetch_s"]) + float(measured["result_canonicalize_s"])
    report = {
        "system": "MongoDB",
        "engine": "MongoDB aggregation-pipeline full-instance materialization",
        "query": plan.query_id,
        "result_semantics": plan.result_semantics,
        "instances_csv": str(instances_path),
        **{key: measured[key] for key in ("result_count", "duplicate_count", "result_digest_sha256")},
        "timing": {
            "store_open_s": store_open_s,
            "compile_s": compile_s,
            "warmup_runs": max(0, args.warmup_runs),
            "warmup_s": warmup_s,
            "aggregate_execute_fetch_s": measured["aggregate_execute_fetch_s"],
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
        "server_settings": {"allowDiskUse": True, "cursor_batch_size": 4096},
    }
    (args.outdir / "compiled_pipeline.json").write_text(
        json.dumps(compiled_json, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (args.outdir / "execution_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
