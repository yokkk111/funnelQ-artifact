#!/usr/bin/env python3
"""Official FunnelQ/HVCEQ full-event-binding NFT query entry.

The Rust executor performs the system-specific funnel stages (anchor recall,
Base synopsis, and Refined synopsis) and reuses the same compact integer event
runtime to enumerate exact selected-five matches.  Python resolves dictionaries
only for matched events and materializes the shared canonical result contract.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import json
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


def _path_supplied(path: object) -> bool:
    return str(path or "").strip() not in {"", "."}

from systems.nft.common.canonical_results import canonical_row, canonicalize_rows, write_normalized_csv
from systems.nft.common.full_binding_spec import load_full_binding_plan, query_path
from systems.nft.common.generic_exact import iter_plan_matches, normalize_event
from systems.nft.common.nft_semantics import configure_sqlite
from systems.nft.common.platform_paths import cargo_binary
from systems.nft.hvceq.selected5_query_contract import (
    SELECTED5,
    validate_selected5_query_contract,
)

def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_candidates(path: Path) -> list[dict[str, int]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    out: list[dict[str, int]] = []
    seen: set[tuple[int, int]] = set()
    for row in rows:
        closure_id = int(row["closure_id"])
        anchor_order = int(row["anchor_order"])
        key = (closure_id, anchor_order)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "closure_id": closure_id,
            "anchor_order": anchor_order,
            "end_ts": int(row["end_ts"]),
            "collection_id": int(row.get("collection_id") or 0),
        })
    out.sort(key=lambda r: (r["anchor_order"], r["closure_id"]))
    return out


def _read_exact_match_orders(path: Path) -> list[tuple[int, int | None, int]]:
    """Read the compact Rust exact-match projection.

    Only event order identifiers cross the Rust/Python boundary.  The expensive
    event/value dictionaries are resolved later and only for events that occur
    in final matches.
    """
    out: list[tuple[int, int | None, int]] = []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            middle_raw = str(row.get("middle_order") or "").strip()
            out.append((
                int(row["root_order"]),
                int(middle_raw) if middle_raw else None,
                int(row["back_order"]),
            ))
    return out


def _load_value_projection(
    conn: sqlite3.Connection,
    value_kind: str,
    value_ids: set[int],
) -> dict[int, str]:
    result: dict[int, str] = {}
    values = sorted(value_ids)
    for chunk in _chunks(values):
        marks = ",".join("?" for _ in chunk)
        sql = (
            "SELECT value_id,value_text FROM nft_role_value_dict "
            f"WHERE value_kind=? AND value_id IN ({marks})"
        )
        for value_id, value_text in conn.execute(sql, [value_kind, *chunk]):
            result[int(value_id)] = str(value_text)
    missing = [value_id for value_id in values if value_id not in result]
    if missing:
        raise RuntimeError(
            f"matched-event projection is missing {value_kind} dictionary values; "
            f"sample={missing[:20]}"
        )
    return result


def _load_matched_event_projection(
    conn: sqlite3.Connection,
    matches: list[tuple[int, int | None, int]],
) -> tuple[dict[int, dict[str, Any]], dict[str, dict[int, str]]]:
    orders = sorted({
        order
        for root_order, middle_order, back_order in matches
        for order in (root_order, middle_order, back_order)
        if order is not None
    })
    events: dict[int, dict[str, Any]] = {}
    for chunk in _chunks(orders):
        marks = ",".join("?" for _ in chunk)
        sql = (
            "SELECT order_seq,event_id,src_addr_id,dst_addr_id,nft_id,collection_id "
            f"FROM nft_role_transition_edges WHERE order_seq IN ({marks})"
        )
        for row in conn.execute(sql, chunk):
            order_seq = int(row[0])
            events[order_seq] = {
                "order_seq": order_seq,
                "event_id": str(row[1]),
                "src_addr_id": int(row[2]),
                "dst_addr_id": int(row[3]),
                "nft_id": int(row[4]),
                "collection_id": int(row[5]),
            }
    missing_orders = [order for order in orders if order not in events]
    if missing_orders:
        raise RuntimeError(
            "Rust exact output references missing compact events; "
            f"sample={missing_orders[:20]}"
        )

    address_ids = {
        int(event[field])
        for event in events.values()
        for field in ("src_addr_id", "dst_addr_id")
    }
    nft_ids = {int(event["nft_id"]) for event in events.values()}
    collection_ids = {int(event["collection_id"]) for event in events.values()}
    values = {
        "address": _load_value_projection(conn, "address", address_ids),
        "nft": _load_value_projection(conn, "nft", nft_ids),
        "collection": _load_value_projection(conn, "collection", collection_ids),
    }
    return events, values


def _materialize_selected_exact_matches(
    plan: Any,
    matches: list[tuple[int, int | None, int]],
    events: dict[int, dict[str, Any]],
    values: dict[str, dict[int, str]],
) -> list[dict[str, Any]]:
    qid = str(plan.query_id)
    branch_id = str(plan.branches[0].branch_id)
    addresses = values["address"]
    nfts = values["nft"]
    collections = values["collection"]
    out: list[dict[str, Any]] = []

    for root_order, middle_order, back_order in matches:
        root = events[root_order]
        back = events[back_order]
        a = addresses[int(root["src_addr_id"])]
        b = addresses[int(root["dst_addr_id"])]
        x = nfts[int(root["nft_id"])]
        collection = collections[int(root["collection_id"])]
        event_bindings = {
            "root": str(root["event_id"]),
            "back": str(back["event_id"]),
        }
        event_orders = {"root": root_order, "back": back_order}
        bindings = {"A": a, "B": b, "x": x, "collection": collection}

        if middle_order is None:
            if qid == "Q2_CROSS_NFT_RECIPROCAL":
                bindings["y"] = nfts[int(back["nft_id"])]
        else:
            middle = events[middle_order]
            event_bindings = {
                "root": str(root["event_id"]),
                "middle": str(middle["event_id"]),
                "back": str(back["event_id"]),
            }
            event_orders = {
                "root": root_order,
                "middle": middle_order,
                "back": back_order,
            }
            bindings["C"] = addresses[int(middle["dst_addr_id"])]
            if qid == "Q4_THREE_NFT_THREE_ACTOR_CYCLE":
                bindings["y"] = nfts[int(middle["nft_id"])]
                bindings["z"] = nfts[int(back["nft_id"])]
            elif qid == "Q5_TWO_NFT_RETURN_CYCLE":
                bindings["y"] = nfts[int(middle["nft_id"])]

        out.append(canonical_row(qid, branch_id, event_bindings, event_orders, bindings))
    return out


def _chunks(values: list[int], size: int = 800) -> Iterable[list[int]]:
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _load_closure_descriptors(
    conn: sqlite3.Connection,
    candidates: list[dict[str, int]],
    delta_seconds: int | None = None,
) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    ids = [int(r["closure_id"]) for r in candidates]
    for chunk in _chunks(ids):
        marks = ",".join("?" for _ in chunk)
        sql = (
            "SELECT closure_id,anchor_order_seq,collection_id,start_ts,end_ts "
            f"FROM nft_closure_summary WHERE closure_id IN ({marks})"
        )
        for row in conn.execute(sql, chunk):
            start_ts = int(row[3])
            end_ts = int(row[4])
            if delta_seconds is not None:
                end_ts = min(end_ts, start_ts + int(delta_seconds))
            result[int(row[0])] = {
                "closure_id": int(row[0]),
                "anchor_order": int(row[1]),
                "collection_id": int(row[2]),
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
    missing = [x for x in ids if x not in result]
    if missing:
        raise RuntimeError(f"Cand2 references missing closure records; sample={missing[:20]}")
    return result


def _row_to_event(row: sqlite3.Row, *, include_tx: bool = False) -> dict[str, Any]:
    # The compact runtime stores exactly the fields required by the neutral
    # full-binding contract.  Wide block/transaction columns are intentionally
    # absent because none of the final-five predicates or canonical result keys
    # depend on them.
    raw = {
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "nft": row["nft"],
            "collection_contract": row["collection_contract"],
            "move_from": row["move_from"],
            "move_to": row["move_to"],
            "block_ts": row["block_ts"],
            "order_seq": row["order_seq"],
    }
    if include_tx:
        raw["tx_hash"] = str(row["tx_id"])
    return normalize_event(raw)


def _load_collection_ranges(
    conn: sqlite3.Connection,
    descriptors: dict[int, dict[str, Any]],
    *,
    include_tx: bool = False,
) -> tuple[dict[int, list[dict[str, Any]]], int]:
    by_collection_desc: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for desc in descriptors.values():
        by_collection_desc[int(desc["collection_id"])].append(desc)

    events_by_collection: dict[int, list[dict[str, Any]]] = {}
    loaded = 0
    for collection_id, descs in by_collection_desc.items():
        min_order = min(int(d["anchor_order"]) for d in descs)
        max_ts = max(int(d["end_ts"]) for d in descs)
        tx_select = ", tx.tx_id AS tx_id" if include_tx else ""
        tx_join = "JOIN txdb.nft_event_tx_ids tx ON tx.order_seq=e.order_seq" if include_tx else ""
        cursor = conn.execute(
            f"""
            SELECT e.event_id,
                   t.event_type,
                   e.block_ts,
                   e.order_seq,
                   nft.value_text AS nft,
                   coll.value_text AS collection_contract,
                   src.value_text AS move_from,
                   dst.value_text AS move_to
                   {tx_select}
            FROM nft_role_transition_edges e
            {tx_join}
            JOIN nft_event_type_dict t ON t.event_type_id=e.event_type_id
            JOIN nft_role_value_dict nft
              ON nft.value_kind='nft' AND nft.value_id=e.nft_id
            JOIN nft_role_value_dict coll
              ON coll.value_kind='collection' AND coll.value_id=e.collection_id
            JOIN nft_role_value_dict src
              ON src.value_kind='address' AND src.value_id=e.src_addr_id
            JOIN nft_role_value_dict dst
              ON dst.value_kind='address' AND dst.value_id=e.dst_addr_id
            WHERE e.collection_id=? AND e.order_seq>=? AND e.block_ts<=?
            ORDER BY e.order_seq,e.event_id
            """,
            (collection_id, min_order, max_ts),
        )
        rows = [_row_to_event(row, include_tx=include_tx) for row in cursor]
        events_by_collection[collection_id] = rows
        loaded += len(rows)
    return events_by_collection, loaded


def _match_loaded_closures(
    plan: Any,
    candidates: list[dict[str, int]],
    descriptors: dict[int, dict[str, Any]],
    events_by_collection: dict[int, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], int, int]:
    instances: list[dict[str, Any]] = []
    closures_examined = 0
    local_events_examined = 0
    order_arrays = {
        collection: [int(e["order_seq"]) for e in events]
        for collection, events in events_by_collection.items()
    }
    timestamp_arrays = {
        collection: [int(e["block_ts"]) for e in events]
        for collection, events in events_by_collection.items()
    }
    monotonic_timestamps = {
        collection: all(left <= right for left, right in zip(values, values[1:]))
        for collection, values in timestamp_arrays.items()
    }
    for cand in candidates:
        desc = descriptors[int(cand["closure_id"])]
        collection_id = int(desc["collection_id"])
        all_collection_events = events_by_collection.get(collection_id, [])
        orders = order_arrays.get(collection_id, [])
        lo = bisect.bisect_left(orders, int(desc["anchor_order"]))
        timestamps = timestamp_arrays.get(collection_id, [])
        hi = (
            bisect.bisect_right(timestamps, int(desc["end_ts"]), lo=lo)
            if monotonic_timestamps.get(collection_id, False)
            else len(all_collection_events)
        )
        closure_events: list[dict[str, Any]] = []
        root: dict[str, Any] | None = None
        for event in all_collection_events[lo:hi]:
            if int(event["block_ts"]) > int(desc["end_ts"]):
                continue
            closure_events.append(event)
            if int(event["order_seq"]) == int(desc["anchor_order"]):
                root = event
        if root is None:
            raise RuntimeError(
                f"closure {cand['closure_id']} has no root event at order {desc['anchor_order']}"
            )
        closures_examined += 1
        local_events_examined += len(closure_events)
        if plan.query_id in {
            "Q1_RAPID_SAME_NFT_ROUNDTRIP",
            "Q2_CROSS_NFT_RECIPROCAL",
            "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
            "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
            "Q5_TWO_NFT_RETURN_CYCLE",
        }:
            instances.extend(_match_selected_cycle(plan, closure_events, root))
        else:
            instances.extend(iter_plan_matches(plan, closure_events, fixed_root=root))
    return instances, closures_examined, local_events_examined


def _match_selected_cycle(
    plan: Any,
    closure_events: list[dict[str, Any]],
    root: dict[str, Any],
) -> list[dict[str, Any]]:
    """Integer-like direct join for the five selected cycle queries.

    The generic DFS repeatedly normalized and rescanned every closure.  These
    five plans have a fixed two/three-edge cycle shape, so FunnelQ can perform
    its exact closure-local join directly without changing the neutral result
    contract.
    """
    qid = str(plan.query_id)
    branch_id = str(plan.branches[0].branch_id)
    a = str(root["move_from"])
    b = str(root["move_to"])
    x = str(root["nft"])
    collection = str(root["collection_contract"])
    root_order = int(root["order_seq"])
    root_ts = int(root["block_ts"])
    root_tx = str(root.get("tx_hash") or "")
    root_event_id = str(root["event_id"])
    deadline = root_ts + int(plan.delta_seconds)
    if str(root.get("event_type")) != "NFTTrade" or not root_tx:
        return []

    by_src: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in closure_events:
        if (
            str(event.get("event_type")) == "NFTTrade"
            and str(event.get("collection_contract")) == collection
            and int(event.get("order_seq") or 0) > root_order
            and root_ts < int(event.get("block_ts") or 0) <= deadline
        ):
            by_src[str(event.get("move_from") or "")].append(event)

    def tx_distinct(*events: dict[str, Any]) -> bool:
        txs = [str(event.get("tx_hash") or "") for event in events]
        return all(txs) and len(txs) == len(set(txs))

    out: list[dict[str, Any]] = []
    if qid in {"Q1_RAPID_SAME_NFT_ROUNDTRIP", "Q2_CROSS_NFT_RECIPROCAL"}:
        for back in by_src.get(b, []):
            back_event_id = str(back["event_id"])
            if back_event_id == root_event_id:
                continue
            y = str(back["nft"])
            if str(back["move_to"]) != a:
                continue
            if qid.startswith("Q1_") and y != x:
                continue
            if qid.startswith("Q2_") and y == x:
                continue
            if not tx_distinct(root, back):
                continue
            bindings = {"A": a, "B": b, "x": x, "collection": collection}
            if qid.startswith("Q2_"):
                bindings["y"] = y
            out.append(canonical_row(
                qid,
                branch_id,
                {"root": root_event_id, "back": back_event_id},
                {"root": root_order, "back": int(back["order_seq"])},
                bindings,
            ))
        return out

    for middle in by_src.get(b, []):
        middle_event_id = str(middle["event_id"])
        if middle_event_id == root_event_id:
            continue
        c = str(middle["move_to"])
        y = str(middle["nft"])
        middle_order = int(middle["order_seq"])
        middle_ts = int(middle["block_ts"])
        if len({a, b, c}) != 3:
            continue
        if qid == "Q3_SAME_NFT_THREE_ACTOR_CYCLE" and y != x:
            continue
        if qid in {
            "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
            "Q5_TWO_NFT_RETURN_CYCLE",
        } and y == x:
            continue
        for back in by_src.get(c, []):
            back_event_id = str(back["event_id"])
            if len({root_event_id, middle_event_id, back_event_id}) != 3:
                continue
            z = str(back["nft"])
            if (
                str(back["move_to"]) != a
                or int(back["order_seq"]) <= middle_order
                or int(back["block_ts"]) <= middle_ts
            ):
                continue
            if qid == "Q3_SAME_NFT_THREE_ACTOR_CYCLE" and z != x:
                continue
            if qid == "Q4_THREE_NFT_THREE_ACTOR_CYCLE" and len({x, y, z}) != 3:
                continue
            if qid == "Q5_TWO_NFT_RETURN_CYCLE" and z != x:
                continue
            if not tx_distinct(root, middle, back):
                continue
            bindings = {
                "A": a,
                "B": b,
                "C": c,
                "x": x,
                "collection": collection,
            }
            if qid == "Q4_THREE_NFT_THREE_ACTOR_CYCLE":
                bindings.update({"y": y, "z": z})
            elif qid == "Q5_TWO_NFT_RETURN_CYCLE":
                bindings["y"] = y
            out.append(canonical_row(
                qid,
                branch_id,
                {
                    "root": root_event_id,
                    "middle": middle_event_id,
                    "back": back_event_id,
                },
                {
                    "root": root_order,
                    "middle": middle_order,
                    "back": int(back["order_seq"]),
                },
                bindings,
            ))
    return out


def _run_query_legacy(
    db: Path,
    query_json: Path,
    outdir: Path,
    *,
    ablation: str = "full",
    candidate_limit: int = 0,
    anchor_limit: int = 0,
    exe: Path | list[str] | None = None,
    threads: int = 8,
    tx_sidecar_db: Path | None = None,
    warmup_runs: int = 1,
    phase_marker: Path | None = None,
) -> dict[str, Any]:
    outdir.mkdir(parents=True, exist_ok=True)
    query_document, plan = load_full_binding_plan(query_json)
    if plan.query_id in SELECTED5:
        query_contract = validate_selected5_query_contract(query_document)
    else:
        query_contract = {
            "query_id": plan.query_id,
            "status": "NOT_APPLICABLE_LEGACY_QUERY",
        }
    needs_tx = any(
        constraint.get("type") == "distinct_transactions"
        for branch in plan.branches
        for constraint in branch.constraints
    )
    if needs_tx and tx_sidecar_db is None:
        raise ValueError(f"{plan.query_id} requires --tx-sidecar-db for distinct transaction semantics")
    if tx_sidecar_db is not None and not tx_sidecar_db.is_file():
        raise FileNotFoundError(f"FunnelQ transaction sidecar not found: {tx_sidecar_db}")
    crate = Path(__file__).resolve().parent / "local_executor_rust"
    rust_exe = exe.resolve() if exe else cargo_binary(crate, "nft_hvceq_local_executor", build=True)

    candidate_csv = outdir / "cand2_closures.csv"
    candidate_report_path = outdir / "candidate_report.json"
    cmd = [
        str(rust_exe),
        "--db", str(db.resolve()),
        "--query", plan.query_id,
        "--query-json", str(query_json.resolve()),
        "--out-csv", str(candidate_csv.resolve()),
        "--report-json", str(candidate_report_path.resolve()),
        "--mode", "candidates",
        "--ablation", ablation,
        "--limit", str(candidate_limit if candidate_limit > 0 else 2_147_483_647),
        "--anchor-limit", str(anchor_limit if anchor_limit > 0 else 2_147_483_647),
        "--threads", str(max(1, int(threads))),
        "--warmup-runs", str(max(0, int(warmup_runs))),
    ]
    if phase_marker is not None:
        phase_marker.unlink(missing_ok=True)
        cmd += ["--phase-marker", str(phase_marker.resolve())]
    wall0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True)
    (outdir / "hvceq_candidate.stdout.txt").write_text(proc.stdout, encoding="utf-8")
    (outdir / "hvceq_candidate.stderr.txt").write_text(proc.stderr, encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"FunnelQ candidate executor failed ({proc.returncode}): {proc.stderr[-4000:]}")
    if not candidate_report_path.exists() or not candidate_csv.exists():
        raise FileNotFoundError("FunnelQ candidate executor did not emit required artifacts")
    candidate_report = _read_json(candidate_report_path)
    if phase_marker is not None:
        phase_marker.parent.mkdir(parents=True, exist_ok=True)
        phase_marker.write_text("query\n", encoding="utf-8")
    try:
        t_candidate_read = time.perf_counter()
        candidates = _read_candidates(candidate_csv)
        candidate_csv_read_s = time.perf_counter() - t_candidate_read
    finally:
        if phase_marker is not None:
            phase_marker.unlink(missing_ok=True)

    t_open = time.perf_counter()
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    configure_sqlite(conn)
    if needs_tx:
        conn.execute("ATTACH DATABASE ? AS txdb", (str(tx_sidecar_db.resolve()),))
    store_open_s = time.perf_counter() - t_open

    if phase_marker is not None:
        phase_marker.parent.mkdir(parents=True, exist_ok=True)
        phase_marker.write_text("query\n", encoding="utf-8")
    t_load = time.perf_counter()
    try:
        descriptors = _load_closure_descriptors(conn, candidates, plan.delta_seconds)
        events_by_collection, edge_rows_loaded = _load_collection_ranges(
            conn, descriptors, include_tx=needs_tx
        )
    finally:
        conn.close()
        if phase_marker is not None:
            phase_marker.unlink(missing_ok=True)
    closure_input_load_s = time.perf_counter() - t_load

    exact_warmup_s = 0.0
    for _ in range(max(0, int(warmup_runs))):
        twarm = time.perf_counter()
        warm_instances, _, _ = _match_loaded_closures(
            plan, candidates, descriptors, events_by_collection
        )
        canonicalize_rows(warm_instances)
        exact_warmup_s += time.perf_counter() - twarm

    if phase_marker is not None:
        phase_marker.parent.mkdir(parents=True, exist_ok=True)
        phase_marker.write_text("query\n", encoding="utf-8")
    try:
        t_match = time.perf_counter()
        instances, closures_examined, local_events_examined = _match_loaded_closures(
            plan, candidates, descriptors, events_by_collection
        )
        exact_match_s = time.perf_counter() - t_match
        t_canon = time.perf_counter()
        normalized, material = canonicalize_rows(instances)
        result_canonicalize_s = time.perf_counter() - t_canon
    finally:
        if phase_marker is not None:
            phase_marker.unlink(missing_ok=True)
    t_write = time.perf_counter()
    instances_path = outdir / "instances.csv"
    write_normalized_csv(instances_path, normalized)
    csv_write_s = time.perf_counter() - t_write
    material = {"instances_csv": str(instances_path), **material}
    candidate_query_s = float(candidate_report.get("query_s", 0.0))
    candidate_csv_write_s = float(candidate_report.get("csv_write_s", 0.0))
    query_latency_s = (
        candidate_query_s + candidate_csv_write_s + candidate_csv_read_s
        + closure_input_load_s + exact_match_s + result_canonicalize_s
    )
    report = {
        "system": "FunnelQ",
        "engine": "Rust anchor/base/refined funnel + closure-local full-binding exact matcher",
        "query": plan.query_id,
        "query_contract": query_contract,
        "result_semantics": plan.result_semantics,
        **material,
        "candidate_report": str(candidate_report_path),
        "candidate_csv": str(candidate_csv),
        "tx_sidecar_db": str(tx_sidecar_db.resolve()) if needs_tx and tx_sidecar_db else None,
        "candidate_metrics": candidate_report,
        "cand2_closure_count": len(candidates),
        "closures_examined": closures_examined,
        "edge_rows_loaded": edge_rows_loaded,
        "local_events_examined": local_events_examined,
        "ablation": ablation,
        "timing": {
            "candidate_query_s": candidate_query_s,
            "candidate_csv_write_s": candidate_csv_write_s,
            "candidate_csv_read_s": candidate_csv_read_s,
            "store_open_s": store_open_s,
            "closure_input_load_s": closure_input_load_s,
            "warmup_runs": max(0, int(warmup_runs)),
            "exact_warmup_s": exact_warmup_s,
            "exact_match_s": exact_match_s,
            "result_canonicalize_s": result_canonicalize_s,
            "csv_write_s": csv_write_s,
            "query_latency_s": query_latency_s,
            "wall_s": time.perf_counter() - wall0,
            "runtime_load_excluded_from_query_latency": True,
            "query_dependent_closure_input_load_included": True,
            "same_runtime_component_warmup": True,
            "offline_index_build_excluded": True,
            "result_file_write_excluded_from_query_latency": True,
        },
    }
    (outdir / "execution_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report



def run_query(
    db: Path,
    query_json: Path,
    outdir: Path,
    *,
    ablation: str = "full",
    candidate_limit: int = 0,
    anchor_limit: int = 0,
    exe: Path | None = None,
    threads: int = 8,
    tx_sidecar_db: Path | None = None,
    warmup_runs: int = 1,
    phase_marker: Path | None = None,
) -> dict[str, Any]:
    """Run FunnelQ with fused Rust exact execution for the selected five.

    The Rust runtime already loads the compact integer event stream to perform
    Refined pruning.  Reusing that same runtime for exact enumeration removes
    the old query-dependent closure reload.  Python resolves dictionaries only
    for events that occur in final matches and constructs the neutral canonical
    output.  Non-selected legacy queries retain the previous generic fallback.
    """
    query_document, plan = load_full_binding_plan(query_json)
    if plan.query_id not in SELECTED5:
        return _run_query_legacy(
            db,
            query_json,
            outdir,
            ablation=ablation,
            candidate_limit=candidate_limit,
            anchor_limit=anchor_limit,
            exe=exe,
            threads=threads,
            tx_sidecar_db=tx_sidecar_db,
            warmup_runs=warmup_runs,
            phase_marker=phase_marker,
        )

    outdir.mkdir(parents=True, exist_ok=True)
    query_contract = validate_selected5_query_contract(query_document)
    needs_tx = any(
        constraint.get("type") == "distinct_transactions"
        for branch in plan.branches
        for constraint in branch.constraints
    )
    if not needs_tx:
        raise RuntimeError(
            f"selected-five contract unexpectedly lacks distinct transaction semantics: {plan.query_id}"
        )
    if tx_sidecar_db is None:
        raise ValueError(f"{plan.query_id} requires --tx-sidecar-db")
    if not tx_sidecar_db.is_file():
        raise FileNotFoundError(f"FunnelQ transaction sidecar not found: {tx_sidecar_db}")

    crate = Path(__file__).resolve().parent / "local_executor_rust"
    if isinstance(exe, (list, tuple)):
        rust_cmd = [str(part) for part in exe]
    else:
        rust_exe = exe.resolve() if exe else cargo_binary(
            crate, "nft_hvceq_local_executor", build=True
        )
        rust_cmd = [str(rust_exe)]
    exact_orders_csv = outdir / "exact_match_orders.csv"
    candidate_report_path = outdir / "candidate_report.json"
    cmd = rust_cmd + [
        "--db", str(db.resolve()),
        "--query", plan.query_id,
        "--query-json", str(query_json.resolve()),
        "--tx-sidecar-db", str(tx_sidecar_db.resolve()),
        "--out-csv", str(exact_orders_csv.resolve()),
        "--report-json", str(candidate_report_path.resolve()),
        "--mode", "query",
        "--ablation", ablation,
        "--limit", str(candidate_limit if candidate_limit > 0 else 2_147_483_647),
        "--anchor-limit", str(anchor_limit if anchor_limit > 0 else 2_147_483_647),
        "--threads", str(max(1, int(threads))),
        "--warmup-runs", str(max(0, int(warmup_runs))),
    ]
    if phase_marker is not None:
        phase_marker.unlink(missing_ok=True)
        cmd += ["--phase-marker", str(phase_marker.resolve())]

    wall0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True)
    (outdir / "hvceq_candidate.stdout.txt").write_text(proc.stdout, encoding="utf-8")
    (outdir / "hvceq_candidate.stderr.txt").write_text(proc.stderr, encoding="utf-8")
    if phase_marker is not None:
        phase_marker.unlink(missing_ok=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"FunnelQ fused executor failed ({proc.returncode}): {proc.stderr[-4000:]}"
        )
    if not candidate_report_path.exists() or not exact_orders_csv.exists():
        raise FileNotFoundError("FunnelQ fused executor did not emit required artifacts")
    candidate_report = _read_json(candidate_report_path)
    if str(candidate_report.get("output_mode")) != "query":
        raise RuntimeError(
            f"unexpected Rust output mode: {candidate_report.get('output_mode')!r}"
        )

    # Fail closed if an old executable is accidentally reused.  Full and all
    # variants that retain Refined must use the fused posting engine; variants
    # that remove Refined must perform a generic ordered closure scan and must
    # not access the Refined posting at exact-execution time.
    normalized_ablation = ablation.strip().lower().replace("-", "_")
    expected_exact_engine = (
        "generic_closure_scan"
        if normalized_ablation in {"no_refined", "no_pruning"}
        else "refined_posting_fused"
    )
    expected_uses_refined = expected_exact_engine == "refined_posting_fused"
    actual_exact_engine = str(candidate_report.get("exact_engine") or "")
    actual_uses_refined = bool(candidate_report.get("uses_refined_posting"))
    if (
        actual_exact_engine != expected_exact_engine
        or actual_uses_refined != expected_uses_refined
    ):
        raise RuntimeError(
            "FunnelQ exact-engine isolation contract failed: "
            f"ablation={ablation!r} expected_engine={expected_exact_engine!r} "
            f"actual_engine={actual_exact_engine!r} "
            f"expected_uses_refined={expected_uses_refined} "
            f"actual_uses_refined={actual_uses_refined}. "
            "Rebuild the Rust release executable from the patched source."
        )

    if normalized_ablation in {"no_anchor", "no_anchor_directory"}:
        expected_anchor_engine = "generic_anchor_scan"
        expected_uses_anchor_directory = False
    elif normalized_ablation in {"no_pruning", "no_lex_anchor"}:
        expected_anchor_engine = "disabled"
        expected_uses_anchor_directory = False
    else:
        expected_anchor_engine = "anchor_directory"
        expected_uses_anchor_directory = True
    actual_anchor_engine = str(candidate_report.get("anchor_engine") or "")
    actual_uses_anchor_directory = bool(candidate_report.get("uses_anchor_directory"))
    anchor_engine_contract_ok = bool(
        candidate_report.get("anchor_engine_isolation_contract_ok", False)
        and actual_anchor_engine == expected_anchor_engine
        and actual_uses_anchor_directory == expected_uses_anchor_directory
    )
    if not anchor_engine_contract_ok:
        raise RuntimeError(
            "FunnelQ anchor-engine isolation contract failed: "
            f"ablation={ablation!r} expected_engine={expected_anchor_engine!r} "
            f"actual_engine={actual_anchor_engine!r} "
            f"expected_uses_directory={expected_uses_anchor_directory} "
            f"actual_uses_directory={actual_uses_anchor_directory}. "
            "Rebuild the Rust release executable from the no-anchor patch."
        )

    if phase_marker is not None:
        phase_marker.parent.mkdir(parents=True, exist_ok=True)
        phase_marker.write_text("query\n", encoding="utf-8")
    try:
        t_read = time.perf_counter()
        exact_matches = _read_exact_match_orders(exact_orders_csv)
        exact_order_csv_read_s = time.perf_counter() - t_read
    finally:
        if phase_marker is not None:
            phase_marker.unlink(missing_ok=True)
    expected_rows = int(candidate_report.get("rows", -1))
    if expected_rows != len(exact_matches):
        raise RuntimeError(
            "Rust exact output row-count mismatch: "
            f"report={expected_rows} csv={len(exact_matches)}"
        )

    t_open = time.perf_counter()
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    configure_sqlite(conn)
    conn.execute("PRAGMA mmap_size=1073741824")
    conn.execute("PRAGMA cache_size=-131072")
    store_open_s = time.perf_counter() - t_open

    if phase_marker is not None:
        phase_marker.parent.mkdir(parents=True, exist_ok=True)
        phase_marker.write_text("query\n", encoding="utf-8")
    try:
        t_projection = time.perf_counter()
        matched_events, matched_values = _load_matched_event_projection(conn, exact_matches)
        result_projection_load_s = time.perf_counter() - t_projection
        conn.close()

        t_materialize = time.perf_counter()
        instances = _materialize_selected_exact_matches(
            plan, exact_matches, matched_events, matched_values
        )
        result_materialize_s = time.perf_counter() - t_materialize

        t_canon = time.perf_counter()
        normalized, material = canonicalize_rows(instances)
        result_canonicalize_s = time.perf_counter() - t_canon
    finally:
        try:
            conn.close()
        except Exception:
            pass
        if phase_marker is not None:
            phase_marker.unlink(missing_ok=True)

    t_write = time.perf_counter()
    instances_path = outdir / "instances.csv"
    write_normalized_csv(instances_path, normalized)
    csv_write_s = time.perf_counter() - t_write
    material = {"instances_csv": str(instances_path), **material}

    candidate_query_s = float(candidate_report.get("query_s", 0.0))
    exact_match_s = float(candidate_report.get("local_exact_s", 0.0))
    search_only_direct_timer = bool(candidate_report.get("search_only_direct_timer", False))
    if not search_only_direct_timer or "search_only_s" not in candidate_report:
        raise RuntimeError(
            "FunnelQ executable does not expose the direct search-only timer. "
            "Rebuild the Rust release executable from the search-only patch."
        )
    search_only_s = float(candidate_report["search_only_s"])
    search_component_sum_s = candidate_query_s + exact_match_s
    search_timer_overhead_s = search_only_s - search_component_sum_s
    if search_only_s < 0.0 or search_timer_overhead_s < -1e-6:
        raise RuntimeError(
            "invalid FunnelQ search-only timing contract: "
            f"direct={search_only_s:.9f}s "
            f"components={search_component_sum_s:.9f}s"
        )

    exact_order_csv_write_s = float(candidate_report.get("csv_write_s", 0.0))
    closure_input_load_s = 0.0
    candidate_csv_write_s = exact_order_csv_write_s
    candidate_csv_read_s = exact_order_csv_read_s
    output_materialization_s = (
        candidate_csv_write_s
        + candidate_csv_read_s
        + closure_input_load_s
        + result_projection_load_s
        + result_materialize_s
        + result_canonicalize_s
    )
    query_latency_s = search_only_s + output_materialization_s

    report = {
        "system": "FunnelQ",
        "engine": (
            "Rust candidate funnel + "
            f"{actual_exact_engine} + deferred matched-event projection"
        ),
        "exact_engine": actual_exact_engine,
        "uses_refined_posting": actual_uses_refined,
        "fused_refined_exact": bool(candidate_report.get("fused_refined_exact")),
        "exact_engine_isolation_contract_ok": True,
        "anchor_engine": actual_anchor_engine,
        "uses_anchor_directory": actual_uses_anchor_directory,
        "anchor_scan_closures": int(candidate_report.get("anchor_scan_closures", 0)),
        "anchor_directory_entries_read": int(
            candidate_report.get("anchor_directory_entries_read", 0)
        ),
        "anchor_engine_isolation_contract_ok": anchor_engine_contract_ok,
        "query": plan.query_id,
        "query_contract": query_contract,
        "result_semantics": plan.result_semantics,
        **material,
        "candidate_report": str(candidate_report_path),
        "candidate_csv": None,
        "exact_match_orders_csv": str(exact_orders_csv),
        "tx_sidecar_db": str(tx_sidecar_db.resolve()),
        "candidate_metrics": candidate_report,
        "cand2_closure_count": int(candidate_report.get("cand2_refined_hits", 0)),
        "closures_examined": int(
            candidate_report.get(
                "exact_candidate_closures",
                candidate_report.get("cand2_refined_hits", 0),
            )
        ),
        "edge_rows_loaded": 0,
        "runtime_edge_rows_loaded": int(candidate_report.get("edge_rows_loaded", 0)),
        "matched_projection_event_count": len(matched_events),
        "local_events_examined": int(candidate_report.get("local_edges_examined", 0)),
        "ablation": ablation,
        "timing": {
            "candidate_query_s": candidate_query_s,
            "candidate_csv_write_s": candidate_csv_write_s,
            "candidate_csv_read_s": candidate_csv_read_s,
            "closure_input_load_s": closure_input_load_s,
            "exact_match_s": exact_match_s,
            "search_only_s": search_only_s,
            "search_only_direct_timer": search_only_direct_timer,
            "search_component_sum_s": search_component_sum_s,
            "search_timer_overhead_s": search_timer_overhead_s,
            "output_materialization_s": output_materialization_s,
            "result_canonicalize_s": result_canonicalize_s,
            "exact_order_csv_write_s": exact_order_csv_write_s,
            "exact_order_csv_read_s": exact_order_csv_read_s,
            "result_projection_load_s": result_projection_load_s,
            "result_materialize_s": result_materialize_s,
            "store_open_s": store_open_s,
            "warmup_runs": max(0, int(warmup_runs)),
            "csv_write_s": csv_write_s,
            "query_latency_s": query_latency_s,
            "wall_s": time.perf_counter() - wall0,
            "runtime_load_excluded_from_query_latency": True,
            "query_dependent_closure_input_load_included": False,
            "query_dependent_result_projection_included": True,
            "search_only_excludes_output_materialization": True,
            "output_materialization_definition": (
                "exact_order_csv_write+exact_order_csv_read+"
                "matched_event_projection+result_materialization+canonicalization"
            ),
            "closure_reloading_eliminated": True,
            "exact_engine": actual_exact_engine,
            "uses_refined_posting": actual_uses_refined,
            "exact_engine_isolation_contract_ok": True,
            "anchor_engine": actual_anchor_engine,
            "uses_anchor_directory": actual_uses_anchor_directory,
            "anchor_engine_isolation_contract_ok": anchor_engine_contract_ok,
            "same_runtime_component_warmup": True,
            "offline_index_build_excluded": True,
            "result_file_write_excluded_from_query_latency": True,
        },
    }
    (outdir / "execution_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report

def main() -> int:
    ap = argparse.ArgumentParser(description="Run FunnelQ NFT full-event-binding query.")
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--query-json", default="", type=Path)
    ap.add_argument("--query-id", default="")
    ap.add_argument("--query-dir", default=str(ROOT / "queries" / "nft" / "final5"), type=Path)
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--ablation", default="full")
    ap.add_argument("--candidate-limit", type=int, default=0)
    ap.add_argument("--anchor-limit", type=int, default=0)
    ap.add_argument("--exe", default="", type=Path)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--tx-sidecar-db", default="", type=Path)
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--phase-marker", default="", type=Path)
    args = ap.parse_args()
    query_json = args.query_json if _path_supplied(args.query_json) else query_path(args.query_dir, args.query_id)
    report = run_query(
        args.db,
        query_json,
        args.outdir,
        ablation=args.ablation,
        candidate_limit=args.candidate_limit,
        anchor_limit=args.anchor_limit,
        exe=args.exe if _path_supplied(args.exe) else None,
        threads=max(1, args.threads),
        tx_sidecar_db=args.tx_sidecar_db if _path_supplied(args.tx_sidecar_db) else None,
        warmup_runs=max(0, args.warmup_runs),
        phase_marker=args.phase_marker if _path_supplied(args.phase_marker) else None,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
