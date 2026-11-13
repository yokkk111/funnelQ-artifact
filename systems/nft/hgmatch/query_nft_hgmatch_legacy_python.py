#!/usr/bin/env python3


from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
import time
from collections import defaultdict
from bisect import bisect_left
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple, Optional

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from systems.nft.common.nft_semantics import (
    RUNTIME_COLUMNS, CompiledQuery, QueryPattern, configure_sqlite,
    constraints_ok, event_matches_pattern, is_variable_token, load_query,
    materialized_row, output_columns, compile_query, var_name,
)

ROLE_FIELDS = ("seller", "buyer", "nft", "collection_contract", "from_address", "to_address", "move_from", "move_to")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run paper-style NFT HGMatch full-instance query.")
    ap.add_argument("--db", required=True)
    ap.add_argument("--query-json", default="", help="Legacy full-instance query JSON.")
    ap.add_argument("--query-id", default="", help="Compressed final-five QuerySpec: C1,C2,L3_RETURN,L6_NEG_RETURN_SUPPORT,H5_23.")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--threads", type=int, default=1, help="Requested execution width; this reference implementation currently executes one deterministic worker.")
    ap.add_argument("--summary-only", action="store_true", help="Unsupported for fair-cost runs: instances are always materialized.")
    ap.add_argument("--write-bindings", action="store_true")
    ap.add_argument("--limit", type=int, default=1_000_000_000)
    ap.add_argument("--anchor-limit", type=int, default=1_000_000_000)
    ap.add_argument("--window-sec", type=int, default=86400)
    ap.add_argument("--k", type=int, default=2)
    ap.add_argument("--min-c", type=int, default=2)
    return ap.parse_args()


ZERO_ADDR = "0x0000000000000000000000000000000000000000"
FINAL5 = {"C1", "C2", "L3_RETURN", "L6_NEG_RETURN_SUPPORT", "H5_23"}


@dataclass(frozen=True)
class Edge:
    order_seq: int
    block_ts: int
    event_type: str
    collection: str
    nft: str
    src: str
    dst: str


@dataclass(frozen=True)
class Anchor:
    closure_id: int
    order_seq: int
    end_ts: int
    collection: str
    nft: str
    a: str
    b: str
    block_ts: int


def _valid_role_value(x: Any) -> bool:
    s = str(x or "")
    return bool(s) and s.lower() != ZERO_ADDR


def _list_values(values: Iterable[Any], max_items: Optional[int] = None) -> str:
    vals = sorted({str(v) for v in values if str(v) != ""})
    if max_items is not None:
        vals = vals[:max_items]
    return ",".join(vals)


def _orders(values: Iterable[int], max_items: Optional[int] = None) -> str:
    vals = sorted({int(v) for v in values})
    if max_items is not None:
        vals = vals[:max_items]
    return ",".join(str(v) for v in vals)


CANONICAL_COMPRESSED_COLUMNS = [
    "query_id",
    "closure_id",
    "anchor_order",
    "evidence_kind",
    "evidence_orders",
    "evidence_nfts",
    "evidence_addresses",
    "evidence_json",
]


def _canonical_compressed_row(
    query_id: str,
    closure_id: int,
    anchor_order: int,
    evidence_kind: str,
    orders: Iterable[Any] = (),
    nfts: Iterable[Any] = (),
    addresses: Iterable[Any] = (),
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "query_id": query_id,
        "closure_id": int(closure_id),
        "anchor_order": int(anchor_order),
        "evidence_kind": evidence_kind,
        "evidence_orders": _list_values(orders),
        "evidence_nfts": _list_values(nfts),
        "evidence_addresses": _list_values(addresses),
        "evidence_json": json.dumps({
            "compressed_granularity": "closure_anchor",
            **(payload or {}),
        }, ensure_ascii=False, separators=(",", ":")),
    }


def _edge_orders(edges: List[Edge]) -> List[int]:
    return [e.order_seq for e in edges]


def load_final5_hg_edges(conn: sqlite3.Connection) -> Dict[str, Any]:


    t0 = time.perf_counter()
    by_collection: Dict[str, List[Edge]] = defaultdict(list)
    by_src: Dict[str, List[Edge]] = defaultdict(list)
    all_edges: List[Edge] = []
    for r in conn.execute(
        """
        SELECT order_seq, block_ts, event_type, collection_contract, nft, move_from, move_to
        FROM hgmatch_events
        WHERE event_type IN ('NFTTrade','NFTTransfer')
          AND collection_contract<>'' AND nft<>'' AND move_from<>'' AND move_to<>''
          AND lower(move_from)<>lower(?) AND lower(move_to)<>lower(?)
        ORDER BY order_seq
        """,
        (ZERO_ADDR, ZERO_ADDR),
    ):
        src = str(r["move_from"])
        dst = str(r["move_to"])
        if src == dst or not (_valid_role_value(src) and _valid_role_value(dst)):
            continue
        e = Edge(
            order_seq=int(r["order_seq"]),
            block_ts=int(r["block_ts"]),
            event_type=str(r["event_type"]),
            collection=str(r["collection_contract"]),
            nft=str(r["nft"]),
            src=src,
            dst=dst,
        )
        all_edges.append(e)
        by_collection[e.collection].append(e)
        by_src[e.src].append(e)
    order_cache: Dict[int, List[int]] = {}
    return {
        "all": all_edges,
        "by_collection": by_collection,
        "by_src": by_src,
        "order_cache": order_cache,
        "load_s": time.perf_counter() - t0,
    }


def edge_window(index: Dict[str, Any], records: List[Edge], start_order: int, end_ts: int, collection: str = "") -> List[Edge]:
    if not records:
        return []
    rid = id(records)
    orders = index["order_cache"].get(rid)
    if orders is None:
        orders = _edge_orders(records)
        index["order_cache"][rid] = orders
    start = bisect_left(orders, int(start_order))
    out: List[Edge] = []
    for e in records[start:]:
        if e.block_ts > end_ts:
            if e.order_seq > start_order:


                break
        if collection and e.collection != collection:
            continue
        if e.block_ts <= end_ts:
            out.append(e)
    return out


def final5_anchors(index: Dict[str, Any], limit: int, window_sec: int) -> List[Anchor]:
    anchors: List[Anchor] = []
    for e in index["all"]:
        anchors.append(Anchor(
            closure_id=e.order_seq,
            order_seq=e.order_seq,
            end_ts=e.block_ts + int(window_sec),
            collection=e.collection,
            nft=e.nft,
            a=e.src,
            b=e.dst,
            block_ts=e.block_ts,
        ))
        if len(anchors) >= int(limit):
            break
    return anchors


def _c1_header() -> List[str]:
    return CANONICAL_COMPRESSED_COLUMNS


def _c2_header() -> List[str]:
    return CANONICAL_COMPRESSED_COLUMNS


def _return_header() -> List[str]:
    return CANONICAL_COMPRESSED_COLUMNS


def _h5_header() -> List[str]:
    return CANONICAL_COMPRESSED_COLUMNS


def _window_for_anchor(index: Dict[str, Any], a: Anchor) -> List[Edge]:
    return edge_window(index, index["by_collection"].get(a.collection, []), a.order_seq, a.end_ts)


def run_final5_c1(index: Dict[str, Any], writer: csv.DictWriter, anchors: List[Anchor], args: argparse.Namespace) -> Tuple[int, int, float]:
    rows = 0
    local_edges = 0
    csv_s = 0.0
    for a in anchors:
        if rows >= args.limit:
            break
        edges = _window_for_anchor(index, a)
        local_edges += len(edges)
        if not edges:
            continue
        nft_counts: Dict[str, int] = defaultdict(int)
        addrs: Set[str] = set()
        for e in edges:
            nft_counts[e.nft] += 1
            addrs.add(e.src)
            addrs.add(e.dst)
        evidence_nfts = sorted(n for n, c in nft_counts.items() if c >= 2)
        if len(evidence_nfts) < int(args.k) or len(addrs) < 3 or len(addrs) > 6:
            continue
        tw = time.perf_counter()
        writer.writerow(_canonical_compressed_row(
            "C1_collection_micro_community",
            a.closure_id,
            a.order_seq,
            "community_closure",
            orders=(min(e.order_seq for e in edges), max(e.order_seq for e in edges)),
            nfts=evidence_nfts,
            addresses=addrs,
            payload={
                "collection_contract": a.collection,
                "nft_count": len(evidence_nfts),
                "community_size": len(addrs),
                "transition_count": len(edges),
            },
        ))
        csv_s += time.perf_counter() - tw
        rows += 1
    return rows, local_edges, csv_s


def run_final5_c2(index: Dict[str, Any], writer: csv.DictWriter, anchors: List[Anchor], args: argparse.Namespace) -> Tuple[int, int, float]:
    rows = 0
    local_edges = 0
    csv_s = 0.0
    for a in anchors:
        if rows >= args.limit:
            break
        out = [
            e for e in edge_window(index, index["by_src"].get(a.b, []), a.order_seq + 1, a.end_ts, a.collection)
            if e.nft != a.nft
        ]
        local_edges += len(out)
        first_order_by_c: Dict[str, int] = {}
        cs: Set[str] = set()
        out_nfts: Set[str] = set()
        for e in out:
            cs.add(e.dst)
            out_nfts.add(e.nft)
            first_order_by_c[e.dst] = min(first_order_by_c.get(e.dst, e.order_seq), e.order_seq)
        if len(cs) < int(args.min_c):
            continue
        returns_by_d: Dict[str, List[Tuple[str, Edge]]] = defaultdict(list)
        for c, lower in first_order_by_c.items():
            rets = edge_window(index, index["by_src"].get(c, []), lower + 1, a.end_ts, a.collection)
            local_edges += len(rets)
            for r in rets:
                returns_by_d[r.dst].append((c, r))
        best_d = ""
        best: List[Tuple[str, Edge]] = []
        best_source_count = 0
        for d, rs in returns_by_d.items():
            sources = {c for c, _ in rs}
            if d == a.a or len(sources) >= 2:
                if len(sources) > best_source_count or (len(sources) == best_source_count and len(rs) > len(best)):
                    best_d = d
                    best = rs
                    best_source_count = len(sources)
        if not best:
            continue
        tw = time.perf_counter()
        writer.writerow(_canonical_compressed_row(
            "C2_dispersal_then_return_flow",
            a.closure_id,
            a.order_seq,
            "dispersal_return",
            orders=[r.order_seq for _, r in best],
            nfts=set(out_nfts) | {a.nft},
            addresses=set(cs) | {a.a, a.b, best_d},
            payload={
                "A": a.a,
                "B": a.b,
                "anchor_nft": a.nft,
                "dispersed_to": len(cs),
                "dispersed_nfts": len(out_nfts),
                "return_target": best_d,
                "returning_sources": best_source_count,
            },
        ))
        csv_s += time.perf_counter() - tw
        rows += 1
    return rows, local_edges, csv_s


def run_final5_return_support(index: Dict[str, Any], writer: csv.DictWriter, anchors: List[Anchor], args: argparse.Namespace, negative: bool) -> Tuple[int, int, float]:
    rows = 0
    local_edges = 0
    csv_s = 0.0
    for a in anchors:
        if rows >= args.limit:
            break
        if negative:
            resale = [
                e for e in edge_window(index, index["by_src"].get(a.b, []), a.order_seq + 1, a.end_ts, a.collection)
                if e.event_type == "NFTTrade" and e.nft == a.nft
            ]
            local_edges += len(resale)
            if resale:
                continue
        path_count = 0
        c_ids: Set[str] = set()
        nft1_ids: Set[str] = set()
        nft2_ids: Set[str] = set()
        evidence_paths: List[str] = []
        first_order = 10**30
        last_order = -1
        e1s = [
            e for e in edge_window(index, index["by_src"].get(a.b, []), a.order_seq + 1, a.end_ts, a.collection)
            if e.nft != a.nft
        ]
        local_edges += len(e1s)
        for e1 in e1s:
            e2s = [
                e for e in edge_window(index, index["by_src"].get(e1.dst, []), e1.order_seq + 1, a.end_ts, a.collection)
                if e.dst == a.a and e.nft != a.nft
            ]
            local_edges += len(e2s)
            for e2 in e2s:
                path_count += 1
                c_ids.add(e1.dst)
                nft1_ids.add(e1.nft)
                nft2_ids.add(e2.nft)
                first_order = min(first_order, e1.order_seq)
                last_order = max(last_order, e2.order_seq)
                evidence_paths.append(f"{e1.order_seq}>{e2.order_seq}")


        nft_support = len(nft1_ids | nft2_ids)
        if len(c_ids) < 2 or nft_support < 2:
            continue
        tw = time.perf_counter()
        writer.writerow(_canonical_compressed_row(
            "L6_negative_return_support_closure" if negative else "L3_return_to_seller_convergence",
            a.closure_id,
            a.order_seq,
            "negative_return_support" if negative else "return_support",
            orders=evidence_paths + [first_order, last_order],
            nfts=set(nft1_ids) | set(nft2_ids) | {a.nft},
            addresses=set(c_ids) | {a.a, a.b},
            payload={
                "path_count": path_count,
                "C_count": len(c_ids),
                "nft_support": nft_support,
                "negative_condition": "no_anchor_nft_trade_from_B_within_24h" if negative else "",
            },
        ))
        csv_s += time.perf_counter() - tw
        rows += 1
    return rows, local_edges, csv_s


def run_final5_h5(index: Dict[str, Any], writer: csv.DictWriter, anchors: List[Anchor], args: argparse.Namespace) -> Tuple[int, int, float]:
    rows = 0
    local_edges = 0
    csv_s = 0.0
    max_depth = 3
    for a in anchors:
        if rows >= args.limit:
            break
        stack: List[Tuple[str, int, int, Tuple[int, ...], Tuple[str, ...]]] = [(a.b, a.order_seq, 0, tuple(), tuple())]
        found: Optional[Tuple[int, Tuple[int, ...], Tuple[str, ...]]] = None
        while stack:
            cur, last_order, depth, orders, nfts = stack.pop()
            if 2 <= depth <= max_depth and cur == a.a:
                found = (depth, orders, nfts)
                break
            if depth >= max_depth:
                continue
            nxts = [
                e for e in edge_window(index, index["by_src"].get(cur, []), last_order + 1, a.end_ts, a.collection)
                if e.nft != a.nft
            ]
            local_edges += len(nxts)
            for e in nxts:
                stack.append((e.dst, e.order_seq, depth + 1, orders + (e.order_seq,), nfts + (e.nft,)))
        if found is None:
            continue
        depth, orders, nfts = found
        tw = time.perf_counter()
        writer.writerow(_canonical_compressed_row(
            "H5_23",
            a.closure_id,
            a.order_seq,
            "variable_path",
            orders=orders,
            nfts=set(nfts) | {a.nft},
            addresses={a.a, a.b},
            payload={
                "A": a.a,
                "B": a.b,
                "x": a.nft,
                "path_length": depth,
            },
        ))
        csv_s += time.perf_counter() - tw
        rows += 1
    return rows, local_edges, csv_s


def run_final5_hgmatch(conn: sqlite3.Connection, query_id: str, out_csv: Path, args: argparse.Namespace) -> Dict[str, Any]:
    q = query_id.upper()
    if q not in FINAL5:
        raise ValueError(f"Unsupported HGMatch final QuerySpec {query_id}; expected {sorted(FINAL5)}")
    t0 = time.perf_counter()
    index = load_final5_hg_edges(conn)
    anchors = final5_anchors(index, int(args.anchor_limit), int(args.window_sec))
    if q == "C1":
        header = _c1_header()
    elif q == "C2":
        header = _c2_header()
    elif q in {"L3_RETURN", "L6_NEG_RETURN_SUPPORT"}:
        header = _return_header()
    else:
        header = _h5_header()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        if q == "C1":
            rows, local_edges, csv_s = run_final5_c1(index, writer, anchors, args)
        elif q == "C2":
            rows, local_edges, csv_s = run_final5_c2(index, writer, anchors, args)
        elif q == "L3_RETURN":
            rows, local_edges, csv_s = run_final5_return_support(index, writer, anchors, args, False)
        elif q == "L6_NEG_RETURN_SUPPORT":
            rows, local_edges, csv_s = run_final5_return_support(index, writer, anchors, args, True)
        else:
            rows, local_edges, csv_s = run_final5_h5(index, writer, anchors, args)
    total_s = time.perf_counter() - t0
    return {
        "query": q,
        "rows": rows,
        "total_s": total_s,
        "query_only_s": max(0.0, total_s - csv_s),
        "materialization_s": csv_s,
        "load_hypergraph_s": index["load_s"],
        "candidate_count": len(anchors),
        "closure_count": len(anchors),
        "local_edges_examined": local_edges,
        "evidence_csv": str(out_csv),
    }


def pattern_types(pattern: QueryPattern) -> Set[str]:
    return {event_type for alt in pattern.alternatives for event_type in alt.event_types}


def pattern_vars(pattern: QueryPattern) -> Set[str]:
    return {var_name(str(raw)) for alt in pattern.alternatives for _field, raw in alt.roles if is_variable_token(raw)}


def matching_order(compiled: CompiledQuery, type_postings: Dict[str, Set[int]]) -> List[QueryPattern]:

    remaining = list(compiled.patterns)
    def cardinality(p: QueryPattern) -> int:
        return sum(len(type_postings.get(t, set())) for t in pattern_types(p))
    first = min(remaining, key=lambda p: (cardinality(p), p.qid))
    order = [first]
    remaining.remove(first)
    seen = pattern_vars(first)
    while remaining:
        nxt = min(remaining, key=lambda p: (-len(seen & pattern_vars(p)), cardinality(p), p.qid))
        order.append(nxt)
        remaining.remove(nxt)
        seen |= pattern_vars(nxt)
    return order


def load_hypergraph(conn: sqlite3.Connection, compiled: CompiledQuery) -> Tuple[List[Dict[str, Any]], Dict[str, Set[int]], Dict[Tuple[str, str], Set[int]]]:
    needed_types = sorted({t for p in compiled.patterns for t in pattern_types(p)})
    placeholders = ",".join("?" for _ in needed_types)
    cols = ",".join(RUNTIME_COLUMNS)
    rows = conn.execute(f"SELECT {cols} FROM hgmatch_events WHERE event_type IN ({placeholders})", needed_types)
    events: List[Dict[str, Any]] = []
    by_type: Dict[str, Set[int]] = defaultdict(set)
    postings: Dict[Tuple[str, str], Set[int]] = defaultdict(set)
    for idx, raw in enumerate(rows):
        event = dict(zip(RUNTIME_COLUMNS, raw))
        event["order_seq"] = int(event["order_seq"])
        event["block_ts"] = int(event["block_ts"])
        events.append(event)
        by_type[str(event["event_type"])].add(idx)
        for field in ROLE_FIELDS:
            value = str(event.get(field, ""))
            if value:
                postings[(field, value)].add(idx)
    return events, by_type, postings


def candidates(pattern: QueryPattern, bindings: Dict[str, str], by_type: Dict[str, Set[int]], postings: Dict[Tuple[str, str], Set[int]]) -> Set[int]:

    union: Set[int] = set()
    for alt in pattern.alternatives:
        sets: List[Set[int]] = [by_type.get(event_type, set()) for event_type in alt.event_types]
        for field, raw in alt.roles:
            value = bindings.get(var_name(str(raw)), "") if is_variable_token(raw) else str(raw)
            if value:
                sets.append(postings.get((field, value), set()))
        if not sets:
            continue
        base = min(sets, key=len).copy()
        for part in sets:
            if part is not base:
                base.intersection_update(part)
                if not base:
                    break
        union.update(base)
    return union


def run_hgmatch(conn: sqlite3.Connection, query: Dict[str, Any], out_csv: Path) -> Dict[str, Any]:
    compiled = compile_query(query)
    t0 = time.perf_counter()
    events, by_type, postings = load_hypergraph(conn, compiled)
    order = matching_order(compiled, by_type)
    load_s = time.perf_counter() - t0
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    cols = output_columns(compiled)
    num_matches = 0
    nfts: Set[str] = set()
    patterns: Set[str] = set()
    candidate_counts = [0] * len(order)
    partial_counts = [0] * len(order)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()

        def expand(pos: int, assignment: Dict[str, Dict[str, Any]], bindings: Dict[str, str], used: Set[int]) -> None:
            nonlocal num_matches
            pattern = order[pos]
            ids = candidates(pattern, bindings, by_type, postings)
            candidate_counts[pos] += len(ids)
            for idx in ids:
                if idx in used:
                    continue
                event = events[idx]
                new_bindings = event_matches_pattern(event, pattern, bindings)
                if new_bindings is None:
                    continue
                new_assignment = dict(assignment)
                new_assignment[pattern.qid] = event
                if not constraints_ok(compiled, new_assignment, new_bindings, final=(pos + 1 == len(order))):
                    continue
                partial_counts[pos] += 1
                if pos + 1 == len(order):
                    num_matches += 1
                    row = materialized_row(compiled, new_assignment, new_bindings, num_matches)
                    writer.writerow({key: row.get(key, "") for key in cols})
                    nfts.add(str(row.get("nft", "")))
                    patterns.add(str(row.get("pattern_key", "")))
                else:
                    expand(pos + 1, new_assignment, new_bindings, used | {idx})

        expand(0, {}, {}, set())
    total_s = time.perf_counter() - t0
    return {
        "compiled": compiled,
        "num_matches": num_matches,
        "num_distinct_nfts": len({x for x in nfts if x}),
        "num_patterns": len({x for x in patterns if x}),
        "matched_instances_csv": str(out_csv),
        "total_s": total_s,
        "load_s": load_s,
        "plan": {"matching_order": [p.qid for p in order], "candidate_hyperedges_examined": candidate_counts, "valid_partial_embeddings": partial_counts},
    }


def main() -> int:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(args.db) as conn:
        conn.row_factory = sqlite3.Row
        configure_sqlite(conn)
        if args.query_id:
            result = run_final5_hgmatch(conn, args.query_id, outdir / "compressed_evidence.csv", args)
            report = {
                "query_name": result["query"],
                "query": result["query"],
                "scope": "nft_episode",
                "system": "HGMatch",
                "baseline_variant": "global_hyperedge_role_posting_compressed_evidence",
                "storage_layout": "nft_hgmatch_role_hyperedge_store",
                "full_result_materialized": True,
                "compressed_evidence_csv_written": True,
                "compressed_evidence_csv": result["evidence_csv"],
                "evidence_csv": result["evidence_csv"],
                "threads_requested": int(args.threads),
                "threads_effective": 1,
                "compressed_evidence_rows": result["rows"],
                "total_evidence_count": result["rows"],
                "num_matches": result["rows"],
                "candidate_count": result["candidate_count"],
                "closure_count": result["closure_count"],
                "local_edges_examined": result["local_edges_examined"],
                "metrics": {
                    "query_latency_s": result["total_s"],
                    "query_wall_time_s": result["total_s"],
                    "query_only_latency_s": result["query_only_s"],
                    "materialization_latency_s": result["materialization_s"],
                    "load_hypergraph_s": result["load_hypergraph_s"],
                    "full_result_materialized": True,
                    "runtime_representation": "hgmatch_global_hyperedge_role_postings",
                },
            }
            (outdir / "execution_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
            return 0
        if not args.query_json:
            raise SystemExit("--query-json is required unless --query-id is provided")
        query = load_query(Path(args.query_json))
        result = run_hgmatch(conn, query, outdir / "matched_instances.csv")
    report = {
        "query_name": query["query_name"], "scope": "nft_episode", "system": "HGMatch",
        "baseline_variant": "match_by_hyperedge_full_instance_materialization",
        "storage_layout": "nft_hgmatch_role_hyperedge_store", "full_result_materialized": True,
        "matched_instances_csv_written": True, "matched_instances_csv": result["matched_instances_csv"],
        "threads_requested": int(args.threads), "threads_effective": 1, "num_matches": result["num_matches"],
        "num_distinct_nfts": result["num_distinct_nfts"], "num_patterns": result["num_patterns"],
        "compiled_query_plan": result["plan"],
        "metrics": {"query_latency_s": result["total_s"], "query_wall_time_s": result["total_s"], "load_hypergraph_s": result["load_s"], "full_result_materialized": True, "runtime_representation": "hgmatch_match_by_hyperedge"},
    }
    (outdir / "compiled_query_plan.json").write_text(json.dumps(result["plan"], ensure_ascii=False, indent=2), encoding="utf-8")
    (outdir / "execution_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
