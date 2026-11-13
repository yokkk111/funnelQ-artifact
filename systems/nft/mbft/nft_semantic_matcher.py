#!/usr/bin/env python3


from __future__ import annotations

import argparse
import bisect
import itertools
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from systems.nft.common.canonical_results import canonical_row, canonicalize_rows, write_normalized_csv
from systems.nft.common.full_binding_spec import BranchPlan, EventPattern, FullBindingPlan, is_variable, load_full_binding_plan, query_path, variable_name

FINAL5 = ["C1", "C2", "L3_RETURN", "L6_NEG_RETURN_SUPPORT", "H5_23"]
ZERO = "0x0000000000000000000000000000000000000000"


def sval(value: Any) -> str:
    return str(value or "").strip()


class CompactEvent:


    __slots__ = (
        "event_id", "event_type", "move_from", "move_to", "nft",
        "collection_contract", "block_ts", "order_seq", "tx_hash",
    )

    def __init__(
        self,
        event_id: str,
        event_type: str,
        move_from: str,
        move_to: str,
        nft: str,
        collection_contract: str,
        block_ts: int,
        order_seq: int,
        tx_hash: str,
    ) -> None:
        self.event_id = event_id
        self.event_type = event_type
        self.move_from = move_from
        self.move_to = move_to
        self.nft = nft
        self.collection_contract = collection_contract
        self.block_ts = block_ts
        self.order_seq = order_seq
        self.tx_hash = tx_hash

    def get(self, name: str, default: Any = None) -> Any:
        return getattr(self, name, default)

    def __getitem__(self, name: str) -> Any:
        try:
            return getattr(self, name)
        except AttributeError as exc:
            raise KeyError(name) from exc


def _event_identity(event: CompactEvent) -> tuple[Any, ...]:
    return tuple(
        getattr(event, name)
        for name in CompactEvent.__slots__
    )


def load_jsonl(paths: list[Path]) -> tuple[list[CompactEvent], int]:
    by_id: dict[str, CompactEvent] = {}
    identical_duplicate_count = 0
    address_pool: dict[str, str] = {}
    nft_pool: dict[str, str] = {}
    collection_pool: dict[str, str] = {}
    tx_pool: dict[str, str] = {}

    def pooled(pool: dict[str, str], value: Any) -> str:
        text = sval(value)
        prior = pool.get(text)
        if prior is not None:
            return prior
        pool[text] = text
        return text

    for path in paths:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                raw = json.loads(line)
                event_id = sval(raw.get("event_id"))
                if not event_id:
                    raise ValueError(f"MBFT candidate without event_id in {path}")
                raw_type = sval(raw.get("event_type"))
                event_type = {"nfttrade": "NFTTrade", "nfttransfer": "NFTTransfer"}.get(raw_type.lower(), raw_type)
                event = CompactEvent(
                    event_id=event_id,
                    event_type=event_type,
                    move_from=pooled(address_pool, raw.get("move_from") or raw.get("src_addr") or raw.get("src")),
                    move_to=pooled(address_pool, raw.get("move_to") or raw.get("dst_addr") or raw.get("dst")),
                    nft=pooled(nft_pool, raw.get("nft")),
                    collection_contract=pooled(collection_pool, raw.get("collection_contract") or raw.get("collection")),
                    block_ts=int(raw.get("block_ts") or raw.get("block_time_epoch") or 0),
                    order_seq=int(raw.get("order_seq") or 0),
                    tx_hash=pooled(tx_pool, raw.get("tx_hash") or raw.get("transaction_hash")),
                )
                prior = by_id.get(event_id)
                if prior is not None:
                    if _event_identity(prior) != _event_identity(event):
                        raise ValueError(
                            "conflicting MBFT candidate records share event_id "
                            f"{event_id!r}: prior={_event_identity(prior)!r} "
                            f"new={_event_identity(event)!r}"
                        )
                    identical_duplicate_count += 1
                    continue
                by_id[event_id] = event
    rows = list(by_id.values())
    rows.sort(key=lambda x: (x["order_seq"], x["event_id"]))
    return rows, identical_duplicate_count


def valid_edge(event: Mapping[str, Any]) -> bool:
    src, dst = sval(event.get("move_from")), sval(event.get("move_to"))
    return (
        sval(event.get("event_type")) in {"NFTTrade", "NFTTransfer"}
        and bool(src and dst and sval(event.get("nft")) and sval(event.get("collection_contract")))
        and src.lower() != ZERO
        and dst.lower() != ZERO
        and src != dst
    )


def field(event: Mapping[str, Any], name: str) -> str:
    return sval(event.get(name))


def bind_pattern(pattern: EventPattern, event: Mapping[str, Any], bindings: Mapping[str, str]) -> Optional[dict[str, str]]:
    if field(event, "event_type") not in set(pattern.event_types):
        return None
    out = dict(bindings)
    for role, raw in pattern.roles:
        actual = field(event, role)
        if is_variable(raw):
            if not actual:
                return None
            name = variable_name(raw)
            old = out.get(name)
            if old is not None and old != actual:
                return None
            out[name] = actual
        elif actual != sval(raw):
            return None
    return out


def resolve(raw: Any, assigned: Mapping[str, Mapping[str, Any]], bindings: Mapping[str, str]) -> Optional[str]:
    if not isinstance(raw, str):
        return sval(raw)
    if raw.startswith("$"):
        return bindings.get(variable_name(raw))
    if "." in raw:
        pid, role = raw.split(".", 1)
        event = assigned.get(pid)
        return None if event is None else field(event, role)
    return sval(raw)


class ClosureView:
    __slots__ = ("rows", "lo", "hi", "root_order", "start_ts", "end_ts", "strict")

    def __init__(
        self,
        rows: list[CompactEvent],
        lo: int,
        hi: int,
        root_order: int,
        start_ts: int,
        end_ts: int,
        strict: bool,
    ) -> None:
        self.rows = rows
        self.lo = lo
        self.hi = hi
        self.root_order = root_order
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.strict = strict

    def contains(self, event: CompactEvent) -> bool:
        if event.order_seq <= self.root_order:
            return False
        if self.strict:
            return self.start_ts < event.block_ts <= self.end_ts
        return event.block_ts <= self.end_ts

    def __iter__(self):
        rows = itertools.islice(self.rows, self.lo, self.hi)
        if self.strict:
            return (event for event in rows if event.order_seq > self.root_order)
        return (event for event in rows if event.order_seq > self.root_order and event.block_ts <= self.end_ts)

    def __len__(self) -> int:
        return sum(1 for _ in self)


class AdapterIndex:
    def __init__(
        self,
        events: list[CompactEvent],
        *,
        need_strict_time: bool,
        need_order_time: bool,
    ):
        self.events = [e for e in events if valid_edge(e)]
        self.by_collection: dict[str, list[CompactEvent]] = defaultdict(list)
        self.collection_orders: dict[str, list[int]] = {}
        self.by_collection_time: dict[str, list[CompactEvent]] = {}
        self.collection_timestamps: dict[str, list[int]] = {}
        self.by_src: dict[tuple[str, str], list[CompactEvent]] = defaultdict(list)
        self.by_dst: dict[tuple[str, str], list[CompactEvent]] = defaultdict(list)
        self.by_nft: dict[tuple[str, str], list[CompactEvent]] = defaultdict(list)
        for e in self.events:
            coll = field(e, "collection_contract")
            if need_order_time or need_strict_time:
                self.by_collection[coll].append(e)
            self.by_src[(coll, field(e, "move_from"))].append(e)
            self.by_dst[(coll, field(e, "move_to"))].append(e)
            self.by_nft[(coll, field(e, "nft"))].append(e)
        for coll, rows in self.by_collection.items():
            rows.sort(key=lambda e: (int(e["order_seq"]), field(e, "event_id")))
            if need_order_time:
                self.collection_orders[coll] = [int(e["order_seq"]) for e in rows]
            if need_strict_time:
                time_rows = sorted(
                    rows,
                    key=lambda e: (int(e["block_ts"]), int(e["order_seq"]), field(e, "event_id")),
                )
                self.by_collection_time[coll] = time_rows
                self.collection_timestamps[coll] = [int(e["block_ts"]) for e in time_rows]
        if not need_order_time:
            self.by_collection.clear()
        self.roots = self.events

    def closure(
        self,
        root: Mapping[str, Any],
        delta: int,
        *,
        strict_time_order: bool = False,
    ) -> ClosureView:
        coll = field(root, "collection_contract")
        if strict_time_order:
            rows = self.by_collection_time.get(coll, [])
            timestamps = self.collection_timestamps.get(coll, [])
            root_ts = int(root["block_ts"])
            lo = bisect.bisect_right(timestamps, root_ts)
            hi = bisect.bisect_right(timestamps, root_ts + int(delta))
            root_order = int(root["order_seq"])
            return ClosureView(rows, lo, hi, root_order, root_ts, root_ts + int(delta), True)
        rows = self.by_collection.get(coll, [])
        orders = self.collection_orders.get(coll, [])
        start = bisect.bisect_left(orders, int(root["order_seq"]))
        end_ts = int(root["block_ts"]) + int(delta)
        return ClosureView(
            rows, start, len(rows), int(root["order_seq"]),
            int(root["block_ts"]), end_ts, False,
        )

    def candidates(self, pattern: EventPattern, bindings: Mapping[str, str], closure: ClosureView) -> list[CompactEvent]:
        coll = bindings.get("collection")
        sources: list[list[CompactEvent]] = []
        for role, raw in pattern.roles:
            if not is_variable(raw):
                continue
            value = bindings.get(variable_name(raw))
            if value is None or coll is None:
                continue
            if role == "move_from":
                sources.append(self.by_src.get((coll, value), []))
            elif role == "move_to":
                sources.append(self.by_dst.get((coll, value), []))
            elif role == "nft":
                sources.append(self.by_nft.get((coll, value), []))
        base = min(sources, key=len) if sources else closure
        return [e for e in base if closure.contains(e)]


def constraint_pass(
    branch: BranchPlan,
    assigned: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, str],
    closure: list[dict[str, Any]],
    *,
    final: bool,
) -> bool:
    ids = [field(e, "event_id") for e in assigned.values()]
    if len(ids) != len(set(ids)):
        return False
    for c in branch.constraints:
        typ = str(c.get("type") or "")
        if typ == "order":
            seq = [str(x) for x in c.get("sequence", []) or []]
            for a, b in zip(seq, seq[1:]):
                if a in assigned and b in assigned:
                    if int(assigned[b]["order_seq"]) <= int(assigned[a]["order_seq"]):
                        return False
                elif final:
                    return False
        elif typ == "strict_time_order":
            seq = [str(x) for x in c.get("sequence", []) or []]
            for a, b in zip(seq, seq[1:]):
                if a in assigned and b in assigned:
                    if int(assigned[b]["block_ts"]) <= int(assigned[a]["block_ts"]):
                        return False
                elif final:
                    return False
        elif typ == "within":
            a, b = str(c.get("earlier")), str(c.get("later"))
            if a in assigned and b in assigned:
                if int(assigned[b]["order_seq"]) <= int(assigned[a]["order_seq"]):
                    return False
                if int(assigned[b]["block_ts"]) > int(assigned[a]["block_ts"]) + int(c.get("delta_seconds") or 86400):
                    return False
            elif final:
                return False
        elif typ in {"not_equal", "less_than"}:
            left, right = resolve(c.get("left"), assigned, bindings), resolve(c.get("right"), assigned, bindings)
            if left is not None and right is not None:
                if typ == "not_equal" and left == right:
                    return False
                if typ == "less_than" and not left < right:
                    return False
            elif final:
                return False
        elif typ in {"min_distinct", "distinct_count_between"}:
            if not final:
                continue
            values = [resolve(v, assigned, bindings) for v in c.get("values", []) or []]
            count = len({v for v in values if v})
            if typ == "min_distinct" and count < int(c.get("min_count") or 0):
                return False
            if typ == "distinct_count_between" and not (int(c.get("min_count") or 0) <= count <= int(c.get("max_count") or 2**31 - 1)):
                return False
        elif typ == "distinct_event_ids":
            pids = [str(x) for x in c.get("events", []) or []]
            ready = [field(assigned[x], "event_id") for x in pids if x in assigned]
            if len(ready) != len(set(ready)):
                return False
            if final and len(ready) != len(pids):
                return False
        elif typ == "distinct_transactions":
            pids = [str(x) for x in c.get("events", []) or []]
            ready = [field(assigned[x], "tx_hash") for x in pids if x in assigned]
            if any(not value for value in ready):
                return False
            if len(ready) != len(set(ready)):
                return False
            if final and len(ready) != len(pids):
                return False
        elif typ == "no_event":
            if not final:
                continue
            anchor = assigned.get(str(c.get("after") or "root"))
            if anchor is None:
                return False
            types = {str(x) for x in c.get("event_type", []) or []}
            src = resolve(c.get("src"), assigned, bindings)
            dst = resolve(c.get("dst"), assigned, bindings) if c.get("dst") is not None else None
            nft = resolve(c.get("nft"), assigned, bindings)
            coll = resolve(c.get("collection"), assigned, bindings) if c.get("collection") is not None else None
            end_ts = int(anchor["block_ts"]) + int(c.get("within_seconds") or 86400)
            for e in closure:
                if types and field(e, "event_type") not in types:
                    continue
                if int(e["order_seq"]) <= int(anchor["order_seq"]) or int(e["block_ts"]) > end_ts:
                    continue
                if src is not None and field(e, "move_from") != src:
                    continue
                if dst is not None and field(e, "move_to") != dst:
                    continue
                if nft is not None and field(e, "nft") != nft:
                    continue
                if coll is not None and field(e, "collection_contract") != coll:
                    continue
                return False
        else:
            raise ValueError(f"unsupported MBFT adapter constraint: {typ}")
    return True


def match_branch(index: AdapterIndex, plan: FullBindingPlan, branch: BranchPlan, root: Mapping[str, Any], closure: list[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    initial = bind_pattern(branch.event_patterns[0], root, {})
    if initial is None:
        return []
    assigned: dict[str, Mapping[str, Any]] = {"root": root}
    out: list[dict[str, Any]] = []

    def dfs(pos: int, bindings: dict[str, str]) -> None:
        if pos == len(branch.event_patterns):
            if constraint_pass(branch, assigned, bindings, closure, final=True):
                events = {p.pattern_id: field(assigned[p.pattern_id], "event_id") for p in branch.event_patterns}
                orders = {p.pattern_id: int(assigned[p.pattern_id]["order_seq"]) for p in branch.event_patterns}
                out.append(canonical_row(plan.query_id, branch.branch_id, events, orders, bindings))
            return
        pattern = branch.event_patterns[pos]
        used = {field(e, "event_id") for e in assigned.values()}
        for event in index.candidates(pattern, bindings, closure):
            if field(event, "event_id") in used:
                continue
            next_bindings = bind_pattern(pattern, event, bindings)
            if next_bindings is None:
                continue
            assigned[pattern.pattern_id] = event
            if constraint_pass(branch, assigned, next_bindings, closure, final=False):
                dfs(pos + 1, next_bindings)
            assigned.pop(pattern.pattern_id, None)

    if constraint_pass(branch, assigned, initial, closure, final=False):
        dfs(1, initial)
    return out


def run_match(
    events: list[CompactEvent],
    query_dir: Path,
    qids: list[str],
    outdir: Path,
    root_limit: int = 0,
    *,
    candidate_load_s: float = 0.0,
    warmup_runs: int = 1,
    phase_marker: Path | None = None,
) -> dict[str, Any]:
    plans: dict[str, FullBindingPlan] = {}
    strict_by_query: dict[str, bool] = {}
    for qid in qids:
        _, plan = load_full_binding_plan(query_path(query_dir, qid))
        plans[qid] = plan
        strict_by_query[qid] = any(
            constraint.get("type") == "strict_time_order"
            for branch in plan.branches
            for constraint in branch.constraints
        )
    tprep = time.perf_counter()
    index = AdapterIndex(
        events,
        need_strict_time=any(strict_by_query.values()),
        need_order_time=not all(strict_by_query.values()),
    )
    roots = index.roots[:root_limit] if root_limit > 0 else index.roots
    prepare_s = time.perf_counter() - tprep
    if phase_marker is not None:
        phase_marker.unlink(missing_ok=True)
    reports = []
    for qid in qids:
        plan = plans[qid]
        strict_time_order = strict_by_query[qid]
        def execute_plan() -> tuple[list[dict[str, Any]], dict[str, Any], int, float, float]:
            t0 = time.perf_counter()
            instances: list[dict[str, Any]] = []
            closure_rows = 0
            for root in roots:
                closure = index.closure(
                    root,
                    plan.delta_seconds,
                    strict_time_order=strict_time_order,
                )
                closure_rows += len(closure)
                for branch in plan.branches:
                    instances.extend(match_branch(index, plan, branch, root, closure))
            match_s = time.perf_counter() - t0
            tc = time.perf_counter()
            normalized, material = canonicalize_rows(instances)
            return (
                normalized,
                material,
                closure_rows,
                match_s,
                time.perf_counter() - tc,
            )

        twarm = time.perf_counter()
        for _ in range(max(0, warmup_runs)):
            execute_plan()
        warmup_s = time.perf_counter() - twarm
        if phase_marker is not None:
            phase_marker.parent.mkdir(parents=True, exist_ok=True)
            phase_marker.write_text("query\n", encoding="utf-8")
        try:
            normalized, material, closure_rows, match_s, result_canonicalize_s = execute_plan()
        finally:
            if phase_marker is not None:
                phase_marker.unlink(missing_ok=True)
        tw = time.perf_counter()
        instances_path = outdir / qid / "instances.csv"
        write_normalized_csv(instances_path, normalized)
        csv_write_s = time.perf_counter() - tw
        material = {"instances_csv": str(instances_path), **material}
        reports.append({
            "query": qid,
            **material,
            "roots_examined": len(roots),
            "closure_event_rows_examined": closure_rows,
            "warmup_runs": max(0, warmup_runs),
            "warmup_s": warmup_s,
            "match_s": match_s,
            "result_canonicalize_s": result_canonicalize_s,
            "csv_write_s": csv_write_s,
        })
    report = {
        "mode": "MBFT_NFT_independent_full_binding_matcher_v2",
        "event_count": len(index.events),
        "root_count": len(roots),
        "candidate_load_s": float(candidate_load_s),
        "prepare_s": prepare_s,
        "warmup_runs": max(0, warmup_runs),
        "same_runtime_warmup": True,
        "imports_funnelq_verifier": False,
        "queries": reports,
    }
    (outdir / "matcher_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-jsonl", nargs="+", required=True, type=Path)
    ap.add_argument("--query-dir", required=True, type=Path)
    ap.add_argument("--queries", default=",".join(FINAL5))
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--root-limit", type=int, default=0)
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--phase-marker", default="", type=Path)
    args = ap.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    qids = [q.strip().upper() for q in args.queries.split(",") if q.strip()]
    phase_marker = (
        args.phase_marker
        if str(args.phase_marker).strip() not in {"", "."}
        else None
    )
    if phase_marker is not None:
        phase_marker.parent.mkdir(parents=True, exist_ok=True)
        phase_marker.write_text("query\n", encoding="utf-8")
    tload = time.perf_counter()
    try:
        events, identical_duplicate_count = load_jsonl(args.events_jsonl)
        candidate_load_s = time.perf_counter() - tload
        report = run_match(
            events, args.query_dir, qids, args.outdir, args.root_limit,
            candidate_load_s=candidate_load_s,
            warmup_runs=max(0, args.warmup_runs),
            phase_marker=phase_marker,
        )
    except Exception:
        if phase_marker is not None:
            phase_marker.unlink(missing_ok=True)
        raise
    report["identical_duplicate_event_rows"] = identical_duplicate_count
    (args.outdir / "matcher_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
