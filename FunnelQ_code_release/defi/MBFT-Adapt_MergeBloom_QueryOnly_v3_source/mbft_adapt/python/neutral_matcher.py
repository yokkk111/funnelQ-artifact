#!/usr/bin/env python3
"""Neutral complex-event matcher for MBFT-Adapt verified atomic events.

The matcher builds all temporary role/order indexes from the verified JSONL payloads.
It does not read FunnelQ's anchor directories, closure synopses, support tables, or
precomputed terminal counters. Index construction time is therefore part of the
MBFT-Adapt client cost.
"""
from __future__ import annotations

import argparse
import bisect
import hashlib
import json
try:
    import resource
except ImportError:  # Native Windows
    resource = None
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Sequence, Tuple


def norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def is_var(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("$")


@dataclass(slots=True)
class Event:
    event_id: str
    closure_id: str
    tx_hash: str
    block_number: int
    block_time_epoch: int
    evt_index: int
    seq_index: int
    event_type: str
    protocol: str
    roles: Dict[str, str]
    attrs: Dict[str, Any]
    global_tx_ord: int = -1
    global_event_ord: int = -1

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> "Event":
        return cls(
            event_id=str(obj.get("event_id", "")),
            closure_id=str(obj.get("closure_id", "")),
            tx_hash=norm(obj.get("tx_hash", "")),
            block_number=int(obj.get("block_number", 0) or 0),
            block_time_epoch=int(obj.get("block_time_epoch", 0) or 0),
            evt_index=int(obj.get("evt_index", 0) or 0),
            seq_index=int(obj.get("seq_index", 0) or 0),
            event_type=norm(obj.get("event_type", "")),
            protocol=norm(obj.get("protocol", "")),
            roles={str(k): norm(v) for k, v in dict(obj.get("roles", {}) or {}).items()},
            attrs=dict(obj.get("attrs", {}) or {}),
        )

    def field(self, name: str) -> Any:
        if name == "event_id":
            return self.event_id
        if name == "tx_hash":
            return self.tx_hash
        if name == "protocol":
            return self.protocol
        if name == "event_type":
            return self.event_type
        if name in self.roles:
            return self.roles[name]
        if name in self.attrs:
            return self.attrs[name]
        return ""


def load_verified_events(paths: Sequence[Path]) -> List[Event]:
    events: List[Event] = []
    seen: set[str] = set()
    for path in paths:
        with path.open("r", encoding="utf-8") as stream:
            for line_no, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                try:
                    event = Event.from_json(json.loads(line))
                except Exception as exc:  # pragma: no cover - diagnostic path
                    raise ValueError(f"invalid verified event at {path}:{line_no}: {exc}") from exc
                if not event.event_id:
                    raise ValueError(f"missing event_id at {path}:{line_no}")
                if event.event_id in seen:
                    continue
                seen.add(event.event_id)
                events.append(event)
    return events


def load_query(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as stream:
        query = json.load(stream)
    if not query.get("events"):
        raise ValueError("query has no events")
    return query


def check_attr_filters(query: Mapping[str, Any], qid: str, event: Event) -> bool:
    for flt in query.get("attr_filters", []) or []:
        if str(flt.get("event", "")) != qid:
            continue
        field = str(flt.get("field", ""))
        op = str(flt.get("op", "=")).lower()
        wanted = flt.get("value")
        actual = event.field(field)
        try:
            if op in {">", ">=", "<", "<="}:
                a = float(actual)
                b = float(wanted)
                if op == ">" and not (a > b): return False
                if op == ">=" and not (a >= b): return False
                if op == "<" and not (a < b): return False
                if op == "<=" and not (a <= b): return False
            elif op in {"!=", "ne"}:
                if norm(actual) == norm(wanted): return False
            else:
                if norm(actual) != norm(wanted): return False
        except (TypeError, ValueError):
            return False
    return True


def assign_event(
    query: Mapping[str, Any],
    spec: Mapping[str, Any],
    event: Event,
    bindings: Mapping[str, str],
) -> Dict[str, str] | None:
    if event.event_type != norm(spec.get("type", "")):
        return None
    protocol = norm(spec.get("protocol", ""))
    if protocol and event.protocol != protocol:
        return None
    qid = str(spec["id"])
    if not check_attr_filters(query, qid, event):
        return None
    new_bindings = dict(bindings)
    for field, raw in dict(spec.get("roles", {}) or {}).items():
        actual = norm(event.field(str(field)))
        if not actual:
            return None
        if is_var(raw):
            var = str(raw)
            bound = new_bindings.get(var)
            if bound is not None and bound != actual:
                return None
            new_bindings[var] = actual
        elif actual != norm(raw):
            return None
    return new_bindings


def neq_ok(query: Mapping[str, Any], selected: Mapping[str, Event], new_qid: str | None = None) -> bool:
    for left, left_field, right, right_field in query.get("neq_constraints", []) or []:
        left = str(left); right = str(right)
        if left in selected and right in selected:
            if norm(selected[left].field(str(left_field))) == norm(selected[right].field(str(right_field))):
                return False
    return True


def order_ok_same_tx(query: Mapping[str, Any], selected: Mapping[str, Event]) -> bool:
    for left, right in query.get("order", []) or []:
        left = str(left); right = str(right)
        if left in selected and right in selected:
            if selected[left].seq_index >= selected[right].seq_index:
                return False
    return True


def order_ok_episode(query: Mapping[str, Any], selected: Mapping[str, Event]) -> bool:
    for left, right in query.get("order", []) or []:
        left = str(left); right = str(right)
        if left in selected and right in selected:
            if selected[left].global_event_ord >= selected[right].global_event_ord:
                return False
    return True


def canonical_match_hash(bindings: Iterable[Tuple[str, Sequence[str]]]) -> str:
    h = hashlib.sha256()
    for closure, ids in sorted((str(c), tuple(v)) for c, v in bindings):
        h.update(closure.encode())
        h.update(b"|")
        for event_id in ids:
            h.update(str(event_id).encode())
            h.update(b",")
        h.update(b"\n")
    return h.hexdigest()


class SameTxMatcher:
    def __init__(self, events: Sequence[Event]):
        self.groups: Dict[str, Dict[str, List[Event]]] = defaultdict(lambda: defaultdict(list))
        for event in events:
            self.groups[event.tx_hash][event.event_type].append(event)
        for by_type in self.groups.values():
            for items in by_type.values():
                items.sort(key=lambda e: (e.seq_index, e.evt_index, e.event_id))

    def run(self, query: Mapping[str, Any], materialize_limit: int = 0) -> dict:
        specs = [dict(item) for item in query["events"]]
        required = Counter(norm(spec["type"]) for spec in specs)
        result_count = 0
        closure_count = 0
        partial_bindings = 0
        full_checks = 0
        sample_bindings: List[Tuple[str, Tuple[str, ...]]] = []

        for tx_hash, by_type in self.groups.items():
            if any(len(by_type.get(event_type, ())) < count for event_type, count in required.items()):
                continue
            local_count = 0
            selected: Dict[str, Event] = {}

            def rec(pos: int, bindings: Dict[str, str]) -> None:
                nonlocal result_count, local_count, partial_bindings, full_checks
                if pos == len(specs):
                    result_count += 1
                    local_count += 1
                    full_checks += 1
                    if materialize_limit and len(sample_bindings) < materialize_limit:
                        sample_bindings.append((tx_hash, tuple(selected[str(spec["id"])].event_id for spec in specs)))
                    return
                spec = specs[pos]
                qid = str(spec["id"])
                for event in by_type.get(norm(spec["type"]), ()):
                    if any(event.event_id == chosen.event_id for chosen in selected.values()):
                        continue
                    partial_bindings += 1
                    new_bindings = assign_event(query, spec, event, bindings)
                    if new_bindings is None:
                        continue
                    selected[qid] = event
                    if order_ok_same_tx(query, selected) and neq_ok(query, selected, qid):
                        rec(pos + 1, new_bindings)
                    selected.pop(qid, None)

            rec(0, {})
            if local_count:
                closure_count += 1

        return {
            "result_count": result_count,
            "matching_closure_count": closure_count,
            "partial_binding_attempts": partial_bindings,
            "full_match_checks": full_checks,
            "sample_result_hash": canonical_match_hash(sample_bindings) if sample_bindings else "",
            "sample_bindings": sample_bindings,
        }


class EpisodeMatcher:
    def __init__(self, events: Sequence[Event]):
        # Match the supplied FunnelQ builder's fallback order exactly:
        # transactions by (block_number, block_time, tx_hash), then events by
        # (seq_index, evt_index, event_id).
        tx_events: Dict[Tuple[str, str], List[Event]] = defaultdict(list)
        for event in events:
            tx_events[(event.closure_id, event.tx_hash)].append(event)
        tx_keys = sorted(
            tx_events,
            key=lambda key: (
                min(e.block_number for e in tx_events[key]),
                min(e.block_time_epoch for e in tx_events[key]),
                key[1],
            ),
        )
        ordered: List[Event] = []
        self.tx_timestamps: List[int] = []
        self.tx_max_event_ord: List[int] = []
        for tx_ord, key in enumerate(tx_keys):
            items = tx_events[key]
            items.sort(key=lambda e: (e.seq_index, e.evt_index, e.event_id))
            self.tx_timestamps.append(min(e.block_time_epoch for e in items))
            for event in items:
                event.global_tx_ord = tx_ord
                event.global_event_ord = len(ordered)
                ordered.append(event)
            self.tx_max_event_ord.append(len(ordered) - 1)
        self.events = ordered

        self.by_type: Dict[str, List[Event]] = defaultdict(list)
        self.postings: Dict[Tuple[str, str, str], List[Event]] = defaultdict(list)
        # Compound role postings are neutral, query-independent temporary indexes.
        # They are built from verified candidates and their construction cost is
        # reported as client index-build time.
        self.swap_taker_sold: Dict[Tuple[str, str], List[Event]] = defaultdict(list)
        self.swap_taker_sold_bought: Dict[Tuple[str, str, str], List[Event]] = defaultdict(list)
        self.swap_taker_sold_pool: Dict[Tuple[str, str, str], List[Event]] = defaultdict(list)
        self.swap_taker_sold_bought_pool: Dict[Tuple[str, str, str, str], List[Event]] = defaultdict(list)
        self.borrow_borrower_asset: Dict[Tuple[str, str], List[Event]] = defaultdict(list)
        self.borrow_borrower_asset_pool: Dict[Tuple[str, str, str], List[Event]] = defaultdict(list)
        for event in ordered:
            self.by_type[event.event_type].append(event)
            for field, value in event.roles.items():
                if value:
                    self.postings[(event.event_type, field, value)].append(event)
            if event.event_type == "swap":
                taker = event.roles.get("taker", "")
                sold = event.roles.get("token_sold", "")
                bought = event.roles.get("token_bought", "")
                pool = event.roles.get("pool", "")
                if taker and sold and bought and pool:
                    self.swap_taker_sold[(taker, sold)].append(event)
                    self.swap_taker_sold_bought[(taker, sold, bought)].append(event)
                    self.swap_taker_sold_pool[(taker, sold, pool)].append(event)
                    self.swap_taker_sold_bought_pool[(taker, sold, bought, pool)].append(event)
            elif event.event_type == "borrow":
                borrower = event.roles.get("borrower", "")
                asset = event.roles.get("asset", "")
                pool = event.roles.get("pool", "")
                if borrower and asset and pool:
                    self.borrow_borrower_asset[(borrower, asset)].append(event)
                    self.borrow_borrower_asset_pool[(borrower, asset, pool)].append(event)
        self.type_ords = {k: [e.global_event_ord for e in v] for k, v in self.by_type.items()}
        self.posting_ords = {k: [e.global_event_ord for e in v] for k, v in self.postings.items()}
        self.swap_taker_sold_ords = {k: [e.global_event_ord for e in v] for k, v in self.swap_taker_sold.items()}
        self.swap_taker_sold_bought_ords = {k: [e.global_event_ord for e in v] for k, v in self.swap_taker_sold_bought.items()}
        self.swap_taker_sold_pool_ords = {k: [e.global_event_ord for e in v] for k, v in self.swap_taker_sold_pool.items()}
        self.swap_taker_sold_bought_pool_ords = {k: [e.global_event_ord for e in v] for k, v in self.swap_taker_sold_bought_pool.items()}
        self.borrow_borrower_asset_ords = {k: [e.global_event_ord for e in v] for k, v in self.borrow_borrower_asset.items()}
        self.borrow_borrower_asset_pool_ords = {k: [e.global_event_ord for e in v] for k, v in self.borrow_borrower_asset_pool.items()}

    @staticmethod
    def _slice(items: Sequence[Event], ords: Sequence[int], lo_exclusive: int, hi_inclusive: int) -> Sequence[Event]:
        left = bisect.bisect_right(ords, lo_exclusive)
        right = bisect.bisect_right(ords, hi_inclusive)
        return items[left:right]

    def candidates(
        self,
        spec: Mapping[str, Any],
        bindings: Mapping[str, str],
        lo_exclusive: int,
        hi_inclusive: int,
    ) -> Sequence[Event]:
        event_type = norm(spec["type"])
        choices: List[Tuple[Sequence[Event], Sequence[int]]] = [
            (self.by_type.get(event_type, ()), self.type_ords.get(event_type, ()))
        ]
        bound_fields: Dict[str, str] = {}
        for field, raw in dict(spec.get("roles", {}) or {}).items():
            value = bindings.get(str(raw)) if is_var(raw) else norm(raw)
            if value:
                bound_fields[str(field)] = value
                key = (event_type, str(field), value)
                choices.append((self.postings.get(key, ()), self.posting_ords.get(key, ())))
        if event_type == "swap" and "taker" in bound_fields and "token_sold" in bound_fields:
            key2 = (bound_fields["taker"], bound_fields["token_sold"])
            choices.append((self.swap_taker_sold.get(key2, ()), self.swap_taker_sold_ords.get(key2, ())))
        elif event_type == "borrow" and "borrower" in bound_fields and "asset" in bound_fields:
            key2 = (bound_fields["borrower"], bound_fields["asset"])
            choices.append((self.borrow_borrower_asset.get(key2, ()), self.borrow_borrower_asset_ords.get(key2, ())))
        items, ords = min(choices, key=lambda pair: len(pair[0]))
        return self._slice(items, ords, lo_exclusive, hi_inclusive)

    @staticmethod
    def _count_ords(ords: Sequence[int] | None, lo_exclusive: int, hi_inclusive: int) -> int:
        if not ords:
            return 0
        return max(0, bisect.bisect_right(ords, hi_inclusive) - bisect.bisect_right(ords, lo_exclusive))

    def terminal_aggregate_count(
        self,
        query: Mapping[str, Any],
        qid: str,
        spec: Mapping[str, Any],
        selected: Mapping[str, Event],
        bindings: Mapping[str, str],
        lo_exclusive: int,
        hi_inclusive: int,
    ) -> int | None:
        if norm(spec.get("protocol", "")):
            return None
        if any(str(flt.get("event", "")) == qid for flt in query.get("attr_filters", []) or []):
            return None
        roles = dict(spec.get("roles", {}) or {})
        bound: Dict[str, str] = {}
        for field, raw in roles.items():
            if is_var(raw):
                if str(raw) in bindings:
                    bound[str(field)] = bindings[str(raw)]
            else:
                bound[str(field)] = norm(raw)

        excluded: Dict[str, set[str]] = defaultdict(set)
        for left, left_field, right, right_field in query.get("neq_constraints", []) or []:
            left = str(left); right = str(right)
            if left != qid and right != qid:
                continue
            if left == qid:
                terminal_field, other_qid, other_field = str(left_field), right, str(right_field)
            else:
                terminal_field, other_qid, other_field = str(right_field), left, str(left_field)
            if other_qid not in selected:
                return None
            if terminal_field == "event_id":
                # Strict temporal lower bounds place selected predecessors outside
                # the terminal domain, so injectivity is already guaranteed.
                continue
            excluded[terminal_field].add(norm(selected[other_qid].field(other_field)))

        etype = norm(spec.get("type", ""))
        if etype == "swap":
            if set(roles) != {"taker", "token_sold", "token_bought", "pool"}:
                return None
            if set(bound) != {"taker", "token_sold"}:
                return None
            taker, sold = bound["taker"], bound["token_sold"]
            bought_ex = {v for v in excluded.get("token_bought", set()) if v}
            pool_ex = {v for v in excluded.get("pool", set()) if v}
            unsupported = set(excluded) - {"token_bought", "pool"}
            if unsupported:
                return None
            total = self._count_ords(self.swap_taker_sold_ords.get((taker, sold)), lo_exclusive, hi_inclusive)
            removed_bought = sum(
                self._count_ords(self.swap_taker_sold_bought_ords.get((taker, sold, bought)), lo_exclusive, hi_inclusive)
                for bought in bought_ex
            )
            removed_pool = sum(
                self._count_ords(self.swap_taker_sold_pool_ords.get((taker, sold, pool)), lo_exclusive, hi_inclusive)
                for pool in pool_ex
            )
            restored = sum(
                self._count_ords(
                    self.swap_taker_sold_bought_pool_ords.get((taker, sold, bought, pool)),
                    lo_exclusive,
                    hi_inclusive,
                )
                for bought in bought_ex
                for pool in pool_ex
            )
            return int(total - removed_bought - removed_pool + restored)

        if etype == "borrow":
            if set(roles) != {"borrower", "asset", "pool"}:
                return None
            if set(bound) != {"borrower", "asset"}:
                return None
            unsupported = set(excluded) - {"pool"}
            if unsupported:
                return None
            borrower, asset = bound["borrower"], bound["asset"]
            total = self._count_ords(self.borrow_borrower_asset_ords.get((borrower, asset)), lo_exclusive, hi_inclusive)
            removed = sum(
                self._count_ords(
                    self.borrow_borrower_asset_pool_ords.get((borrower, asset, pool)),
                    lo_exclusive,
                    hi_inclusive,
                )
                for pool in excluded.get("pool", set()) if pool
            )
            return int(total - removed)
        return None

    def run(self, query: Mapping[str, Any], materialize_limit: int = 0) -> dict:
        specs = [dict(item) for item in query["events"]]
        by_qid = {str(spec["id"]): spec for spec in specs}
        anchor_qid = str(query.get("scope_params", {}).get("semantic_anchor_event", specs[0]["id"]))
        anchor_spec = by_qid[anchor_qid]
        if anchor_qid != str(specs[0]["id"]):
            raise ValueError("neutral matcher currently requires the semantic anchor to be the first query event")
        window = int(query.get("scope_params", {}).get("forward_window_sec", 0))
        if window <= 0:
            raise ValueError("Episode query requires a positive forward_window_sec")

        result_count = 0
        episode_count = 0
        partial_bindings = 0
        full_checks = 0
        sample_bindings: List[Tuple[str, Tuple[str, ...]]] = []

        for anchor in self.by_type.get(norm(anchor_spec["type"]), ()):
            anchor_bindings = assign_event(query, anchor_spec, anchor, {})
            if anchor_bindings is None:
                continue
            end_time = anchor.block_time_epoch + window
            end_tx = bisect.bisect_right(self.tx_timestamps, end_time) - 1
            if end_tx < anchor.global_tx_ord:
                end_tx = anchor.global_tx_ord
            hi_ord = self.tx_max_event_ord[end_tx]
            selected: Dict[str, Event] = {anchor_qid: anchor}
            local_count = 0

            def rec(pos: int, bindings: Dict[str, str]) -> None:
                nonlocal result_count, local_count, partial_bindings, full_checks
                if pos == len(specs):
                    result_count += 1
                    local_count += 1
                    full_checks += 1
                    if materialize_limit and len(sample_bindings) < materialize_limit:
                        episode_id = f"{anchor.global_event_ord}::w{window}"
                        sample_bindings.append((episode_id, tuple(selected[str(spec["id"])].event_id for spec in specs)))
                    return
                spec = specs[pos]
                qid = str(spec["id"])
                if qid == anchor_qid:
                    rec(pos + 1, bindings)
                    return
                lo_ord = anchor.global_event_ord
                # Compile every already-bound predecessor into the lower bound.
                for left, right in query.get("order", []) or []:
                    if str(right) == qid and str(left) in selected:
                        lo_ord = max(lo_ord, selected[str(left)].global_event_ord)
                candidate_events = self.candidates(spec, bindings, lo_ord, hi_ord)
                # The final pattern event can be counted directly after exact
                # Feasible/neq checks; no result materialization or recursive
                # frame is needed. This is still client-side recomputation over
                # the authenticated atomic candidates.
                if pos == len(specs) - 1 and not materialize_limit:
                    aggregated = self.terminal_aggregate_count(
                        query, qid, spec, selected, bindings, lo_ord, hi_ord
                    )
                    if aggregated is not None:
                        partial_bindings += 1
                        result_count += aggregated
                        local_count += aggregated
                        full_checks += aggregated
                        return
                    selected_ids = {chosen.event_id for chosen in selected.values()}
                    for event in candidate_events:
                        if event.event_id in selected_ids:
                            continue
                        partial_bindings += 1
                        new_bindings = assign_event(query, spec, event, bindings)
                        if new_bindings is None:
                            continue
                        selected[qid] = event
                        if order_ok_episode(query, selected) and neq_ok(query, selected, qid):
                            result_count += 1
                            local_count += 1
                            full_checks += 1
                        selected.pop(qid, None)
                    return
                for event in candidate_events:
                    if any(event.event_id == chosen.event_id for chosen in selected.values()):
                        continue
                    partial_bindings += 1
                    new_bindings = assign_event(query, spec, event, bindings)
                    if new_bindings is None:
                        continue
                    selected[qid] = event
                    if order_ok_episode(query, selected) and neq_ok(query, selected, qid):
                        rec(pos + 1, new_bindings)
                    selected.pop(qid, None)

            rec(1, anchor_bindings)
            if local_count:
                episode_count += 1

        return {
            "result_count": result_count,
            "matching_episode_count": episode_count,
            "partial_binding_attempts": partial_bindings,
            "full_match_checks": full_checks,
            "sample_result_hash": canonical_match_hash(sample_bindings) if sample_bindings else "",
            "sample_bindings": sample_bindings,
        }


def peak_rss_mb() -> float:
    if resource is not None:
        # Linux ru_maxrss is KiB.
        return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0
    import psutil
    info = psutil.Process().memory_info()
    return float(getattr(info, "peak_wset", info.rss)) / (1024.0 * 1024.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a neutral complex-event matcher over MBFT-verified events")
    parser.add_argument("--query", required=True, type=Path)
    parser.add_argument("--events", required=True, type=Path, action="append", help="verified JSONL; repeat for each event type")
    parser.add_argument("--scope", choices=["same_tx", "episode"], required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--materialize-limit", type=int, default=0)
    parser.add_argument("--warmup-runs", type=int, default=0)
    parser.add_argument("--phase-marker", type=Path)
    args = parser.parse_args()

    t0 = time.perf_counter()
    events = load_verified_events(args.events)
    t1 = time.perf_counter()
    query = load_query(args.query)
    if args.scope == "same_tx":
        matcher = SameTxMatcher(events)
    else:
        matcher = EpisodeMatcher(events)
    t2 = time.perf_counter()
    warmup_runs = max(0, int(args.warmup_runs))
    for _ in range(warmup_runs):
        matcher.run(query, materialize_limit=0)
    t_query = time.perf_counter()
    if args.phase_marker is not None:
        args.phase_marker.parent.mkdir(parents=True, exist_ok=True)
        args.phase_marker.write_text("query\n", encoding="utf-8")
    try:
        result = matcher.run(query, materialize_limit=max(0, args.materialize_limit))
    finally:
        if args.phase_marker is not None:
            args.phase_marker.unlink(missing_ok=True)
    t3 = time.perf_counter()

    result.update({
        "query_name": query.get("query_name", args.query.stem),
        "scope": args.scope,
        "verified_atomic_event_count": len(events),
        "client_candidate_load_ms": (t1 - t0) * 1000.0,
        "client_temporary_index_build_ms": (t2 - t1) * 1000.0,
        "client_complex_match_ms": (t3 - t_query) * 1000.0,
        "client_match_total_ms": (t3 - t0) * 1000.0,
        "warmup_runs": warmup_runs,
        "client_peak_rss_mb": peak_rss_mb(),
    })
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
