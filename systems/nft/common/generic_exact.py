#!/usr/bin/env python3


from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from systems.nft.common.canonical_results import canonical_row
from systems.nft.common.full_binding_spec import BranchPlan, FullBindingPlan, EventPattern, is_variable, variable_name

ZERO = "0x0000000000000000000000000000000000000000"


def text(value: Any) -> str:
    return str(value or "").strip()


def normalize_event(raw: Mapping[str, Any]) -> dict[str, Any]:
    event_id = text(raw.get("event_id"))
    if not event_id:
        raise ValueError("canonical NFT event has no event_id")
    event_type = text(raw.get("event_type"))
    src = text(raw.get("move_from") or raw.get("src") or raw.get("src_addr"))
    dst = text(raw.get("move_to") or raw.get("dst") or raw.get("dst_addr"))
    nft = text(raw.get("nft"))
    collection = text(raw.get("collection_contract") or raw.get("collection"))
    block_ts = int(raw.get("block_ts") or raw.get("block_time_epoch") or 0)
    order_seq = int(raw.get("order_seq") or 0)
    tx_hash = text(raw.get("tx_hash") or raw.get("tx_id"))
    return {
        **dict(raw),
        "event_id": event_id,
        "event_type": event_type,
        "move_from": src,
        "move_to": dst,
        "nft": nft,
        "collection_contract": collection,
        "block_ts": block_ts,
        "order_seq": order_seq,
        "tx_hash": tx_hash,
    }


def valid_transition(event: Mapping[str, Any]) -> bool:
    src = text(event.get("move_from"))
    dst = text(event.get("move_to"))
    return (
        text(event.get("event_type")) in {"NFTTrade", "NFTTransfer"}
        and bool(src and dst and text(event.get("nft")) and text(event.get("collection_contract")))
        and src.lower() != ZERO
        and dst.lower() != ZERO
        and src != dst
    )


def event_field(event: Mapping[str, Any], field: str) -> str:
    if field in {"order_seq", "block_ts"}:
        return str(int(event.get(field) or 0))
    return text(event.get(field))


def _match_pattern(pattern: EventPattern, event: Mapping[str, Any], bindings: Mapping[str, str]) -> Optional[dict[str, str]]:
    if text(event.get("event_type")) not in set(pattern.event_types):
        return None
    out = dict(bindings)
    for field, expected in pattern.roles:
        actual = event_field(event, field)
        if is_variable(expected):
            if not actual:
                return None
            name = variable_name(expected)
            previous = out.get(name)
            if previous is not None and previous != actual:
                return None
            out[name] = actual
        elif actual != text(expected):
            return None
    return out


def _resolve(value: Any, assignment: Mapping[str, Mapping[str, Any]], bindings: Mapping[str, str]) -> Optional[str]:
    if not isinstance(value, str):
        return text(value)
    if value.startswith("$"):
        return bindings.get(variable_name(value))
    if "." in value:
        pid, field = value.split(".", 1)
        event = assignment.get(pid)
        return None if event is None else event_field(event, field)
    return text(value)


def constraints_ok(
    branch: BranchPlan,
    assignment: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, str],
    all_events: Sequence[Mapping[str, Any]],
    *,
    final: bool,
) -> bool:
    event_ids = [text(e.get("event_id")) for e in assignment.values()]
    if len(event_ids) != len(set(event_ids)):
        return False
    for c in branch.constraints:
        typ = str(c.get("type") or "")
        if typ == "order":
            seq = [str(x) for x in c.get("sequence", []) or []]
            for left, right in zip(seq, seq[1:]):
                if left in assignment and right in assignment:
                    if int(assignment[right]["order_seq"]) <= int(assignment[left]["order_seq"]):
                        return False
                elif final:
                    return False
        elif typ == "strict_time_order":
            seq = [str(x) for x in c.get("sequence", []) or []]
            for left, right in zip(seq, seq[1:]):
                if left in assignment and right in assignment:
                    if int(assignment[right]["block_ts"]) <= int(assignment[left]["block_ts"]):
                        return False
                elif final:
                    return False
        elif typ == "within":
            earlier = str(c.get("earlier") or "")
            later = str(c.get("later") or "")
            if earlier in assignment and later in assignment:
                e = assignment[earlier]
                l = assignment[later]
                if int(l["order_seq"]) <= int(e["order_seq"]):
                    return False
                if int(l["block_ts"]) > int(e["block_ts"]) + int(c.get("delta_seconds") or 86400):
                    return False
            elif final:
                return False
        elif typ in {"not_equal", "less_than"}:
            left = _resolve(c.get("left"), assignment, bindings)
            right = _resolve(c.get("right"), assignment, bindings)
            if left is not None and right is not None:
                if typ == "not_equal" and left == right:
                    return False
                if typ == "less_than" and not (left < right):
                    return False
            elif final:
                return False
        elif typ == "min_distinct":
            vals = [_resolve(v, assignment, bindings) for v in c.get("values", []) or []]
            ready = [v for v in vals if v is not None]
            if final and len(set(ready)) < int(c.get("min_count") or 0):
                return False
        elif typ == "distinct_count_between":
            vals = [_resolve(v, assignment, bindings) for v in c.get("values", []) or []]
            ready = [v for v in vals if v is not None and v != ""]
            if final:
                count = len(set(ready))
                if count < int(c.get("min_count") or 0) or count > int(c.get("max_count") or 2**31 - 1):
                    return False
        elif typ == "distinct_event_ids":
            ids = [str(x) for x in c.get("events", []) or []]
            ready = [text(assignment[x].get("event_id")) for x in ids if x in assignment]
            if len(ready) != len(set(ready)):
                return False
            if final and len(ready) != len(ids):
                return False
        elif typ == "distinct_transactions":
            ids = [str(x) for x in c.get("events", []) or []]
            ready = [text(assignment[x].get("tx_hash")) for x in ids if x in assignment]
            if any(not value for value in ready):
                return False
            if len(ready) != len(set(ready)):
                return False
            if final and len(ready) != len(ids):
                return False
        elif typ == "no_event":
            if not final:
                continue
            after = str(c.get("after") or "root")
            anchor = assignment.get(after)
            if anchor is None:
                return False
            types = {str(x) for x in c.get("event_type", []) or []}
            src = _resolve(c.get("src"), assignment, bindings)
            dst = _resolve(c.get("dst"), assignment, bindings) if c.get("dst") is not None else None
            nft = _resolve(c.get("nft"), assignment, bindings)
            coll = _resolve(c.get("collection"), assignment, bindings) if c.get("collection") is not None else None
            start_order = int(anchor["order_seq"])
            end_ts = int(anchor["block_ts"]) + int(c.get("within_seconds") or 86400)
            for event in all_events:
                if types and text(event.get("event_type")) not in types:
                    continue
                if int(event.get("order_seq") or 0) <= start_order or int(event.get("block_ts") or 0) > end_ts:
                    continue
                if src is not None and event_field(event, "move_from") != src:
                    continue
                if dst is not None and event_field(event, "move_to") != dst:
                    continue
                if nft is not None and event_field(event, "nft") != nft:
                    continue
                if coll is not None and event_field(event, "collection_contract") != coll:
                    continue
                return False
        else:
            raise ValueError(f"unsupported full-binding constraint: {typ}")
    return True


def _domain_for_pattern(
    pattern: EventPattern,
    events: Sequence[Mapping[str, Any]],
    bindings: Mapping[str, str],
) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    allowed = set(pattern.event_types)
    for event in events:
        if text(event.get("event_type")) not in allowed:
            continue
        possible = True
        for field, expected in pattern.roles:
            if is_variable(expected):
                bound = bindings.get(variable_name(expected))
                if bound is not None and event_field(event, field) != bound:
                    possible = False
                    break
            elif event_field(event, field) != text(expected):
                possible = False
                break
        if possible:
            out.append(event)
    return out


def iter_branch_matches(
    plan: FullBindingPlan,
    branch: BranchPlan,
    events: Sequence[Mapping[str, Any]],
    *,
    fixed_root: Optional[Mapping[str, Any]] = None,
) -> Iterator[dict[str, Any]]:


    normalized_events = [
        e if all(
            key in e
            for key in (
                "event_id", "event_type", "move_from", "move_to", "nft",
                "collection_contract", "block_ts", "order_seq", "tx_hash",
            )
        ) else normalize_event(e)
        for e in events
    ]
    ordered_events = sorted(
        (e for e in normalized_events if valid_transition(e)),
        key=lambda e: (e["order_seq"], e["event_id"]),
    )
    assignment: dict[str, Mapping[str, Any]] = {}
    bindings: dict[str, str] = {}
    start_index = 0
    if fixed_root is not None:
        root = (
            fixed_root
            if all(
                key in fixed_root
                for key in (
                    "event_id", "event_type", "move_from", "move_to", "nft",
                    "collection_contract", "block_ts", "order_seq", "tx_hash",
                )
            )
            else normalize_event(fixed_root)
        )
        b = _match_pattern(branch.event_patterns[0], root, {})
        if b is None:
            return
        assignment["root"] = root
        bindings = b
        if not constraints_ok(branch, assignment, bindings, ordered_events, final=False):
            return
        start_index = 1

    def dfs(index: int, current_bindings: dict[str, str]) -> Iterator[dict[str, Any]]:
        if index >= len(branch.event_patterns):
            if constraints_ok(branch, assignment, current_bindings, ordered_events, final=True):
                event_bindings = {p.pattern_id: text(assignment[p.pattern_id]["event_id"]) for p in branch.event_patterns}
                event_orders = {p.pattern_id: int(assignment[p.pattern_id]["order_seq"]) for p in branch.event_patterns}
                yield canonical_row(plan.query_id, branch.branch_id, event_bindings, event_orders, current_bindings)
            return
        pattern = branch.event_patterns[index]
        used = {text(e.get("event_id")) for e in assignment.values()}
        for event in _domain_for_pattern(pattern, ordered_events, current_bindings):
            if text(event.get("event_id")) in used:
                continue
            new_bindings = _match_pattern(pattern, event, current_bindings)
            if new_bindings is None:
                continue
            assignment[pattern.pattern_id] = event
            if constraints_ok(branch, assignment, new_bindings, ordered_events, final=False):
                yield from dfs(index + 1, new_bindings)
            assignment.pop(pattern.pattern_id, None)

    yield from dfs(start_index, bindings)


def iter_plan_matches(
    plan: FullBindingPlan,
    events: Sequence[Mapping[str, Any]],
    *,
    fixed_root: Optional[Mapping[str, Any]] = None,
) -> Iterator[dict[str, Any]]:
    for branch in plan.branches:
        yield from iter_branch_matches(plan, branch, events, fixed_root=fixed_root)
