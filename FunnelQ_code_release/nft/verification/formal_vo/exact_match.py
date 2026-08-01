from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, Mapping, Sequence

ROLE_FIELDS = {
    "src": "src_addr",
    "dst": "dst_addr",
    "nft": "nft",
    "collection": "collection",
}


def compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def instance_key(
    query_id: str,
    branch_id: str,
    events: Mapping[str, str],
    variables: Mapping[str, str],
) -> str:
    payload = {
        "query_id": str(query_id),
        "branch_id": str(branch_id),
        "events": {str(key): str(value) for key, value in events.items()},
        "variables": {str(key): str(value) for key, value in sorted(variables.items())},
    }
    return hashlib.sha256(compact(payload).encode("utf-8")).hexdigest()


def _bind_pattern(
    pattern: Mapping[str, Any],
    event: Mapping[str, Any],
    variables: Mapping[str, str],
) -> dict[str, str] | None:
    event_type = pattern.get("event_type")
    allowed = set(event_type if isinstance(event_type, list) else [event_type])
    if str(event["event_type"]) not in {str(value) for value in allowed}:
        return None
    bound = dict(variables)
    for role, field in ROLE_FIELDS.items():
        if role not in pattern:
            continue
        expected = str(pattern[role])
        actual = str(event[field])
        if expected.startswith("$"):
            name = expected[1:]
            if name in bound and bound[name] != actual:
                return None
            bound[name] = actual
        elif expected != actual:
            return None
    return bound


def _constraint_ok(
    constraint: Mapping[str, Any],
    bindings: Mapping[str, Mapping[str, Any]],
    variables: Mapping[str, str],
) -> bool:
    kind = str(constraint.get("type") or "")

    def variable(value: Any) -> str:
        text = str(value)
        return variables.get(text[1:], "") if text.startswith("$") else text

    if kind in {"order", "strict_time_order"}:
        names = [str(value) for value in constraint.get("sequence") or []]
        if not all(name in bindings for name in names):
            return True
        field = "order_seq" if kind == "order" else "block_ts"
        values = [int(bindings[name][field]) for name in names]
        return all(left < right for left, right in zip(values, values[1:]))
    if kind == "within":
        earlier = str(constraint.get("earlier"))
        later = str(constraint.get("later"))
        if earlier not in bindings or later not in bindings:
            return True
        return (
            int(bindings[later]["block_ts"]) - int(bindings[earlier]["block_ts"])
            <= int(constraint.get("delta_seconds") or 0)
        )
    if kind == "not_equal":
        left, right = variable(constraint.get("left")), variable(constraint.get("right"))
        return not left or not right or left != right
    if kind == "min_distinct":
        values = [variable(value) for value in constraint.get("values") or []]
        return any(not value for value in values) or len(set(values)) >= int(
            constraint.get("min_count") or 0
        )
    if kind == "distinct_event_ids":
        names = [str(value) for value in constraint.get("events") or []]
        if not all(name in bindings for name in names):
            return True
        values = [str(bindings[name]["event_id"]) for name in names]
        return len(values) == len(set(values))
    if kind == "distinct_transactions":
        names = [str(value) for value in constraint.get("events") or []]
        if not all(name in bindings for name in names):
            return True
        values = [str(bindings[name]["tx_hash"]) for name in names]
        return all(values) and len(values) == len(set(values))
    raise ValueError(f"unsupported exact constraint: {kind}")


def _constraints_ok(
    constraints: Sequence[Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
    variables: Mapping[str, str],
) -> bool:
    return all(_constraint_ok(constraint, bindings, variables) for constraint in constraints)


def enumerate_instances(
    query: Mapping[str, Any],
    closure_id: int,
    anchor_order: int,
    events: Sequence[Mapping[str, Any]],
) -> Iterable[dict[str, Any]]:
    """Independent, verifier-owned exact matcher for the public QuerySpec."""
    plan = dict(query.get("full_binding_plan") or {})
    query_id = str(plan.get("query_id") or query.get("query_name") or "")
    by_order = {int(event["order_seq"]): event for event in events}
    root = by_order.get(int(anchor_order))
    if root is None:
        raise ValueError("authenticated payload is missing its anchor event")
    ordered = sorted(events, key=lambda event: int(event["order_seq"]))
    for branch in plan.get("branches") or []:
        patterns = list(branch.get("event_patterns") or [])
        constraints = list(branch.get("constraints") or [])
        if not patterns or str(patterns[0].get("id")) != "root":
            raise ValueError("branch does not start with root")
        initial_variables = _bind_pattern(patterns[0], root, {})
        if initial_variables is None:
            continue
        initial_bindings: dict[str, Mapping[str, Any]] = {"root": root}
        if not _constraints_ok(constraints, initial_bindings, initial_variables):
            continue

        def visit(
            pattern_index: int,
            bindings: dict[str, Mapping[str, Any]],
            variables: dict[str, str],
        ) -> Iterable[dict[str, Any]]:
            if pattern_index == len(patterns):
                event_bindings = {
                    name: str(event["event_id"]) for name, event in bindings.items()
                }
                event_orders = {
                    name: int(event["order_seq"]) for name, event in bindings.items()
                }
                branch_id = str(branch.get("branch_id") or "")
                key = instance_key(query_id, branch_id, event_bindings, variables)
                yield {
                    "query_id": query_id,
                    "branch_id": branch_id,
                    "closure_id": int(closure_id),
                    "anchor_order": int(anchor_order),
                    "instance_key": key,
                    "event_bindings_json": compact(event_bindings),
                    "event_orders_json": compact(event_orders),
                    "variable_bindings_json": compact(variables),
                }
                return
            pattern = patterns[pattern_index]
            pattern_id = str(pattern.get("id") or "")
            for event in ordered:
                if int(event["order_seq"]) <= int(anchor_order):
                    continue
                if any(str(event["event_id"]) == str(bound["event_id"]) for bound in bindings.values()):
                    continue
                next_variables = _bind_pattern(pattern, event, variables)
                if next_variables is None:
                    continue
                next_bindings = dict(bindings)
                next_bindings[pattern_id] = event
                if _constraints_ok(constraints, next_bindings, next_variables):
                    yield from visit(pattern_index + 1, next_bindings, next_variables)

        yield from visit(1, initial_bindings, initial_variables)

