#!/usr/bin/env python3
"""Neutral full-event-binding query specification for the NFT workload.

This module only parses and validates the shared query contract.  It contains no
FunnelQ/HVCEQ execution or verification code and is safe for every baseline to
consume.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

ROLE_ALIASES = {
    "src": "move_from",
    "dst": "move_to",
    "collection": "collection_contract",
    "nft": "nft",
}
SUPPORTED_CONSTRAINTS = {
    "order",
    "within",
    "not_equal",
    "less_than",
    "min_distinct",
    "distinct_count_between",
    "distinct_event_ids",
    "distinct_transactions",
    "strict_time_order",
    "no_event",
}


def norm(value: Any) -> str:
    return str(value or "").strip()


def is_variable(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("$") and len(value) > 1


def variable_name(value: str) -> str:
    return value[1:] if value.startswith("$") else value


@dataclass(frozen=True)
class EventPattern:
    pattern_id: str
    event_types: tuple[str, ...]
    roles: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class BranchPlan:
    branch_id: str
    event_patterns: tuple[EventPattern, ...]
    constraints: tuple[dict[str, Any], ...]

    @property
    def pattern_ids(self) -> tuple[str, ...]:
        return tuple(p.pattern_id for p in self.event_patterns)

    @property
    def variables(self) -> tuple[str, ...]:
        out: set[str] = set()
        for p in self.event_patterns:
            for _field, value in p.roles:
                if is_variable(value):
                    out.add(variable_name(value))
        for c in self.constraints:
            for key in ("left", "right", "src", "dst", "nft", "collection"):
                value = c.get(key)
                if is_variable(value):
                    out.add(variable_name(value))
            for value in c.get("values", []) or []:
                if is_variable(value):
                    out.add(variable_name(value))
        return tuple(sorted(out))


@dataclass(frozen=True)
class FullBindingPlan:
    query_id: str
    result_semantics: str
    delta_seconds: int
    branches: tuple[BranchPlan, ...]


def _event_types(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, list):
        return tuple(str(x) for x in raw)
    if raw is None:
        return tuple()
    return (str(raw),)


def parse_full_binding_plan(query: dict[str, Any]) -> FullBindingPlan:
    raw = query.get("full_binding_plan")
    if not isinstance(raw, dict):
        raise ValueError("query is missing full_binding_plan")
    query_id = norm(raw.get("query_id") or query.get("query_name"))
    semantics = norm(raw.get("result_semantics") or query.get("result_semantics"))
    if semantics != "full_event_binding_v1":
        raise ValueError(f"unsupported result semantics: {semantics!r}")
    branches: list[BranchPlan] = []
    for rb in raw.get("branches", []) or []:
        patterns: list[EventPattern] = []
        for rp in rb.get("event_patterns", []) or []:
            pid = norm(rp.get("id"))
            types = _event_types(rp.get("event_type"))
            if not pid or not types:
                raise ValueError(f"invalid event pattern in branch {rb.get('branch_id')}")
            roles: list[tuple[str, str]] = []
            for alias, canonical in ROLE_ALIASES.items():
                if alias in rp:
                    roles.append((canonical, norm(rp[alias])))
            patterns.append(EventPattern(pid, types, tuple(roles)))
        if not patterns or patterns[0].pattern_id != "root":
            raise ValueError("every full-binding branch must start with root")
        constraints = tuple(dict(c) for c in (rb.get("constraints", []) or []))
        unknown = {str(c.get("type")) for c in constraints} - SUPPORTED_CONSTRAINTS
        if unknown:
            raise ValueError(f"unsupported constraints in {query_id}: {sorted(unknown)}")
        branches.append(BranchPlan(norm(rb.get("branch_id")) or f"branch_{len(branches)}", tuple(patterns), constraints))
    if not branches:
        raise ValueError(f"query {query_id} has no full-binding branches")
    return FullBindingPlan(
        query_id=query_id,
        result_semantics=semantics,
        delta_seconds=int(raw.get("delta_seconds") or query.get("delta_seconds") or 86400),
        branches=tuple(branches),
    )


def load_full_binding_plan(path: Path) -> tuple[dict[str, Any], FullBindingPlan]:
    query = json.loads(Path(path).read_text(encoding="utf-8"))
    return query, parse_full_binding_plan(query)


def query_path(query_dir: Path, query_id: str) -> Path:
    q = query_id.strip().upper()
    mapping = {
        "C1": "C1.json",
        "C2": "C2.json",
        "L3": "L3_RETURN.json",
        "L3_RETURN": "L3_RETURN.json",
        "L6": "L6_NEG_RETURN_SUPPORT.json",
        "L6_NEG_RETURN_SUPPORT": "L6_NEG_RETURN_SUPPORT.json",
        "H5": "H5_23.json",
        "H5_23": "H5_23.json",
    }
    if q in mapping:
        return Path(query_dir) / mapping[q]
    direct = Path(query_dir) / f"{q}.json"
    if direct.is_file():
        return direct
    raise ValueError(f"unknown NFT query id: {query_id}")


def all_query_paths(query_dir: Path, query_ids: Iterable[str]) -> list[Path]:
    return [query_path(query_dir, q) for q in query_ids]
