from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .protocol import SIGNATURE_FIELDS, canonical_json, hash_obj

CANONICAL_COLUMNS = [
    "query_id",
    "branch_id",
    "closure_id",
    "anchor_order",
    "instance_key",
    "event_bindings_json",
    "event_orders_json",
    "variable_bindings_json",
]

_PLAN_FIELDS = {"query_id", "result_semantics", "delta_seconds", "branches"}
_BRANCH_FIELDS = {"branch_id", "event_patterns", "constraints"}
_PATTERN_FIELDS = {"id", "event_type", "src", "dst", "nft", "collection"}
_SUPPORTED_CONSTRAINTS = {
    "order",
    "strict_time_order",
    "within",
    "not_equal",
    "min_distinct",
    "distinct_event_ids",
    "distinct_transactions",
}


def _exact_keys(name: str, value: Mapping[str, Any], expected: set[str]) -> None:
    actual = set(value)
    if actual != expected:
        raise ValueError(
            f"{name} fields mismatch: missing={sorted(expected-actual)} "
            f"unknown={sorted(actual-expected)}"
        )


def _variable(value: Any, name: str) -> str:
    text = str(value)
    if not text.startswith("$") or len(text) < 2:
        raise ValueError(f"{name} must be a named variable")
    return text


def _branches(raw: Mapping[str, Any]) -> list[dict[str, Any]]:
    plan = raw.get("full_binding_plan")
    if not isinstance(plan, Mapping):
        raise ValueError("QuerySpec is missing full_binding_plan")
    branches = plan.get("branches")
    if not isinstance(branches, list) or not branches:
        raise ValueError("QuerySpec has no branches")
    if not all(isinstance(branch, Mapping) for branch in branches):
        raise ValueError("QuerySpec branch must be an object")
    return [dict(branch) for branch in branches]


def _branch_domains(branch: Mapping[str, Any]) -> dict[str, set[str]]:
    domains = {"address": set(), "nft": set(), "collection": set()}
    for pattern in list(branch.get("event_patterns") or []):
        domains["address"].add(_variable(pattern.get("src"), "pattern.src"))
        domains["address"].add(_variable(pattern.get("dst"), "pattern.dst"))
        domains["nft"].add(_variable(pattern.get("nft"), "pattern.nft"))
        domains["collection"].add(
            _variable(pattern.get("collection"), "pattern.collection")
        )
    return domains


def _branch_minimum(branch: Mapping[str, Any], role: str) -> int:
    domains = _branch_domains(branch)
    variables = domains[role]
    minimum = 1 if variables and role == "nft" else 0
    for constraint in branch.get("constraints") or []:
        ctype = constraint.get("type")
        if ctype == "min_distinct":
            values = {_variable(value, "min_distinct value") for value in constraint["values"]}
            if values and values <= variables:
                minimum = max(minimum, int(constraint["min_count"]))
        elif ctype == "not_equal":
            values = {
                _variable(constraint["left"], "not_equal.left"),
                _variable(constraint["right"], "not_equal.right"),
            }
            if values <= variables:
                minimum = max(minimum, 2)
    return minimum


def _signature(branch: Mapping[str, Any]) -> str:
    patterns = list(branch.get("event_patterns") or [])
    if len(patterns) == 2:
        root, back = patterns
        if back.get("src") != root.get("dst") or back.get("dst") != root.get("src"):
            raise ValueError("unsupported two-event refined topology")
        return (
            "l2_ba_nft_equal_min_delta"
            if back.get("nft") == root.get("nft")
            else "l2_ba_nft_different_min_delta"
        )
    if len(patterns) == 3:
        root, middle, back = patterns
        if (
            middle.get("src") != root.get("dst")
            or back.get("src") != middle.get("dst")
            or back.get("dst") != root.get("src")
        ):
            raise ValueError("unsupported three-event refined topology")
        nfts = [root.get("nft"), middle.get("nft"), back.get("nft")]
        if len(set(nfts)) == 1:
            return "l3_bcca_all_equal_min_delta"
        if len(set(nfts)) == 3:
            return "l3_bcca_all_distinct_min_delta"
        if nfts[0] == nfts[2] and nfts[0] != nfts[1]:
            return "l3_bcca_root_back_equal_min_delta"
    raise ValueError("QuerySpec does not map to the generic descriptor vocabulary")


def _validate_branch(branch: Mapping[str, Any], plan_delta: int) -> None:
    _exact_keys("branch", branch, _BRANCH_FIELDS)
    branch_id = str(branch.get("branch_id") or "")
    if not branch_id:
        raise ValueError("branch_id is required")
    patterns = branch.get("event_patterns")
    if not isinstance(patterns, list) or len(patterns) not in (2, 3):
        raise ValueError("the descriptor vocabulary supports exactly two or three events")
    ids: list[str] = []
    for offset, pattern in enumerate(patterns):
        if not isinstance(pattern, Mapping):
            raise ValueError("event pattern must be an object")
        _exact_keys("event pattern", pattern, _PATTERN_FIELDS)
        role = str(pattern.get("id") or "")
        if not role or role in ids:
            raise ValueError("event role ids must be non-empty and unique")
        ids.append(role)
        if str(pattern.get("event_type")) != "NFTTrade":
            raise ValueError("generic selected5 descriptors support NFTTrade roles only")
        for field in ("src", "dst", "nft", "collection"):
            _variable(pattern.get(field), f"pattern.{field}")
        if offset == 0 and role != "root":
            raise ValueError("the first event role must be root")
    collections = {str(pattern["collection"]) for pattern in patterns}
    if len(collections) != 1:
        raise ValueError("all selected5 event roles must share one collection variable")

    constraints = branch.get("constraints")
    if not isinstance(constraints, list):
        raise ValueError("branch constraints must be a list")
    seen_singletons: dict[str, int] = {
        "order": 0,
        "strict_time_order": 0,
        "within": 0,
        "distinct_event_ids": 0,
        "distinct_transactions": 0,
    }
    domains = _branch_domains(branch)
    all_variables = set().union(*domains.values())
    not_equal_pairs: set[frozenset[str]] = set()
    min_distinct_constraints: list[tuple[set[str], int]] = []
    for constraint in constraints:
        if not isinstance(constraint, Mapping):
            raise ValueError("constraint must be an object")
        ctype = str(constraint.get("type") or "")
        if ctype not in _SUPPORTED_CONSTRAINTS:
            raise ValueError(f"unsupported constraint type: {ctype}")
        if ctype in seen_singletons:
            seen_singletons[ctype] += 1
        if ctype in ("order", "strict_time_order"):
            _exact_keys(ctype, constraint, {"type", "sequence"})
            if list(constraint["sequence"]) != ids:
                raise ValueError(f"{ctype} must cover all event roles in canonical order")
        elif ctype == "within":
            _exact_keys("within", constraint, {"type", "earlier", "later", "delta_seconds"})
            if str(constraint["earlier"]) != ids[0] or str(constraint["later"]) != ids[-1]:
                raise ValueError("within must bind root to the final role")
            if int(constraint["delta_seconds"]) != plan_delta:
                raise ValueError("plan delta_seconds differs from the exact within constraint")
        elif ctype in ("distinct_event_ids", "distinct_transactions"):
            _exact_keys(ctype, constraint, {"type", "events"})
            if list(constraint["events"]) != ids:
                raise ValueError(f"{ctype} must cover every event role exactly once")
        elif ctype == "not_equal":
            _exact_keys("not_equal", constraint, {"type", "left", "right"})
            left = _variable(constraint["left"], "not_equal.left")
            right = _variable(constraint["right"], "not_equal.right")
            if left == right or left not in all_variables or right not in all_variables:
                raise ValueError("not_equal must reference two distinct declared variables")
            if not any({left, right} <= domain for domain in domains.values()):
                raise ValueError("not_equal variables must belong to the same semantic domain")
            not_equal_pairs.add(frozenset((left, right)))
        elif ctype == "min_distinct":
            _exact_keys("min_distinct", constraint, {"type", "values", "min_count"})
            values = [_variable(value, "min_distinct value") for value in constraint["values"]]
            if not values or len(set(values)) != len(values):
                raise ValueError("min_distinct values must be non-empty and unique")
            minimum = int(constraint["min_count"])
            if minimum < 1 or minimum > len(values):
                raise ValueError("min_distinct min_count is invalid")
            value_set = set(values)
            if not any(value_set <= domain for domain in domains.values()):
                raise ValueError("min_distinct variables must belong to one semantic domain")
            min_distinct_constraints.append((value_set, minimum))
    for ctype, count in seen_singletons.items():
        if count != 1:
            raise ValueError(f"branch must contain exactly one {ctype} constraint")

    signature = _signature(branch)
    nft_variables = [str(pattern["nft"]) for pattern in patterns]

    def guaranteed_all_distinct(values: list[str]) -> bool:
        unique = set(values)
        if len(unique) != len(values):
            return False
        # A min_distinct constraint on a larger superset does not by itself
        # imply that this particular subset is pairwise distinct.  It is safe
        # only when the whole constrained set is required to be all-distinct.
        if any(
            unique <= variables and minimum == len(variables)
            for variables, minimum in min_distinct_constraints
        ):
            return True
        return all(
            frozenset((values[i], values[j])) in not_equal_pairs
            for i in range(len(values))
            for j in range(i + 1, len(values))
        )

    if signature in {
        "l2_ba_nft_different_min_delta",
        "l3_bcca_root_back_equal_min_delta",
    }:
        pair = [nft_variables[0], nft_variables[1]]
        if not guaranteed_all_distinct(pair):
            raise ValueError(
                "Refined different-NFT signature is not implied by exact QuerySpec constraints"
            )
    elif signature == "l3_bcca_all_distinct_min_delta" and not guaranteed_all_distinct(
        nft_variables
    ):
        raise ValueError(
            "Refined all-distinct-NFT signature is not implied by exact QuerySpec constraints"
        )


def compile_transcript(raw: Mapping[str, Any]) -> dict[str, Any]:
    allowed_top = {
        "query_name",
        "query_version",
        "description",
        "full_binding_plan",
        "result_semantics",
    }
    unknown_top = set(raw) - allowed_top
    if unknown_top:
        raise ValueError(f"unknown QuerySpec fields: {sorted(unknown_top)}")
    plan_raw = raw.get("full_binding_plan")
    if not isinstance(plan_raw, Mapping):
        raise ValueError("QuerySpec is missing full_binding_plan")
    plan = dict(plan_raw)
    _exact_keys("full_binding_plan", plan, _PLAN_FIELDS)
    query_id = str(plan.get("query_id") or "")
    if not query_id or query_id != str(raw.get("query_name") or ""):
        raise ValueError("query_name and full_binding_plan.query_id must match")
    query_version = str(raw.get("query_version") or "")
    if not query_version:
        raise ValueError("QuerySpec must declare query_version")
    if plan.get("result_semantics") != "full_event_binding_v1" or raw.get(
        "result_semantics"
    ) != "full_event_binding_v1":
        raise ValueError("selected5 requires full_event_binding_v1 result semantics")
    delta = int(plan.get("delta_seconds") or 0)
    if delta <= 0:
        raise ValueError("QuerySpec delta_seconds must be positive")
    branches = _branches(raw)
    branch_ids: set[str] = set()
    for branch in branches:
        _validate_branch(branch, delta)
        branch_id = str(branch["branch_id"])
        if branch_id in branch_ids:
            raise ValueError("branch_id values must be unique")
        branch_ids.add(branch_id)

    # Common necessary conditions for an OR are minima; using maxima would
    # unsafely remove instances satisfying a weaker branch.
    event_min = min(len(list(branch["event_patterns"])) for branch in branches)
    nft_min = min(_branch_minimum(branch, "nft") for branch in branches)
    addr_min = min(_branch_minimum(branch, "address") for branch in branches)
    signatures = sorted({_signature(branch) for branch in branches})
    if any(signature not in SIGNATURE_FIELDS for signature in signatures):
        raise ValueError("unknown generic descriptor signature")

    transcript = {
        "transcriptVersion": "selected5_full_binding_transcript_v2",
        "queryId": query_id,
        "queryVersion": query_version,
        "querySpecDigest": hash_obj("query-spec", json.loads(canonical_json(raw))),
        "executionSemantics": "Episode",
        "anchor": {
            "directory": "nft_anchor_directory",
            "selectedAtom": "out_from_anchor_buyer",
            "rootEventTypes": sorted(
                {str(branch["event_patterns"][0]["event_type"]) for branch in branches}
            ),
        },
        "base": {
            "minTransitionCount": event_min,
            "minAddressCount": addr_min,
            "minNftCount": nft_min,
        },
        "refined": {
            "genericSignatures": signatures,
            "maxCompletionDeltaSeconds": delta,
        },
        "exact": json.loads(canonical_json(plan)),
        "resultContract": {
            "semantics": "full_event_binding_v1",
            "columns": CANONICAL_COLUMNS,
            "comparison": "canonical_set_equality",
        },
    }
    transcript["transcriptRoot"] = hash_obj("query-transcript", transcript)
    return transcript


def compile_transcript_file(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return raw, compile_transcript(raw)


def base_pass(row: Mapping[str, Any], transcript: Mapping[str, Any]) -> bool:
    base = dict(transcript["base"])
    return (
        int(row["transition_count"]) >= int(base["minTransitionCount"])
        and int(row["addr_count"]) >= int(base["minAddressCount"])
        and int(row["nft_count"]) >= int(base["minNftCount"])
    )


def refined_pass(row: Mapping[str, Any], transcript: Mapping[str, Any]) -> bool:
    refined = dict(transcript["refined"])
    delta = int(refined["maxCompletionDeltaSeconds"])
    return any(int(row[signature]) <= delta for signature in refined["genericSignatures"])
