from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
TEMPLATE_DIR = HERE / "selected5_templates"
SELECTED5 = {
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
}


def _semantic_projection(document: dict[str, Any]) -> dict[str, Any]:
    plan = document.get("full_binding_plan")
    if not isinstance(plan, dict):
        raise ValueError("QuerySpec is missing full_binding_plan")
    query_id = str(plan.get("query_id") or document.get("query_name") or "")
    return {
        "query_name": str(document.get("query_name") or ""),
        "result_semantics": str(document.get("result_semantics") or ""),
        "full_binding_plan": plan,
        "_resolved_query_id": query_id,
    }


def _first_difference(expected: Any, actual: Any, path: str = "$") -> str | None:
    if type(expected) is not type(actual):
        return (
            f"{path}: type mismatch "
            f"expected={type(expected).__name__} actual={type(actual).__name__}"
        )
    if isinstance(expected, dict):
        expected_keys = set(expected)
        actual_keys = set(actual)
        missing = sorted(expected_keys - actual_keys)
        extra = sorted(actual_keys - expected_keys)
        if missing:
            return f"{path}: missing keys {missing}"
        if extra:
            return f"{path}: unexpected keys {extra}"
        for key in expected:
            difference = _first_difference(
                expected[key], actual[key], f"{path}.{key}"
            )
            if difference:
                return difference
        return None
    if isinstance(expected, list):
        if len(expected) != len(actual):
            return f"{path}: list length expected={len(expected)} actual={len(actual)}"
        for index, (left, right) in enumerate(zip(expected, actual)):
            difference = _first_difference(left, right, f"{path}[{index}]")
            if difference:
                return difference
        return None
    if expected != actual:
        return f"{path}: expected={expected!r} actual={actual!r}"
    return None


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_selected5_query_contract(
    query_document: dict[str, Any],
) -> dict[str, str]:


    actual = _semantic_projection(query_document)
    query_id = actual.pop("_resolved_query_id")
    if query_id not in SELECTED5:
        raise ValueError(
            f"FunnelQ selected-five fast path does not support query_id={query_id!r}"
        )
    template_path = TEMPLATE_DIR / f"{query_id}.json"
    if not template_path.is_file():
        raise FileNotFoundError(f"missing FunnelQ query template: {template_path}")
    expected_document = json.loads(template_path.read_text(encoding="utf-8"))
    expected = _semantic_projection(expected_document)
    expected.pop("_resolved_query_id")
    difference = _first_difference(expected, actual)
    if difference:
        raise ValueError(
            "QuerySpec does not match the specialized FunnelQ "
            f"{query_id} template: {difference}"
        )
    return {
        "query_id": query_id,
        "template_path": str(template_path),
        "semantic_sha256": _digest(actual),
        "status": "MATCHED",
    }
