#!/usr/bin/env python3

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

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

FULL_RESULT_DIGEST_ALGORITHM = "sha256_length_prefixed_canonical_rows_v1"


def _compact(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def instance_key(
    query_id: str,
    branch_id: str,
    event_bindings: Mapping[str, str],
    variable_bindings: Mapping[str, str],
) -> str:
    payload = {
        "query_id": str(query_id),
        "branch_id": str(branch_id),
        "events": {str(k): str(v) for k, v in event_bindings.items()},
        "variables": {str(k): str(v) for k, v in sorted(variable_bindings.items())},
    }
    return hashlib.sha256(_compact(payload).encode("utf-8")).hexdigest()


def _json_object(value: Any, field: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        obj = dict(value)
    else:
        try:
            obj = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"{field} is not valid JSON") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"{field} must contain a JSON object")
    return {str(k): v for k, v in obj.items()}


def strict_canonical_row(raw: Mapping[str, Any]) -> dict[str, Any]:


    missing = [column for column in CANONICAL_COLUMNS if column not in raw]
    if missing:
        raise ValueError(f"canonical result row is missing columns: {missing}")
    query_id = str(raw.get("query_id") or "")
    branch_id = str(raw.get("branch_id") or "")
    if not query_id or not branch_id:
        raise ValueError("canonical result row requires query_id and branch_id")
    events_raw = _json_object(raw.get("event_bindings_json"), "event_bindings_json")
    orders_raw = _json_object(raw.get("event_orders_json"), "event_orders_json")
    variables_raw = _json_object(raw.get("variable_bindings_json"), "variable_bindings_json")
    events = {str(k): str(v) for k, v in events_raw.items()}
    try:
        orders = {str(k): int(v) for k, v in orders_raw.items()}
    except (TypeError, ValueError) as exc:
        raise ValueError("event_orders_json contains a non-integer order") from exc
    variables = {str(k): str(v) for k, v in variables_raw.items()}
    if not events or set(events) != set(orders):
        raise ValueError("event binding names and event order names must be equal and non-empty")
    if "root" not in orders:
        raise ValueError("canonical result row is missing the root event")
    if any(not event_id for event_id in events.values()):
        raise ValueError("event_bindings_json contains an empty event id")
    if any(order < 0 for order in orders.values()):
        raise ValueError("event_orders_json contains a negative order")
    anchor_order = int(raw.get("anchor_order"))
    closure_id = int(raw.get("closure_id"))
    if anchor_order != orders["root"]:
        raise ValueError("anchor_order does not equal the root event order")
    if closure_id != anchor_order + 1:
        raise ValueError("closure_id does not equal anchor_order + 1")
    expected_key = instance_key(query_id, branch_id, events, variables)
    if str(raw.get("instance_key") or "").lower() != expected_key:
        raise ValueError("instance_key does not match the event and variable bindings")
    return {
        "query_id": query_id,
        "branch_id": branch_id,
        "closure_id": closure_id,
        "anchor_order": anchor_order,
        "instance_key": expected_key,
        "event_bindings_json": _compact(events),
        "event_orders_json": _compact(orders),
        "variable_bindings_json": _compact(variables),
    }


def canonical_full_result_digest(rows: Iterable[Mapping[str, Any]]) -> tuple[str, int]:


    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in rows:
        row = strict_canonical_row(raw)
        key = row["instance_key"]
        if key in seen:
            raise ValueError(f"duplicate canonical instance key: {key}")
        seen.add(key)
        normalized.append(row)
    normalized.sort(key=lambda row: row["instance_key"])
    digest = hashlib.sha256()
    digest.update(b"NFT_FULL_EVENT_BINDING_RESULT_V1\0")
    for row in normalized:
        payload = _compact({column: row[column] for column in CANONICAL_COLUMNS}).encode("utf-8")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest(), len(normalized)


def canonical_full_result_digest_csv(path: Path) -> tuple[str, int]:
    with Path(path).open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if list(reader.fieldnames or []) != CANONICAL_COLUMNS:
            raise ValueError(
                f"{path} has the wrong canonical columns: {reader.fieldnames}"
            )
        return canonical_full_result_digest(reader)


def canonical_row(
    query_id: str,
    branch_id: str,
    event_bindings: Mapping[str, str],
    event_orders: Mapping[str, int],
    variable_bindings: Mapping[str, str],
) -> dict[str, Any]:
    if "root" not in event_orders:
        raise ValueError("canonical instance is missing root order")
    anchor_order = int(event_orders["root"])
    events = {str(k): str(v) for k, v in event_bindings.items()}
    orders = {str(k): int(v) for k, v in event_orders.items()}
    variables = {str(k): str(v) for k, v in sorted(variable_bindings.items())}
    return {
        "query_id": str(query_id),
        "branch_id": str(branch_id),
        "closure_id": anchor_order + 1,
        "anchor_order": anchor_order,
        "instance_key": instance_key(query_id, branch_id, events, variables),
        "event_bindings_json": _compact(events),
        "event_orders_json": _compact(orders),
        "variable_bindings_json": _compact(variables),
    }


def normalize_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    by_key: dict[str, dict[str, Any]] = {}
    duplicates = 0
    for raw in rows:
        if (
            isinstance(raw, dict)
            and all(column in raw for column in CANONICAL_COLUMNS)
        ):


            row = raw
        else:
            row = {c: raw.get(c, "") for c in CANONICAL_COLUMNS}

        key = str(row["instance_key"])
        if not key:
            raise ValueError("canonical result row has empty instance_key")
        if key in by_key:
            duplicates += 1
        else:
            row["closure_id"] = int(row["closure_id"])
            row["anchor_order"] = int(row["anchor_order"])
            by_key[key] = row
    return [by_key[k] for k in sorted(by_key)], duplicates


def canonicalize_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:

    normalized, duplicate_count = normalize_rows(rows)
    digest = hashlib.sha256(("\n".join(r["instance_key"] for r in normalized) + ("\n" if normalized else "")).encode("ascii")).hexdigest()
    return normalized, {
        "result_count": len(normalized),
        "duplicate_count": duplicate_count,
        "result_digest_sha256": digest,
    }


def write_normalized_csv(path: Path, normalized: Sequence[Mapping[str, Any]]) -> None:

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CANONICAL_COLUMNS)
        writer.writeheader()
        writer.writerows(normalized)


def write_canonical_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    normalized, summary = canonicalize_rows(rows)
    write_normalized_csv(path, normalized)
    return {"instances_csv": str(Path(path)), **summary}


def read_canonical_csv(path: Path) -> list[dict[str, Any]]:
    with Path(path).open("r", newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    normalized, duplicates = normalize_rows(rows)
    if duplicates:
        raise ValueError(f"duplicate instance keys in {path}: {duplicates}")
    return normalized


def summarize_canonical_csv(path: Path) -> dict[str, Any]:
    rows = read_canonical_csv(path)
    digest = hashlib.sha256(("\n".join(r["instance_key"] for r in rows) + ("\n" if rows else "")).encode("ascii")).hexdigest()
    return {"result_count": len(rows), "duplicate_count": 0, "result_digest_sha256": digest}
