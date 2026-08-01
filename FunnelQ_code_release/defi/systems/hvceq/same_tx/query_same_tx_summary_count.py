#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Summary/count-only query runner for encoded same-tx HVCEQ sidecar."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from array import array
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

ROLE_FIELDS = [
    "recipient",
    "borrower",
    "taker",
    "maker",
    "asset",
    "token_sold",
    "token_bought",
    "pool",
]
NUMERIC_FIELDS = ["amount", "amount_in", "amount_out"]
ROLE_ARRAY_NAME = {field: f"{field}_id" for field in ROLE_FIELDS}
NUM_ARRAY_NAME = {field: f"{field}_num" for field in NUMERIC_FIELDS}
SUPPORTED_ATTR_OPS = {">=", ">", "<=", "<", "="}


def ensure_dir(path: str | os.PathLike[str]) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def norm_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def norm_hex_like(x) -> str:
    s = norm_str(x)
    return s.lower() if s.startswith("0x") else s


def load_query_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        q = json.load(f)
    if q.get("scope") != "same_tx":
        raise ValueError("Only scope = same_tx is supported by this runner")
    if not q.get("events"):
        raise ValueError("query JSON is missing events")
    q.setdefault("order", [])
    q.setdefault("attr_filters", [])
    q.setdefault("neq_constraints", [])
    return q


class EncodedSameTxRuntime:
    def __init__(
        self,
        conn: sqlite3.Connection,
        load_event_ids: bool = True,
        build_tx_type_buckets: bool = True,
        role_fields: Sequence[str] | None = None,
        numeric_fields: Sequence[str] | None = None,
        event_types: Sequence[str] | None = None,
    ):
        self.conn = conn
        self.load_event_ids = bool(load_event_ids)
        self.event_type_to_id = self._load_event_type_ids()
        self.role_fields = [
            field for field in ROLE_FIELDS
            if role_fields is None or field in set(role_fields)
        ]
        self.numeric_fields = [
            field for field in NUMERIC_FIELDS
            if numeric_fields is None or field in set(numeric_fields)
        ]
        self.requested_event_type_ids = {
            int(self.event_type_to_id[name])
            for name in (event_types or self.event_type_to_id.keys())
            if name in self.event_type_to_id
        }
        self.value_to_id = self._load_value_ids()
        self.value_count = len(self.value_to_id)
        self.event_count = 0
        self.tx_count = 0
        self._load_events()
        if build_tx_type_buckets:
            self._build_tx_type_buckets()
        else:
            self.tx_type_events = {}
            self.tx_type_counts = {}
            if self.tx_ord:
                self.tx_count = 1 + sum(
                    1
                    for i in range(1, len(self.tx_ord))
                    if self.tx_ord[i] != self.tx_ord[i - 1]
                )

    def _load_event_type_ids(self) -> Dict[str, int]:
        return {str(name): int(eid) for eid, name in self.conn.execute("SELECT event_type_id, event_type FROM event_type_dict")}

    def _load_value_ids(self) -> Dict[str, int]:
        return {str(text): int(vid) for vid, text in self.conn.execute("SELECT value_id, value_text FROM value_dict")}

    def _load_events(self) -> None:
        self.event_ord = array("q")
        self.tx_ord = array("q")
        self.seq_index = array("q")
        self.event_type_id = array("i")
        self.protocol_id = array("i")
        self.role_arrays: Dict[str, array] = {field: array("i") for field in self.role_fields}
        self.num_arrays: Dict[str, array] = {field: array("d") for field in self.numeric_fields}
        self.event_id_by_idx: List[str] = []

        cols = [
            "event_ord",
            *(["event_id"] if self.load_event_ids else []),
            "tx_ord", "seq_index", "event_type_id", "protocol_id",
            *[f"{field}_id" for field in self.role_fields],
            *[f"{field}_num" for field in self.numeric_fields],
        ]
        where = ""
        params: List[int] = []
        if self.requested_event_type_ids:
            params = sorted(self.requested_event_type_ids)
            where = " WHERE event_type_id IN (" + ",".join("?" for _ in params) + ")"
        sql = (
            f"SELECT {', '.join(cols)} FROM same_tx_event_encoded"
            f"{where} ORDER BY tx_ord, seq_index, evt_index, event_ord"
        )
        for row in self.conn.execute(sql, params):
            offset = 0
            self.event_ord.append(int(row[offset] or 0)); offset += 1
            if self.load_event_ids:
                self.event_id_by_idx.append(str(row[offset] or "")); offset += 1
            self.tx_ord.append(int(row[offset] or 0)); offset += 1
            self.seq_index.append(int(row[offset] or 0)); offset += 1
            self.event_type_id.append(int(row[offset] or 0)); offset += 1
            self.protocol_id.append(int(row[offset] or 0)); offset += 1
            for field in self.role_fields:
                self.role_arrays[field].append(int(row[offset] or 0)); offset += 1
            for field in self.numeric_fields:
                self.num_arrays[field].append(float(row[offset] or 0.0)); offset += 1
        self.event_count = len(self.event_ord)

    def _build_tx_type_buckets(self) -> None:
        self.tx_type_events: Dict[int, Dict[int, List[int]]] = defaultdict(lambda: defaultdict(list))
        self.tx_type_counts: Dict[int, Counter] = defaultdict(Counter)
        for idx, tx in enumerate(self.tx_ord):
            tx_i = int(tx)
            et = int(self.event_type_id[idx])
            self.tx_type_events[tx_i][et].append(idx)
            self.tx_type_counts[tx_i][et] += 1
        self.tx_count = len(self.tx_type_events)

    def encode_value(self, value: Any) -> int:
        v = norm_hex_like(value)
        if not v:
            return 0
        return int(self.value_to_id.get(v, 0))

    def role_id(self, idx: int, field: str) -> int:
        if field == "event_id":
            # event_id is represented by the unique event_ord for equality/neq.
            return int(self.event_ord[idx])
        arr = self.role_arrays.get(field)
        return int(arr[idx]) if arr is not None else 0

    def numeric_value(self, idx: int, field: str) -> float:
        arr = self.num_arrays.get(field)
        return float(arr[idx]) if arr is not None else 0.0


def event_id_to_type(query: dict) -> Dict[str, str]:
    return {e["id"]: e["type"] for e in query["events"]}


def type_multiplicity(query: dict, rt: EncodedSameTxRuntime) -> Counter:
    c = Counter()
    for e in query["events"]:
        etid = rt.event_type_to_id.get(e["type"], 0)
        c[etid] += 1
    return c


def build_query_plan(query: dict, rt: EncodedSameTxRuntime) -> List[dict]:
    # Use a deterministic, selectivity-aware order for the HVCEQ encoded runner.
    # This is independent of SQL's join-order policy and does not alter semantics.
    total_by_type = Counter(rt.event_type_id)
    order_pairs = {tuple(x) for x in query.get("order", [])}
    neq_pairs = {tuple(x) for x in query.get("neq_constraints", [])}

    events = []
    for idx, e in enumerate(query["events"]):
        roles = e.get("roles", {}) or {}
        var_count = sum(1 for v in roles.values() if isinstance(v, str) and v.startswith("$"))
        const_count = sum(1 for v in roles.values() if not (isinstance(v, str) and v.startswith("$")) and norm_str(v))
        protocol_const = 1 if norm_str(e.get("protocol", "")) else 0
        etid = rt.event_type_to_id.get(e["type"], 0)
        events.append({
            "idx": idx,
            "qid": e["id"],
            "type": e["type"],
            "event_type_id": etid,
            "roles": roles,
            "protocol_id": rt.encode_value(e.get("protocol", "")),
            "const_weight": const_count + protocol_const,
            "var_weight": var_count,
            "global_type_count": int(total_by_type.get(etid, 10**12)),
        })

    # Greedy order: rare event type first, then constants, then variables.  To
    # keep common patterns efficient, flashloan usually wins by rarity.
    events.sort(key=lambda x: (x["global_type_count"], -x["const_weight"], -x["var_weight"], x["idx"]))
    return events


def candidate_tx_ids(query: dict, rt: EncodedSameTxRuntime) -> List[int]:
    need = type_multiplicity(query, rt)
    if any(etid == 0 for etid in need):
        return []
    out = []
    for tx, counts in rt.tx_type_counts.items():
        ok = True
        for etid, cnt in need.items():
            if counts.get(etid, 0) < cnt:
                ok = False
                break
        if ok:
            out.append(int(tx))
    return out


def compare_numeric(left: float, op: str, right: float) -> bool:
    if op == "=":
        return left == right
    if op == ">=":
        return left >= right
    if op == ">":
        return left > right
    if op == "<=":
        return left <= right
    if op == "<":
        return left < right
    raise ValueError(f"Unsupported attr op: {op}")


def can_assign(
    rt: EncodedSameTxRuntime,
    query: dict,
    spec: dict,
    event_idx: int,
    selected: Dict[str, int],
    bindings: Dict[str, int],
) -> Tuple[bool, List[Tuple[str, int]]]:
    qid = spec["qid"]
    if event_idx in selected.values():
        return False, []
    if int(rt.event_type_id[event_idx]) != int(spec["event_type_id"]):
        return False, []
    if spec.get("protocol_id", 0) and int(rt.protocol_id[event_idx]) != int(spec["protocol_id"]):
        return False, []

    new_bindings: List[Tuple[str, int]] = []
    for field, raw_val in spec["roles"].items():
        val_id = rt.role_id(event_idx, field)
        if isinstance(raw_val, str) and raw_val.startswith("$"):
            if val_id == 0:
                return False, []
            if raw_val in bindings:
                if bindings[raw_val] != val_id:
                    return False, []
            else:
                bindings[raw_val] = val_id
                new_bindings.append((raw_val, val_id))
        else:
            const_id = rt.encode_value(raw_val)
            if const_id == 0 or val_id != const_id:
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)
                return False, []

    # Incremental order constraints.
    for left_id, right_id in query.get("order", []):
        if left_id == qid and right_id in selected:
            if int(rt.seq_index[event_idx]) >= int(rt.seq_index[selected[right_id]]):
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)
                return False, []
        elif right_id == qid and left_id in selected:
            if int(rt.seq_index[selected[left_id]]) >= int(rt.seq_index[event_idx]):
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)
                return False, []

    # Incremental neq constraints.
    for left_id, left_field, right_id, right_field in query.get("neq_constraints", []):
        if left_id == qid and right_id == qid:
            if rt.role_id(event_idx, left_field) == rt.role_id(event_idx, right_field):
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)
                return False, []
        elif left_id == qid and right_id in selected:
            if rt.role_id(event_idx, left_field) == rt.role_id(selected[right_id], right_field):
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)
                return False, []
        elif right_id == qid and left_id in selected:
            if rt.role_id(selected[left_id], left_field) == rt.role_id(event_idx, right_field):
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)
                return False, []

    # Attribute filters.  The current benchmark has none, but keep the runner
    # semantically complete for numeric amount-like fields.
    for flt in query.get("attr_filters", []):
        if flt.get("event") != qid:
            continue
        field = flt.get("field")
        op = flt.get("op")
        if op not in SUPPORTED_ATTR_OPS:
            raise ValueError(f"Unsupported attr op: {op}")
        try:
            rhs = float(flt.get("value"))
        except Exception:
            rhs = 0.0
        if field in NUMERIC_FIELDS:
            lhs = rt.numeric_value(event_idx, field)
            if not compare_numeric(lhs, op, rhs):
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)
                return False, []
        else:
            # Non-numeric equality filters can be represented as role constants.
            if op != "=":
                raise ValueError(f"Non-numeric attr filter only supports '=': {field} {op}")
            lhs = rt.role_id(event_idx, field)
            rhs_id = rt.encode_value(flt.get("value"))
            if lhs != rhs_id:
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)
                return False, []

    return True, new_bindings


def count_query(rt: EncodedSameTxRuntime, query: dict) -> Dict[str, Any]:
    plan = build_query_plan(query, rt)
    tx_ids = candidate_tx_ids(query, rt)
    if not plan or not tx_ids:
        return {
            "num_matches": 0,
            "num_result_txs": 0,
            "num_candidate_txs": len(tx_ids),
            "num_candidate_event_bindings_examined": 0,
            "plan_order": [p["qid"] for p in plan],
        }

    num_matches = 0
    matched_txs = set()
    examined = 0

    for tx in tx_ids:
        selected: Dict[str, int] = {}
        bindings: Dict[str, int] = {}
        tx_matched = False

        def dfs(pos: int) -> None:
            nonlocal num_matches, examined, tx_matched
            if pos >= len(plan):
                num_matches += 1
                tx_matched = True
                return
            spec = plan[pos]
            candidates = rt.tx_type_events.get(tx, {}).get(int(spec["event_type_id"]), [])
            for event_idx in candidates:
                examined += 1
                ok, new_bindings = can_assign(rt, query, spec, event_idx, selected, bindings)
                if not ok:
                    continue
                selected[spec["qid"]] = event_idx
                dfs(pos + 1)
                selected.pop(spec["qid"], None)
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)

        dfs(0)
        if tx_matched:
            matched_txs.add(tx)

    return {
        "num_matches": int(num_matches),
        "num_result_txs": int(len(matched_txs)),
        "num_candidate_txs": int(len(tx_ids)),
        "num_candidate_event_bindings_examined": int(examined),
        "plan_order": [p["qid"] for p in plan],
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run encoded same-tx HVCEQ query in summary/count-only mode")
    ap.add_argument("--db", required=True)
    ap.add_argument("--query-json", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--share-filter-mode", choices=["strict", "off"], default="off", help="Compatibility argument; encoded runner uses exact semantics.")
    ap.add_argument("--sample-limit", type=int, default=0, help="Accepted for compatibility. Default 0; encoded runner does not write matched details.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    ensure_dir(args.outdir)
    query = load_query_json(args.query_json)

    t0 = time.perf_counter()
    conn = sqlite3.connect(args.db)
    try:
        rt = EncodedSameTxRuntime(conn, load_event_ids=False)
    finally:
        conn.close()
    t1 = time.perf_counter()
    result = count_query(rt, query)
    t2 = time.perf_counter()

    report = {
        "query_name": query.get("query_name"),
        "scope": "same_tx",
        "system": "HVCEQ",
        "storage_layout": "encoded_same_tx_integer_sidecar",
        "summary_count_only": True,
        "sample_limit": int(args.sample_limit),
        "matched_instances_csv_written": False,
        "matched_bindings_csv_written": False,
        "full_result_materialized": False,
        "num_matches": int(result["num_matches"]),
        "num_result_units": int(result["num_result_txs"]),
        "num_result_txs": int(result["num_result_txs"]),
        "num_candidate_units": int(result["num_candidate_txs"]),
        "num_candidate_txs": int(result["num_candidate_txs"]),
        "num_candidate_event_bindings_examined": int(result["num_candidate_event_bindings_examined"]),
        "plan_order": result["plan_order"],
        "metrics": {
            "setup_s": round(t1 - t0, 6),
            "query_latency_s": round(t2 - t1, 6),
            "query_wall_time_s": round(t2 - t0, 6),
        },
        "stage_times_s": {
            "setup_load_encoded_sidecar_s": round(t1 - t0, 6),
            "encoded_integer_count_s": round(t2 - t1, 6),
        },
        "runtime_stats": {
            "event_count": int(rt.event_count),
            "tx_count": int(rt.tx_count),
            "event_type_count": int(len(rt.event_type_to_id)),
            "value_count": int(rt.value_count),
        },
    }
    outdir = Path(args.outdir)
    with open(outdir / "query_result.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    # Keep output strictly summary/count-only.
    (outdir / "matched_instances.csv").write_text("", encoding="utf-8")
    (outdir / "matched_bindings.csv").write_text("", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
