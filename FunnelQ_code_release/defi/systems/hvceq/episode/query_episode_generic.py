#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from systems.hvceq.episode.query_episode_encoded_sidecar import EncodedRuntime, load_query
from systems.hvceq.rust_integer_kernel import (
    available as rust_kernel_available,
    match_integer_domains,
    refine_anchor_domains,
)


SUPPORTED_TYPES = {"flashloan", "swap", "borrow"}


def is_var(x: Any) -> bool:
    return isinstance(x, str) and x.startswith("$")


def norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s.lower() if s.startswith("0x") else s


def digest_ids(ids: Sequence[int]) -> str:
    h = hashlib.sha256()
    for x in sorted(int(v) for v in ids):
        h.update(str(x).encode("ascii"))
        h.update(b"\n")
    return h.hexdigest()


def load_value_ids(conn: sqlite3.Connection) -> Dict[str, int]:
    return {str(text): int(vid) for vid, text in conn.execute("SELECT value_id, value_text FROM value_dict")}


def encode_value(value_to_id: Dict[str, int], value: Any) -> int:
    v = norm_str(value)
    if not v:
        return 0
    return int(value_to_id.get(v, 0))


def specs(query: dict) -> Dict[str, dict]:
    out = {str(e["id"]): dict(e) for e in query.get("events", [])}
    for flt in query.get("attr_filters", []) or []:
        qid = str(flt.get("event", ""))
        if qid in out:
            out[qid].setdefault("_attr_filters", []).append(dict(flt))
    return out


def active_roles(spec: dict) -> Dict[str, Any]:
    return dict(spec.get("roles", {}) or {})


def variable_occurrences(query: dict) -> Dict[str, List[Tuple[str, str]]]:
    occ: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for e in query.get("events", []):
        for field, val in active_roles(e).items():
            if is_var(val):
                occ[str(val)].append((str(e["id"]), str(field)))
    return occ


class EpisodeAccessor:
    def __init__(self, rt: EncodedRuntime, value_to_id: Dict[str, int]):
        self.rt = rt
        self.value_to_id = value_to_id
        self._postings: Dict[Tuple[str, str], Dict[int, List[int]]] = {}
        self._posting_sets: Dict[Tuple[str, str, int], set[int]] = {}
        self._np_arrays: Dict[Tuple[str, str], np.ndarray] = {}

    def type_len(self, etype: str) -> int:
        if etype == "flashloan":
            return len(self.rt.a_event_ord)
        if etype == "swap":
            return len(self.rt.s_event_ord)
        if etype == "borrow":
            return len(self.rt.b_event_ord)
        return 0

    def ords(self, etype: str):
        if etype == "flashloan":
            return self.rt.a_event_ord
        if etype == "swap":
            return self.rt.s_event_ord
        if etype == "borrow":
            return self.rt.b_event_ord
        raise ValueError(etype)

    def event_ord(self, etype: str, idx: int) -> int:
        return int(self.ords(etype)[idx])

    def tx_ord(self, etype: str, idx: int) -> int:
        if etype == "flashloan":
            return int(self.rt.a_tx_ord[idx])
        if etype == "swap":
            return int(self.rt.s_tx_ord[idx])
        if etype == "borrow":
            return int(self.rt.b_tx_ord[idx])
        return 0

    def role(self, etype: str, idx: int, field: str) -> int:
        if field == "event_id":
            return self.event_ord(etype, idx)
        if field == "global_event_ord":
            return self.event_ord(etype, idx)
        if field == "global_tx_ord":
            return self.tx_ord(etype, idx)
        if field == "seq_index":
            if etype == "flashloan":
                return int(self.rt.a_seq[idx])
            if etype == "swap":
                return int(self.rt.s_seq[idx])
            if etype == "borrow":
                return int(self.rt.b_seq[idx])
        if field == "protocol":
            if etype == "flashloan":
                return int(self.rt.a_protocol[idx])
            if etype == "swap":
                return int(self.rt.s_protocol[idx])
            if etype == "borrow":
                return int(self.rt.b_protocol[idx])
        if etype == "flashloan":
            if field == "recipient":
                return int(self.rt.a_recipient[idx])
            if field == "asset":
                return int(self.rt.a_asset[idx])
            if field == "pool":
                return int(self.rt.a_pool[idx])
        if etype == "swap":
            if field == "taker":
                return int(self.rt.s_taker[idx])
            if field == "token_sold":
                return int(self.rt.s_sold[idx])
            if field == "token_bought":
                return int(self.rt.s_bought[idx])
            if field == "pool":
                return int(self.rt.s_pool[idx])
        if etype == "borrow":
            if field == "borrower":
                return int(self.rt.b_borrower[idx])
            if field == "asset":
                return int(self.rt.b_asset[idx])
            if field == "pool":
                return int(self.rt.b_pool[idx])
        return 0

    def numeric(self, etype: str, idx: int, field: str) -> float:
        if etype == "flashloan" and field == "amount":
            return float(self.rt.a_amount[idx])
        if etype == "swap" and field == "amount_in":
            return float(self.rt.s_amount_in[idx])
        if etype == "swap" and field == "amount_out":
            return float(self.rt.s_amount_out[idx])
        if etype == "borrow" and field == "amount":
            return float(self.rt.b_amount[idx])
        return 0.0

    def slice_by_ord(self, etype: str, lo_exclusive: int, hi_inclusive: int) -> range:
        ords = self.ords(etype)
        l = bisect.bisect_right(ords, int(lo_exclusive))
        r = bisect.bisect_right(ords, int(hi_inclusive))
        return range(l, r)

    def posting(self, etype: str, field: str) -> Dict[int, List[int]]:

        key = (etype, field)
        if key in self._postings:
            return self._postings[key]
        d: Dict[int, List[int]] = defaultdict(list)
        for idx in range(self.type_len(etype)):
            v = self.role(etype, idx, field)
            if v != 0:
                d[int(v)].append(idx)
        self._postings[key] = dict(d)
        return self._postings[key]

    def posting_set(self, etype: str, field: str, value: int) -> set[int]:
        key = (etype, field, int(value))
        if key not in self._posting_sets:
            self._posting_sets[key] = set(self.posting(etype, field).get(int(value), []))
        return self._posting_sets[key]

    def np_array(self, etype: str, field: str) -> np.ndarray:
        key = (etype, field)
        if key in self._np_arrays:
            return self._np_arrays[key]
        if field in {"event_id", "global_event_ord"}:
            source = self.ords(etype)
        elif field == "global_tx_ord":
            source = {
                "flashloan": self.rt.a_tx_ord,
                "swap": self.rt.s_tx_ord,
                "borrow": self.rt.b_tx_ord,
            }[etype]
        elif field == "seq_index":
            source = {
                "flashloan": self.rt.a_seq,
                "swap": self.rt.s_seq,
                "borrow": self.rt.b_seq,
            }[etype]
        elif field == "protocol":
            source = {
                "flashloan": self.rt.a_protocol,
                "swap": self.rt.s_protocol,
                "borrow": self.rt.b_protocol,
            }[etype]
        else:
            sources = {
                ("flashloan", "recipient"): self.rt.a_recipient,
                ("flashloan", "asset"): self.rt.a_asset,
                ("flashloan", "pool"): self.rt.a_pool,
                ("swap", "taker"): self.rt.s_taker,
                ("swap", "token_sold"): self.rt.s_sold,
                ("swap", "token_bought"): self.rt.s_bought,
                ("swap", "pool"): self.rt.s_pool,
                ("borrow", "borrower"): self.rt.b_borrower,
                ("borrow", "asset"): self.rt.b_asset,
                ("borrow", "pool"): self.rt.b_pool,
            }
            source = sources.get(key)
            if source is None:
                return np.zeros(self.type_len(etype), dtype=np.int32)
        out = np.frombuffer(source, dtype=np.dtype(source.typecode))
        self._np_arrays[key] = out
        return out

    def np_numeric(self, etype: str, field: str) -> np.ndarray:
        key = (etype, f"numeric:{field}")
        if key in self._np_arrays:
            return self._np_arrays[key]
        sources = {
            ("flashloan", "amount"): self.rt.a_amount,
            ("swap", "amount_in"): self.rt.s_amount_in,
            ("swap", "amount_out"): self.rt.s_amount_out,
            ("borrow", "amount"): self.rt.b_amount,
        }
        source = sources.get((etype, field))
        if source is None:
            return np.zeros(self.type_len(etype), dtype=np.float64)
        out = np.frombuffer(source, dtype=np.dtype(source.typecode))
        self._np_arrays[key] = out
        return out


class CandidateBatch:
    def __init__(self, anchor_idx: np.ndarray, lo_ord: np.ndarray, hi_ord: np.ndarray):
        self.anchor_idx = np.asarray(anchor_idx, dtype=np.int64)
        self.lo_ord = np.asarray(lo_ord, dtype=np.int64)
        self.hi_ord = np.asarray(hi_ord, dtype=np.int64)

    def __len__(self) -> int:
        return int(self.anchor_idx.size)

    def __iter__(self):
        return iter(zip(self.anchor_idx.tolist(), self.lo_ord.tolist(), self.hi_ord.tolist()))

    def select(self, mask: np.ndarray) -> "CandidateBatch":
        return CandidateBatch(self.anchor_idx[mask], self.lo_ord[mask], self.hi_ord[mask])

    def ids(self) -> np.ndarray:
        return self.anchor_idx


def event_matches_spec(acc: EpisodeAccessor, spec: dict, idx: int, bindings: Dict[str, int] | None = None) -> Tuple[bool, Dict[str, int]]:
    etype = str(spec.get("type", ""))
    if etype not in SUPPORTED_TYPES:
        return False, {}
    local_new: Dict[str, int] = {}
    b = bindings if bindings is not None else {}
    protocol = norm_str(spec.get("protocol", ""))
    if protocol:
        want = encode_value(acc.value_to_id, protocol)
        if want == 0 or acc.role(etype, idx, "protocol") != want:
            return False, {}
    for field, raw in active_roles(spec).items():
        actual = acc.role(etype, idx, field)
        if is_var(raw):
            if actual == 0:
                return False, {}
            if raw in b:
                if b[raw] != actual:
                    return False, {}
            else:
                local_new[str(raw)] = actual
        else:
            want = encode_value(acc.value_to_id, raw)
            if want == 0 or actual != want:
                return False, {}
    for flt in spec.get("_attr_filters", []) or []:
        field = str(flt.get("field", ""))
        op = str(flt.get("op", ""))
        try:
            actual_f = acc.numeric(etype, idx, field)
            want_f = float(flt.get("value"))
        except (TypeError, ValueError):
            return False, {}
        if op == "=" and actual_f != want_f:
            return False, {}
        if op == ">" and not (actual_f > want_f):
            return False, {}
        if op == ">=" and not (actual_f >= want_f):
            return False, {}
        if op == "<" and not (actual_f < want_f):
            return False, {}
        if op == "<=" and not (actual_f <= want_f):
            return False, {}
        if op not in {"=", ">", ">=", "<", "<="}:
            return False, {}
    return True, local_new


def spec_mask_numpy(acc: EpisodeAccessor, spec: dict) -> np.ndarray:
    etype = str(spec.get("type", ""))
    size = acc.type_len(etype)
    mask = np.ones(size, dtype=np.bool_)
    protocol = norm_str(spec.get("protocol", ""))
    if protocol:
        wanted = encode_value(acc.value_to_id, protocol)
        if wanted == 0:
            return np.zeros(size, dtype=np.bool_)
        mask &= acc.np_array(etype, "protocol") == wanted
    for field, raw in active_roles(spec).items():
        actual = acc.np_array(etype, field)
        if is_var(raw):
            mask &= actual != 0
        else:
            wanted = encode_value(acc.value_to_id, raw)
            if wanted == 0:
                return np.zeros(size, dtype=np.bool_)
            mask &= actual == wanted
    for flt in spec.get("_attr_filters", []) or []:
        actual = acc.np_numeric(etype, str(flt.get("field", "")))
        try:
            wanted = float(flt.get("value"))
        except (TypeError, ValueError):
            return np.zeros(size, dtype=np.bool_)
        op = str(flt.get("op", ""))
        if op == "=":
            mask &= actual == wanted
        elif op == ">":
            mask &= actual > wanted
        elif op == ">=":
            mask &= actual >= wanted
        elif op == "<":
            mask &= actual < wanted
        elif op == "<=":
            mask &= actual <= wanted
        else:
            return np.zeros(size, dtype=np.bool_)
    return mask


def semantic_candidates_numpy(
    acc: EpisodeAccessor, query: dict, semantic_qid: str
) -> CandidateBatch:
    sem_spec = specs(query)[semantic_qid]
    etype = str(sem_spec.get("type", ""))
    anchor_idx = np.flatnonzero(spec_mask_numpy(acc, sem_spec)).astype(np.int64, copy=False)
    txs = acc.np_array(etype, "global_tx_ord")[anchor_idx].astype(np.int64, copy=False)
    lo_ord = acc.np_array(etype, "global_event_ord")[anchor_idx].astype(np.int64, copy=False)
    window_end = np.frombuffer(acc.rt.window_end, dtype=np.dtype(acc.rt.window_end.typecode))
    tx_max_ord = np.frombuffer(
        acc.rt.tx_max_event_ord, dtype=np.dtype(acc.rt.tx_max_event_ord.typecode)
    )
    hi_ord = tx_max_ord[window_end[txs]]
    return CandidateBatch(anchor_idx, lo_ord, hi_ord)


def base_filter_numpy(
    acc: EpisodeAccessor,
    query: dict,
    cand0: CandidateBatch,
    anchor_type: str,
) -> CandidateBatch:
    if not len(cand0):
        return cand0
    txs_all = acc.np_array(anchor_type, "global_tx_ord")
    lo_txs = txs_all[cand0.anchor_idx].astype(np.int64, copy=False)
    window_end = np.frombuffer(acc.rt.window_end, dtype=np.dtype(acc.rt.window_end.typecode))
    end_txs = window_end[lo_txs]
    keep = np.ones(len(cand0), dtype=np.bool_)
    for etype, need in need_counts(query).items():
        if etype not in SUPPORTED_TYPES:
            keep[:] = False
            break
        prefix_source = acc.rt.type_prefix[etype]
        prefix = np.frombuffer(prefix_source, dtype=np.dtype(prefix_source.typecode))
        counts = prefix[end_txs + 1] - prefix[lo_txs]
        keep &= counts >= int(need)
    return cand0.select(keep)


def refined_filter_two_event_numpy(
    acc: EpisodeAccessor,
    query: dict,
    cand1: CandidateBatch,
    semantic_qid: str,
) -> CandidateBatch | None:
    sp = specs(query)
    if len(sp) != 2 or query.get("neq_constraints"):
        return None
    other_qids = [qid for qid in sp if qid != semantic_qid]
    if len(other_qids) != 1:
        return None
    target_qid = other_qids[0]
    sem_spec = sp[semantic_qid]
    target_spec = sp[target_qid]
    sem_type = str(sem_spec.get("type", ""))
    target_type = str(target_spec.get("type", ""))
    sem_vars = {
        str(raw): str(field)
        for field, raw in active_roles(sem_spec).items()
        if is_var(raw)
    }
    joins: List[Tuple[str, str]] = []
    for target_field, raw in active_roles(target_spec).items():
        if is_var(raw) and str(raw) in sem_vars:
            joins.append((sem_vars[str(raw)], str(target_field)))
    if not joins:
        return None

    target_mask = spec_mask_numpy(acc, target_spec)
    target_idx = np.flatnonzero(target_mask).astype(np.int64, copy=False)
    if target_idx.size == 0 or len(cand1) == 0:
        return CandidateBatch(
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.int64),
        )

    fields = [(f"j{i}", "<i8") for i in range(len(joins))] + [("ord", "<i8")]
    target_records = np.empty(target_idx.size, dtype=fields)
    probe_records = np.empty(len(cand1), dtype=fields)
    for i, (sem_field, target_field) in enumerate(joins):
        target_records[f"j{i}"] = acc.np_array(target_type, target_field)[target_idx]
        probe_records[f"j{i}"] = acc.np_array(sem_type, sem_field)[cand1.anchor_idx]
    target_records["ord"] = acc.np_array(target_type, "global_event_ord")[target_idx]
    probe_records["ord"] = cand1.lo_ord
    order_fields = [name for name, _dtype in fields]
    target_records.sort(order=order_fields)
    positions = np.searchsorted(target_records, probe_records, side="right")
    safe_positions = np.minimum(positions, max(0, target_records.size - 1))
    keep = positions < target_records.size
    for i in range(len(joins)):
        keep &= target_records[f"j{i}"][safe_positions] == probe_records[f"j{i}"]
    keep &= target_records["ord"][safe_positions] <= cand1.hi_ord
    return cand1.select(keep)


def need_counts(query: dict) -> Counter:
    c = Counter()
    for e in query.get("events", []):
        c[str(e.get("type", ""))] += 1
    return c


def base_filter(
    acc: EpisodeAccessor,
    query: dict,
    cand0: Sequence[Tuple[int, int, int]],
    anchor_type: str,
) -> List[Tuple[int, int, int]]:
    out: List[Tuple[int, int, int]] = []
    needs = need_counts(query)
    for anchor_idx, lo_ord, hi_ord in cand0:
        lo_tx = acc.tx_ord(anchor_type, anchor_idx)
        end_tx = acc.rt.window_end[lo_tx]
        ok = True
        for etype, need in needs.items():
            if etype not in SUPPORTED_TYPES:
                ok = False
                break
            total = acc.rt.range_count(etype, lo_tx, end_tx)
            if total < need:
                ok = False
                break
        if ok:
            out.append((anchor_idx, lo_ord, hi_ord))
    return out


def candidate_indices(acc: EpisodeAccessor, spec: dict, bindings: Dict[str, int], lo_ord: int, hi_ord: int) -> Iterable[int]:
    etype = str(spec.get("type", ""))
    if etype not in SUPPORTED_TYPES:
        return []


    constraints: List[Tuple[str, int, List[int]]] = []
    protocol = norm_str(spec.get("protocol", ""))
    if protocol:
        want = encode_value(acc.value_to_id, protocol)
        if want == 0:
            return []
        constraints.append(("protocol", want, acc.posting(etype, "protocol").get(want, [])))
    for field, raw in active_roles(spec).items():
        if field == "event_id":
            continue
        if is_var(raw):
            bound = bindings.get(str(raw))
            if bound is not None:
                constraints.append((field, int(bound), acc.posting(etype, field).get(int(bound), [])))
        else:
            want = encode_value(acc.value_to_id, raw)
            if want == 0:
                return []
            constraints.append((field, want, acc.posting(etype, field).get(want, [])))
    window = acc.slice_by_ord(etype, lo_ord, hi_ord)
    if not constraints:
        return window
    if any(len(lst) == 0 for _field, _value, lst in constraints):
        return []


    base_kind = "window"
    base_items: Iterable[int] = window
    base_len = len(window)
    base_satisfied: set[Tuple[str, int]] = set()
    by_field = {field: value for field, value, _lst in constraints}
    if etype == "swap" and "taker" in by_field and "token_sold" in by_field:
        compound = acc.rt.swaps(
            by_field["taker"], by_field["token_sold"], int(lo_ord) + 1, int(hi_ord)
        )
        if len(compound) < base_len:
            base_kind = "compound:swap:taker:token_sold"
            base_items = compound
            base_len = len(compound)
            base_satisfied = {
                ("taker", by_field["taker"]),
                ("token_sold", by_field["token_sold"]),
            }
    elif etype == "borrow" and "borrower" in by_field and "asset" in by_field:
        compound = acc.rt.borrows(
            by_field["borrower"], by_field["asset"], int(lo_ord) + 1, int(hi_ord)
        )
        if len(compound) < base_len:
            base_kind = "compound:borrow:borrower:asset"
            base_items = compound
            base_len = len(compound)
            base_satisfied = {
                ("borrower", by_field["borrower"]),
                ("asset", by_field["asset"]),
            }
    for field, value, lst in constraints:
        if len(lst) < base_len:
            base_kind = f"posting:{field}:{value}"
            base_items = lst
            base_len = len(lst)
            base_satisfied = {(field, value)}
    membership = [
        (field, value, acc.posting_set(etype, field, value))
        for field, value, _lst in constraints
        if (field, value) not in base_satisfied
    ]
    out: List[int] = []
    ords = acc.ords(etype)
    for idx in base_items:
        eo = int(ords[idx])
        if eo <= lo_ord or eo > hi_ord:
            continue
        if all(idx in s for _field, _value, s in membership):
            out.append(idx)
    return out


def refined_filter(acc: EpisodeAccessor, query: dict, cand1: Sequence[Tuple[int, int, int]] | CandidateBatch, semantic_qid: str) -> List[Tuple[int, int, int]] | CandidateBatch:
    if isinstance(cand1, CandidateBatch):
        fast = refined_filter_two_event_numpy(acc, query, cand1, semantic_qid)
        if fast is not None:
            return fast
    sp = specs(query)
    sem_spec = sp[semantic_qid]
    anchor_type = str(sem_spec.get("type", ""))
    out: List[Tuple[int, int, int]] = []
    for anchor_idx, lo_ord, hi_ord in cand1:
        ok, new = event_matches_spec(acc, sem_spec, anchor_idx, {})
        if not ok:
            continue
        bindings = dict(new)
        feasible = True
        for qid, spec in sp.items():
            if qid == semantic_qid:
                continue
            found = False
            for idx in candidate_indices(acc, spec, bindings, lo_ord, hi_ord):
                ok2, _new2 = event_matches_spec(acc, spec, idx, bindings)
                if ok2:
                    found = True
                    break
            if not found:
                feasible = False
                break
        if feasible:
            out.append((anchor_idx, lo_ord, hi_ord))
    return out


def order_ok(acc: EpisodeAccessor, query: dict, selected: Dict[str, Tuple[str, int]]) -> bool:
    for left, right in query.get("order", []):
        if left in selected and right in selected:
            lt, li = selected[left]
            rt, ri = selected[right]
            if acc.event_ord(lt, li) >= acc.event_ord(rt, ri):
                return False
    return True


def candidate_time_bounds(
    acc: EpisodeAccessor,
    query: dict,
    selected: Dict[str, Tuple[str, int]],
    qid: str,
    lo_ord: int,
    hi_ord: int,
) -> Tuple[int, int]:


    low = int(lo_ord)
    high = int(hi_ord)
    for left, right in query.get("order", []):
        left = str(left)
        right = str(right)
        if right == qid and left in selected:
            etype, idx = selected[left]
            low = max(low, acc.event_ord(etype, idx))
        if left == qid and right in selected:
            etype, idx = selected[right]


            high = min(high, acc.event_ord(etype, idx) - 1)
    return low, high


def terminal_aggregate_count(
    acc: EpisodeAccessor,
    query: dict,
    selected: Dict[str, Tuple[str, int]],
    bindings: Dict[str, int],
    qid: str,
    spec: dict,
    lo_ord: int,
    hi_ord: int,
) -> int | None:


    etype = str(spec.get("type", ""))
    roles = active_roles(spec)
    if spec.get("_attr_filters") or norm_str(spec.get("protocol", "")):
        return None


    for selected_type, selected_idx in selected.values():
        selected_ord = acc.event_ord(selected_type, selected_idx)
        if int(lo_ord) < selected_ord <= int(hi_ord):
            return None

    bound: Dict[str, int] = {}
    for field, raw in roles.items():
        if not is_var(raw):
            return None
        if raw in bindings:
            bound[str(field)] = int(bindings[raw])


    excluded: Dict[str, set[int]] = defaultdict(set)
    for left, left_field, right, right_field in query.get("neq_constraints", []):
        left = str(left)
        right = str(right)
        left_field = str(left_field)
        right_field = str(right_field)
        if left != qid and right != qid:
            continue
        if left == qid:
            terminal_field, other_qid, other_field = left_field, right, right_field
        else:
            terminal_field, other_qid, other_field = right_field, left, left_field
        if other_qid not in selected:
            return None
        if terminal_field == "event_id":


            continue
        if terminal_field not in {"token_bought", "pool"}:
            return None
        other_type, other_idx = selected[other_qid]
        excluded[terminal_field].add(acc.role(other_type, other_idx, other_field))


    if etype == "swap":
        schema_roles = {"taker", "token_sold", "token_bought", "pool"}
        if set(roles) != schema_roles or set(bound) != {"taker", "token_sold"}:
            return None
        return acc.rt.count_complete_swaps_with_neq(
            bound["taker"],
            bound["token_sold"],
            int(lo_ord),
            int(hi_ord),
            exclude_bought=excluded.get("token_bought", set()),
            exclude_pool=excluded.get("pool", set()),
        )
    if etype == "borrow":
        schema_roles = {"borrower", "asset", "pool"}
        if set(roles) != schema_roles or set(bound) != {"borrower", "asset"}:
            return None
        return len(acc.rt.complete_borrows(bound["borrower"], bound["asset"], int(lo_ord) + 1, int(hi_ord)))
    return None


def neq_ok(acc: EpisodeAccessor, query: dict, selected: Dict[str, Tuple[str, int]]) -> bool:
    sp = specs(query)
    for le, lf, re, rf in query.get("neq_constraints", []):
        if le in selected and re in selected:
            lt, li = selected[le]
            rt, ri = selected[re]
            if acc.role(lt, li, lf) == acc.role(rt, ri, rf):
                return False
    event_ids = [acc.event_ord(t, i) for t, i in selected.values()]
    return len(set(event_ids)) == len(event_ids)


def choose_next_qid(acc: EpisodeAccessor, query: dict, selected: Dict[str, Tuple[str, int]], bindings: Dict[str, int], lo_ord: int, hi_ord: int) -> str:
    sp = specs(query)
    remaining = [qid for qid in sp if qid not in selected]
    best = None
    for qid in remaining:
        spec = sp[qid]
        bound_roles = sum(1 for v in active_roles(spec).values() if is_var(v) and v in bindings)
        c = 0
        for idx in candidate_indices(acc, spec, bindings, lo_ord, hi_ord):
            ok, _new = event_matches_spec(acc, spec, idx, bindings)
            if ok:
                c += 1
        key = (c if c else 10**12, -bound_roles, qid)
        if best is None or key < best:
            best = key
    if best is None:
        raise RuntimeError("no remaining qid")
    return best[2]


def compile_match_order(query: dict, semantic_qid: str) -> List[str]:

    sp = specs(query)
    remaining = set(sp)
    remaining.remove(semantic_qid)
    result = [semantic_qid]
    placed = {semantic_qid}
    predecessors: Dict[str, set[str]] = {qid: set() for qid in sp}
    for left, right in query.get("order", []):
        predecessors[str(right)].add(str(left))
    introduced = {
        str(value)
        for value in active_roles(sp[semantic_qid]).values()
        if is_var(value)
    }
    while remaining:
        ready = [qid for qid in remaining if predecessors[qid].issubset(placed)]
        if not ready:
            ready = sorted(remaining)
        def key(qid: str) -> Tuple[int, int, str]:
            roles = active_roles(sp[qid]).values()
            bound = sum(1 for value in roles if is_var(value) and str(value) in introduced)
            new = sum(1 for value in roles if is_var(value) and str(value) not in introduced)
            return (-bound, new, qid)
        qid = min(ready, key=key)
        result.append(qid)
        placed.add(qid)
        remaining.remove(qid)
        introduced.update(str(value) for value in active_roles(sp[qid]).values() if is_var(value))
    return result


def write_binding_headers(outdir: Path) -> Tuple[Path, Path]:
    inst = outdir / "matched_instances.csv"
    bind = outdir / "matched_bindings.csv"
    with inst.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["match_id", "episode_id", "anchor_event_ord", "query_name", "num_events"])
    with bind.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["match_id", "episode_id", "query_event_id", "event_type", "event_ord", "tx_ord"])
    return inst, bind


def rust_generic_match(
    acc: EpisodeAccessor,
    query: dict,
    cand2: CandidateBatch,
    semantic_qid: str,
    outdir: Path,
) -> Dict[str, Any]:
    sp = specs(query)
    qids = list(sp)
    eligible = {qid: spec_mask_numpy(acc, sp[qid]) for qid in qids}
    starts = np.empty((len(cand2), len(qids)), dtype=np.uint64)
    ends = np.empty((len(cand2), len(qids)), dtype=np.uint64)
    views: Dict[str, Dict[str, Any]] = {}
    type_view_ids = {etype: i for i, etype in enumerate(sorted(SUPPORTED_TYPES))}
    for col, qid in enumerate(qids):
        etype = str(sp[qid].get("type", ""))
        ords = acc.np_array(etype, "global_event_ord")
        starts[:, col] = np.searchsorted(ords, cand2.lo_ord, side="right")
        ends[:, col] = np.searchsorted(ords, cand2.hi_ord, side="right")
        views[qid] = {"ord": ords, "view_id": type_view_ids[etype]}

    def role_values(qid: str, field: str) -> np.ndarray:
        etype = str(sp[qid].get("type", ""))
        return acc.np_array(etype, field)

    result = match_integer_domains(
        query=query,
        qids=qids,
        anchor_qid=semantic_qid,
        match_order=compile_match_order(query, semantic_qid),
        views=views,
        role_array=role_values,
        eligible=eligible,
        anchor_indices=cand2.anchor_idx.astype(np.uint64, copy=False),
        unit_ids=cand2.lo_ord.astype(np.uint64, copy=False),
        range_starts=starts,
        range_ends=ends,
    )
    inst_path, bind_path = write_binding_headers(outdir)
    return {
        "num_matches": int(result["num_matches"]),
        "num_result_episodes": int(result["num_result_units"]),
        "num_candidate_event_bindings_examined": int(result["examined"]),
        "num_terminal_aggregate_calls": 0,
        "num_terminal_candidates_aggregated": 0,
        "compiled_match_order": compile_match_order(query, semantic_qid),
        "matched_instances_path": str(inst_path),
        "matched_bindings_path": str(bind_path),
        "matching_engine": str(result["engine"]),
    }


def rust_refined_filter(
    acc: EpisodeAccessor,
    query: dict,
    cand1: CandidateBatch,
    semantic_qid: str,
) -> CandidateBatch:
    if len(cand1) == 0:
        return cand1
    sp = specs(query)
    qids = list(sp)
    eligible = {qid: spec_mask_numpy(acc, sp[qid]) for qid in qids}
    starts = np.empty((len(cand1), len(qids)), dtype=np.uint64)
    ends = np.empty((len(cand1), len(qids)), dtype=np.uint64)
    views: Dict[str, Dict[str, Any]] = {}
    type_view_ids = {etype: i for i, etype in enumerate(sorted(SUPPORTED_TYPES))}
    for col, qid in enumerate(qids):
        etype = str(sp[qid].get("type", ""))
        ords = acc.np_array(etype, "global_event_ord")
        starts[:, col] = np.searchsorted(ords, cand1.lo_ord, side="right")
        ends[:, col] = np.searchsorted(ords, cand1.hi_ord, side="right")
        views[qid] = {"ord": ords, "view_id": type_view_ids[etype]}

    def role_values(qid: str, field: str) -> np.ndarray:
        return acc.np_array(str(sp[qid].get("type", "")), field)

    keep = refine_anchor_domains(
        query=query,
        qids=qids,
        anchor_qid=semantic_qid,
        views=views,
        role_array=role_values,
        eligible=eligible,
        anchor_indices=cand1.anchor_idx,
        range_starts=starts,
        range_ends=ends,
    )
    return cand1.select(keep)


def as_candidate_batch(
    candidates: Sequence[Tuple[int, int, int]] | CandidateBatch,
) -> CandidateBatch:
    if isinstance(candidates, CandidateBatch):
        return candidates
    if not candidates:
        empty = np.empty(0, dtype=np.int64)
        return CandidateBatch(empty, empty, empty)
    rows = np.asarray(candidates, dtype=np.int64)
    return CandidateBatch(rows[:, 0], rows[:, 1], rows[:, 2])


def generic_match(
    acc: EpisodeAccessor,
    query: dict,
    cand2: Sequence[Tuple[int, int, int]],
    semantic_qid: str,
    outdir: Path,
    write_bindings: bool,
) -> Dict[str, Any]:
    sp = specs(query)
    sem_spec = sp[semantic_qid]
    anchor_type = str(sem_spec.get("type", ""))
    match_order = compile_match_order(query, semantic_qid)
    inst_path, bind_path = write_binding_headers(outdir)
    inst_f = inst_path.open("a", newline="", encoding="utf-8") if write_bindings else None
    bind_f = bind_path.open("a", newline="", encoding="utf-8") if write_bindings else None
    inst_w = csv.writer(inst_f) if inst_f else None
    bind_w = csv.writer(bind_f) if bind_f else None
    total = 0
    result_episodes = 0
    examined = 0
    terminal_aggregate_calls = 0
    terminal_candidates_aggregated = 0
    try:
        for anchor_idx, lo_ord, hi_ord in cand2:
            selected: Dict[str, Tuple[str, int]] = {semantic_qid: (anchor_type, anchor_idx)}
            ok, new = event_matches_spec(acc, sem_spec, anchor_idx, {})
            if not ok:
                continue
            bindings = dict(new)
            episode_matches = 0

            def emit_match() -> None:
                nonlocal total, episode_matches
                total += 1
                episode_matches += 1
                if inst_w and bind_w:
                    episode_id = f"{acc.event_ord(anchor_type, anchor_idx)}::w{query['scope_params']['forward_window_sec']}"
                    match_id = f"{query.get('query_name')}::{episode_id}::{total}"
                    inst_w.writerow([match_id, episode_id, acc.event_ord(anchor_type, anchor_idx), query.get("query_name"), len(selected)])
                    for out_qid in sp:
                        et, out_idx = selected[out_qid]
                        bind_w.writerow([match_id, episode_id, out_qid, et, acc.event_ord(et, out_idx), acc.tx_ord(et, out_idx)])

            def dfs(depth: int) -> None:
                nonlocal total, episode_matches, examined
                nonlocal terminal_aggregate_calls, terminal_candidates_aggregated
                if depth >= len(match_order):
                    if order_ok(acc, query, selected) and neq_ok(acc, query, selected):
                        emit_match()
                    return
                qid = match_order[depth]
                spec = sp[qid]
                etype = str(spec.get("type"))
                candidate_lo, candidate_hi = candidate_time_bounds(
                    acc, query, selected, qid, lo_ord, hi_ord
                )
                if candidate_lo >= candidate_hi:
                    return
                if depth == len(match_order) - 1 and not write_bindings:
                    aggregate_count = terminal_aggregate_count(
                        acc,
                        query,
                        selected,
                        bindings,
                        qid,
                        spec,
                        candidate_lo,
                        candidate_hi,
                    )
                    if aggregate_count is not None:
                        terminal_aggregate_calls += 1
                        terminal_candidates_aggregated += int(aggregate_count)
                        total += int(aggregate_count)
                        episode_matches += int(aggregate_count)
                        return
                candidates = candidate_indices(
                    acc, spec, bindings, candidate_lo, candidate_hi
                )
                if depth == len(match_order) - 1 and not write_bindings:
                    valid = 0
                    for idx in candidates:
                        examined += 1
                        if (etype, idx) in selected.values():
                            continue
                        ok2, _new2 = event_matches_spec(acc, spec, idx, bindings)
                        if not ok2:
                            continue
                        selected[qid] = (etype, idx)
                        if order_ok(acc, query, selected) and neq_ok(acc, query, selected):
                            valid += 1
                        selected.pop(qid, None)
                    total += valid
                    episode_matches += valid
                    return
                for idx in candidates:
                    examined += 1
                    if (etype, idx) in selected.values():
                        continue
                    ok2, new2 = event_matches_spec(acc, spec, idx, bindings)
                    if not ok2:
                        continue
                    selected[qid] = (etype, idx)
                    for k, v in new2.items():
                        bindings[k] = v
                    if order_ok(acc, query, selected) and neq_ok(acc, query, selected):
                        dfs(depth + 1)
                    selected.pop(qid, None)
                    for k in new2:
                        bindings.pop(k, None)

            dfs(1)
            if episode_matches:
                result_episodes += 1
    finally:
        if inst_f:
            inst_f.close()
        if bind_f:
            bind_f.close()
    return {
        "num_matches": int(total),
        "num_result_episodes": int(result_episodes),
        "num_candidate_event_bindings_examined": int(examined),
        "num_terminal_aggregate_calls": int(terminal_aggregate_calls),
        "num_terminal_candidates_aggregated": int(terminal_candidates_aggregated),
        "compiled_match_order": match_order,
        "matched_instances_path": str(inst_path),
        "matched_bindings_path": str(bind_path),
    }


def run_generic(
    acc: EpisodeAccessor,
    query: dict,
    outdir: Path,
    write_bindings: bool,
    disable_refined: bool = False,
) -> Dict[str, Any]:
    semantic_qid = str(query.get("scope_params", {}).get("semantic_anchor_event", ""))
    if not semantic_qid:
        raise ValueError("anchored_episode query requires scope_params.semantic_anchor_event")
    sem_spec = specs(query)[semantic_qid]
    anchor_type = str(sem_spec.get("type", ""))
    if anchor_type not in SUPPORTED_TYPES:
        raise ValueError(f"unsupported semantic anchor type: {anchor_type}")
    window_sec = int(query["scope_params"]["forward_window_sec"])
    t0 = time.perf_counter()
    cand0 = semantic_candidates_numpy(acc, query, semantic_qid)
    t1 = time.perf_counter()
    cand1 = base_filter_numpy(acc, query, cand0, anchor_type)
    t2 = time.perf_counter()
    if disable_refined:
        cand2 = cand1
        refined_engine = "disabled_for_ablation"
    elif rust_kernel_available() and not getattr(acc.rt, "enable_role_postings", True):
        cand2 = rust_refined_filter(acc, query, cand1, semantic_qid)
        refined_engine = "rust_refined_episode_v1"
    else:
        cand2 = refined_filter(acc, query, cand1, semantic_qid)
        refined_engine = "python_refined_fallback"
    t3 = time.perf_counter()
    if not write_bindings and rust_kernel_available():
        exact = rust_generic_match(
            acc, query, as_candidate_batch(cand2), semantic_qid, outdir
        )
    else:
        exact = generic_match(acc, query, cand2, semantic_qid, outdir, write_bindings)
        exact["matching_engine"] = "python_generic_fallback"
    t4 = time.perf_counter()
    return {
        **exact,
        "semantic_anchor_qid": semantic_qid,
        "anchor_mode": "query-specified-semantic-anchor",
        "forward_window_sec": window_sec,
        "num_cand0": len(cand0),
        "num_cand1": len(cand1),
        "num_cand2": len(cand2),
        "refined_enabled": not bool(disable_refined),
        "refined_engine": refined_engine,
        "candidate_frontier_commitments": {
            "cand0_sha256": digest_ids(cand0.ids()),
            "cand1_sha256": digest_ids(cand1.ids() if isinstance(cand1, CandidateBatch) else [x[0] for x in cand1]),
            "cand2_sha256": digest_ids(cand2.ids() if isinstance(cand2, CandidateBatch) else [x[0] for x in cand2]),
        },
        "stage_times_s": {
            "candidate_generation": round(t1 - t0, 6),
            "base_filter": round(t2 - t1, 6),
            "refined_filter": round(t3 - t2, 6),
            "exact_match": round(t4 - t3, 6),
        },
        "query_latency_s": round(t4 - t0, 6),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generic JSON-compiled HVCEQ anchored-episode runner.")
    ap.add_argument("--episode-sidecar-db", required=True)
    ap.add_argument("--query-json", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--write-bindings", action="store_true")
    ap.add_argument("--warmup-runs", type=int, default=0)
    ap.add_argument("--phase-marker")
    ap.add_argument(
        "--disable-refined",
        action="store_true",
        help="Ablation only: bypass Refined and pass Cand1 directly to Exact.",
    )
    ap.add_argument(
        "--terminal-direct-scan-limit",
        type=int,
        default=2048,
        help=(
            "For terminal neq aggregation, scan small terminal domains directly "
            "and build the fixed anti-join counting directories only above this size."
        ),
    )
    return ap.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    query = load_query(Path(args.query_json))
    setup0 = time.perf_counter()
    conn = sqlite3.connect(args.episode_sidecar_db)
    try:
        summary_count_only = not bool(args.write_bindings)
        rt = EncodedRuntime(
            conn,
            int(query["scope_params"]["forward_window_sec"]),
            enable_terminal_complete_dirs=summary_count_only and not rust_kernel_available(),
            enable_terminal_anti_join_dirs=summary_count_only and not rust_kernel_available(),
            terminal_direct_scan_limit=int(args.terminal_direct_scan_limit),
            load_tx_type_positions=not rust_kernel_available(),
            enable_role_postings=not rust_kernel_available(),
            event_types=(
                {str(event.get("type", "")) for event in query.get("events", [])}
                if rust_kernel_available()
                else None
            ),
        )
        value_to_id = load_value_ids(conn)
    finally:
        conn.close()
    setup1 = time.perf_counter()
    acc = EpisodeAccessor(rt, value_to_id)
    warmup_runs = max(0, int(args.warmup_runs))
    warmup_outdir = outdir / "_warmup"
    for _ in range(warmup_runs):
        warmup_outdir.mkdir(parents=True, exist_ok=True)
        run_generic(acc, query, warmup_outdir, False, bool(args.disable_refined))
    phase_marker = Path(args.phase_marker) if args.phase_marker else None
    if phase_marker is not None:
        phase_marker.parent.mkdir(parents=True, exist_ok=True)
        phase_marker.write_text("query\n", encoding="utf-8")
    try:
        result = run_generic(
            acc, query, outdir, bool(args.write_bindings), bool(args.disable_refined)
        )
    finally:
        if phase_marker is not None:
            phase_marker.unlink(missing_ok=True)
    done = time.perf_counter()
    report = {
        "query_name": query["query_name"],
        "scope": "anchored_episode",
        "system": "HVCEQ",
        "execution_profile": "generic_json_compiled_anchor_synopsis_local_matcher",
        "forward_window_sec": result["forward_window_sec"],
        "anchor_mode": result["anchor_mode"],
        "semantic_anchor_qid": result["semantic_anchor_qid"],
        "compiled_match_order": result["compiled_match_order"],
        "matching_engine": result.get("matching_engine", "python_generic_fallback"),
        "rust_integer_kernel_enabled": result.get("matching_engine", "").startswith("rust_"),
        "refined_enabled": result.get("refined_enabled", True),
        "refined_engine": result.get("refined_engine", ""),
        "base_synopsis_enabled": True,
        "refined_synopsis_enabled": True,
        "summary_count_only": not bool(args.write_bindings),
        "matched_instances_csv_written": bool(args.write_bindings),
        "matched_bindings_csv_written": bool(args.write_bindings),
        "full_result_materialized": bool(args.write_bindings),
        "terminal_complete_dirs_enabled": bool(getattr(acc.rt, "enable_terminal_complete_dirs", False)),
        "terminal_anti_join_dirs_lazy_enabled": bool(getattr(acc.rt, "enable_terminal_anti_join_dirs", False)),
        "terminal_anti_join_dirs_built": bool(getattr(acc.rt, "swap_terminal_anti_join_dirs_built", False)),
        "terminal_direct_scan_limit": int(getattr(acc.rt, "terminal_direct_scan_limit", 0)),
        "num_cand0": result["num_cand0"],
        "num_cand1": result["num_cand1"],
        "num_cand2": result["num_cand2"],
        "num_matches": result["num_matches"],
        "num_result_units": result["num_result_episodes"],
        "num_result_episodes": result["num_result_episodes"],
        "num_residual_episodes": max(0, result["num_cand2"] - result["num_result_episodes"]),
        "num_candidate_event_bindings_examined": result["num_candidate_event_bindings_examined"],
        "num_terminal_aggregate_calls": result["num_terminal_aggregate_calls"],
        "num_terminal_candidates_aggregated": result["num_terminal_candidates_aggregated"],
        "candidate_frontier_commitments": result["candidate_frontier_commitments"],
        "matched_instances_path": result["matched_instances_path"],
        "matched_bindings_path": result["matched_bindings_path"],
        "stage_times_s": {
            "setup": round(setup1 - setup0, 6),
            **result["stage_times_s"],
        },
        "metrics": {
            "setup_s": round(setup1 - setup0, 6),
            "query_latency_s": result["query_latency_s"],
            "query_wall_time_s": round(done - setup0, 6),
            "summary_only": not bool(args.write_bindings),
            "warmup_runs": warmup_runs,
        },
    }
    (outdir / "query_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
