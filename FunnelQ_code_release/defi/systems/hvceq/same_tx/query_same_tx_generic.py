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

from systems.hvceq.same_tx.query_same_tx_summary_count import (
    EncodedSameTxRuntime,
    can_assign,
    ensure_dir,
    load_query_json,
    norm_str,
)
from systems.hvceq.rust_integer_kernel import (
    available as rust_kernel_available,
    match_integer_domains,
    refine_same_tx_domains,
)


def is_var(x: Any) -> bool:
    return isinstance(x, str) and x.startswith("$")


def digest_ids(ids: Sequence[int]) -> str:
    h = hashlib.sha256()
    for x in sorted(int(v) for v in ids):
        h.update(str(x).encode("ascii"))
        h.update(b"\n")
    return h.hexdigest()


def role_id(rt: EncodedSameTxRuntime, event_idx: int, field: str) -> int:
    return rt.role_id(event_idx, field)


def spec_type_id(rt: EncodedSameTxRuntime, spec: dict) -> int:
    return int(rt.event_type_to_id.get(str(spec.get("type", "")), 0))


def active_roles(spec: dict) -> Dict[str, Any]:
    return dict(spec.get("roles", {}) or {})


def compatible_for_anchor(rt: EncodedSameTxRuntime, spec: dict, event_idx: int) -> bool:
    etid = spec_type_id(rt, spec)
    if etid == 0 or int(rt.event_type_id[event_idx]) != etid:
        return False
    protocol = norm_str(spec.get("protocol", ""))
    if protocol:
        pid = rt.encode_value(protocol)
        if pid == 0 or int(rt.protocol_id[event_idx]) != pid:
            return False
    for field, raw_val in active_roles(spec).items():
        if field == "event_id":
            continue
        actual = role_id(rt, event_idx, field)
        if is_var(raw_val):
            if actual == 0:
                return False
        else:
            wanted = rt.encode_value(raw_val)
            if wanted == 0 or actual != wanted:
                return False
    return True


def prepare_numpy_runtime(rt: EncodedSameTxRuntime) -> None:
    rt._np_tx_ord = np.frombuffer(rt.tx_ord, dtype=np.dtype(rt.tx_ord.typecode))
    rt._np_seq_index = np.frombuffer(
        rt.seq_index, dtype=np.dtype(rt.seq_index.typecode)
    )
    rt._np_event_type_id = np.frombuffer(
        rt.event_type_id, dtype=np.dtype(rt.event_type_id.typecode)
    )
    rt._np_protocol_id = np.frombuffer(
        rt.protocol_id, dtype=np.dtype(rt.protocol_id.typecode)
    )
    rt._np_role_arrays = {
        field: np.frombuffer(values, dtype=np.dtype(values.typecode))
        for field, values in rt.role_arrays.items()
    }
    max_tx = int(rt._np_tx_ord.max(initial=0))
    rt._np_type_indices = {}
    rt._np_type_counts = {}
    rt._np_type_min_seq = {}
    rt._np_type_max_seq = {}
    if rt.tx_type_events:
        rt._type_tx_events = {int(etid): {} for etid in rt.event_type_to_id.values()}
        for tx, by_type in rt.tx_type_events.items():
            for etid, event_idxs in by_type.items():
                rt._type_tx_events.setdefault(int(etid), {})[int(tx)] = event_idxs
    else:
        rt._type_tx_events = {}
    rt._type_tx_ids = {}
    rt._type_nonzero_role_fields = {}
    for etid in rt.event_type_to_id.values():
        idxs = np.flatnonzero(rt._np_event_type_id == int(etid)).astype(
            np.int32, copy=False
        )
        rt._np_type_indices[int(etid)] = idxs
        rt._type_nonzero_role_fields[int(etid)] = {
            field
            for field, values in rt._np_role_arrays.items()
            if idxs.size == 0 or bool(np.all(values[idxs] != 0))
        }
        txs = rt._np_tx_ord[idxs].astype(np.int64, copy=False)
        rt._type_tx_ids[int(etid)] = np.unique(txs)
        seqs = rt._np_seq_index[idxs].astype(np.int64, copy=False)
        rt._np_type_counts[int(etid)] = np.bincount(
            txs, minlength=max_tx + 1
        ).astype(np.int32, copy=False)
        min_seq = np.full(max_tx + 1, np.iinfo(np.int64).max, dtype=np.int64)
        max_seq = np.full(max_tx + 1, np.iinfo(np.int64).min, dtype=np.int64)
        np.minimum.at(min_seq, txs, seqs)
        np.maximum.at(max_seq, txs, seqs)
        rt._np_type_min_seq[int(etid)] = min_seq
        rt._np_type_max_seq[int(etid)] = max_seq


def anchor_event_indices_numpy(
    rt: EncodedSameTxRuntime, spec: dict
) -> np.ndarray:
    etid = spec_type_id(rt, spec)
    idxs = rt._np_type_indices.get(etid, np.empty(0, dtype=np.int64))
    if idxs.size == 0:
        return idxs
    keep = np.ones(idxs.size, dtype=np.bool_)
    protocol = norm_str(spec.get("protocol", ""))
    if protocol:
        wanted = rt.encode_value(protocol)
        if wanted == 0:
            return np.empty(0, dtype=np.int64)
        keep &= rt._np_protocol_id[idxs] == wanted
    for field, raw in active_roles(spec).items():
        if field == "event_id":
            actual = np.frombuffer(
                rt.event_ord, dtype=np.dtype(rt.event_ord.typecode)
            )[idxs]
        else:
            actual = rt._np_role_arrays.get(field)
            if actual is None:
                return np.empty(0, dtype=np.int64)
            actual = actual[idxs]
        if is_var(raw):
            keep &= actual != 0
        else:
            wanted = rt.encode_value(raw)
            if wanted == 0:
                return np.empty(0, dtype=np.int64)
            keep &= actual == wanted
    return idxs[keep]


def query_specs(query: dict) -> List[dict]:
    return [dict(e) for e in query.get("events", [])]


def query_by_id(query: dict) -> Dict[str, dict]:
    return {str(e["id"]): dict(e) for e in query_specs(query)}


def variable_occurrences(query: dict) -> Dict[str, List[Tuple[str, str]]]:
    occ: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for e in query_specs(query):
        for field, value in active_roles(e).items():
            if is_var(value):
                occ[str(value)].append((str(e["id"]), str(field)))
    return occ


def event_constant_strength(spec: dict, var_occ: Dict[str, List[Tuple[str, str]]]) -> int:
    strength = 0
    if norm_str(spec.get("protocol", "")):
        strength += 1
    for _field, value in active_roles(spec).items():
        if is_var(value):
            if len(var_occ.get(str(value), [])) >= 2:
                strength += 1
        elif norm_str(value):
            strength += 2
    return strength


def estimate_anchor_hits(rt: EncodedSameTxRuntime, spec: dict) -> Tuple[int, int]:
    etid = spec_type_id(rt, spec)
    if etid == 0:
        return (10**18, 10**18)


    has_value_constant = bool(norm_str(spec.get("protocol", ""))) or any(
        (not is_var(raw)) and norm_str(raw)
        for field, raw in active_roles(spec).items()
        if field != "event_id"
    )
    if not has_value_constant:
        if hasattr(rt, "_type_tx_ids"):
            closure_hits = int(len(rt._type_tx_ids.get(etid, ())))
            event_hits = int(rt._np_type_indices.get(etid, ()).size)
        else:
            closure_hits = sum(1 for counts in rt.tx_type_counts.values() if counts.get(etid, 0) > 0)
            event_hits = sum(int(counts.get(etid, 0)) for counts in rt.tx_type_counts.values())
        return closure_hits, event_hits
    hits = anchor_event_indices_numpy(rt, spec)
    if hits.size == 0:
        return 0, 0
    return int(np.unique(rt._np_tx_ord[hits]).size), int(hits.size)


def select_anchor(rt: EncodedSameTxRuntime, query: dict) -> Tuple[str, Dict[str, Any]]:
    var_occ = variable_occurrences(query)
    candidates = []
    for spec in query_specs(query):
        closure_hits, event_hits = estimate_anchor_hits(rt, spec)
        strength = event_constant_strength(spec, var_occ)
        qid = str(spec["id"])
        candidates.append(((closure_hits, event_hits, -strength, qid), qid, spec, {
            "closure_hits": closure_hits,
            "event_hits": event_hits,
            "constraint_strength": strength,
        }))
    candidates.sort(key=lambda x: x[0])
    if not candidates:
        raise ValueError("query contains no events")
    _key, qid, spec, stats = candidates[0]
    return qid, {
        "anchor_qid": qid,
        "anchor_event_type": spec.get("type"),
        "anchor_selection": "LexAnchor-statistical",
        "anchor_stats": stats,
        "all_anchor_candidates": [
            {
                "qid": row[1],
                "event_type": row[2].get("type"),
                **row[3],
            }
            for row in candidates
        ],
    }


def anchor_recall(
    rt: EncodedSameTxRuntime, query: dict, anchor_qid: str
) -> Tuple[Sequence[int], Dict[int, List[int]] | np.ndarray]:
    specs = query_by_id(query)
    anchor_spec = specs[anchor_qid]
    etid = spec_type_id(rt, anchor_spec)
    variable_fields = {
        str(field)
        for field, raw in active_roles(anchor_spec).items()
        if field != "event_id" and is_var(raw)
    }
    has_constants = bool(norm_str(anchor_spec.get("protocol", ""))) or any(
        field != "event_id" and not is_var(raw) and norm_str(raw)
        for field, raw in active_roles(anchor_spec).items()
    )
    if (
        not has_constants
        and variable_fields.issubset(rt._type_nonzero_role_fields.get(etid, set()))
    ):
        hits = rt._np_type_indices.get(etid, np.empty(0, dtype=np.int32))
    else:
        hits = anchor_event_indices_numpy(rt, anchor_spec)
    if hits.size == 0:
        return [], {}
    hit_txs = rt._np_tx_ord[hits].astype(np.int64, copy=False)
    unique_txs, starts, counts = np.unique(
        hit_txs, return_index=True, return_counts=True
    )
    cand0 = unique_txs.astype(np.int64, copy=False)
    if not rt.tx_type_events:
        return cand0, hits.astype(np.int32, copy=False)
    anchor_events_by_tx = {
        int(tx): hits[int(start): int(start + count)].astype(np.int64, copy=False).tolist()
        for tx, start, count in zip(unique_txs, starts, counts)
    }
    return cand0, anchor_events_by_tx


def type_needs(rt: EncodedSameTxRuntime, query: dict) -> Counter:
    c = Counter()
    for spec in query_specs(query):
        c[spec_type_id(rt, spec)] += 1
    return c


def type_temporal_feasible(rt: EncodedSameTxRuntime, query: dict, tx: int) -> bool:
    specs = query_by_id(query)
    by_type = rt.tx_type_events.get(int(tx), {})
    for left, right in query.get("order", []):
        lspec = specs[str(left)]
        rspec = specs[str(right)]
        ltype = spec_type_id(rt, lspec)
        rtype = spec_type_id(rt, rspec)
        left_events = by_type.get(ltype, [])
        right_events = by_type.get(rtype, [])
        ok = False
        for li in left_events:
            for ri in right_events:
                if li != ri and int(rt.seq_index[li]) < int(rt.seq_index[ri]):
                    ok = True
                    break
            if ok:
                break
        if not ok:
            return False
    return True


def base_filter(rt: EncodedSameTxRuntime, query: dict, cand0: Sequence[int]) -> np.ndarray:
    if len(cand0) == 0:
        return np.empty(0, dtype=np.int64)
    txs = np.asarray(cand0, dtype=np.int64)
    needs = type_needs(rt, query)
    keep = np.ones(txs.size, dtype=np.bool_)
    for etid, need in needs.items():
        counts = rt._np_type_counts.get(int(etid))
        if int(etid) == 0 or counts is None:
            keep[:] = False
            break
        keep &= counts[txs] >= int(need)
    specs = query_by_id(query)
    for left, right in query.get("order", []):
        left_type = spec_type_id(rt, specs[str(left)])
        right_type = spec_type_id(rt, specs[str(right)])
        left_min = rt._np_type_min_seq.get(left_type)
        right_max = rt._np_type_max_seq.get(right_type)
        if left_min is None or right_max is None:
            keep[:] = False
            break
        keep &= left_min[txs] < right_max[txs]
    return txs[keep]


def closure_has_constant(rt: EncodedSameTxRuntime, tx: int, spec: dict, field: str, value: Any) -> bool:
    wanted = rt.encode_value(value)
    if wanted == 0:
        return False
    by_type = rt.tx_type_events.get(int(tx), {})
    etid = spec_type_id(rt, spec)
    for idx in by_type.get(etid, []):
        if role_id(rt, idx, field) == wanted:
            return True
    return False


def closure_has_role_nonempty(rt: EncodedSameTxRuntime, tx: int, spec: dict, field: str) -> bool:
    by_type = rt.tx_type_events.get(int(tx), {})
    etid = spec_type_id(rt, spec)
    for idx in by_type.get(etid, []):
        if role_id(rt, idx, field) != 0:
            return True
    return False


def closure_has_shared_value(rt: EncodedSameTxRuntime, tx: int, left: Tuple[str, str], right: Tuple[str, str], specs: Dict[str, dict]) -> bool:
    lqid, lf = left
    rqid, rf = right
    by_type = rt.tx_type_events.get(int(tx), {})
    l_events = by_type.get(spec_type_id(rt, specs[lqid]), [])
    r_events = by_type.get(spec_type_id(rt, specs[rqid]), [])
    left_values = {role_id(rt, idx, lf) for idx in l_events if role_id(rt, idx, lf) != 0}
    if not left_values:
        return False
    for idx in r_events:
        v = role_id(rt, idx, rf)
        if v != 0 and v in left_values:
            return True
    return False


def refined_filter(rt: EncodedSameTxRuntime, query: dict, cand1: Sequence[int]) -> List[int]:
    specs = query_by_id(query)
    var_occ = variable_occurrences(query)
    out: List[int] = []
    for tx in cand1:
        ok = True
        for spec in specs.values():
            for field, raw in active_roles(spec).items():
                if field == "event_id":
                    continue
                if is_var(raw):
                    if not closure_has_role_nonempty(rt, int(tx), spec, field):
                        ok = False
                        break
                elif norm_str(raw) and not closure_has_constant(rt, int(tx), spec, field, raw):
                    ok = False
                    break
            if not ok:
                break
        if not ok:
            continue
        for occs in var_occ.values():
            if len(occs) < 2:
                continue
            for i in range(len(occs)):
                for j in range(i + 1, len(occs)):
                    if not closure_has_shared_value(rt, int(tx), occs[i], occs[j], specs):
                        ok = False
                        break
                if not ok:
                    break
            if not ok:
                break
        if ok:
            out.append(int(tx))
    return out


def spec_mask_numpy(rt: EncodedSameTxRuntime, query: dict, qid: str, spec: dict) -> np.ndarray:
    mask = rt._np_event_type_id == spec_type_id(rt, spec)
    protocol = norm_str(spec.get("protocol", ""))
    if protocol:
        wanted = rt.encode_value(protocol)
        mask &= rt._np_protocol_id == wanted if wanted else False
    for field, raw in active_roles(spec).items():
        if field == "event_id":
            actual = np.frombuffer(rt.event_ord, dtype=np.dtype(rt.event_ord.typecode))
        else:
            actual = rt._np_role_arrays.get(field)
            if actual is None:
                return np.zeros(int(rt.event_count), dtype=np.bool_)
        if is_var(raw):
            mask &= actual != 0
        else:
            wanted = rt.encode_value(raw)
            mask &= actual == wanted if wanted else False
    for flt in query.get("attr_filters", []):
        if str(flt.get("event")) != qid:
            continue
        field = str(flt.get("field", ""))
        op = str(flt.get("op", ""))
        try:
            wanted_f = float(flt.get("value"))
        except (TypeError, ValueError):
            return np.zeros(int(rt.event_count), dtype=np.bool_)
        source = rt.num_arrays.get(field)
        if source is None:
            return np.zeros(int(rt.event_count), dtype=np.bool_)
        actual_f = np.frombuffer(source, dtype=np.dtype(source.typecode))
        if op == "=":
            mask &= actual_f == wanted_f
        elif op == ">":
            mask &= actual_f > wanted_f
        elif op == ">=":
            mask &= actual_f >= wanted_f
        elif op == "<":
            mask &= actual_f < wanted_f
        elif op == "<=":
            mask &= actual_f <= wanted_f
        else:
            return np.zeros(int(rt.event_count), dtype=np.bool_)
    return np.ascontiguousarray(mask, dtype=np.bool_)


def compile_rust_match_order(query: dict, anchor_qid: str) -> List[str]:
    sp = query_by_id(query)
    result = [anchor_qid]
    remaining = set(sp) - {anchor_qid}
    introduced = {
        str(value)
        for value in active_roles(sp[anchor_qid]).values()
        if is_var(value)
    }
    while remaining:
        def key(qid: str) -> Tuple[int, int, str]:
            roles = active_roles(sp[qid]).values()
            bound = sum(1 for value in roles if is_var(value) and str(value) in introduced)
            new = sum(1 for value in roles if is_var(value) and str(value) not in introduced)
            return (-bound, new, qid)
        qid = min(remaining, key=key)
        result.append(qid)
        remaining.remove(qid)
        introduced.update(
            str(value) for value in active_roles(sp[qid]).values() if is_var(value)
        )
    return result


def rust_generic_match(
    rt: EncodedSameTxRuntime,
    query: dict,
    cand2: Sequence[int],
    anchor_qid: str,
    anchor_events_by_tx: Dict[int, List[int]] | np.ndarray,
    outdir: Path,
) -> Dict[str, Any]:
    sp = query_by_id(query)
    qids = list(sp)
    txs_all = rt._np_tx_ord.astype(np.int64, copy=False)
    cand_txs = np.asarray(cand2, dtype=np.int64)
    if isinstance(anchor_events_by_tx, np.ndarray):
        all_anchors = anchor_events_by_tx.astype(np.int64, copy=False)
        all_units = txs_all[all_anchors]
        if cand_txs.size == 0:
            anchors_array = np.empty(0, dtype=np.int64)
            units_array = np.empty(0, dtype=np.int64)
        else:
            positions = np.searchsorted(cand_txs, all_units)
            safe = np.minimum(positions, cand_txs.size - 1)
            keep = (positions < cand_txs.size) & (cand_txs[safe] == all_units)
            anchors_array = all_anchors[keep]
            units_array = all_units[keep]
    else:
        anchors: List[int] = []
        units: List[int] = []
        for tx in cand_txs:
            for anchor_idx in anchor_events_by_tx.get(int(tx), []):
                anchors.append(int(anchor_idx))
                units.append(int(tx))
        anchors_array = np.asarray(anchors, dtype=np.int64)
        units_array = np.asarray(units, dtype=np.int64)
    anchor_starts = np.searchsorted(txs_all, units_array, side="left")
    anchor_ends = np.searchsorted(txs_all, units_array, side="right")
    starts = np.repeat(anchor_starts[:, None], len(qids), axis=1)
    ends = np.repeat(anchor_ends[:, None], len(qids), axis=1)
    eligible = {
        qid: spec_mask_numpy(rt, query, qid, sp[qid])
        for qid in qids
    }
    ords = np.frombuffer(rt.seq_index, dtype=np.dtype(rt.seq_index.typecode))
    event_ords = np.frombuffer(rt.event_ord, dtype=np.dtype(rt.event_ord.typecode))

    def role_values(qid: str, field: str) -> np.ndarray:
        if field in {"event_id", "global_event_ord"}:
            return event_ords
        values = rt._np_role_arrays.get(field)
        if values is None:
            return np.zeros(int(rt.event_count), dtype=np.uint32)
        return values

    views = {
        qid: {"ord": ords, "view_id": 0}
        for qid in qids
    }
    result = match_integer_domains(
        query=query,
        qids=qids,
        anchor_qid=anchor_qid,
        match_order=compile_rust_match_order(query, anchor_qid),
        views=views,
        role_array=role_values,
        eligible=eligible,
        anchor_indices=anchors_array,
        unit_ids=units_array,
        range_starts=starts,
        range_ends=ends,
    )
    inst_path, bind_path = write_binding_headers(outdir)
    return {
        "num_matches": int(result["num_matches"]),
        "num_result_txs": int(result["num_result_units"]),
        "num_candidate_event_bindings_examined": int(result["examined"]),
        "num_terminal_aggregate_calls": 0,
        "num_terminal_candidates_aggregated": 0,
        "terminal_aggregation_enabled": False,
        "terminal_aggregation_mode": "rust",
        "terminal_aggregation_reason": "rust_exact_integer_domain_kernel",
        "matched_instances_path": str(inst_path),
        "matched_bindings_path": str(bind_path),
        "matching_engine": str(result["engine"]),
        "compiled_match_order": compile_rust_match_order(query, anchor_qid),
    }


def rust_refined_filter(
    rt: EncodedSameTxRuntime,
    query: dict,
    cand1: Sequence[int],
) -> np.ndarray:
    txs = np.asarray(cand1, dtype=np.int64)
    if txs.size == 0:
        return txs
    qids = list(query_by_id(query))
    starts_one = np.searchsorted(rt._np_tx_ord, txs, side="left")
    ends_one = np.searchsorted(rt._np_tx_ord, txs, side="right")
    starts = np.repeat(starts_one[:, None], len(qids), axis=1)
    ends = np.repeat(ends_one[:, None], len(qids), axis=1)
    event_ords = np.frombuffer(rt.event_ord, dtype=np.dtype(rt.event_ord.typecode))

    def role_values(_qid: str, field: str) -> np.ndarray:
        if field == "event_id":
            return event_ords
        values = rt._np_role_arrays.get(field)
        if values is None:
            return np.zeros(int(rt.event_count), dtype=np.int32)
        return values

    keep = refine_same_tx_domains(
        query=query,
        qids=qids,
        role_array=role_values,
        encode_constant=rt.encode_value,
        event_type_array=rt._np_event_type_id,
        event_type_ids=rt.event_type_to_id,
        range_starts=starts,
        range_ends=ends,
    )
    return txs[keep]


def choose_next_qid(rt: EncodedSameTxRuntime, query: dict, tx: int, selected: Dict[str, int], bindings: Dict[str, int]) -> str:
    specs = query_by_id(query)
    remaining = [qid for qid in specs if qid not in selected]
    best: Tuple[int, int, str] | None = None
    for qid in remaining:
        spec = specs[qid]
        etid = spec_type_id(rt, spec)
        candidates = rt.tx_type_events.get(int(tx), {}).get(etid, [])
        count = 0
        bound_roles = 0
        for _field, raw in active_roles(spec).items():
            if is_var(raw) and raw in bindings:
                bound_roles += 1
        for idx in candidates:
            local_bindings = dict(bindings)
            ok, _new = can_assign(rt, query, {
                "qid": qid,
                "event_type_id": etid,
                "protocol_id": rt.encode_value(spec.get("protocol", "")),
                "roles": active_roles(spec),
            }, idx, selected, local_bindings)
            if ok:
                count += 1
        key = (count if count else 10**12, -bound_roles, qid)
        if best is None or key < best:
            best = key
    if best is None:
        raise RuntimeError("no remaining query event")
    return best[2]


def spec_for_can_assign(rt: EncodedSameTxRuntime, qid: str, spec: dict) -> dict:
    return {
        "qid": qid,
        "event_type_id": spec_type_id(rt, spec),
        "protocol_id": rt.encode_value(spec.get("protocol", "")),
        "roles": active_roles(spec),
    }


def candidate_events_for_spec(
    rt: EncodedSameTxRuntime,
    query: dict,
    tx: int,
    qid: str,
    spec: dict,
    selected: Dict[str, int],
    bindings: Dict[str, int],
) -> List[int]:


    etid = spec_type_id(rt, spec)
    raw_candidates = rt.tx_type_events.get(int(tx), {}).get(etid, [])
    if not raw_candidates:
        return []
    protocol_id = rt.encode_value(spec.get("protocol", ""))
    out: List[int] = []
    for idx in raw_candidates:
        if idx in selected.values():
            continue
        if protocol_id and int(rt.protocol_id[idx]) != protocol_id:
            continue
        ok = True
        for field, raw in active_roles(spec).items():
            actual = role_id(rt, idx, field)
            if is_var(raw):
                if actual == 0:
                    ok = False
                    break
                bound = bindings.get(str(raw))
                if bound is not None and actual != bound:
                    ok = False
                    break
            else:
                wanted = rt.encode_value(raw)
                if wanted == 0 or actual != wanted:
                    ok = False
                    break
        if not ok:
            continue

        for left, right in query.get("order", []):
            if left == qid and right in selected:
                if int(rt.seq_index[idx]) >= int(rt.seq_index[selected[right]]):
                    ok = False
                    break
            elif right == qid and left in selected:
                if int(rt.seq_index[selected[left]]) >= int(rt.seq_index[idx]):
                    ok = False
                    break
        if ok:
            out.append(idx)
    return out


class SameTxTerminalCounter:


    def __init__(self, rt: EncodedSameTxRuntime):
        self.rt = rt
        self.swap_ts: Dict[Tuple[int, int, int], List[int]] = {}
        self.swap_ts_bought: Dict[Tuple[int, int, int, int], List[int]] = {}
        self.swap_ts_pool: Dict[Tuple[int, int, int, int], List[int]] = {}
        self.swap_ts_bought_pool: Dict[Tuple[int, int, int, int, int], List[int]] = {}
        self.borrow_ba: Dict[Tuple[int, int, int], List[int]] = {}
        self.borrow_ba_pool: Dict[Tuple[int, int, int, int], List[int]] = {}
        self._swap_base_ready = False
        self._swap_bought_ready = False
        self._swap_pool_ready = False
        self._swap_bought_pool_ready = False
        self._borrow_base_ready = False
        self._borrow_pool_ready = False
        id_to_type = {int(v): str(k) for k, v in rt.event_type_to_id.items()}
        self._swap_type_id = next((eid for eid, name in id_to_type.items() if name == "swap"), 0)
        self._borrow_type_id = next((eid for eid, name in id_to_type.items() if name == "borrow"), 0)

    @staticmethod
    def _sort_directory(bucket: Dict[Any, List[int]]) -> None:
        for seqs in bucket.values():
            seqs.sort()

    def _ensure_swap_dirs(self, need_bought: bool, need_pool: bool) -> None:
        build_base = not self._swap_base_ready
        build_bought = bool(need_bought) and not self._swap_bought_ready
        build_pool = bool(need_pool) and not self._swap_pool_ready
        build_overlap = bool(need_bought and need_pool) and not self._swap_bought_pool_ready
        if not any((build_base, build_bought, build_pool, build_overlap)):
            return
        base = defaultdict(list) if build_base else None
        bought_dir = defaultdict(list) if build_bought else None
        pool_dir = defaultdict(list) if build_pool else None
        overlap_dir = defaultdict(list) if build_overlap else None
        rt = self.rt
        for idx in range(int(rt.event_count)):
            if int(rt.event_type_id[idx]) != int(self._swap_type_id):
                continue
            taker = role_id(rt, idx, "taker")
            sold = role_id(rt, idx, "token_sold")
            bought = role_id(rt, idx, "token_bought")
            pool = role_id(rt, idx, "pool")
            if not (taker and sold and bought and pool):
                continue
            tx = int(rt.tx_ord[idx])
            seq = int(rt.seq_index[idx])
            if base is not None:
                base[(tx, taker, sold)].append(seq)
            if bought_dir is not None:
                bought_dir[(tx, taker, sold, bought)].append(seq)
            if pool_dir is not None:
                pool_dir[(tx, taker, sold, pool)].append(seq)
            if overlap_dir is not None:
                overlap_dir[(tx, taker, sold, bought, pool)].append(seq)
        if base is not None:
            self._sort_directory(base)
            self.swap_ts = base
            self._swap_base_ready = True
        if bought_dir is not None:
            self._sort_directory(bought_dir)
            self.swap_ts_bought = bought_dir
            self._swap_bought_ready = True
        if pool_dir is not None:
            self._sort_directory(pool_dir)
            self.swap_ts_pool = pool_dir
            self._swap_pool_ready = True
        if overlap_dir is not None:
            self._sort_directory(overlap_dir)
            self.swap_ts_bought_pool = overlap_dir
            self._swap_bought_pool_ready = True

    def _ensure_borrow_dirs(self, need_pool: bool) -> None:
        build_base = not self._borrow_base_ready
        build_pool = bool(need_pool) and not self._borrow_pool_ready
        if not (build_base or build_pool):
            return
        base = defaultdict(list) if build_base else None
        pool_dir = defaultdict(list) if build_pool else None
        rt = self.rt
        for idx in range(int(rt.event_count)):
            if int(rt.event_type_id[idx]) != int(self._borrow_type_id):
                continue
            borrower = role_id(rt, idx, "borrower")
            asset = role_id(rt, idx, "asset")
            pool = role_id(rt, idx, "pool")
            if not (borrower and asset and pool):
                continue
            tx = int(rt.tx_ord[idx])
            seq = int(rt.seq_index[idx])
            if base is not None:
                base[(tx, borrower, asset)].append(seq)
            if pool_dir is not None:
                pool_dir[(tx, borrower, asset, pool)].append(seq)
        if base is not None:
            self._sort_directory(base)
            self.borrow_ba = base
            self._borrow_base_ready = True
        if pool_dir is not None:
            self._sort_directory(pool_dir)
            self.borrow_ba_pool = pool_dir
            self._borrow_pool_ready = True

    @staticmethod
    def _count_between(seqs: Sequence[int] | None, lo_exclusive: int, hi_exclusive: int) -> int:
        if not seqs:
            return 0
        left = bisect.bisect_right(seqs, int(lo_exclusive))
        right = bisect.bisect_left(seqs, int(hi_exclusive))
        return max(0, right - left)

    def count_complete_swaps_with_neq(
        self,
        tx: int,
        taker: int,
        sold: int,
        lo_exclusive: int,
        hi_exclusive: int,
        exclude_bought: Iterable[int] = (),
        exclude_pool: Iterable[int] = (),
    ) -> int:
        tx_i = int(tx)
        t = int(taker)
        s = int(sold)
        lo = int(lo_exclusive)
        hi = int(hi_exclusive)
        bought_values = {int(v) for v in exclude_bought if int(v) != 0}
        pool_values = {int(v) for v in exclude_pool if int(v) != 0}
        self._ensure_swap_dirs(bool(bought_values), bool(pool_values))
        total = self._count_between(self.swap_ts.get((tx_i, t, s)), lo, hi)
        if not bought_values and not pool_values:
            return total
        removed_bought = sum(
            self._count_between(self.swap_ts_bought.get((tx_i, t, s, b)), lo, hi)
            for b in bought_values
        )
        removed_pool = sum(
            self._count_between(self.swap_ts_pool.get((tx_i, t, s, p)), lo, hi)
            for p in pool_values
        )
        restored_overlap = sum(
            self._count_between(self.swap_ts_bought_pool.get((tx_i, t, s, b, p)), lo, hi)
            for b in bought_values
            for p in pool_values
        )
        return int(total - removed_bought - removed_pool + restored_overlap)

    def count_complete_borrows(
        self,
        tx: int,
        borrower: int,
        asset: int,
        lo_exclusive: int,
        hi_exclusive: int,
        exclude_pool: Iterable[int] = (),
    ) -> int:
        tx_i = int(tx)
        b = int(borrower)
        a = int(asset)
        lo = int(lo_exclusive)
        hi = int(hi_exclusive)
        pool_values = {int(v) for v in exclude_pool if int(v) != 0}
        self._ensure_borrow_dirs(bool(pool_values))
        total = self._count_between(self.borrow_ba.get((tx_i, b, a)), lo, hi)
        if not pool_values:
            return total
        removed_pool = sum(
            self._count_between(self.borrow_ba_pool.get((tx_i, b, a, p)), lo, hi)
            for p in pool_values
        )
        return int(total - removed_pool)


def candidate_seq_bounds(
    rt: EncodedSameTxRuntime,
    query: dict,
    selected: Dict[str, int],
    qid: str,
) -> Tuple[int, int]:
    low = -10**18
    high = 10**18
    for left, right in query.get("order", []):
        left = str(left)
        right = str(right)
        if right == qid and left in selected:
            low = max(low, int(rt.seq_index[selected[left]]))
        if left == qid and right in selected:
            high = min(high, int(rt.seq_index[selected[right]]))
    return int(low), int(high)


def terminal_aggregate_count(
    rt: EncodedSameTxRuntime,
    counter: SameTxTerminalCounter,
    query: dict,
    tx: int,
    selected: Dict[str, int],
    bindings: Dict[str, int],
    qid: str,
    spec: dict,
    lo_seq: int,
    hi_seq: int,
) -> int | None:


    if lo_seq >= hi_seq:
        return 0
    etype = str(spec.get("type", ""))
    roles = active_roles(spec)
    if norm_str(spec.get("protocol", "")):
        return None
    for flt in query.get("attr_filters", []):
        if str(flt.get("event")) == qid:
            return None


    for selected_idx in selected.values():
        seq = int(rt.seq_index[selected_idx])
        if int(lo_seq) < seq < int(hi_seq):
            return None

    bound: Dict[str, int] = {}
    for field, raw in roles.items():
        field = str(field)
        if is_var(raw):
            if str(raw) in bindings:
                bound[field] = int(bindings[str(raw)])
        else:
            wanted = rt.encode_value(raw)
            if wanted == 0:
                return 0
            bound[field] = int(wanted)

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
        if etype == "swap" and terminal_field in {"token_bought", "pool"}:
            excluded[terminal_field].add(role_id(rt, selected[other_qid], other_field))
        elif etype == "borrow" and terminal_field == "pool":
            excluded[terminal_field].add(role_id(rt, selected[other_qid], other_field))
        else:
            return None

    if etype == "swap":
        schema_roles = {"taker", "token_sold", "token_bought", "pool"}
        if set(roles) != schema_roles or set(bound) != {"taker", "token_sold"}:
            return None
        return counter.count_complete_swaps_with_neq(
            int(tx),
            bound["taker"],
            bound["token_sold"],
            int(lo_seq),
            int(hi_seq),
            exclude_bought=excluded.get("token_bought", set()),
            exclude_pool=excluded.get("pool", set()),
        )
    if etype == "borrow":
        schema_roles = {"borrower", "asset", "pool"}
        if set(roles) != schema_roles or set(bound) != {"borrower", "asset"}:
            return None
        return counter.count_complete_borrows(
            int(tx),
            bound["borrower"],
            bound["asset"],
            int(lo_seq),
            int(hi_seq),
            exclude_pool=excluded.get("pool", set()),
        )
    return None


def write_binding_headers(outdir: Path) -> Tuple[Path, Path]:
    inst = outdir / "matched_instances.csv"
    bind = outdir / "matched_bindings.csv"
    with inst.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["match_id", "closure_id", "tx_ord", "query_name", "num_events"])
    with bind.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["match_id", "closure_id", "tx_ord", "query_event_id", "event_id", "event_ord", "event_type", "seq_index"])
    return inst, bind


def should_enable_terminal_counter(
    rt: EncodedSameTxRuntime,
    query: dict,
    cand2: Sequence[int],
    anchor_events_by_tx: Dict[int, List[int]],
    mode: str,
    write_bindings: bool,
) -> Tuple[bool, str]:


    if write_bindings:
        return False, "disabled_for_bindings_materialization"
    mode = str(mode or "auto").lower()
    if mode == "off":
        return False, "disabled_by_option"
    if mode == "on":
        return True, "forced_on"


    cand2_n = len(cand2)
    anchor_seed_n = sum(len(anchor_events_by_tx.get(int(tx), [])) for tx in cand2)

    if cand2_n < 50_000 and anchor_seed_n < 100_000:
        return False, f"auto_disabled_small_sametx_frontier(cand2={cand2_n},seeds={anchor_seed_n})"
    return True, f"auto_enabled_large_sametx_frontier(cand2={cand2_n},seeds={anchor_seed_n})"


def generic_match(
    rt: EncodedSameTxRuntime,
    query: dict,
    cand2: Sequence[int],
    anchor_qid: str,
    anchor_events_by_tx: Dict[int, List[int]],
    outdir: Path,
    write_bindings: bool,
    terminal_aggregation_mode: str = "auto",
) -> Dict[str, Any]:
    specs = query_by_id(query)
    terminal_counter_enabled, terminal_counter_reason = should_enable_terminal_counter(
        rt, query, cand2, anchor_events_by_tx, terminal_aggregation_mode, write_bindings
    )
    if terminal_counter_enabled:
        terminal_counter = getattr(rt, "_same_tx_terminal_counter", None)
        if terminal_counter is None:
            terminal_counter = SameTxTerminalCounter(rt)
            rt._same_tx_terminal_counter = terminal_counter
    else:
        terminal_counter = None
    match_count = 0
    result_txs: set[int] = set()
    examined = 0
    terminal_aggregate_calls = 0
    terminal_candidates_aggregated = 0
    inst_path, bind_path = write_binding_headers(outdir)
    inst_f = inst_path.open("a", newline="", encoding="utf-8") if write_bindings else None
    bind_f = bind_path.open("a", newline="", encoding="utf-8") if write_bindings else None
    inst_w = csv.writer(inst_f) if inst_f else None
    bind_w = csv.writer(bind_f) if bind_f else None
    try:
        for tx in cand2:
            for anchor_idx in anchor_events_by_tx.get(int(tx), []):
                selected: Dict[str, int] = {}
                bindings: Dict[str, int] = {}
                ok, new_bindings = can_assign(
                    rt,
                    query,
                    spec_for_can_assign(rt, anchor_qid, specs[anchor_qid]),
                    anchor_idx,
                    selected,
                    bindings,
                )
                if not ok:
                    continue
                selected[anchor_qid] = anchor_idx

                def dfs() -> None:
                    nonlocal match_count, examined
                    nonlocal terminal_aggregate_calls, terminal_candidates_aggregated
                    if len(selected) == len(specs):
                        match_count += 1
                        result_txs.add(int(tx))
                        if inst_w and bind_w:
                            match_id = f"{query.get('query_name')}::{tx}::{match_count}"
                            inst_w.writerow([match_id, tx, tx, query.get("query_name"), len(selected)])
                            for qid in specs:
                                idx = selected[qid]
                                bind_w.writerow([
                                    match_id,
                                    tx,
                                    tx,
                                    qid,
                                    rt.event_id_by_idx[idx],
                                    int(rt.event_ord[idx]),
                                    int(rt.event_type_id[idx]),
                                    int(rt.seq_index[idx]),
                                ])
                        return


                    remaining = [q for q in specs if q not in selected]
                    if len(remaining) == 1:
                        qid = remaining[0]
                    else:
                        qid = min(
                            remaining,
                            key=lambda q: (
                                len(candidate_events_for_spec(rt, query, int(tx), q, specs[q], selected, bindings)) or 10**12,
                                q,
                            ),
                        )
                    spec = specs[qid]
                    lo_seq, hi_seq = candidate_seq_bounds(rt, query, selected, qid)
                    if len(remaining) == 1 and terminal_counter is not None:
                        aggregate_count = terminal_aggregate_count(
                            rt,
                            terminal_counter,
                            query,
                            int(tx),
                            selected,
                            bindings,
                            qid,
                            spec,
                            lo_seq,
                            hi_seq,
                        )
                        if aggregate_count is not None:
                            aggregate_count = int(aggregate_count)
                            terminal_aggregate_calls += 1
                            terminal_candidates_aggregated += aggregate_count
                            if aggregate_count > 0:
                                match_count += aggregate_count
                                result_txs.add(int(tx))
                            return
                    for idx in candidate_events_for_spec(rt, query, int(tx), qid, spec, selected, bindings):
                        examined += 1
                        ok2, new2 = can_assign(rt, query, spec_for_can_assign(rt, qid, spec), idx, selected, bindings)
                        if not ok2:
                            continue
                        selected[qid] = idx
                        dfs()
                        selected.pop(qid, None)
                        for var, _ in reversed(new2):
                            bindings.pop(var, None)

                dfs()
                selected.pop(anchor_qid, None)
                for var, _ in reversed(new_bindings):
                    bindings.pop(var, None)
    finally:
        if inst_f:
            inst_f.close()
        if bind_f:
            bind_f.close()
    return {
        "num_matches": int(match_count),
        "num_result_txs": int(len(result_txs)),
        "num_candidate_event_bindings_examined": int(examined),
        "num_terminal_aggregate_calls": int(terminal_aggregate_calls),
        "num_terminal_candidates_aggregated": int(terminal_candidates_aggregated),
        "terminal_aggregation_enabled": bool(terminal_counter_enabled),
        "terminal_aggregation_mode": str(terminal_aggregation_mode),
        "terminal_aggregation_reason": str(terminal_counter_reason),
        "matched_instances_path": str(inst_path),
        "matched_bindings_path": str(bind_path),
    }


def run_generic(
    rt: EncodedSameTxRuntime,
    query: dict,
    outdir: Path,
    write_bindings: bool,
    terminal_aggregation_mode: str = "auto",
    disable_refined: bool = False,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    anchor_qid, anchor_meta = select_anchor(rt, query)
    cand0, anchor_events_by_tx = anchor_recall(rt, query, anchor_qid)
    t1 = time.perf_counter()
    cand1 = base_filter(rt, query, cand0)
    t2 = time.perf_counter()
    if disable_refined:
        cand2 = cand1
        refined_engine = "disabled_for_ablation"
    elif rust_kernel_available() and not rt.tx_type_events:
        cand2 = rust_refined_filter(rt, query, cand1)
        refined_engine = "rust_refined_same_tx_v1"
    else:
        cand2 = refined_filter(rt, query, cand1)
        refined_engine = "python_refined_fallback"
    t3 = time.perf_counter()
    if not write_bindings and rust_kernel_available():
        exact = rust_generic_match(
            rt, query, cand2, anchor_qid, anchor_events_by_tx, outdir
        )
    else:
        exact = generic_match(
            rt, query, cand2, anchor_qid, anchor_events_by_tx, outdir, write_bindings, terminal_aggregation_mode
        )
        exact["matching_engine"] = "python_generic_fallback"
    t4 = time.perf_counter()
    return {
        **exact,
        **anchor_meta,
        "num_cand0": int(len(cand0)),
        "num_cand1": int(len(cand1)),
        "num_cand2": int(len(cand2)),
        "refined_enabled": not bool(disable_refined),
        "refined_engine": refined_engine,
        "candidate_frontier_commitments": {
            "cand0_sha256": digest_ids(cand0),
            "cand1_sha256": digest_ids(cand1),
            "cand2_sha256": digest_ids(cand2),
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
    ap = argparse.ArgumentParser(description="Generic JSON-compiled HVCEQ SameTx runner.")
    ap.add_argument("--db", required=True)
    ap.add_argument("--query-json", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--sample-limit", type=int, default=0)
    ap.add_argument("--ablation", default="full", help="Accepted for compatibility; generic runner always uses all layers.")
    ap.add_argument("--write-bindings", action="store_true", help="Write matched_instances.csv and matched_bindings.csv.")
    ap.add_argument("--warmup-runs", type=int, default=0)
    ap.add_argument("--phase-marker")
    ap.add_argument(
        "--disable-refined",
        action="store_true",
        help="Ablation only: bypass Refined and pass Cand1 directly to Exact.",
    )
    ap.add_argument(
        "--same-tx-terminal-aggregation",
        choices=["auto", "on", "off"],
        default="auto",
        help="SameTx count-only terminal aggregation policy. Default auto keeps heavy directories off for small SameTx frontiers.",
    )
    return ap.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    outdir = Path(args.outdir)
    ensure_dir(outdir)
    query = load_query_json(args.query_json)
    setup0 = time.perf_counter()
    conn = sqlite3.connect(args.db)
    try:
        requested_roles = {
            str(field)
            for event in query.get("events", [])
            for field in (event.get("roles", {}) or {})
            if str(field) != "event_id"
        }
        requested_roles.update(
            str(field)
            for left, left_field, right, right_field in query.get("neq_constraints", [])
            for field in (left_field, right_field)
            if str(field) != "event_id"
        )
        requested_numeric = {
            str(flt.get("field", ""))
            for flt in query.get("attr_filters", [])
            if str(flt.get("field", "")) in {"amount", "amount_in", "amount_out"}
        }
        rt = EncodedSameTxRuntime(
            conn,
            load_event_ids=bool(args.write_bindings),
            build_tx_type_buckets=not rust_kernel_available(),
            role_fields=requested_roles if rust_kernel_available() else None,
            numeric_fields=requested_numeric if rust_kernel_available() else None,
            event_types=(
                {str(event.get("type", "")) for event in query.get("events", [])}
                if rust_kernel_available()
                else None
            ),
        )
        prepare_numpy_runtime(rt)
    finally:
        conn.close()
    setup1 = time.perf_counter()
    warmup_runs = max(0, int(args.warmup_runs))
    warmup_outdir = outdir / "_warmup"
    for _ in range(warmup_runs):
        warmup_outdir.mkdir(parents=True, exist_ok=True)
        run_generic(
            rt, query, warmup_outdir, False, str(args.same_tx_terminal_aggregation),
            bool(args.disable_refined),
        )
    phase_marker = Path(args.phase_marker) if args.phase_marker else None
    if phase_marker is not None:
        phase_marker.parent.mkdir(parents=True, exist_ok=True)
        phase_marker.write_text("query\n", encoding="utf-8")
    try:
        result = run_generic(
            rt, query, outdir, bool(args.write_bindings), str(args.same_tx_terminal_aggregation),
            bool(args.disable_refined),
        )
    finally:
        if phase_marker is not None:
            phase_marker.unlink(missing_ok=True)
    end = time.perf_counter()
    report = {
        "query_name": query.get("query_name"),
        "scope": "same_tx",
        "system": "HVCEQ",
        "execution_profile": "generic_json_compiled_anchor_synopsis_local_matcher",
        "storage_layout": "encoded_same_tx_integer_sidecar",
        "summary_count_only": not bool(args.write_bindings),
        "matched_instances_csv_written": bool(args.write_bindings),
        "matched_bindings_csv_written": bool(args.write_bindings),
        "full_result_materialized": bool(args.write_bindings),
        "base_synopsis_enabled": True,
        "refined_synopsis_enabled": True,
        "anchor_mode": result["anchor_selection"],
        "anchor_qid": result["anchor_qid"],
        "anchor_event_type": result["anchor_event_type"],
        "anchor_stats": result["anchor_stats"],
        "all_anchor_candidates": result["all_anchor_candidates"],
        "num_matches": result["num_matches"],
        "num_result_units": result["num_result_txs"],
        "num_result_txs": result["num_result_txs"],
        "num_terminal_aggregate_calls": result["num_terminal_aggregate_calls"],
        "num_terminal_candidates_aggregated": result["num_terminal_candidates_aggregated"],
        "terminal_aggregation_enabled": result.get("terminal_aggregation_enabled", False),
        "terminal_aggregation_mode": result.get("terminal_aggregation_mode", "auto"),
        "terminal_aggregation_reason": result.get("terminal_aggregation_reason", ""),
        "num_candidate_units": result["num_cand2"],
        "num_candidate_txs": result["num_cand2"],
        "num_cand0": result["num_cand0"],
        "num_cand1": result["num_cand1"],
        "num_cand2": result["num_cand2"],
        "num_candidate_event_bindings_examined": result["num_candidate_event_bindings_examined"],
        "candidate_frontier_commitments": result["candidate_frontier_commitments"],
        "matched_instances_path": result["matched_instances_path"],
        "matched_bindings_path": result["matched_bindings_path"],
        "plan_order": ["anchor_recall", "base_synopsis_filter", "refined_synopsis_filter", "generic_local_match"],
        "matching_engine": result.get("matching_engine", "python_generic_fallback"),
        "rust_integer_kernel_enabled": result.get("matching_engine", "").startswith("rust_"),
        "refined_enabled": result.get("refined_enabled", True),
        "refined_engine": result.get("refined_engine", ""),
        "stage_times_s": {
            "setup_load_encoded_sidecar_s": round(setup1 - setup0, 6),
            **result["stage_times_s"],
        },
        "metrics": {
            "setup_s": round(setup1 - setup0, 6),
            "query_latency_s": result["query_latency_s"],
            "query_wall_time_s": round(end - setup0, 6),
            "summary_only": not bool(args.write_bindings),
            "warmup_runs": warmup_runs,
        },
        "runtime_stats": {
            "event_count": int(rt.event_count),
            "tx_count": int(rt.tx_count),
            "event_type_count": int(len(rt.event_type_to_id)),
            "value_count": int(rt.value_count),
        },
    }
    (outdir / "query_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
