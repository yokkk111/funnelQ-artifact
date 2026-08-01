from __future__ import annotations

import ctypes
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, Tuple

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
DLL = ROOT / "systems/hvceq/rust_kernel/target/release/hvceq_integer_kernel.dll"


class Occurrence(ctypes.Structure):
    _fields_ = [("var_id", ctypes.c_uint32), ("values", ctypes.POINTER(ctypes.c_int32))]


class SpecView(ctypes.Structure):
    _fields_ = [
        ("len", ctypes.c_uint64),
        ("ord", ctypes.POINTER(ctypes.c_int64)),
        ("eligible", ctypes.POINTER(ctypes.c_uint8)),
        ("occurrences", ctypes.POINTER(Occurrence)),
        ("occurrence_count", ctypes.c_uint32),
        ("view_id", ctypes.c_uint32),
    ]


class OrderConstraint(ctypes.Structure):
    _fields_ = [("left_spec", ctypes.c_uint32), ("right_spec", ctypes.c_uint32)]


class NeqConstraint(ctypes.Structure):
    _fields_ = [
        ("left_spec", ctypes.c_uint32),
        ("right_spec", ctypes.c_uint32),
        ("left_values", ctypes.POINTER(ctypes.c_int32)),
        ("right_values", ctypes.POINTER(ctypes.c_int32)),
    ]


class MatchRequest(ctypes.Structure):
    _fields_ = [
        ("specs", ctypes.POINTER(SpecView)),
        ("spec_count", ctypes.c_uint32),
        ("variable_count", ctypes.c_uint32),
        ("match_order", ctypes.POINTER(ctypes.c_uint32)),
        ("anchor_spec", ctypes.c_uint32),
        ("anchor_count", ctypes.c_uint64),
        ("anchor_indices", ctypes.POINTER(ctypes.c_uint64)),
        ("unit_ids", ctypes.POINTER(ctypes.c_uint64)),
        ("range_starts", ctypes.POINTER(ctypes.c_uint64)),
        ("range_ends", ctypes.POINTER(ctypes.c_uint64)),
        ("orders", ctypes.POINTER(OrderConstraint)),
        ("order_count", ctypes.c_uint32),
        ("neqs", ctypes.POINTER(NeqConstraint)),
        ("neq_count", ctypes.c_uint32),
    ]


class MatchResult(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_int32),
        ("num_matches", ctypes.c_uint64),
        ("num_result_units", ctypes.c_uint64),
        ("examined", ctypes.c_uint64),
    ]


class RefineOccurrence(ctypes.Structure):
    _fields_ = [
        ("spec_id", ctypes.c_uint32),
        ("variable_id", ctypes.c_uint32),
        ("values", ctypes.POINTER(ctypes.c_int32)),
        ("wanted", ctypes.c_int32),
        ("is_variable", ctypes.c_uint32),
        ("check_existence", ctypes.c_uint32),
        ("event_types", ctypes.POINTER(ctypes.c_int32)),
        ("wanted_event_type", ctypes.c_int32),
    ]


class SameTxRefineRequest(ctypes.Structure):
    _fields_ = [
        ("spec_count", ctypes.c_uint32),
        ("variable_count", ctypes.c_uint32),
        ("occurrences", ctypes.POINTER(RefineOccurrence)),
        ("occurrence_count", ctypes.c_uint32),
        ("candidate_count", ctypes.c_uint64),
        ("range_starts", ctypes.POINTER(ctypes.c_uint64)),
        ("range_ends", ctypes.POINTER(ctypes.c_uint64)),
        ("output_keep", ctypes.POINTER(ctypes.c_uint8)),
    ]


class AnchorRefineRequest(ctypes.Structure):
    _fields_ = [
        ("specs", ctypes.POINTER(SpecView)),
        ("spec_count", ctypes.c_uint32),
        ("variable_count", ctypes.c_uint32),
        ("anchor_spec", ctypes.c_uint32),
        ("candidate_count", ctypes.c_uint64),
        ("anchor_indices", ctypes.POINTER(ctypes.c_uint64)),
        ("range_starts", ctypes.POINTER(ctypes.c_uint64)),
        ("range_ends", ctypes.POINTER(ctypes.c_uint64)),
        ("output_keep", ctypes.POINTER(ctypes.c_uint8)),
    ]


_LIB = None


def available() -> bool:
    return DLL.exists() and os.environ.get("HVCEQ_DISABLE_RUST_KERNEL", "") != "1"


def _library():
    global _LIB
    if _LIB is None:
        if not available():
            raise RuntimeError(f"Rust HVCEQ kernel is unavailable: {DLL}")
        _LIB = ctypes.CDLL(str(DLL))
        _LIB.hvceq_match_integer_domains.argtypes = [
            ctypes.POINTER(MatchRequest),
            ctypes.POINTER(MatchResult),
        ]
        _LIB.hvceq_match_integer_domains.restype = ctypes.c_int32
        _LIB.hvceq_refine_same_tx_domains.argtypes = [
            ctypes.POINTER(SameTxRefineRequest)
        ]
        _LIB.hvceq_refine_same_tx_domains.restype = ctypes.c_int32
        _LIB.hvceq_refine_anchor_domains.argtypes = [
            ctypes.POINTER(AnchorRefineRequest)
        ]
        _LIB.hvceq_refine_anchor_domains.restype = ctypes.c_int32
    return _LIB


def _i32(values: np.ndarray) -> np.ndarray:
    return np.ascontiguousarray(values, dtype=np.int32)


def _i64(values: np.ndarray) -> np.ndarray:
    return np.ascontiguousarray(values, dtype=np.int64)


def _u64(values: np.ndarray) -> np.ndarray:
    return np.ascontiguousarray(values, dtype=np.uint64)


def _pi32(values: np.ndarray):
    return values.ctypes.data_as(ctypes.POINTER(ctypes.c_int32))


def _pi64(values: np.ndarray):
    return values.ctypes.data_as(ctypes.POINTER(ctypes.c_int64))


def _p64(values: np.ndarray):
    return values.ctypes.data_as(ctypes.POINTER(ctypes.c_uint64))


def match_integer_domains(
    *,
    query: dict,
    qids: Sequence[str],
    anchor_qid: str,
    match_order: Sequence[str],
    views: Dict[str, Dict[str, Any]],
    role_array: Callable[[str, str], np.ndarray],
    eligible: Dict[str, np.ndarray],
    anchor_indices: np.ndarray,
    unit_ids: np.ndarray,
    range_starts: np.ndarray,
    range_ends: np.ndarray,
) -> Dict[str, int | str]:
    """Execute a compiled count-only integer match directly over NumPy buffers."""
    qids = list(qids)
    qpos = {qid: i for i, qid in enumerate(qids)}
    variables: Dict[str, int] = {}
    keepalive: List[Any] = []
    spec_rows: List[SpecView] = []
    occurrences_by_spec: List[Any] = []
    event_specs = {str(e["id"]): e for e in query.get("events", [])}

    for qid in qids:
        spec = event_specs[qid]
        ords = _i64(np.asarray(views[qid]["ord"]))
        mask = np.ascontiguousarray(eligible[qid], dtype=np.uint8)
        keepalive.extend([ords, mask])
        occ_rows: List[Occurrence] = []
        for field, raw in (spec.get("roles", {}) or {}).items():
            if isinstance(raw, str) and raw.startswith("$"):
                var_id = variables.setdefault(raw, len(variables))
                values = _i32(np.asarray(role_array(qid, str(field))))
                keepalive.append(values)
                occ_rows.append(Occurrence(var_id, _pi32(values)))
        occ_array = (Occurrence * len(occ_rows))(*occ_rows)
        occurrences_by_spec.append(occ_array)
        spec_rows.append(
            SpecView(
                len(ords),
                _pi64(ords),
                mask.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
                ctypes.cast(occ_array, ctypes.POINTER(Occurrence)),
                len(occ_rows),
                int(views[qid]["view_id"]),
            )
        )

    order_rows = [
        OrderConstraint(qpos[str(left)], qpos[str(right)])
        for left, right in query.get("order", [])
    ]
    order_array = (OrderConstraint * len(order_rows))(*order_rows)
    neq_rows: List[NeqConstraint] = []
    for left, left_field, right, right_field in query.get("neq_constraints", []):
        lv = _i32(np.asarray(role_array(str(left), str(left_field))))
        rv = _i32(np.asarray(role_array(str(right), str(right_field))))
        keepalive.extend([lv, rv])
        neq_rows.append(
            NeqConstraint(qpos[str(left)], qpos[str(right)], _pi32(lv), _pi32(rv))
        )
    neq_array = (NeqConstraint * len(neq_rows))(*neq_rows)
    specs_array = (SpecView * len(spec_rows))(*spec_rows)
    order_ids = np.ascontiguousarray([qpos[q] for q in match_order], dtype=np.uint32)
    anchors = _u64(anchor_indices)
    units = _u64(unit_ids)
    starts = _u64(range_starts).reshape(-1)
    ends = _u64(range_ends).reshape(-1)
    keepalive.extend(
        [specs_array, occurrences_by_spec, order_array, neq_array, order_ids, anchors, units, starts, ends]
    )
    req = MatchRequest(
        specs_array,
        len(spec_rows),
        len(variables),
        order_ids.ctypes.data_as(ctypes.POINTER(ctypes.c_uint32)),
        qpos[anchor_qid],
        len(anchors),
        _p64(anchors),
        _p64(units),
        _p64(starts),
        _p64(ends),
        order_array,
        len(order_rows),
        neq_array,
        len(neq_rows),
    )
    out = MatchResult()
    rc = _library().hvceq_match_integer_domains(ctypes.byref(req), ctypes.byref(out))
    if rc != 0 or out.status != 0:
        raise RuntimeError(f"Rust HVCEQ kernel failed: rc={rc}, status={out.status}")
    return {
        "num_matches": int(out.num_matches),
        "num_result_units": int(out.num_result_units),
        "examined": int(out.examined),
        "engine": "rust_cdylib_integer_domains_v1",
    }


def refine_same_tx_domains(
    *,
    query: dict,
    qids: Sequence[str],
    role_array: Callable[[str, str], np.ndarray],
    encode_constant: Callable[[Any], int],
    event_type_array: np.ndarray,
    event_type_ids: Dict[str, int],
    range_starts: np.ndarray,
    range_ends: np.ndarray,
) -> np.ndarray:
    qids = list(qids)
    qpos = {qid: i for i, qid in enumerate(qids)}
    variables: Dict[str, int] = {}
    rows: List[RefineOccurrence] = []
    keepalive: List[Any] = []
    event_types = _i32(np.asarray(event_type_array))
    keepalive.append(event_types)
    for event in query.get("events", []):
        qid = str(event["id"])
        for field, raw in (event.get("roles", {}) or {}).items():
            field = str(field)
            is_variable = isinstance(raw, str) and raw.startswith("$")
            if field == "event_id" and not is_variable:
                continue
            values = _i32(np.asarray(role_array(qid, field)))
            keepalive.append(values)
            if is_variable:
                variable_id = variables.setdefault(str(raw), len(variables))
                wanted = 0
            else:
                variable_id = 0
                wanted = int(encode_constant(raw))
            rows.append(
                RefineOccurrence(
                    qpos[qid],
                    variable_id,
                    _pi32(values),
                    wanted,
                    1 if is_variable else 0,
                    0 if field == "event_id" else 1,
                    _pi32(event_types),
                    int(event_type_ids.get(str(event.get("type", "")), 0)),
                )
            )
    occurrence_array = (RefineOccurrence * len(rows))(*rows)
    starts = _u64(range_starts).reshape(-1)
    ends = _u64(range_ends).reshape(-1)
    candidate_count = int(np.asarray(range_starts).shape[0])
    output = np.zeros(candidate_count, dtype=np.uint8)
    req = SameTxRefineRequest(
        len(qids),
        len(variables),
        occurrence_array,
        len(rows),
        candidate_count,
        _p64(starts),
        _p64(ends),
        output.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
    )
    rc = _library().hvceq_refine_same_tx_domains(ctypes.byref(req))
    if rc != 0:
        raise RuntimeError(f"Rust SameTx Refined kernel failed: rc={rc}")
    return output.astype(np.bool_, copy=False)


def refine_anchor_domains(
    *,
    query: dict,
    qids: Sequence[str],
    anchor_qid: str,
    views: Dict[str, Dict[str, Any]],
    role_array: Callable[[str, str], np.ndarray],
    eligible: Dict[str, np.ndarray],
    anchor_indices: np.ndarray,
    range_starts: np.ndarray,
    range_ends: np.ndarray,
) -> np.ndarray:
    qids = list(qids)
    qpos = {qid: i for i, qid in enumerate(qids)}
    variables: Dict[str, int] = {}
    keepalive: List[Any] = []
    spec_rows: List[SpecView] = []
    occurrence_arrays: List[Any] = []
    event_specs = {str(e["id"]): e for e in query.get("events", [])}
    for qid in qids:
        spec = event_specs[qid]
        ords = _i64(np.asarray(views[qid]["ord"]))
        mask = np.ascontiguousarray(eligible[qid], dtype=np.uint8)
        keepalive.extend([ords, mask])
        rows: List[Occurrence] = []
        for field, raw in (spec.get("roles", {}) or {}).items():
            if isinstance(raw, str) and raw.startswith("$"):
                var_id = variables.setdefault(raw, len(variables))
                values = _i32(np.asarray(role_array(qid, str(field))))
                keepalive.append(values)
                rows.append(Occurrence(var_id, _pi32(values)))
        occurrence_array = (Occurrence * len(rows))(*rows)
        occurrence_arrays.append(occurrence_array)
        spec_rows.append(
            SpecView(
                len(ords),
                _pi64(ords),
                mask.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
                ctypes.cast(occurrence_array, ctypes.POINTER(Occurrence)),
                len(rows),
                int(views[qid]["view_id"]),
            )
        )
    specs_array = (SpecView * len(spec_rows))(*spec_rows)
    anchors = _u64(anchor_indices)
    starts = _u64(range_starts).reshape(-1)
    ends = _u64(range_ends).reshape(-1)
    output = np.zeros(len(anchors), dtype=np.uint8)
    req = AnchorRefineRequest(
        specs_array,
        len(spec_rows),
        len(variables),
        qpos[anchor_qid],
        len(anchors),
        _p64(anchors),
        _p64(starts),
        _p64(ends),
        output.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
    )
    rc = _library().hvceq_refine_anchor_domains(ctypes.byref(req))
    if rc != 0:
        raise RuntimeError(f"Rust Episode Refined kernel failed: rc={rc}")
    return output.astype(np.bool_, copy=False)
