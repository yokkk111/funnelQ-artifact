#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sqlite3
import time
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


def _load_v5_module():
    here = Path(__file__).resolve().parent
    candidates = [
        here / "build_ceh_same_tx_runtime_v5.py",
        here.parent / "build_ceh_same_tx_runtime_v5.py",
        here.parent / "scripts" / "build_ceh_same_tx_runtime_v5.py",
    ]
    for p in candidates:
        if p.exists():
            spec = importlib.util.spec_from_file_location("build_ceh_same_tx_runtime_v5_dyn", str(p))
            if spec is None or spec.loader is None:
                continue
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
    raise ModuleNotFoundError(
        "Cannot locate build_ceh_same_tx_runtime_v5.py. Tried: " + ", ".join(str(p) for p in candidates)
    )


_v5 = _load_v5_module()


def _get_required_attr(mod, name: str):
    if not hasattr(mod, name):
        raise AttributeError(f"Your build_ceh_same_tx_runtime_v5.py does not export required helper: {name}")
    return getattr(mod, name)


def _fallback_ensure_dir(path: str):
    if path:
        Path(path).mkdir(parents=True, exist_ok=True)


def _fallback_norm_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def _fallback_registry_df_from_snapshot(snapshot) -> pd.DataFrame:
    if snapshot is None:
        return pd.DataFrame()
    if isinstance(snapshot, pd.DataFrame):
        return snapshot.copy()
    if isinstance(snapshot, list):
        if len(snapshot) == 0:
            return pd.DataFrame()
        if isinstance(snapshot[0], dict):
            return pd.DataFrame(snapshot)
        return pd.DataFrame({"value": snapshot})
    if isinstance(snapshot, dict):
        all_values_dict = all(isinstance(v, dict) for v in snapshot.values())
        if all_values_dict:
            rows = []
            for k, v in snapshot.items():
                row = dict(v)
                if "event_type" not in row:
                    row["event_type"] = k
                rows.append(row)
            return pd.DataFrame(rows)
        return pd.DataFrame([snapshot])
    return pd.DataFrame()


def _fallback_role_coverage_df(event_atoms: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if event_atoms is None or event_atoms.empty:
        return pd.DataFrame(columns=["event_type", "role_name", "non_empty_count", "total_count", "coverage_ratio"])
    for et, g in event_atoms.groupby("event_type", sort=False):
        total = int(len(g))
        role_counter = defaultdict(int)
        for r in g.itertuples(index=False):
            try:
                roles = json.loads(r.roles_json) if getattr(r, "roles_json", None) else {}
            except Exception:
                roles = {}
            for k, v in roles.items():
                if _fallback_norm_str(v):
                    role_counter[str(k)] += 1
        for role_name, cnt in sorted(role_counter.items()):
            rows.append(
                {
                    "event_type": str(et),
                    "role_name": role_name,
                    "non_empty_count": int(cnt),
                    "total_count": total,
                    "coverage_ratio": 0.0 if total == 0 else round(cnt / total, 6),
                }
            )
    return pd.DataFrame(rows)


def _jsonify_sqlite_cell(x):
    if isinstance(x, (dict, list, tuple, set)):
        return json.dumps(x, ensure_ascii=False, separators=(",", ":"))
    return x


def sanitize_dataframe_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].map(_jsonify_sqlite_cell)
    return out


ensure_dir = getattr(_v5, "ensure_dir", _fallback_ensure_dir)
norm_str = getattr(_v5, "norm_str", _fallback_norm_str)

ensure_role_keys = _get_required_attr(_v5, "ensure_role_keys")
normalize_flashloans = _get_required_attr(_v5, "normalize_flashloans")
normalize_borrow = _get_required_attr(_v5, "normalize_borrow")
normalize_dex = _get_required_attr(_v5, "normalize_dex")
records_to_event_atoms_df = _get_required_attr(_v5, "records_to_event_atoms_df")
type_multiset = _get_required_attr(_v5, "type_multiset")
temporal_type_pairs = _get_required_attr(_v5, "temporal_type_pairs")
role_pairs_for_type_share = _get_required_attr(_v5, "role_pairs_for_type_share")
role_interface_pairs = _get_required_attr(_v5, "role_interface_pairs")
role_member_index = _get_required_attr(_v5, "role_member_index")

REGISTRY_SNAPSHOT = getattr(_v5, "REGISTRY_SNAPSHOT", None)

if hasattr(_v5, "registry_df"):
    _registry_df_fn = _v5.registry_df
else:
    def _registry_df_fn():
        return _fallback_registry_df_from_snapshot(REGISTRY_SNAPSHOT)

if hasattr(_v5, "role_coverage_df"):
    role_coverage_df = _v5.role_coverage_df
else:
    role_coverage_df = _fallback_role_coverage_df


def canonical_registry_df() -> pd.DataFrame:
    raw = _registry_df_fn()
    if raw is None:
        raw = pd.DataFrame()
    elif not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)

    if raw.empty:
        return pd.DataFrame(columns=["rule_id", "event_type", "source_table", "role_schema_json", "anchorable_fields_json"])

    rows = []
    for rec in raw.to_dict("records"):
        event_type = norm_str(rec.get("event_type") or rec.get("type"))
        if not event_type:
            continue
        rule_id = norm_str(rec.get("rule_id") or event_type)
        source_table = norm_str(rec.get("source_table") or rec.get("source") or "")

        role_schema = rec.get("role_schema_json", rec.get("role_schema", {}))
        if isinstance(role_schema, str):
            try:
                role_schema = json.loads(role_schema)
            except Exception:
                role_schema = {}
        if not isinstance(role_schema, dict):
            role_schema = {}

        anchorable_fields = rec.get("anchorable_fields_json", rec.get("anchorable_fields", []))
        if isinstance(anchorable_fields, str):
            try:
                anchorable_fields = json.loads(anchorable_fields)
            except Exception:
                anchorable_fields = []
        if isinstance(anchorable_fields, dict):
            anchorable_fields = list(anchorable_fields.keys())
        if not isinstance(anchorable_fields, list):
            anchorable_fields = []

        rows.append(
            {
                "rule_id": rule_id,
                "event_type": event_type,
                "source_table": source_table,
                "role_schema_json": json.dumps(role_schema, ensure_ascii=False, separators=(",", ":")),
                "anchorable_fields_json": json.dumps(anchorable_fields, ensure_ascii=False, separators=(",", ":")),
            }
        )
    return pd.DataFrame(rows)


def canonical_registry_by_type() -> Dict[str, dict]:
    df = canonical_registry_df()
    out = {}
    for rec in df.to_dict("records"):
        out[rec["event_type"]] = {
            "rule_id": rec["rule_id"],
            "event_type": rec["event_type"],
            "source_table": rec["source_table"],
            "role_schema": json.loads(rec["role_schema_json"]),
            "anchorable_fields": json.loads(rec["anchorable_fields_json"]),
        }
    return out


def type_position_index(events: List[dict]) -> Dict[str, List[int]]:
    out = defaultdict(list)
    for e in events:
        out[str(e["event_type"])].append(int(e["seq_index"]))
    return {k: sorted(v) for k, v in sorted(out.items())}


def parse_anchor_component(comp: str) -> dict:
    parts = [p for p in str(comp).split("|") if p]
    out = {
        "anchor_component": str(comp),
        "event_type": "",
        "field_count": 0,
        "has_protocol": 0,
        "constant_field_count": 0,
        "fields": [],
    }
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        if k == "type":
            out["event_type"] = v
        elif k == "protocol":
            out["has_protocol"] = 1
            out["field_count"] += 1
            out["fields"].append(k)
        else:
            out["constant_field_count"] += 1
            out["field_count"] += 1
            out["fields"].append(k)
    out["fields"] = sorted(set(out["fields"]))
    return out


def anchor_components_for_event(event_obj: dict, reg_by_type: Dict[str, dict]) -> List[str]:
    et = str(event_obj["event_type"])
    spec = reg_by_type[et]
    protocol = norm_str(event_obj.get("protocol", ""))
    role_map = ensure_role_keys(et, event_obj.get("roles", {}), contract=norm_str(event_obj.get("contract", "")))

    comps = set()
    comps.add(f"type={et}")
    if protocol:
        comps.add(f"type={et}|protocol={protocol}")

    anchorable_fields = spec.get("anchorable_fields", [])
    if isinstance(anchorable_fields, str):
        try:
            anchorable_fields = json.loads(anchorable_fields)
        except Exception:
            anchorable_fields = []
    if not isinstance(anchorable_fields, list):
        anchorable_fields = []

    constant_fields = []
    for field in anchorable_fields:
        if field in {"type", "protocol"}:
            continue
        v = norm_str(role_map.get(field, ""))
        if v:
            constant_fields.append((field, v))

    for field, val in constant_fields:
        comps.add(f"type={et}|{field}={val}")
        if protocol:
            comps.add(f"type={et}|protocol={protocol}|{field}={val}")

    max_combo = min(2, len(constant_fields))
    for k in range(2, max_combo + 1):
        for subset in combinations(constant_fields, k):
            suffix = "|".join(f"{field}={val}" for field, val in subset)
            comps.add(f"type={et}|{suffix}")
            if protocol:
                comps.add(f"type={et}|protocol={protocol}|{suffix}")
    return sorted(comps)


def build_runtime_tables(event_atoms: pd.DataFrame):
    reg_by_type = canonical_registry_by_type()

    evidence_rows = []
    base_rows = []
    refined_rows = []
    anchor_stats_rows = []
    closure_event_rows = []
    closure_role_rows = []
    closure_attr_rows = []
    anchor_posting_rows = []

    posting_counts: Dict[str, Tuple[int, set]] = defaultdict(lambda: [0, set()])

    if event_atoms.empty:
        return {
            "evidence": pd.DataFrame(columns=["closure_id", "tx_hash", "block_number", "block_time", "event_count"]),
            "base": pd.DataFrame(columns=["closure_id", "type_mult_json", "temporal_pairs_json", "share_type_pairs_json"]),
            "refined": pd.DataFrame(columns=["closure_id", "role_members_json", "iface_pairs_json", "type_positions_json"]),
            "anchor_stats": pd.DataFrame(columns=["anchor_component", "event_type", "field_count", "has_protocol", "constant_field_count", "fields_json", "num_events", "num_closures", "scope", "scope_compatibility"]),
            "closure_events": pd.DataFrame(columns=["closure_id", "event_id", "event_type", "seq_index", "protocol"]),
            "closure_roles": pd.DataFrame(columns=["closure_id", "event_id", "role_name", "role_value"]),
            "closure_attrs": pd.DataFrame(columns=["closure_id", "event_id", "attr_name", "attr_value"]),
            "anchor_posting_rows": pd.DataFrame(columns=["anchor_component", "closure_id", "event_id", "pos"]),
        }

    grouped = event_atoms.sort_values(["tx_hash", "seq_index", "event_id"], kind="mergesort").groupby("tx_hash", sort=False)

    for tx_hash, g2 in grouped:
        closure_id = norm_str(tx_hash)
        events: List[dict] = []

        for r in g2.itertuples(index=False):
            roles_raw = json.loads(r.roles_json) if getattr(r, "roles_json", None) else {}
            attrs_raw = json.loads(r.attrs_json) if getattr(r, "attrs_json", None) else {}
            roles = ensure_role_keys(r.event_type, roles_raw, contract=norm_str(r.contract))
            attrs = attrs_raw if isinstance(attrs_raw, dict) else {}

            evt = {
                "event_id": norm_str(r.event_id),
                "event_type": norm_str(r.event_type),
                "seq_index": int(r.seq_index),
                "evt_index": None if pd.isna(r.evt_index) else int(r.evt_index),
                "protocol": norm_str(r.protocol),
                "version": norm_str(r.version),
                "contract": norm_str(r.contract),
                "tx_hash": norm_str(r.tx_hash),
                "block_number": None if pd.isna(r.block_number) else int(r.block_number),
                "block_time": norm_str(r.block_time),
                "roles": roles,
                "attrs": attrs,
            }
            events.append(evt)

            closure_event_rows.append(
                {
                    "closure_id": closure_id,
                    "event_id": evt["event_id"],
                    "event_type": evt["event_type"],
                    "seq_index": int(evt["seq_index"]),
                    "protocol": evt["protocol"],
                }
            )

            for role_name, role_value in sorted(roles.items()):
                rv = norm_str(role_value)
                if rv:
                    closure_role_rows.append(
                        {
                            "closure_id": closure_id,
                            "event_id": evt["event_id"],
                            "role_name": str(role_name),
                            "role_value": rv,
                        }
                    )

            for attr_name, attr_value in sorted(attrs.items()):
                av = norm_str(attr_value)
                if av:
                    closure_attr_rows.append(
                        {
                            "closure_id": closure_id,
                            "event_id": evt["event_id"],
                            "attr_name": str(attr_name),
                            "attr_value": av,
                        }
                    )

        evidence_rows.append(
            {
                "closure_id": closure_id,
                "tx_hash": norm_str(g2["tx_hash"].iloc[0]),
                "block_number": None if pd.isna(g2["block_number"].iloc[0]) else int(g2["block_number"].iloc[0]),
                "block_time": norm_str(g2["block_time"].iloc[0]),
                "event_count": int(len(events)),
            }
        )

        type_mult = type_multiset(events)
        temporal_pairs = temporal_type_pairs(events)
        share_type_pairs = role_pairs_for_type_share(events, reg_by_type)
        role_members = role_member_index(events, reg_by_type)
        iface_pairs = role_interface_pairs(events, reg_by_type)
        type_positions = type_position_index(events)

        base_rows.append(
            {
                "closure_id": closure_id,
                "type_mult_json": json.dumps(type_mult, ensure_ascii=False, separators=(",", ":")),
                "temporal_pairs_json": json.dumps(temporal_pairs, ensure_ascii=False, separators=(",", ":")),
                "share_type_pairs_json": json.dumps(share_type_pairs, ensure_ascii=False, separators=(",", ":")),
            }
        )

        refined_rows.append(
            {
                "closure_id": closure_id,
                "role_members_json": json.dumps(role_members, ensure_ascii=False, separators=(",", ":")),
                "iface_pairs_json": json.dumps(iface_pairs, ensure_ascii=False, separators=(",", ":")),
                "type_positions_json": json.dumps(type_positions, ensure_ascii=False, separators=(",", ":")),
            }
        )

        for evt in events:
            for comp in anchor_components_for_event(evt, reg_by_type):
                anchor_posting_rows.append(
                    {
                        "anchor_component": comp,
                        "closure_id": closure_id,
                        "event_id": evt["event_id"],
                        "pos": int(evt["seq_index"]),
                    }
                )
                posting_counts[comp][0] += 1
                posting_counts[comp][1].add(closure_id)

    for comp, value in posting_counts.items():
        num_events, closure_set = value
        meta = parse_anchor_component(comp)
        anchor_stats_rows.append(
            {
                "anchor_component": comp,
                "event_type": meta["event_type"],
                "field_count": int(meta["field_count"]),
                "has_protocol": int(meta["has_protocol"]),
                "constant_field_count": int(meta["constant_field_count"]),
                "fields_json": json.dumps(meta["fields"], ensure_ascii=False, separators=(",", ":")),
                "num_events": int(num_events),
                "num_closures": int(len(closure_set)),
                "scope": "same_tx",
                "scope_compatibility": 1.0,
            }
        )

    return {
        "evidence": pd.DataFrame(evidence_rows),
        "base": pd.DataFrame(base_rows),
        "refined": pd.DataFrame(refined_rows),
        "anchor_stats": pd.DataFrame(anchor_stats_rows),
        "closure_events": pd.DataFrame(closure_event_rows),
        "closure_roles": pd.DataFrame(closure_role_rows),
        "closure_attrs": pd.DataFrame(closure_attr_rows),
        "anchor_posting_rows": pd.DataFrame(anchor_posting_rows),
    }


def build_core_runtime_tables(tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    closures = tables["evidence"][["closure_id"]].copy()
    closures = closures.sort_values(["closure_id"], kind="mergesort").reset_index(drop=True)
    closures["closure_int"] = range(1, len(closures) + 1)
    closures = closures[["closure_int", "closure_id"]]

    ce = tables["closure_events"].copy()
    ce = ce.sort_values(["closure_id", "seq_index", "event_id"], kind="mergesort").reset_index(drop=True)
    ce["event_idx"] = ce.groupby("closure_id").cumcount().astype(int)
    ce = ce.merge(closures, on="closure_id", how="left")
    ce["protocol"] = ce["protocol"].fillna("")

    event_key = ce[["closure_id", "event_id", "closure_int", "event_idx"]].copy()

    cr = tables["closure_roles"].merge(event_key, on=["closure_id", "event_id"], how="left")
    cr = cr[["closure_int", "event_idx", "role_name", "role_value"]].dropna(subset=["closure_int", "event_idx"]).copy()
    cr["closure_int"] = cr["closure_int"].astype(int)
    cr["event_idx"] = cr["event_idx"].astype(int)

    ca = tables["closure_attrs"].merge(event_key, on=["closure_id", "event_id"], how="left")
    ca = ca[["closure_int", "event_idx", "attr_name", "attr_value"]].dropna(subset=["closure_int", "event_idx"]).copy()
    ca["closure_int"] = ca["closure_int"].astype(int)
    ca["event_idx"] = ca["event_idx"].astype(int)

    ap = tables["anchor_posting_rows"].merge(event_key, on=["closure_id", "event_id"], how="left")
    ap = ap[["anchor_component", "closure_int", "event_idx"]].dropna(subset=["closure_int", "event_idx"]).copy()
    ap["closure_int"] = ap["closure_int"].astype(int)
    ap["event_idx"] = ap["event_idx"].astype(int)

    core_events = ce[["closure_int", "event_idx", "event_id", "event_type", "seq_index", "protocol"]].copy()
    core_events["closure_int"] = core_events["closure_int"].astype(int)
    core_events["event_idx"] = core_events["event_idx"].astype(int)
    core_events["seq_index"] = core_events["seq_index"].astype(int)

    event_types = pd.DataFrame({"event_type": sorted({norm_str(x) for x in core_events["event_type"] if norm_str(x)})})
    event_types["type_int"] = range(1, len(event_types) + 1)
    event_types = event_types[["type_int", "event_type"]]
    event_types_map = {r.event_type: int(r.type_int) for r in event_types.itertuples(index=False)}
    core_events["type_int"] = core_events["event_type"].map(event_types_map).astype(int)

    protocol_values = sorted({norm_str(x) for x in core_events["protocol"] if norm_str(x)})
    protocols = pd.DataFrame({"protocol": protocol_values})
    protocols["protocol_int"] = range(1, len(protocols) + 1)
    protocols = protocols[["protocol_int", "protocol"]]
    protocols_map = {r.protocol: int(r.protocol_int) for r in protocols.itertuples(index=False)}
    core_events["protocol_int"] = core_events["protocol"].map(lambda x: protocols_map.get(norm_str(x), None))

    role_names = pd.DataFrame({"role_name": sorted({norm_str(x) for x in cr["role_name"] if norm_str(x)})})
    role_names["role_name_int"] = range(1, len(role_names) + 1)
    role_names = role_names[["role_name_int", "role_name"]]
    role_names_map = {r.role_name: int(r.role_name_int) for r in role_names.itertuples(index=False)}
    cr["role_name_int"] = cr["role_name"].map(role_names_map).astype(int)

    attr_names = pd.DataFrame({"attr_name": sorted({norm_str(x) for x in ca["attr_name"] if norm_str(x)})})
    attr_names["attr_name_int"] = range(1, len(attr_names) + 1)
    attr_names = attr_names[["attr_name_int", "attr_name"]]
    attr_names_map = {r.attr_name: int(r.attr_name_int) for r in attr_names.itertuples(index=False)}
    ca["attr_name_int"] = ca["attr_name"].map(attr_names_map).astype(int)

    anchor_components = pd.DataFrame({"anchor_component": sorted({norm_str(x) for x in ap["anchor_component"] if norm_str(x)})})
    anchor_components["comp_int"] = range(1, len(anchor_components) + 1)
    anchor_components = anchor_components[["comp_int", "anchor_component"]]
    anchor_components_map = {r.anchor_component: int(r.comp_int) for r in anchor_components.itertuples(index=False)}
    ap["comp_int"] = ap["anchor_component"].map(anchor_components_map).astype(int)

    value_texts = sorted({norm_str(x) for x in pd.concat([cr["role_value"], ca["attr_value"]], ignore_index=True) if norm_str(x)})
    value_map_df = pd.DataFrame({"value_text": value_texts})
    value_map_df["value_int"] = range(1, len(value_map_df) + 1)
    value_map_df = value_map_df[["value_int", "value_text"]]
    value_lookup = {r.value_text: int(r.value_int) for r in value_map_df.itertuples(index=False)}

    cr["value_int"] = cr["role_value"].map(value_lookup).astype(int)
    ca["value_int"] = ca["attr_value"].map(value_lookup).astype(int)

    core_events = core_events[["closure_int", "event_idx", "event_id", "type_int", "seq_index", "protocol_int"]].copy()
    core_roles = cr[["closure_int", "event_idx", "role_name_int", "value_int"]].copy()
    core_attrs = ca[["closure_int", "event_idx", "attr_name_int", "value_int"]].copy()
    core_postings = ap[["comp_int", "closure_int", "event_idx"]].drop_duplicates().copy()

    return {
        "closures_map": closures,
        "event_types_map": event_types,
        "protocols_map": protocols,
        "role_names_map": role_names,
        "attr_names_map": attr_names,
        "anchor_components_map": anchor_components,
        "string_values_map": value_map_df,
        "closure_events_core": core_events,
        "closure_event_roles_core": core_roles,
        "closure_event_attrs_core": core_attrs,
        "anchor_postings_core": core_postings,
    }


def _executemany(conn: sqlite3.Connection, sql: str, rows, batch_size: int = 50000):
    cur = conn.cursor()
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)


def write_sqlite_runtime(sqlite_path: str, tables: Dict[str, pd.DataFrame], registry_df_obj: pd.DataFrame) -> None:
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)

    core = build_core_runtime_tables(tables)

    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA page_size=65536")
        cur.execute("PRAGMA journal_mode=OFF")
        cur.execute("PRAGMA synchronous=OFF")
        cur.execute("PRAGMA temp_store=MEMORY")
        cur.execute("PRAGMA cache_size=-250000")

        sanitize_dataframe_for_sqlite(registry_df_obj).to_sql("registry_snapshot", conn, if_exists="replace", index=False)
        sanitize_dataframe_for_sqlite(tables["evidence"]).to_sql("evidence_closures", conn, if_exists="replace", index=False)
        sanitize_dataframe_for_sqlite(tables["base"]).to_sql("base_synopsis", conn, if_exists="replace", index=False)
        sanitize_dataframe_for_sqlite(tables["refined"]).to_sql("refined_synopsis", conn, if_exists="replace", index=False)
        sanitize_dataframe_for_sqlite(tables["anchor_stats"]).to_sql("anchor_stats_snapshot", conn, if_exists="replace", index=False)

        cur.executescript(
            """
            CREATE TABLE closures_map (
                closure_int INTEGER PRIMARY KEY,
                closure_id TEXT NOT NULL UNIQUE
            );

            CREATE TABLE event_types_map (
                type_int INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL UNIQUE
            ) WITHOUT ROWID;

            CREATE TABLE protocols_map (
                protocol_int INTEGER PRIMARY KEY,
                protocol TEXT NOT NULL UNIQUE
            ) WITHOUT ROWID;

            CREATE TABLE role_names_map (
                role_name_int INTEGER PRIMARY KEY,
                role_name TEXT NOT NULL UNIQUE
            ) WITHOUT ROWID;

            CREATE TABLE attr_names_map (
                attr_name_int INTEGER PRIMARY KEY,
                attr_name TEXT NOT NULL UNIQUE
            ) WITHOUT ROWID;

            CREATE TABLE anchor_components_map (
                comp_int INTEGER PRIMARY KEY,
                anchor_component TEXT NOT NULL UNIQUE
            ) WITHOUT ROWID;

            CREATE TABLE string_values_map (
                value_int INTEGER PRIMARY KEY,
                value_text TEXT NOT NULL UNIQUE
            ) WITHOUT ROWID;

            CREATE TABLE closure_events_core (
                closure_int INTEGER NOT NULL,
                event_idx INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                type_int INTEGER NOT NULL,
                seq_index INTEGER NOT NULL,
                protocol_int INTEGER,
                PRIMARY KEY (closure_int, event_idx)
            ) WITHOUT ROWID;

            CREATE TABLE closure_event_roles_core (
                closure_int INTEGER NOT NULL,
                event_idx INTEGER NOT NULL,
                role_name_int INTEGER NOT NULL,
                value_int INTEGER NOT NULL,
                PRIMARY KEY (closure_int, event_idx, role_name_int)
            ) WITHOUT ROWID;

            CREATE TABLE closure_event_attrs_core (
                closure_int INTEGER NOT NULL,
                event_idx INTEGER NOT NULL,
                attr_name_int INTEGER NOT NULL,
                value_int INTEGER NOT NULL,
                PRIMARY KEY (closure_int, event_idx, attr_name_int)
            ) WITHOUT ROWID;

            CREATE TABLE anchor_postings_core (
                comp_int INTEGER NOT NULL,
                closure_int INTEGER NOT NULL,
                event_idx INTEGER NOT NULL,
                PRIMARY KEY (comp_int, closure_int, event_idx)
            ) WITHOUT ROWID;
            """
        )

        _executemany(conn, "INSERT INTO closures_map(closure_int,closure_id) VALUES (?,?)", ((int(r.closure_int), str(r.closure_id)) for r in core["closures_map"].itertuples(index=False)))
        _executemany(conn, "INSERT INTO event_types_map(type_int,event_type) VALUES (?,?)", ((int(r.type_int), str(r.event_type)) for r in core["event_types_map"].itertuples(index=False)))
        _executemany(conn, "INSERT INTO protocols_map(protocol_int,protocol) VALUES (?,?)", ((int(r.protocol_int), str(r.protocol)) for r in core["protocols_map"].itertuples(index=False)))
        _executemany(conn, "INSERT INTO role_names_map(role_name_int,role_name) VALUES (?,?)", ((int(r.role_name_int), str(r.role_name)) for r in core["role_names_map"].itertuples(index=False)))
        _executemany(conn, "INSERT INTO attr_names_map(attr_name_int,attr_name) VALUES (?,?)", ((int(r.attr_name_int), str(r.attr_name)) for r in core["attr_names_map"].itertuples(index=False)))
        _executemany(conn, "INSERT INTO anchor_components_map(comp_int,anchor_component) VALUES (?,?)", ((int(r.comp_int), str(r.anchor_component)) for r in core["anchor_components_map"].itertuples(index=False)))
        _executemany(conn, "INSERT INTO string_values_map(value_int,value_text) VALUES (?,?)", ((int(r.value_int), str(r.value_text)) for r in core["string_values_map"].itertuples(index=False)))
        _executemany(
            conn,
            "INSERT INTO closure_events_core(closure_int,event_idx,event_id,type_int,seq_index,protocol_int) VALUES (?,?,?,?,?,?)",
            ((int(r.closure_int), int(r.event_idx), str(r.event_id), int(r.type_int), int(r.seq_index), None if pd.isna(r.protocol_int) else int(r.protocol_int)) for r in core["closure_events_core"].itertuples(index=False)),
        )
        _executemany(
            conn,
            "INSERT INTO closure_event_roles_core(closure_int,event_idx,role_name_int,value_int) VALUES (?,?,?,?)",
            ((int(r.closure_int), int(r.event_idx), int(r.role_name_int), int(r.value_int)) for r in core["closure_event_roles_core"].itertuples(index=False)),
        )
        _executemany(
            conn,
            "INSERT INTO closure_event_attrs_core(closure_int,event_idx,attr_name_int,value_int) VALUES (?,?,?,?)",
            ((int(r.closure_int), int(r.event_idx), int(r.attr_name_int), int(r.value_int)) for r in core["closure_event_attrs_core"].itertuples(index=False)),
        )
        _executemany(
            conn,
            "INSERT INTO anchor_postings_core(comp_int,closure_int,event_idx) VALUES (?,?,?)",
            ((int(r.comp_int), int(r.closure_int), int(r.event_idx)) for r in core["anchor_postings_core"].itertuples(index=False)),
        )

        cur.executescript(
            """
            CREATE UNIQUE INDEX idx_closures_map_id ON closures_map(closure_id);
            CREATE UNIQUE INDEX idx_event_types_name ON event_types_map(event_type);
            CREATE UNIQUE INDEX idx_protocols_name ON protocols_map(protocol);
            CREATE UNIQUE INDEX idx_role_names_name ON role_names_map(role_name);
            CREATE UNIQUE INDEX idx_attr_names_name ON attr_names_map(attr_name);
            CREATE UNIQUE INDEX idx_anchor_comp_name ON anchor_components_map(anchor_component);
            CREATE UNIQUE INDEX idx_string_values_text ON string_values_map(value_text);

            CREATE INDEX idx_ce_core_lookup ON closure_events_core(closure_int, type_int, seq_index, event_idx);
            CREATE INDEX idx_cer_core_lookup ON closure_event_roles_core(role_name_int, closure_int, event_idx);
            CREATE INDEX idx_cea_core_lookup ON closure_event_attrs_core(attr_name_int, closure_int, event_idx);
            CREATE INDEX idx_ap_core_lookup ON anchor_postings_core(comp_int, closure_int, event_idx);

            CREATE VIEW closure_events AS
            SELECT c.closure_id AS closure_id,
                   e.event_id AS event_id,
                   t.event_type AS event_type,
                   e.seq_index AS seq_index,
                   COALESCE(p.protocol, '') AS protocol
            FROM closure_events_core e
            JOIN closures_map c ON c.closure_int = e.closure_int
            JOIN event_types_map t ON t.type_int = e.type_int
            LEFT JOIN protocols_map p ON p.protocol_int = e.protocol_int;

            CREATE VIEW closure_event_roles AS
            SELECT c.closure_id AS closure_id,
                   e.event_id AS event_id,
                   rn.role_name AS role_name,
                   sv.value_text AS role_value
            FROM closure_event_roles_core r
            JOIN closures_map c ON c.closure_int = r.closure_int
            JOIN closure_events_core e ON e.closure_int = r.closure_int AND e.event_idx = r.event_idx
            JOIN role_names_map rn ON rn.role_name_int = r.role_name_int
            JOIN string_values_map sv ON sv.value_int = r.value_int;

            CREATE VIEW closure_event_attrs AS
            SELECT c.closure_id AS closure_id,
                   e.event_id AS event_id,
                   an.attr_name AS attr_name,
                   sv.value_text AS attr_value
            FROM closure_event_attrs_core a
            JOIN closures_map c ON c.closure_int = a.closure_int
            JOIN closure_events_core e ON e.closure_int = a.closure_int AND e.event_idx = a.event_idx
            JOIN attr_names_map an ON an.attr_name_int = a.attr_name_int
            JOIN string_values_map sv ON sv.value_int = a.value_int;

            CREATE VIEW anchor_posting_rows AS
            SELECT ac.anchor_component AS anchor_component,
                   c.closure_id AS closure_id,
                   e.event_id AS event_id,
                   e.seq_index AS pos
            FROM anchor_postings_core p
            JOIN anchor_components_map ac ON ac.comp_int = p.comp_int
            JOIN closures_map c ON c.closure_int = p.closure_int
            JOIN closure_events_core e ON e.closure_int = p.closure_int AND e.event_idx = p.event_idx;
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_base_closure ON base_synopsis(closure_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_refined_closure ON refined_synopsis(closure_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_anchor_stats_comp ON anchor_stats_snapshot(anchor_component)")
        conn.commit()
        cur.execute("VACUUM")
        conn.commit()
    finally:
        conn.close()


def parse_args():
    ap = argparse.ArgumentParser(description="Build compact_v6 slim runtime with value-dictionary core views")
    ap.add_argument("--flashloans", required=True)
    ap.add_argument("--borrow", required=True)
    ap.add_argument("--dex", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--sqlite-out", required=True)
    return ap.parse_args()


def main():
    args = parse_args()
    ensure_dir(args.outdir)
    ensure_dir(os.path.dirname(args.sqlite_out) or ".")

    t0 = time.perf_counter()
    raw_input_total_bytes = sum(os.path.getsize(p) for p in [args.flashloans, args.borrow, args.dex] if os.path.exists(p))

    flash_df = pd.read_csv(args.flashloans, low_memory=False, dtype=str)
    borrow_df = pd.read_csv(args.borrow, low_memory=False, dtype=str)
    dex_df = pd.read_csv(args.dex, low_memory=False, dtype=str)
    t1 = time.perf_counter()

    records = []
    records.extend(normalize_flashloans(flash_df))
    records.extend(normalize_borrow(borrow_df))
    records.extend(normalize_dex(dex_df))
    event_atoms = records_to_event_atoms_df(records)
    t2 = time.perf_counter()

    tables = build_runtime_tables(event_atoms)
    t3 = time.perf_counter()

    event_atoms_csv = os.path.join(args.outdir, "event_atoms.csv")
    role_cov_csv = os.path.join(args.outdir, "event_role_coverage.csv")
    base_csv = os.path.join(args.outdir, "base_synopsis.csv")
    refined_csv = os.path.join(args.outdir, "refined_synopsis.csv")
    anchor_stats_csv = os.path.join(args.outdir, "anchor_stats_snapshot.csv")
    slim_manifest_csv = os.path.join(args.outdir, "slim_storage_manifest.csv")

    event_atoms.to_csv(event_atoms_csv, index=False)
    role_coverage_df(event_atoms).to_csv(role_cov_csv, index=False)
    tables["base"].to_csv(base_csv, index=False)
    tables["refined"].to_csv(refined_csv, index=False)
    tables["anchor_stats"].to_csv(anchor_stats_csv, index=False)

    pd.DataFrame(
        {
            "table_name": [
                "base_synopsis",
                "refined_synopsis",
                "closures_map+event/role/attr/anchor core tables + legacy views",
            ],
            "row_count": [
                int(len(tables["base"])),
                int(len(tables["refined"])),
                int(len(tables["closure_events"])) + int(len(tables["closure_roles"])) + int(len(tables["closure_attrs"])) + int(len(tables["anchor_posting_rows"])),
            ],
            "blob_bytes": [0, 0, 0],
        }
    ).to_csv(slim_manifest_csv, index=False)
    t4 = time.perf_counter()

    reg_df_obj = canonical_registry_df()
    reg_df_obj = sanitize_dataframe_for_sqlite(reg_df_obj)

    write_sqlite_runtime(args.sqlite_out, tables, reg_df_obj)
    t5 = time.perf_counter()

    sqlite_size = os.path.getsize(args.sqlite_out) if os.path.exists(args.sqlite_out) else 0

    metrics = {
        "storage_layout": "compact_v6_slim_value_dict_views",
        "index_build_time_s": round(t5 - t0, 6),
        "stage_times_s": {
            "read_csv": round(t1 - t0, 6),
            "normalize_atoms": round(t2 - t1, 6),
            "build_runtime": round(t3 - t2, 6),
            "write_artifacts": round(t4 - t3, 6),
            "write_sqlite": round(t5 - t4, 6),
        },
        "raw_input_total_bytes": int(raw_input_total_bytes),
        "sqlite_size_bytes": int(sqlite_size),
        "event_atoms_rows": int(len(event_atoms)),
        "closure_rows": int(len(tables["evidence"])),
        "posting_rows": int(len(tables["anchor_posting_rows"])),
        "posting_components": int(len(tables["anchor_stats"])),
        "core_physical_tables": {
            "closure_events_core": int(len(tables["closure_events"])),
            "closure_event_roles_core": int(len(tables["closure_roles"])),
            "closure_event_attrs_core": int(len(tables["closure_attrs"])),
            "anchor_postings_core": int(len(tables["anchor_posting_rows"])),
        },
        "artifacts": {
            "event_atoms_csv": int(os.path.getsize(event_atoms_csv)) if os.path.exists(event_atoms_csv) else 0,
            "base_synopsis_csv": int(os.path.getsize(base_csv)) if os.path.exists(base_csv) else 0,
            "refined_synopsis_csv": int(os.path.getsize(refined_csv)) if os.path.exists(refined_csv) else 0,
            "anchor_stats_snapshot_csv": int(os.path.getsize(anchor_stats_csv)) if os.path.exists(anchor_stats_csv) else 0,
            "slim_storage_manifest_csv": int(os.path.getsize(slim_manifest_csv)) if os.path.exists(slim_manifest_csv) else 0,
        },
        "role_coverage_csv": role_cov_csv,
    }

    print("=== Compact Build Metrics (v6 slim value-dict views) ===")
    print(f"index_build_time_s   = {metrics['index_build_time_s']}")
    print(f"sqlite_size_bytes    = {metrics['sqlite_size_bytes']}")
    print(f"event_atoms_rows     = {metrics['event_atoms_rows']}")
    print(f"closure_rows         = {metrics['closure_rows']}")
    print(f"posting_rows         = {metrics['posting_rows']}")
    print(f"posting_components   = {metrics['posting_components']}")
    print("=== Done ===")
    print(f"Output directory: {args.outdir}")

    with open(os.path.join(args.outdir, "build_metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
