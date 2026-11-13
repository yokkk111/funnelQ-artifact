#!/usr/bin/env python3


from __future__ import annotations

import argparse
import bisect
import json
import os
import sqlite3
import time
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Tuple

import pandas as pd

TX_ORDER_CANDIDATES = [
    "tx_position_in_block",
    "tx_index",
    "transaction_index",
    "position_in_block",
    "tx_pos_in_block",
    "index_in_block",
]

REQUIRED_EVENT_COLS = [
    "event_id",
    "closure_id",
    "tx_hash",
    "block_number",
    "block_time",
    "evt_index",
    "event_type",
    "protocol",
    "roles_json",
    "seq_index",
]

SUPPORTED_EVENT_TYPES = {"flashloan", "swap", "borrow", "repay", "liquidation"}


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def norm_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def parse_json_dict(x) -> Dict[str, str]:
    if isinstance(x, dict):
        obj = x
    else:
        s = norm_str(x)
        if not s:
            return {}
        try:
            obj = json.loads(s)
        except Exception:
            return {}
    if not isinstance(obj, dict):
        return {}
    out = {}
    for k, v in obj.items():
        ks = norm_str(k)
        vs = norm_str(v)
        if ks and vs:
            out[ks] = vs
    return out


def compact_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_windows_sec(text: str) -> List[int]:
    out = []
    for part in str(text).split(","):
        s = norm_str(part)
        if not s:
            continue
        v = int(s)
        if v > 0:
            out.append(v)
    out = sorted(set(out))
    if not out:
        raise ValueError("windows-sec must contain at least one positive integer")
    return out


def pick_tx_order_column(df: pd.DataFrame) -> str:
    lower_to_real = {str(c).lower(): str(c) for c in df.columns}
    for cand in TX_ORDER_CANDIDATES:
        if cand.lower() in lower_to_real:
            return lower_to_real[cand.lower()]
    return ""


def parse_args():
    ap = argparse.ArgumentParser(description="Build anchored-episode runtime sidecar mainline (encoded)")
    ap.add_argument("--event-atoms-csv", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--sqlite-out", required=True)
    ap.add_argument("--windows-sec", default="60,300,600")
    ap.add_argument("--write-csv", action="store_true", help="also dump CSV files")
    return ap.parse_args()


def load_event_atoms(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False, dtype=str)
    missing = [c for c in REQUIRED_EVENT_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"event_atoms.csv is missing required columns: {missing}")
    for optional in ["contract"]:
        if optional not in df.columns:
            df[optional] = ""
    return df


def build_timelines(event_atoms: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str]]:
    df = event_atoms.copy()
    for c in ["event_id", "closure_id", "tx_hash", "event_type", "protocol", "contract", "block_time"]:
        df[c] = df[c].map(norm_str)
    df["block_number"] = pd.to_numeric(df["block_number"], errors="coerce").fillna(-1).astype("int64")
    df["evt_index"] = pd.to_numeric(df["evt_index"], errors="coerce").fillna(-1).astype("int64")
    df["seq_index"] = pd.to_numeric(df["seq_index"], errors="coerce").fillna(-1).astype("int64")
    ts = pd.to_datetime(df["block_time"], utc=True, errors="coerce")
    if ts.isna().any():
        raise ValueError(f"block_time has {int(ts.isna().sum())} rows that cannot be parsed")
    df["block_time_unix"] = (ts.astype("int64") // 10**9).astype("int64")

    tx_order_col = pick_tx_order_column(df)
    if tx_order_col:
        df["tx_order_in_block"] = pd.to_numeric(df[tx_order_col], errors="coerce").fillna(-1).astype("int64")
        order_quality = "exact_if_source_column_is_canonical"
        tx_order_source = tx_order_col
    else:
        df["tx_order_in_block"] = -1
        order_quality = "approximate_tx_hash_lex"
        tx_order_source = "tx_hash_lex_fallback"

    tx_df = (
        df.groupby(["closure_id", "tx_hash"], sort=False)
        .agg(
            block_number=("block_number", "min"),
            block_time_unix=("block_time_unix", "min"),
            tx_order_in_block=("tx_order_in_block", "min"),
            event_count=("event_id", "count"),
            min_seq_index=("seq_index", "min"),
            max_seq_index=("seq_index", "max"),
        )
        .reset_index()
    )
    if tx_order_col:
        tx_df = tx_df.sort_values(["block_number", "block_time_unix", "tx_order_in_block", "tx_hash"], kind="mergesort").reset_index(drop=True)
    else:
        tx_df = tx_df.sort_values(["block_number", "block_time_unix", "tx_hash"], kind="mergesort").reset_index(drop=True)
        tx_df["tx_order_in_block"] = tx_df.groupby("block_number", sort=False).cumcount().astype("int64")
    tx_df["global_tx_ord"] = range(len(tx_df))

    df = df.drop(columns=["tx_order_in_block"], errors="ignore").merge(
        tx_df[["closure_id", "tx_hash", "global_tx_ord", "tx_order_in_block"]],
        on=["closure_id", "tx_hash"],
        how="left",
    )
    df = df.sort_values(["global_tx_ord", "seq_index", "evt_index", "event_id"], kind="mergesort").reset_index(drop=True)
    df["global_event_ord"] = range(len(df))

    tx_ord_stats = (
        df.groupby("global_tx_ord", sort=False)
        .agg(min_global_event_ord=("global_event_ord", "min"), max_global_event_ord=("global_event_ord", "max"))
        .reset_index()
    )
    tx_df = tx_df.merge(tx_ord_stats, on="global_tx_ord", how="left")

    event_timeline = df[[
        "global_event_ord",
        "event_id",
        "closure_id",
        "tx_hash",
        "block_number",
        "block_time_unix",
        "tx_order_in_block",
        "global_tx_ord",
        "seq_index",
        "evt_index",
        "event_type",
        "protocol",
        "contract",
    ]].copy()

    tx_timeline = tx_df[[
        "global_tx_ord",
        "closure_id",
        "tx_hash",
        "block_number",
        "block_time_unix",
        "tx_order_in_block",
        "event_count",
        "min_seq_index",
        "max_seq_index",
        "min_global_event_ord",
        "max_global_event_ord",
    ]].copy()

    meta = {
        "order_quality": order_quality,
        "tx_order_source": tx_order_source,
    }
    return event_timeline, tx_timeline, meta


def encode_values(event_timeline: pd.DataFrame, event_atoms: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int], Dict[str, int]]:
    event_types = sorted({norm_str(x) for x in event_timeline["event_type"].tolist() if norm_str(x)})
    event_type_df = pd.DataFrame({
        "event_type_id": range(1, len(event_types) + 1),
        "event_type": event_types,
    })
    event_type_to_id = {et: idx for idx, et in zip(event_type_df["event_type_id"], event_type_df["event_type"])}

    value_set = set()
    for col in ["tx_hash", "protocol", "contract"]:
        value_set.update(norm_str(x) for x in event_timeline[col].tolist() if norm_str(x))

    for roles_json in event_atoms["roles_json"].tolist():
        for _k, v in parse_json_dict(roles_json).items():
            value_set.add(v)

    values = sorted(value_set)
    value_df = pd.DataFrame({
        "value_id": range(1, len(values) + 1),
        "value_text": values,
    })
    value_to_id = {v: idx for idx, v in zip(value_df["value_id"], value_df["value_text"])}
    return event_type_df, value_df, event_type_to_id, value_to_id


def build_window_endpoints(tx_timeline: pd.DataFrame, windows_sec: List[int]) -> pd.DataFrame:
    tx_df = tx_timeline.sort_values("global_tx_ord", kind="mergesort").reset_index(drop=True)
    ts = tx_df["block_time_unix"].astype("int64").tolist()
    rows = []
    for row in tx_df.itertuples(index=False):
        start_ord = int(row.global_tx_ord)
        start_ts = int(row.block_time_unix)
        for w in windows_sec:
            end_ts = start_ts + int(w)
            r = bisect.bisect_right(ts, end_ts) - 1
            if r < start_ord:
                r = start_ord
            rows.append({"window_sec": int(w), "start_tx_ord": start_ord, "end_tx_ord": int(r)})
    return pd.DataFrame(rows)


def build_tx_summaries(event_atoms: pd.DataFrame, event_timeline: pd.DataFrame, event_type_to_id: Dict[str, int], value_to_id: Dict[str, int]):
    event_rows = event_timeline.merge(event_atoms[["event_id", "roles_json"]], on="event_id", how="left")
    event_rows["roles_json"] = event_rows["roles_json"].fillna("{}")
    event_rows = event_rows.sort_values(["global_tx_ord", "global_event_ord"], kind="mergesort").reset_index(drop=True)

    type_count_rows = []
    base_rows = []
    refined_rows = []

    for tx_ord, g in event_rows.groupby("global_tx_ord", sort=False):
        events = []
        for r in g.itertuples(index=False):
            roles = parse_json_dict(r.roles_json)
            events.append({
                "event_type": norm_str(r.event_type),
                "seq_index": int(r.seq_index),
                "roles": roles,
            })

        type_mult = Counter(e["event_type"] for e in events)
        for et, cnt in sorted(type_mult.items()):
            type_count_rows.append({
                "global_tx_ord": int(tx_ord),
                "event_type_id": int(event_type_to_id[et]),
                "count": int(cnt),
            })

        temporal_pairs = set()
        positions = defaultdict(list)
        iface_pairs = set()
        role_presence = defaultdict(set)

        for e in events:
            positions[event_type_to_id[e["event_type"]]].append(int(e["seq_index"]))
            for role_name, role_value in e["roles"].items():
                role_presence[f"{e['event_type']}|{role_name}"].add(int(value_to_id.get(role_value, 0)))

        for i in range(len(events)):
            ei = events[i]
            eti = int(event_type_to_id[ei["event_type"]])
            for j in range(i + 1, len(events)):
                ej = events[j]
                etj = int(event_type_to_id[ej["event_type"]])
                temporal_pairs.add((eti, etj))
                for ri, vi in ei["roles"].items():
                    ivi = int(value_to_id.get(vi, 0))
                    if not ivi:
                        continue
                    for rj, vj in ej["roles"].items():
                        ivj = int(value_to_id.get(vj, 0))
                        if ivi and ivj and ivi == ivj:
                            iface_pairs.add((eti, ri, etj, rj))

        base_rows.append({
            "global_tx_ord": int(tx_ord),
            "temporal_pairs_json": compact_json(sorted([[a, b] for a, b in temporal_pairs])),
        })
        refined_rows.append({
            "global_tx_ord": int(tx_ord),
            "type_positions_json": compact_json({str(k): v for k, v in sorted(positions.items())}),
            "iface_pairs_json": compact_json(sorted([[a, b, c, d] for a, b, c, d in iface_pairs])),
            "role_presence_json": compact_json({k: sorted(vs) for k, vs in sorted(role_presence.items())}),
        })

    return pd.DataFrame(type_count_rows), pd.DataFrame(base_rows), pd.DataFrame(refined_rows), event_rows


def encode_event_timeline(event_timeline: pd.DataFrame, event_type_to_id: Dict[str, int], value_to_id: Dict[str, int]) -> pd.DataFrame:
    df = event_timeline.copy()
    df["event_type_id"] = df["event_type"].map(lambda x: int(event_type_to_id[norm_str(x)]))
    df["tx_hash_id"] = df["tx_hash"].map(lambda x: int(value_to_id[norm_str(x)]))
    df["protocol_id"] = df["protocol"].map(lambda x: int(value_to_id.get(norm_str(x), 0)))
    df["contract_id"] = df["contract"].map(lambda x: int(value_to_id.get(norm_str(x), 0)))
    return df[[
        "global_event_ord", "event_id", "global_tx_ord", "seq_index", "evt_index",
        "event_type_id", "tx_hash_id", "protocol_id", "contract_id"
    ]].copy()


def encode_tx_timeline(tx_timeline: pd.DataFrame, value_to_id: Dict[str, int]) -> pd.DataFrame:
    df = tx_timeline.copy()
    df["tx_hash_id"] = df["tx_hash"].map(lambda x: int(value_to_id[norm_str(x)]))
    return df[[
        "global_tx_ord", "tx_hash_id", "block_number", "block_time_unix", "tx_order_in_block",
        "event_count", "min_seq_index", "max_seq_index", "min_global_event_ord", "max_global_event_ord"
    ]].copy()


def build_semantic_anchor_occurrences(event_timeline_enc: pd.DataFrame) -> pd.DataFrame:
    return event_timeline_enc[["global_event_ord", "event_type_id", "global_tx_ord", "seq_index"]].copy()


def build_support_tables(event_rows: pd.DataFrame, value_to_id: Dict[str, int], event_type_to_id: Dict[str, int]) -> Dict[str, pd.DataFrame]:
    rows_by_type: Dict[str, List[dict]] = defaultdict(list)
    for r in event_rows.itertuples(index=False):
        et = norm_str(r.event_type)
        if et not in SUPPORTED_EVENT_TYPES:
            continue
        roles = parse_json_dict(r.roles_json)
        base = {
            "global_event_ord": int(r.global_event_ord),
            "global_tx_ord": int(r.global_tx_ord),
            "seq_index": int(r.seq_index),
            "protocol_id": int(value_to_id.get(norm_str(r.protocol), 0)),
        }
        if et == "flashloan":
            rows_by_type[et].append({
                **base,
                "recipient_id": int(value_to_id.get(norm_str(roles.get("recipient")), 0)),
                "asset_id": int(value_to_id.get(norm_str(roles.get("asset")), 0)),
                "pool_id": int(value_to_id.get(norm_str(roles.get("pool")), 0)),
            })
        elif et == "swap":
            rows_by_type[et].append({
                **base,
                "taker_id": int(value_to_id.get(norm_str(roles.get("taker")), 0)),
                "token_sold_id": int(value_to_id.get(norm_str(roles.get("token_sold")), 0)),
                "token_bought_id": int(value_to_id.get(norm_str(roles.get("token_bought")), 0)),
                "pool_id": int(value_to_id.get(norm_str(roles.get("pool")), 0)),
            })
        elif et == "borrow":
            rows_by_type[et].append({
                **base,
                "borrower_id": int(value_to_id.get(norm_str(roles.get("borrower")), 0)),
                "asset_id": int(value_to_id.get(norm_str(roles.get("asset")), 0)),
                "pool_id": int(value_to_id.get(norm_str(roles.get("pool")), 0)),
            })
        elif et == "repay":
            rows_by_type[et].append({
                **base,
                "repayer_id": int(value_to_id.get(norm_str(roles.get("repayer")), 0)),
                "asset_id": int(value_to_id.get(norm_str(roles.get("asset")), 0)),
                "pool_id": int(value_to_id.get(norm_str(roles.get("pool")), 0)),
            })
        elif et == "liquidation":
            rows_by_type[et].append({
                **base,
                "liquidator_id": int(value_to_id.get(norm_str(roles.get("liquidator")), 0)),
                "debt_asset_id": int(value_to_id.get(norm_str(roles.get("debt_asset") or roles.get("asset")), 0)),
                "collateral_asset_id": int(value_to_id.get(norm_str(roles.get("collateral_asset")), 0)),
                "pool_id": int(value_to_id.get(norm_str(roles.get("pool")), 0)),
            })

    table_specs = {
        "support_flashloan": (rows_by_type.get("flashloan", []), ["global_event_ord", "global_tx_ord", "seq_index", "protocol_id", "recipient_id", "asset_id", "pool_id"]),
        "support_swap": (rows_by_type.get("swap", []), ["global_event_ord", "global_tx_ord", "seq_index", "protocol_id", "taker_id", "token_sold_id", "token_bought_id", "pool_id"]),
        "support_borrow": (rows_by_type.get("borrow", []), ["global_event_ord", "global_tx_ord", "seq_index", "protocol_id", "borrower_id", "asset_id", "pool_id"]),
        "support_repay": (rows_by_type.get("repay", []), ["global_event_ord", "global_tx_ord", "seq_index", "protocol_id", "repayer_id", "asset_id", "pool_id"]),
        "support_liquidation": (rows_by_type.get("liquidation", []), ["global_event_ord", "global_tx_ord", "seq_index", "protocol_id", "liquidator_id", "debt_asset_id", "collateral_asset_id", "pool_id"]),
    }

    out = {}
    for name, (rows, cols) in table_specs.items():
        if rows:
            out[name] = pd.DataFrame(rows)[cols]
        else:
            out[name] = pd.DataFrame(columns=cols)
    return out


def maybe_write_csv(outdir: str, tables: Dict[str, pd.DataFrame], enabled: bool) -> None:
    if not enabled:
        return
    ensure_dir(outdir)
    for name, df in tables.items():
        df.to_csv(os.path.join(outdir, f"{name}.csv"), index=False, encoding="utf-8-sig")


def write_sqlite(path: str, tables: Dict[str, pd.DataFrame], meta: Dict[str, str]) -> None:
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=OFF")
        cur.execute("PRAGMA synchronous=OFF")
        cur.execute("PRAGMA temp_store=MEMORY")
        cur.execute("PRAGMA cache_size=-200000")
        cur.execute("PRAGMA page_size=32768")
        conn.commit()

        for name, df in tables.items():
            df.to_sql(name, conn, index=False, if_exists="replace")

        cur = conn.cursor()
        cur.execute("CREATE UNIQUE INDEX idx_event_type_dict_id ON event_type_dict(event_type_id)")
        cur.execute("CREATE UNIQUE INDEX idx_event_type_dict_val ON event_type_dict(event_type)")
        cur.execute("CREATE UNIQUE INDEX idx_value_dict_id ON value_dict(value_id)")
        cur.execute("CREATE UNIQUE INDEX idx_value_dict_val ON value_dict(value_text)")

        cur.execute("CREATE INDEX idx_event_timeline_ord ON episode_event_timeline(global_event_ord)")
        cur.execute("CREATE INDEX idx_event_timeline_tx ON episode_event_timeline(global_tx_ord, global_event_ord)")
        cur.execute("CREATE INDEX idx_event_timeline_type ON episode_event_timeline(event_type_id, global_tx_ord, seq_index)")
        cur.execute("CREATE INDEX idx_tx_timeline_ord ON episode_tx_timeline(global_tx_ord)")
        cur.execute("CREATE INDEX idx_window_endpoints ON episode_window_endpoints(window_sec, start_tx_ord)")
        cur.execute("CREATE INDEX idx_anchor_occ_type ON semantic_anchor_occurrences(event_type_id, global_tx_ord, global_event_ord)")
        cur.execute("CREATE INDEX idx_tx_type_counts ON episode_tx_type_counts(event_type_id, global_tx_ord)")
        cur.execute("CREATE INDEX idx_tx_base ON episode_tx_base_synopsis(global_tx_ord)")
        cur.execute("CREATE INDEX idx_tx_refined ON episode_tx_refined_synopsis(global_tx_ord)")

        if "support_flashloan" in tables:
            cur.execute("CREATE INDEX idx_support_flashloan_ra ON support_flashloan(recipient_id, asset_id, global_tx_ord, global_event_ord)")
            cur.execute("CREATE INDEX idx_support_flashloan_r ON support_flashloan(recipient_id, global_tx_ord, global_event_ord)")
            cur.execute("CREATE INDEX idx_support_flashloan_a ON support_flashloan(asset_id, global_tx_ord, global_event_ord)")
        if "support_swap" in tables:
            cur.execute("CREATE INDEX idx_support_swap_ts ON support_swap(taker_id, token_sold_id, global_tx_ord, global_event_ord)")
            cur.execute("CREATE INDEX idx_support_swap_tb ON support_swap(taker_id, token_bought_id, global_tx_ord, global_event_ord)")
            cur.execute("CREATE INDEX idx_support_swap_t ON support_swap(taker_id, global_tx_ord, global_event_ord)")
            cur.execute("CREATE INDEX idx_support_swap_s ON support_swap(token_sold_id, global_tx_ord, global_event_ord)")
            cur.execute("CREATE INDEX idx_support_swap_b ON support_swap(token_bought_id, global_tx_ord, global_event_ord)")
        if "support_borrow" in tables:
            cur.execute("CREATE INDEX idx_support_borrow_ba ON support_borrow(borrower_id, asset_id, global_tx_ord, global_event_ord)")
            cur.execute("CREATE INDEX idx_support_borrow_b ON support_borrow(borrower_id, global_tx_ord, global_event_ord)")
            cur.execute("CREATE INDEX idx_support_borrow_a ON support_borrow(asset_id, global_tx_ord, global_event_ord)")
        if "support_repay" in tables:
            cur.execute("CREATE INDEX idx_support_repay_ra ON support_repay(repayer_id, asset_id, global_tx_ord, global_event_ord)")
        if "support_liquidation" in tables:
            cur.execute("CREATE INDEX idx_support_liquidation_ld ON support_liquidation(liquidator_id, debt_asset_id, global_tx_ord, global_event_ord)")

        meta_df = pd.DataFrame([{"meta_key": k, "meta_value": str(v)} for k, v in sorted(meta.items())])
        meta_df.to_sql("episode_sidecar_meta", conn, index=False, if_exists="replace")
        cur.execute("CREATE UNIQUE INDEX idx_episode_meta_key ON episode_sidecar_meta(meta_key)")
        conn.commit()
        cur.execute("VACUUM")
        conn.commit()
    finally:
        conn.close()


def main():
    args = parse_args()
    ensure_dir(args.outdir)
    windows_sec = parse_windows_sec(args.windows_sec)

    t0 = time.time()
    event_atoms = load_event_atoms(args.event_atoms_csv)
    t1 = time.time()

    event_timeline_raw, tx_timeline_raw, tl_meta = build_timelines(event_atoms)
    t2 = time.time()

    event_type_dict, value_dict, event_type_to_id, value_to_id = encode_values(event_timeline_raw, event_atoms)
    event_timeline = encode_event_timeline(event_timeline_raw, event_type_to_id, value_to_id)
    tx_timeline = encode_tx_timeline(tx_timeline_raw, value_to_id)
    t3 = time.time()

    tx_type_counts, tx_base, tx_refined, event_rows = build_tx_summaries(event_atoms, event_timeline_raw, event_type_to_id, value_to_id)
    t4 = time.time()

    semantic_anchor_occ = build_semantic_anchor_occurrences(event_timeline)
    support_tables = build_support_tables(event_rows, value_to_id, event_type_to_id)
    t5 = time.time()

    window_endpoints = build_window_endpoints(tx_timeline, windows_sec)
    t6 = time.time()

    tables = {
        "event_type_dict": event_type_dict,
        "value_dict": value_dict,
        "episode_event_timeline": event_timeline,
        "episode_tx_timeline": tx_timeline,
        "episode_tx_type_counts": tx_type_counts,
        "episode_tx_base_synopsis": tx_base,
        "episode_tx_refined_synopsis": tx_refined,
        "semantic_anchor_occurrences": semantic_anchor_occ,
        "episode_window_endpoints": window_endpoints,
    }
    tables.update(support_tables)

    maybe_write_csv(args.outdir, tables, args.write_csv)
    t7 = time.time()

    meta = {
        "build_version": "anchored_episode_mainline_v2_encoded",
        "windows_sec": ",".join(str(x) for x in windows_sec),
        "order_quality": tl_meta["order_quality"],
        "tx_order_source": tl_meta["tx_order_source"],
        "size_strategy": "dict_encoded_support_tables_and_minimal_anchor_directory",
        "note_suffix_strategy": "anchor_tx_suffix_is_derived_online_from_single_tx_event_rows",
        "event_atoms_rows": str(len(event_atoms)),
        "tx_rows": str(len(tx_timeline)),
    }
    write_sqlite(args.sqlite_out, tables, meta)
    t8 = time.time()

    size_bytes = os.path.getsize(args.sqlite_out)
    metrics = {
        "storage_layout": "anchored_episode_mainline_v2_encoded",
        "stage_times_s": {
            "read_csv": round(t1 - t0, 6),
            "build_timelines": round(t2 - t1, 6),
            "encode_dictionaries": round(t3 - t2, 6),
            "build_tx_summaries": round(t4 - t3, 6),
            "build_anchor_and_support": round(t5 - t4, 6),
            "build_window_endpoints": round(t6 - t5, 6),
            "write_csv": round(t7 - t6, 6),
            "write_sqlite": round(t8 - t7, 6),
        },
        "row_counts": {k: int(len(v)) for k, v in tables.items()},
        "sqlite_size_bytes": int(size_bytes),
        "meta": meta,
    }

    with open(os.path.join(args.outdir, "build_metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    print("=== Anchored episode runtime sidecar build done ===")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
