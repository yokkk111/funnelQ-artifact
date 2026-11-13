#!/usr/bin/env python3


from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
from typing import Iterable, Optional

import pandas as pd


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def norm_str(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def norm_hex_like(x) -> str:
    return norm_str(x).lower()


def pick_existing_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def pick_series(
    df: pd.DataFrame,
    candidates: Iterable[str],
    default: str = "",
    normalize_hex: bool = False,
) -> pd.Series:
    col = pick_existing_col(df, candidates)
    if col is None:
        s = pd.Series([default] * len(df), index=df.index, dtype="object")
    else:
        s = df[col].astype("object").map(norm_str)
    if normalize_hex:
        s = s.map(norm_hex_like)
    return s


def first_non_empty(*series_list: pd.Series) -> pd.Series:
    out = series_list[0].astype("object").fillna("").map(norm_str)
    for s in series_list[1:]:
        s2 = s.astype("object").fillna("").map(norm_str)
        mask = out.eq("")
        out.loc[mask] = s2.loc[mask]
    return out


def to_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def stable_hash(text: str, n: int = 20) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:n]


def normalize_flashloans(df: pd.DataFrame) -> pd.DataFrame:
    raw_row_id = pd.Series(df.index, index=df.index)
    tx_hash = pick_series(df, ["tx_hash", "evt_tx_hash", "transaction_hash", "hash"], normalize_hex=True)
    block_number = pick_series(df, ["block_number", "evt_block_number", "block_num"])
    block_time = pick_series(df, ["block_time", "evt_block_time", "block_timestamp"])
    evt_index = pick_series(df, ["evt_index", "log_index", "event_index", "index"])
    protocol = first_non_empty(
        pick_series(df, ["project"]),
        pick_series(df, ["protocol"]),
        pick_series(df, ["project_name"]),
        pick_series(df, ["platform"]),
    )
    version = first_non_empty(
        pick_series(df, ["version"]),
        pick_series(df, ["protocol_version"]),
    )
    contract = first_non_empty(
        pick_series(df, ["project_contract_address"], normalize_hex=True),
        pick_series(df, ["contract_address"], normalize_hex=True),
        pick_series(df, ["pool_address"], normalize_hex=True),
        pick_series(df, ["pool"], normalize_hex=True),
    )
    recipient = first_non_empty(
        pick_series(df, ["recipient"], normalize_hex=True),
        pick_series(df, ["receiver"], normalize_hex=True),
        pick_series(df, ["borrower"], normalize_hex=True),
        pick_series(df, ["user"], normalize_hex=True),
        pick_series(df, ["on_behalf_of"], normalize_hex=True),
    )
    asset = first_non_empty(
        pick_series(df, ["token_address"], normalize_hex=True),
        pick_series(df, ["asset_address"], normalize_hex=True),
        pick_series(df, ["reserve_address"], normalize_hex=True),
        pick_series(df, ["underlying_address"], normalize_hex=True),
    )
    pool = first_non_empty(
        pick_series(df, ["pool"], normalize_hex=True),
        pick_series(df, ["pool_address"], normalize_hex=True),
        contract,
    )
    amount = first_non_empty(
        pick_series(df, ["amount"]),
        pick_series(df, ["amount_raw"]),
        pick_series(df, ["amount_original"]),
        pick_series(df, ["amount_usd"]),
    )
    return pd.DataFrame(
        {
            "event_id": [
                "e_" + stable_hash(f"flashloan|{tx}|{idx}|{rid}")
                for tx, idx, rid in zip(tx_hash, evt_index, raw_row_id)
            ],
            "tx_hash": tx_hash,
            "block_number": to_numeric_series(block_number),
            "block_time": block_time,
            "evt_index": to_numeric_series(evt_index),
            "seq_index": pd.Series([pd.NA] * len(df), index=df.index, dtype="object"),
            "event_type": "flashloan",
            "protocol": protocol,
            "version": version,
            "contract": contract,
            "recipient": recipient,
            "borrower": "",
            "taker": "",
            "maker": "",
            "asset": asset,
            "token_sold": "",
            "token_bought": "",
            "pool": pool,
            "amount": amount,
            "amount_in": "",
            "amount_out": "",
            "raw_source": "flashloans",
            "raw_row_id": raw_row_id,
        }
    )


def normalize_borrow(df: pd.DataFrame) -> pd.DataFrame:
    raw_row_id = pd.Series(df.index, index=df.index)
    tx_hash = pick_series(df, ["tx_hash", "evt_tx_hash", "transaction_hash", "hash"], normalize_hex=True)
    block_number = pick_series(df, ["block_number", "evt_block_number", "block_num"])
    block_time = pick_series(df, ["block_time", "evt_block_time", "block_timestamp"])
    evt_index = pick_series(df, ["evt_index", "log_index", "event_index", "index"])
    protocol = first_non_empty(
        pick_series(df, ["project"]),
        pick_series(df, ["protocol"]),
        pick_series(df, ["project_name"]),
        pick_series(df, ["platform"]),
    )
    version = first_non_empty(
        pick_series(df, ["version"]),
        pick_series(df, ["protocol_version"]),
    )
    contract = first_non_empty(
        pick_series(df, ["project_contract_address"], normalize_hex=True),
        pick_series(df, ["contract_address"], normalize_hex=True),
        pick_series(df, ["pool_address"], normalize_hex=True),
        pick_series(df, ["pool"], normalize_hex=True),
    )
    borrower = first_non_empty(
        pick_series(df, ["borrower"], normalize_hex=True),
        pick_series(df, ["user"], normalize_hex=True),
        pick_series(df, ["on_behalf_of"], normalize_hex=True),
    )
    asset = first_non_empty(
        pick_series(df, ["reserve_address"], normalize_hex=True),
        pick_series(df, ["token_address"], normalize_hex=True),
        pick_series(df, ["asset_address"], normalize_hex=True),
        pick_series(df, ["underlying_address"], normalize_hex=True),
    )
    pool = first_non_empty(
        pick_series(df, ["pool"], normalize_hex=True),
        pick_series(df, ["pool_address"], normalize_hex=True),
        contract,
    )
    amount = first_non_empty(
        pick_series(df, ["amount"]),
        pick_series(df, ["amount_raw"]),
        pick_series(df, ["amount_original"]),
        pick_series(df, ["amount_usd"]),
    )
    return pd.DataFrame(
        {
            "event_id": [
                "e_" + stable_hash(f"borrow|{tx}|{idx}|{rid}")
                for tx, idx, rid in zip(tx_hash, evt_index, raw_row_id)
            ],
            "tx_hash": tx_hash,
            "block_number": to_numeric_series(block_number),
            "block_time": block_time,
            "evt_index": to_numeric_series(evt_index),
            "seq_index": pd.Series([pd.NA] * len(df), index=df.index, dtype="object"),
            "event_type": "borrow",
            "protocol": protocol,
            "version": version,
            "contract": contract,
            "recipient": "",
            "borrower": borrower,
            "taker": "",
            "maker": "",
            "asset": asset,
            "token_sold": "",
            "token_bought": "",
            "pool": pool,
            "amount": amount,
            "amount_in": "",
            "amount_out": "",
            "raw_source": "borrow",
            "raw_row_id": raw_row_id,
        }
    )


def normalize_dex(df: pd.DataFrame) -> pd.DataFrame:
    raw_row_id = pd.Series(df.index, index=df.index)
    tx_hash = pick_series(df, ["tx_hash", "evt_tx_hash", "transaction_hash", "hash"], normalize_hex=True)
    block_number = pick_series(df, ["block_number", "evt_block_number", "block_num"])
    block_time = pick_series(df, ["block_time", "evt_block_time", "block_timestamp"])
    evt_index = pick_series(df, ["evt_index", "log_index", "event_index", "index"])
    protocol = first_non_empty(
        pick_series(df, ["project"]),
        pick_series(df, ["protocol"]),
        pick_series(df, ["project_name"]),
        pick_series(df, ["platform"]),
    )
    version = first_non_empty(
        pick_series(df, ["version"]),
        pick_series(df, ["protocol_version"]),
    )
    contract = first_non_empty(
        pick_series(df, ["project_contract_address"], normalize_hex=True),
        pick_series(df, ["contract_address"], normalize_hex=True),
        pick_series(df, ["pool_address"], normalize_hex=True),
        pick_series(df, ["pool"], normalize_hex=True),
    )
    taker = first_non_empty(
        pick_series(df, ["taker"], normalize_hex=True),
        pick_series(df, ["trader"], normalize_hex=True),
        pick_series(df, ["tx_from"], normalize_hex=True),
        pick_series(df, ["from"], normalize_hex=True),
    )
    maker = first_non_empty(
        pick_series(df, ["maker"], normalize_hex=True),
        pick_series(df, ["tx_to"], normalize_hex=True),
        pick_series(df, ["to"], normalize_hex=True),
    )
    token_sold = first_non_empty(
        pick_series(df, ["token_sold_address"], normalize_hex=True),
        pick_series(df, ["token_in"], normalize_hex=True),
        pick_series(df, ["sold_token_address"], normalize_hex=True),
    )
    token_bought = first_non_empty(
        pick_series(df, ["token_bought_address"], normalize_hex=True),
        pick_series(df, ["token_out"], normalize_hex=True),
        pick_series(df, ["bought_token_address"], normalize_hex=True),
    )
    pool = first_non_empty(
        pick_series(df, ["pool_address"], normalize_hex=True),
        pick_series(df, ["pool"], normalize_hex=True),
        contract,
    )
    amount_in = first_non_empty(
        pick_series(df, ["token_sold_amount"]),
        pick_series(df, ["amount_in"]),
        pick_series(df, ["sold_amount"]),
    )
    amount_out = first_non_empty(
        pick_series(df, ["token_bought_amount"]),
        pick_series(df, ["amount_out"]),
        pick_series(df, ["bought_amount"]),
    )
    return pd.DataFrame(
        {
            "event_id": [
                "e_" + stable_hash(f"swap|{tx}|{idx}|{rid}")
                for tx, idx, rid in zip(tx_hash, evt_index, raw_row_id)
            ],
            "tx_hash": tx_hash,
            "block_number": to_numeric_series(block_number),
            "block_time": block_time,
            "evt_index": to_numeric_series(evt_index),
            "seq_index": pd.Series([pd.NA] * len(df), index=df.index, dtype="object"),
            "event_type": "swap",
            "protocol": protocol,
            "version": version,
            "contract": contract,
            "recipient": "",
            "borrower": "",
            "taker": taker,
            "maker": maker,
            "asset": "",
            "token_sold": token_sold,
            "token_bought": token_bought,
            "pool": pool,
            "amount": "",
            "amount_in": amount_in,
            "amount_out": amount_out,
            "raw_source": "dex",
            "raw_row_id": raw_row_id,
        }
    )


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--flashloans", required=True)
    ap.add_argument("--borrow", required=True)
    ap.add_argument("--dex", required=True)
    ap.add_argument("--sqlite-out", required=True)
    ap.add_argument("--report-json", required=True)
    return ap.parse_args()


def main():
    args = parse_args()
    ensure_dir(os.path.dirname(args.sqlite_out) or ".")
    ensure_dir(os.path.dirname(args.report_json) or ".")

    t0 = time.perf_counter()
    df_flash = pd.read_csv(args.flashloans, low_memory=False, dtype=str)
    df_borrow = pd.read_csv(args.borrow, low_memory=False, dtype=str)
    df_dex = pd.read_csv(args.dex, low_memory=False, dtype=str)
    t1 = time.perf_counter()

    flash = normalize_flashloans(df_flash)
    borrow = normalize_borrow(df_borrow)
    dex = normalize_dex(df_dex)

    event_atoms_flat = pd.concat([flash, borrow, dex], ignore_index=True)
    event_atoms_flat = event_atoms_flat[event_atoms_flat["tx_hash"].map(norm_str) != ""].copy()

    event_atoms_flat["_evt_sort"] = pd.to_numeric(event_atoms_flat["evt_index"], errors="coerce").fillna(10**12)
    event_atoms_flat = event_atoms_flat.sort_values(
        by=["tx_hash", "block_number", "_evt_sort", "raw_source", "raw_row_id"],
        kind="mergesort",
    ).reset_index(drop=True)
    event_atoms_flat["seq_index"] = event_atoms_flat.groupby("tx_hash").cumcount().astype("int64")
    event_atoms_flat = event_atoms_flat.drop(columns=["_evt_sort"])
    t2 = time.perf_counter()

    conn = sqlite3.connect(args.sqlite_out)
    try:
        event_atoms_flat.to_sql("event_atoms_flat", conn, if_exists="replace", index=False)
        cur = conn.cursor()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_type_tx_seq ON event_atoms_flat(event_type, tx_hash, seq_index)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_tx_seq ON event_atoms_flat(tx_hash, seq_index)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_recipient ON event_atoms_flat(recipient)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_borrower ON event_atoms_flat(borrower)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_taker ON event_atoms_flat(taker)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_asset ON event_atoms_flat(asset)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_token_sold ON event_atoms_flat(token_sold)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_token_bought ON event_atoms_flat(token_bought)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_pool ON event_atoms_flat(pool)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_evt_protocol ON event_atoms_flat(protocol)")
        conn.commit()
    finally:
        conn.close()
    t3 = time.perf_counter()

    sqlite_size = os.path.getsize(args.sqlite_out) if os.path.exists(args.sqlite_out) else 0
    report = {
        "baseline_name": "relational_same_tx_flat_store_with_generic_indexes",
        "scope": "same_tx",
        "build_time_s": round(t3 - t0, 6),
        "stage_times_s": {
            "read_csv": round(t1 - t0, 6),
            "normalize_flat_events": round(t2 - t1, 6),
            "write_sqlite_and_generic_indexes": round(t3 - t2, 6),
        },
        "row_counts": {
            "event_atoms_flat": int(len(event_atoms_flat)),
        },
        "storage_size_bytes": {
            "sqlite_db": int(sqlite_size),
        },
    }

    with open(args.report_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("=== Relational Same-Tx Flat Store Build Done ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
