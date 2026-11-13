#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path

import pandas as pd

ZERO = "0x0000000000000000000000000000000000000000"
USECOLS = {
    "event_id", "event_type", "tx_hash", "nft", "seller", "buyer",
    "from_address", "to_address",
}


def clean(chunk: pd.DataFrame, name: str, *, lower: bool = False) -> pd.Series:
    out = chunk[name].astype("string").fillna("").str.strip()
    out = out.mask(out.str.lower().isin(["nan", "none", "null", "nat", "<nil>"]), "")
    return out.str.lower() if lower else out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-atoms-csv", required=True, type=Path)
    ap.add_argument("--sqlite-out", required=True, type=Path)
    ap.add_argument("--report-json", default="", type=Path)
    ap.add_argument("--chunk-rows", type=int, default=100000)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    out = args.sqlite_out.resolve()
    if out.exists():
        if not args.force:
            raise FileExistsError(out)
        out.unlink()
    out.parent.mkdir(parents=True, exist_ok=True)
    report_path = args.report_json if str(args.report_json) not in {"", "."} else out.with_suffix(".report.json")
    t0 = time.perf_counter()
    source_rows = valid_rows = 0
    with sqlite3.connect(out) as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=FILE;
            CREATE TABLE nft_tx_hash_dict(
                tx_id INTEGER PRIMARY KEY,
                tx_hash TEXT NOT NULL UNIQUE
            );
            CREATE TABLE nft_event_tx_ids(
                order_seq INTEGER PRIMARY KEY,
                tx_id INTEGER NOT NULL
            );
            """
        )
        reader = pd.read_csv(
            args.event_atoms_csv,
            usecols=lambda c: c in USECOLS,
            dtype="string",
            chunksize=max(1000, args.chunk_rows),
            low_memory=False,
        )
        for chunk in reader:
            missing = sorted(USECOLS - set(chunk.columns))
            if missing:
                raise ValueError(f"missing canonical columns: {missing}")
            start = source_rows
            source_rows += len(chunk)
            event_id = clean(chunk, "event_id")
            event_type = clean(chunk, "event_type")
            tx_hash = clean(chunk, "tx_hash")
            nft = clean(chunk, "nft", lower=True)
            collection = nft.str.split(":", n=1).str[0].fillna("")
            seller = clean(chunk, "seller", lower=True)
            buyer = clean(chunk, "buyer", lower=True)
            from_addr = clean(chunk, "from_address", lower=True)
            to_addr = clean(chunk, "to_address", lower=True)
            src = pd.Series("", index=chunk.index, dtype="string")
            dst = pd.Series("", index=chunk.index, dtype="string")
            trade = event_type.eq("NFTTrade")
            transfer = event_type.eq("NFTTransfer")
            src.loc[trade] = seller.loc[trade]
            dst.loc[trade] = buyer.loc[trade]
            src.loc[transfer] = from_addr.loc[transfer]
            dst.loc[transfer] = to_addr.loc[transfer]
            valid = (
                event_id.ne("")
                & event_type.isin(["NFTTrade", "NFTTransfer"])
                & tx_hash.ne("")
                & nft.ne("")
                & collection.ne("")
                & src.ne("")
                & dst.ne("")
                & src.ne(dst)
                & src.str.lower().ne(ZERO)
                & dst.str.lower().ne(ZERO)
            )
            selected_hashes = tx_hash[valid]
            unique_hashes = [str(x) for x in pd.unique(selected_hashes)]
            conn.executemany(
                "INSERT OR IGNORE INTO nft_tx_hash_dict(tx_hash) VALUES (?)",
                ((value,) for value in unique_hashes),
            )
            mapping: dict[str, int] = {}
            for i in range(0, len(unique_hashes), 800):
                part = unique_hashes[i:i + 800]
                marks = ",".join("?" for _ in part)
                for tx_id, value in conn.execute(
                    f"SELECT tx_id,tx_hash FROM nft_tx_hash_dict WHERE tx_hash IN ({marks})",
                    part,
                ):
                    mapping[str(value)] = int(tx_id)
            orders = pd.Series(range(start, source_rows), index=chunk.index, dtype="int64")[valid]
            tx_ids = selected_hashes.map(mapping)
            if tx_ids.isna().any():
                raise RuntimeError("transaction dictionary projection failed")
            conn.executemany(
                "INSERT INTO nft_event_tx_ids(order_seq,tx_id) VALUES (?,?)",
                zip(orders.astype("int64"), tx_ids.astype("int64")),
            )
            valid_rows += int(valid.sum())
            conn.commit()
            print(f"[tx-sidecar] source={source_rows:,} valid={valid_rows:,}", flush=True)
        conn.execute("ANALYZE")
        conn.commit()
        tx_count = int(conn.execute("SELECT COUNT(*) FROM nft_tx_hash_dict").fetchone()[0])
        edge_count = int(conn.execute("SELECT COUNT(*) FROM nft_event_tx_ids").fetchone()[0])
    report = {
        "format": "NFT-transaction-id-sidecar-v1",
        "event_atoms_csv": str(args.event_atoms_csv.resolve()),
        "sqlite_out": str(out),
        "source_rows": source_rows,
        "valid_event_rows": edge_count,
        "distinct_transactions": tx_count,
        "sqlite_size_bytes": out.stat().st_size,
        "build_s": time.perf_counter() - t0,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
