#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import time
from collections import Counter
from pathlib import Path


ROLE_FIELDS = [
    "recipient", "borrower", "taker", "maker", "asset",
    "token_sold", "token_bought", "pool",
]


def norm(value) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text.lower() if text.startswith("0x") else text


def as_int(value, default=0) -> int:
    try:
        return int(float(norm(value)))
    except (TypeError, ValueError):
        return int(default)


def as_float(value, default=0.0) -> float:
    try:
        return float(norm(value))
    except (TypeError, ValueError):
        return float(default)


def json_object_from(row, column: str) -> dict[str, str]:
    try:
        obj = json.loads(row.get(column, "") or "{}")
    except json.JSONDecodeError:
        obj = {}
    if not isinstance(obj, dict):
        return {}
    return {norm(k): norm(v) for k, v in obj.items() if norm(k) and norm(v)}


def roles_from(row) -> dict[str, str]:
    return json_object_from(row, "roles_json")


def attrs_from(row) -> dict[str, str]:
    return json_object_from(row, "attrs_json")


def numeric_attr(row, attrs: dict[str, str], field: str) -> float:
    return as_float(attrs.get(field) or row.get(field), 0.0)


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE event_type_dict(event_type_id INTEGER PRIMARY KEY, event_type TEXT NOT NULL UNIQUE);
        CREATE TABLE value_dict(value_id INTEGER PRIMARY KEY, value_text TEXT NOT NULL UNIQUE);
        CREATE TABLE tx_dict(
            tx_ord INTEGER PRIMARY KEY,
            tx_hash TEXT NOT NULL UNIQUE,
            block_number INTEGER,
            block_time TEXT,
            event_count INTEGER NOT NULL
        );
        CREATE TABLE same_tx_event_encoded(
            event_ord INTEGER PRIMARY KEY,
            event_id TEXT NOT NULL,
            tx_ord INTEGER NOT NULL,
            seq_index INTEGER NOT NULL,
            evt_index INTEGER NOT NULL,
            event_type_id INTEGER NOT NULL,
            protocol_id INTEGER NOT NULL,
            recipient_id INTEGER NOT NULL,
            borrower_id INTEGER NOT NULL,
            taker_id INTEGER NOT NULL,
            maker_id INTEGER NOT NULL,
            asset_id INTEGER NOT NULL,
            token_sold_id INTEGER NOT NULL,
            token_bought_id INTEGER NOT NULL,
            pool_id INTEGER NOT NULL,
            amount_num REAL NOT NULL,
            amount_in_num REAL NOT NULL,
            amount_out_num REAL NOT NULL
        );
        CREATE TABLE value_stage(value_text TEXT PRIMARY KEY) WITHOUT ROWID;
    """)


def first_pass(conn: sqlite3.Connection, atoms: Path) -> tuple[Counter, int, int]:
    event_counts = Counter()
    tx_ord = 0
    current_tx = None
    current_meta = None
    values: set[str] = set()
    rows = 0

    def flush_values():
        nonlocal values
        if values:
            conn.executemany(
                "INSERT OR IGNORE INTO value_stage(value_text) VALUES (?)",
                ((value,) for value in values),
            )
            values = set()

    def flush_tx():
        if current_meta is not None:
            conn.execute(
                "INSERT INTO tx_dict(tx_ord,tx_hash,block_number,block_time,event_count) VALUES (?,?,?,?,?)",
                current_meta,
            )

    with atoms.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows += 1
            tx_hash = norm(row.get("tx_hash") or row.get("closure_id")) or f"missing_tx_{rows}"
            if tx_hash != current_tx:
                flush_tx()
                tx_ord += 1
                current_tx = tx_hash
                current_meta = [
                    tx_ord, tx_hash, as_int(row.get("block_number")), norm(row.get("block_time")), 0
                ]
            current_meta[4] += 1
            event_type = norm(row.get("event_type"))
            event_counts[event_type] += 1
            protocol = norm(row.get("protocol"))
            if protocol:
                values.add(protocol)
            roles = roles_from(row)
            values.update(v for field in ROLE_FIELDS if (v := norm(roles.get(field))))
            if len(values) >= 100_000:
                flush_values()
                conn.commit()
            if rows % 100_000 == 0:
                print(f"[same-tx pass1] rows={rows} tx={tx_ord}", flush=True)
    flush_tx()
    flush_values()
    conn.commit()
    return event_counts, rows, tx_ord


def build_dictionaries(conn: sqlite3.Connection, event_counts: Counter) -> tuple[dict, dict]:
    event_types = sorted(key for key in event_counts if key)
    conn.executemany(
        "INSERT INTO event_type_dict(event_type_id,event_type) VALUES (?,?)",
        ((index, value) for index, value in enumerate(event_types, 1)),
    )
    conn.execute("""
        INSERT INTO value_dict(value_id,value_text)
        SELECT ROW_NUMBER() OVER (ORDER BY value_text), value_text
        FROM value_stage ORDER BY value_text
    """)
    conn.commit()
    event_ids = {value: index for index, value in enumerate(event_types, 1)}
    value_ids = {
        value: int(index)
        for index, value in conn.execute("SELECT value_id,value_text FROM value_dict")
    }
    return event_ids, value_ids


def second_pass(conn, atoms: Path, event_ids: dict, value_ids: dict) -> int:
    columns = [
        "event_ord", "event_id", "tx_ord", "seq_index", "evt_index",
        "event_type_id", "protocol_id",
        *[f"{field}_id" for field in ROLE_FIELDS],
        "amount_num", "amount_in_num", "amount_out_num",
    ]
    sql = (
        f"INSERT INTO same_tx_event_encoded({','.join(columns)}) "
        f"VALUES ({','.join('?' for _ in columns)})"
    )
    batch = []
    event_ord = 0
    tx_ord = 0
    current_tx = None
    with atoms.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            event_ord += 1
            tx_hash = norm(row.get("tx_hash") or row.get("closure_id")) or f"missing_tx_{event_ord}"
            if tx_hash != current_tx:
                current_tx = tx_hash
                tx_ord += 1
            roles = roles_from(row)
            attrs = attrs_from(row)
            batch.append((
                event_ord,
                norm(row.get("event_id")) or f"event_{event_ord}",
                tx_ord,
                as_int(row.get("seq_index"), event_ord),
                as_int(row.get("evt_index"), event_ord),
                event_ids.get(norm(row.get("event_type")), 0),
                value_ids.get(norm(row.get("protocol")), 0),
                *[value_ids.get(norm(roles.get(field)), 0) for field in ROLE_FIELDS],
                numeric_attr(row, attrs, "amount"),
                numeric_attr(row, attrs, "amount_in"),
                numeric_attr(row, attrs, "amount_out"),
            ))
            if len(batch) >= 20_000:
                conn.executemany(sql, batch)
                conn.commit()
                batch.clear()
                if event_ord % 100_000 == 0:
                    print(f"[same-tx pass2] rows={event_ord}", flush=True)
    if batch:
        conn.executemany(sql, batch)
        conn.commit()
    return event_ord


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--builder-script", default="")
    parser.add_argument("--event-atoms-csv", required=True, type=Path)
    parser.add_argument("--outdir", required=True, type=Path)
    parser.add_argument("--sqlite-out", required=True, type=Path)
    parser.add_argument("--write-debug-csv", action="store_true")
    args = parser.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    args.sqlite_out.parent.mkdir(parents=True, exist_ok=True)
    if args.sqlite_out.exists():
        raise RuntimeError(f"Refusing to overwrite existing database: {args.sqlite_out}")

    t0 = time.perf_counter()
    conn = sqlite3.connect(args.sqlite_out)
    try:
        create_schema(conn)
        event_counts, event_rows, tx_rows = first_pass(conn, args.event_atoms_csv)
        t1 = time.perf_counter()
        event_ids, value_ids = build_dictionaries(conn, event_counts)
        t2 = time.perf_counter()
        encoded_rows = second_pass(conn, args.event_atoms_csv, event_ids, value_ids)
        t3 = time.perf_counter()
        meta = {
            "storage_layout": "encoded_same_tx_integer_sidecar",
            "build_mode": "from_event_atoms_streaming",
            "event_rows": encoded_rows,
            "tx_rows": tx_rows,
            "event_type_count": len(event_ids),
            "value_count": len(value_ids),
            "event_type_counts": dict(sorted(event_counts.items())),
        }
        conn.executemany(
            "INSERT INTO metadata(key,value) VALUES (?,?)",
            (
                (str(key), json.dumps(value) if isinstance(value, (dict, list)) else str(value))
                for key, value in meta.items()
            ),
        )
        conn.executescript("""
            DROP TABLE value_stage;
            CREATE INDEX idx_same_tx_event_tx_type_seq ON same_tx_event_encoded(tx_ord,event_type_id,seq_index,event_ord);
            CREATE INDEX idx_same_tx_event_type_tx_seq ON same_tx_event_encoded(event_type_id,tx_ord,seq_index,event_ord);
            CREATE INDEX idx_same_tx_event_flashloan_key ON same_tx_event_encoded(event_type_id,recipient_id,asset_id,tx_ord);
            CREATE INDEX idx_same_tx_event_swap_key ON same_tx_event_encoded(event_type_id,taker_id,token_sold_id,token_bought_id,pool_id,tx_ord);
            CREATE INDEX idx_same_tx_event_borrow_key ON same_tx_event_encoded(event_type_id,borrower_id,asset_id,pool_id,tx_ord);
            ANALYZE;
        """)
        conn.commit()
        t4 = time.perf_counter()
    finally:
        conn.close()
    metrics = {
        **meta,
        "index_build_time_s": round(t4 - t0, 6),
        "stage_times_s": {
            "scan_and_stage_dictionaries": round(t1 - t0, 6),
            "build_dictionaries": round(t2 - t1, 6),
            "encode_and_write_rows": round(t3 - t2, 6),
            "build_indexes": round(t4 - t3, 6),
        },
        "sqlite_size_bytes": os.path.getsize(args.sqlite_out),
        "debug_csv_written": False,
    }
    (args.outdir / "build_metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
