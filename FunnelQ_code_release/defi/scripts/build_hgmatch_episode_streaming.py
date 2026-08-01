from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BASE_PATH = ROOT / "scripts/build_hgmatch_same_tx_streaming.py"
SPEC = importlib.util.spec_from_file_location("hgmatch_stream_base", BASE_PATH)
BASE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(BASE)


def timestamp_seconds(value) -> int:
    text = BASE.norm(value)
    if not text:
        return 0
    for fmt in ("%Y-%m-%d %H:%M:%S.%f UTC", "%Y-%m-%d %H:%M:%S UTC"):
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            pass
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def raw_events(kind: str, path: Path):
    with path.open("r", encoding="utf-8", newline="") as handle:
        for index, row in enumerate(csv.DictReader(handle)):
            tx = BASE.norm(row.get("tx_hash"))
            if not tx:
                continue
            log_index = int(float(BASE.norm(row.get("evt_index")) or 0))
            block_ts = timestamp_seconds(row.get("block_time"))
            block_number = int(float(BASE.norm(row.get("block_number")) or 0))
            tx_position = 0  # No tx_index/transaction_index column in this dataset.
            project, version = BASE.norm(row.get("project")), BASE.norm(row.get("version"))
            proto = f"{project}:{version}" if project and version else project or version
            if kind == "flashloan":
                typ = "flashloan"
                roles = {
                    "asset": BASE.norm(row.get("token_address")),
                    "borrower": "",
                    "recipient": BASE.norm(row.get("recipient")),
                }
                attrs = {
                    "amount": BASE.norm(row.get("amount")),
                    "amount_usd": BASE.norm(row.get("amount_usd")),
                    "asset_symbol": BASE.norm(row.get("symbol")),
                }
            elif kind == "borrow":
                typ = "borrow"
                roles = {
                    "asset": BASE.norm(row.get("token_address")),
                    "borrower": BASE.norm(row.get("borrower")),
                    "pool": BASE.norm(row.get("project_contract_address")),
                }
                attrs = {
                    "amount": BASE.norm(row.get("amount")),
                    "amount_usd": BASE.norm(row.get("amount_usd")),
                    "asset_symbol": BASE.norm(row.get("symbol")),
                }
            else:
                typ = "swap"
                roles = {
                    "pool": BASE.norm(row.get("project_contract_address")),
                    "taker": BASE.norm(row.get("taker")),
                    "token_bought": BASE.norm(row.get("token_bought_address")),
                    "token_sold": BASE.norm(row.get("token_sold_address")),
                }
                attrs = {
                    "amount_bought": BASE.norm(row.get("token_bought_amount")),
                    "amount_sold": BASE.norm(row.get("token_sold_amount")),
                    "amount_usd": BASE.norm(row.get("amount_usd")),
                    "token_bought_symbol": BASE.norm(row.get("token_bought_symbol")),
                    "token_sold_symbol": BASE.norm(row.get("token_sold_symbol")),
                }
            event_id = f"{tx}::{typ}::{log_index}::{index}"
            yield (
                event_id, tx, tx, typ, proto, log_index,
                block_ts, block_number, tx_position, log_index,
                json.dumps(roles, separators=(",", ":"), sort_keys=True, ensure_ascii=False),
                json.dumps(attrs, separators=(",", ":"), sort_keys=True, ensure_ascii=False),
            )


def stage(conn, args) -> int:
    conn.execute("""
        CREATE TABLE atom_stage(
            event_id TEXT PRIMARY KEY,closure_id TEXT,tx_hash TEXT,event_type TEXT,
            protocol TEXT,seq_index INTEGER,block_time_ts INTEGER,block_number INTEGER,
            tx_position INTEGER,log_index INTEGER,roles_json TEXT,attrs_json TEXT
        )
    """)
    sql = "INSERT INTO atom_stage VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
    total = 0
    for kind, arg_name in (("flashloan", "flashloans"), ("borrow", "borrow"), ("dex", "dex")):
        batch = []
        for row in raw_events(kind, getattr(args, arg_name)):
            batch.append(row)
            total += 1
            if len(batch) >= 20_000:
                conn.executemany(sql, batch)
                conn.commit()
                batch.clear()
                print(f"[episode normalize] {kind} total={total}", flush=True)
        if batch:
            conn.executemany(sql, batch)
            conn.commit()
    conn.execute("""
        CREATE INDEX idx_atom_global_order ON atom_stage(
            block_time_ts,block_number,tx_position,log_index,tx_hash,event_type,event_id
        )
    """)
    conn.execute("""
        CREATE INDEX idx_atom_closure_order ON atom_stage(
            closure_id,block_time_ts,block_number,tx_position,log_index,tx_hash,event_type,event_id
        )
    """)
    conn.commit()
    return total


def edge_for(row):
    (event_id, closure, tx, typ, proto, seq, block_ts, block_number,
     tx_position, log_index, roles_json, attrs_json) = row
    roles, attrs = json.loads(roles_json), json.loads(attrs_json)
    vertices = [{
        "vertex_id": f"etype::{typ}", "label": f"etype::{typ}",
        "source": "etype", "role_name": None,
    }]
    for role_name, role_label in sorted(BASE.ROLE_SCHEMA[typ].items()):
        value = BASE.norm(roles.get(role_name))
        if not value:
            continue
        slot = f"slot::{typ}::{role_name}"
        vertices.append({"vertex_id": slot, "label": slot, "source": "role_slot", "role_name": role_name})
        vertices.append({"vertex_id": f"{role_label}::{value}", "label": role_label, "source": f"role_value::{role_name}", "role_name": role_name})
    labels = [vertex["label"] for vertex in vertices]
    signature = BASE.SEP.join(sorted(labels))
    edge = {
        "edge_id": event_id, "closure_id": closure, "tx_hash": tx,
        "event_id": event_id, "event_type": typ, "protocol": proto,
        "seq_index": seq, "block_time_ts": block_ts, "block_number": block_number,
        "tx_position": tx_position, "log_index": log_index,
        "roles": roles, "attrs": attrs, "vertices": vertices,
        "signature_key": signature, "signature_labels": labels,
    }
    return signature, vertices, json.dumps(edge, separators=(",", ":"), ensure_ascii=False)


def build_events(conn) -> int:
    query = """
        SELECT event_id,closure_id,tx_hash,event_type,protocol,seq_index,
               block_time_ts,block_number,tx_position,log_index,roles_json,attrs_json
        FROM atom_stage
        ORDER BY block_time_ts,block_number,tx_position,log_index,tx_hash,event_type,event_id
    """
    eb, hb, pb, ib = [], [], [], []
    count = 0
    for row in conn.execute(query):
        count += 1
        sig, vertices, edge_json = edge_for(row)
        eb.append((row[0], row[1], row[2], row[3], row[4], row[5], row[10], row[11]))
        hb.append((row[0], row[1], row[2], sig, row[3], row[5], edge_json))
        pb.append((sig, row[0]))
        ib.extend((sig, vertex_id, row[0]) for vertex_id in sorted({v["vertex_id"] for v in vertices}))
        if len(eb) >= 10_000:
            conn.executemany("INSERT INTO event_atoms VALUES (?,?,?,?,?,?,?,?)", eb)
            conn.executemany("INSERT INTO hg_hyperedges VALUES (?,?,?,?,?,?,?)", hb)
            conn.executemany("INSERT INTO partition_stage VALUES (?,?)", pb)
            conn.executemany("INSERT INTO posting_stage VALUES (?,?,?)", ib)
            conn.commit()
            eb.clear(); hb.clear(); pb.clear(); ib.clear()
            if count % 100_000 == 0:
                print(f"[episode runtime] events={count}", flush=True)
    if eb:
        conn.executemany("INSERT INTO event_atoms VALUES (?,?,?,?,?,?,?,?)", eb)
        conn.executemany("INSERT INTO hg_hyperedges VALUES (?,?,?,?,?,?,?)", hb)
        conn.executemany("INSERT INTO partition_stage VALUES (?,?)", pb)
        conn.executemany("INSERT INTO posting_stage VALUES (?,?,?)", ib)
        conn.commit()
    return count


def build_closures(conn) -> int:
    query = """
        SELECT closure_id,event_id,event_type
        FROM atom_stage
        ORDER BY closure_id,block_time_ts,block_number,tx_position,log_index,tx_hash,event_type,event_id
    """
    current = None
    ids, types = [], []
    count = 0
    for closure, event_id, typ in conn.execute(query):
        if closure != current and current is not None:
            conn.execute("INSERT INTO evidence_closures VALUES (?,?,?,?,?)", (
                current, current, len(ids),
                json.dumps(ids, separators=(",", ":"), ensure_ascii=False),
                json.dumps(types, separators=(",", ":"), ensure_ascii=False),
            ))
            count += 1
            ids, types = [], []
        current = closure
        ids.append(event_id); types.append(typ)
    if current is not None:
        conn.execute("INSERT INTO evidence_closures VALUES (?,?,?,?,?)", (
            current, current, len(ids),
            json.dumps(ids, separators=(",", ":"), ensure_ascii=False),
            json.dumps(types, separators=(",", ":"), ensure_ascii=False),
        ))
        count += 1
    conn.commit()
    return count


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--flashloans", required=True, type=Path)
    parser.add_argument("--borrow", required=True, type=Path)
    parser.add_argument("--dex", required=True, type=Path)
    parser.add_argument("--outdir", required=True, type=Path)
    parser.add_argument("--sqlite-out", required=True, type=Path)
    args = parser.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    if args.sqlite_out.exists():
        raise RuntimeError(f"Refusing to overwrite {args.sqlite_out}")
    t0 = time.perf_counter()
    conn = sqlite3.connect(args.sqlite_out)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        atoms = stage(conn, args)
        t1 = time.perf_counter()
        BASE.create_final_schema(conn)
        events = build_events(conn)
        closures = build_closures(conn)
        t2 = time.perf_counter()
        conn.execute("CREATE INDEX idx_partition_stage ON partition_stage(signature_key,edge_id)")
        conn.execute("CREATE INDEX idx_posting_stage ON posting_stage(signature_key,vertex_id,edge_id)")
        conn.commit()
        partitions = BASE.grouped_lists(
            conn,
            "SELECT signature_key,edge_id FROM partition_stage ORDER BY signature_key,edge_id",
            "INSERT INTO hg_partitions VALUES (?,?,?,?)",
            labels=True,
        )
        postings = BASE.grouped_lists(
            conn,
            "SELECT signature_key,vertex_id,edge_id FROM posting_stage ORDER BY signature_key,vertex_id,edge_id",
            "INSERT INTO hg_inverted_index VALUES (?,?,?,?)",
        )
        conn.executescript("""
            DROP TABLE atom_stage; DROP TABLE partition_stage; DROP TABLE posting_stage;
            CREATE INDEX idx_event_atoms_closure ON event_atoms(closure_id,seq_index);
            CREATE INDEX idx_hg_hyperedges_closure ON hg_hyperedges(closure_id,seq_index);
            CREATE INDEX idx_hg_hyperedges_signature ON hg_hyperedges(signature_key);
        """)
        conn.commit()
        conn.execute("VACUUM")
        t3 = time.perf_counter()
    finally:
        conn.close()
    report = {
        "registry_version": "hgmatch_episode_rust_fair_v1",
        "scope": "global_event_store",
        "index_build_time_s": t3 - t0,
        "stage_times_s": {
            "read_csv_and_normalize_sources": t1 - t0,
            "build_hgmatch_runtime": t2 - t1,
            "build_disk_grouped_indexes": t3 - t2,
        },
        "row_counts": {
            "event_atoms": events, "evidence_closures": closures,
            "hg_hyperedges": events, "hg_partitions": partitions,
            "hg_inverted_index_rows": postings,
        },
        "index_storage_size_bytes": {"sqlite_db": args.sqlite_out.stat().st_size},
        "input_atoms": atoms,
        "build_mode": "streaming_disk_staged_semantics_preserving",
    }
    (args.outdir / "build_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
