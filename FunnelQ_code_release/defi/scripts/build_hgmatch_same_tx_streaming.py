from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import time
from pathlib import Path


SEP = "\x1f"
ROLE_SCHEMA = {
    "flashloan": {"asset": "token", "borrower": "addr", "recipient": "addr"},
    "borrow": {"asset": "token", "borrower": "addr", "pool": "pool"},
    "swap": {"pool": "pool", "taker": "addr", "token_bought": "token", "token_sold": "token"},
}
REGISTRY = [
    ("rule_flashloan_v1", "flashloan", "lending.flashloans", ROLE_SCHEMA["flashloan"],
     ["event_type", "protocol", "asset", "borrower", "recipient"]),
    ("rule_borrow_v1", "borrow", "lending.borrow", ROLE_SCHEMA["borrow"],
     ["event_type", "protocol", "asset", "borrower"]),
    ("rule_swap_v1", "swap", "dex.trades", ROLE_SCHEMA["swap"],
     ["event_type", "protocol", "taker", "token_sold", "token_bought", "pool"]),
]


def norm(value) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def protocol(row) -> str:
    project, version = norm(row.get("project")), norm(row.get("version"))
    return f"{project}:{version}" if project and version else project or version


def event_rows(kind: str, path: Path):
    with path.open("r", encoding="utf-8", newline="") as handle:
        for index, row in enumerate(csv.DictReader(handle)):
            tx = norm(row.get("tx_hash"))
            if not tx:
                continue
            seq = int(float(norm(row.get("evt_index")) or 0))
            if kind == "flashloan":
                event_type = "flashloan"
                roles = {
                    "asset": norm(row.get("token_address")),
                    "borrower": norm(row.get("borrower")),
                    "recipient": norm(row.get("recipient")),
                }
                attrs = {
                    "amount": norm(row.get("amount")),
                    "amount_usd": norm(row.get("amount_usd")),
                    "asset_symbol": norm(row.get("symbol")),
                }
            elif kind == "borrow":
                event_type = "borrow"
                roles = {
                    "asset": norm(row.get("token_address")),
                    "borrower": norm(row.get("borrower")) or norm(row.get("on_behalf_of")),
                    "pool": norm(row.get("project_contract_address")),
                }
                attrs = {
                    "amount": norm(row.get("amount")),
                    "amount_usd": norm(row.get("amount_usd")),
                    "asset_symbol": norm(row.get("symbol")),
                }
            else:
                event_type = "swap"
                roles = {
                    "pool": norm(row.get("project_contract_address")),
                    "taker": norm(row.get("taker")) or norm(row.get("tx_from")),
                    "token_bought": norm(row.get("token_bought_address")),
                    "token_sold": norm(row.get("token_sold_address")),
                }
                attrs = {
                    "amount_bought": norm(row.get("token_bought_amount")),
                    "amount_sold": norm(row.get("token_sold_amount")),
                    "amount_usd": norm(row.get("amount_usd")),
                    "token_bought_symbol": norm(row.get("token_bought_symbol")),
                    "token_sold_symbol": norm(row.get("token_sold_symbol")),
                }
            event_id = f"{tx}::{event_type}::{seq}::{index}"
            yield (
                event_id, tx, tx, event_type, protocol(row), seq,
                json.dumps(roles, separators=(",", ":"), sort_keys=True, ensure_ascii=False),
                json.dumps(attrs, separators=(",", ":"), sort_keys=True, ensure_ascii=False),
            )


def stage_atoms(conn, args) -> int:
    conn.execute("""
        CREATE TABLE atom_stage(
            event_id TEXT PRIMARY KEY, closure_id TEXT, tx_hash TEXT,
            event_type TEXT, protocol TEXT, seq_index INTEGER,
            roles_json TEXT, attrs_json TEXT
        )
    """)
    sql = "INSERT INTO atom_stage VALUES (?,?,?,?,?,?,?,?)"
    total = 0
    for kind, arg_name in (("flashloan", "flashloans"), ("borrow", "borrow"), ("dex", "dex")):
        batch = []
        for item in event_rows(kind, getattr(args, arg_name)):
            batch.append(item)
            total += 1
            if len(batch) >= 20_000:
                conn.executemany(sql, batch)
                conn.commit()
                batch.clear()
                print(f"[hgmatch normalize] {kind} total={total}", flush=True)
        if batch:
            conn.executemany(sql, batch)
            conn.commit()
    conn.execute(
        "CREATE INDEX idx_atom_stage_order ON atom_stage(closure_id,seq_index,event_type,event_id)"
    )
    conn.commit()
    return total


def create_final_schema(conn):
    conn.executescript("""
        CREATE TABLE registry_snapshot(rule_id TEXT PRIMARY KEY,event_type TEXT NOT NULL,source_table TEXT NOT NULL,role_schema_json TEXT NOT NULL,anchorable_fields_json TEXT NOT NULL);
        CREATE TABLE evidence_closures(closure_id TEXT PRIMARY KEY,tx_hash TEXT NOT NULL,num_events INTEGER NOT NULL,event_ids_json TEXT NOT NULL,event_types_json TEXT NOT NULL);
        CREATE TABLE event_atoms(event_id TEXT PRIMARY KEY,closure_id TEXT NOT NULL,tx_hash TEXT NOT NULL,event_type TEXT NOT NULL,protocol TEXT NOT NULL,seq_index INTEGER NOT NULL,roles_json TEXT NOT NULL,attrs_json TEXT NOT NULL);
        CREATE TABLE hg_hyperedges(edge_id TEXT PRIMARY KEY,closure_id TEXT NOT NULL,tx_hash TEXT NOT NULL,signature_key TEXT NOT NULL,event_type TEXT NOT NULL,seq_index INTEGER NOT NULL,edge_json TEXT NOT NULL);
        CREATE TABLE hg_partitions(signature_key TEXT PRIMARY KEY,signature_labels_json TEXT NOT NULL,num_edges INTEGER NOT NULL,edge_ids_json TEXT NOT NULL);
        CREATE TABLE hg_inverted_index(signature_key TEXT NOT NULL,vertex_id TEXT NOT NULL,num_edges INTEGER NOT NULL,posting_json TEXT NOT NULL,PRIMARY KEY(signature_key,vertex_id));
        CREATE TABLE partition_stage(signature_key TEXT,edge_id TEXT);
        CREATE TABLE posting_stage(signature_key TEXT,vertex_id TEXT,edge_id TEXT);
    """)
    conn.executemany(
        "INSERT INTO registry_snapshot VALUES (?,?,?,?,?)",
        (
            (rule, typ, source, json.dumps(schema, separators=(",", ":"), sort_keys=True, ensure_ascii=False),
             json.dumps(fields, separators=(",", ":"), ensure_ascii=False))
            for rule, typ, source, schema, fields in REGISTRY
        ),
    )


def edge_for(row):
    event_id, closure, tx, typ, proto, seq, roles_json, attrs_json = row
    roles = json.loads(roles_json)
    attrs = json.loads(attrs_json)
    vertices = [{
        "vertex_id": f"etype::{typ}", "label": f"etype::{typ}",
        "source": "etype", "role_name": None,
    }]
    for role_name, role_label in sorted(ROLE_SCHEMA[typ].items()):
        value = norm(roles.get(role_name))
        if not value:
            continue
        slot = f"slot::{typ}::{role_name}"
        vertices.append({
            "vertex_id": slot, "label": slot,
            "source": "role_slot", "role_name": role_name,
        })
        vertices.append({
            "vertex_id": f"{role_label}::{value}", "label": role_label,
            "source": f"role_value::{role_name}", "role_name": role_name,
        })
    labels = [vertex["label"] for vertex in vertices]
    signature = SEP.join(sorted(labels))
    edge = {
        "edge_id": event_id, "closure_id": closure, "tx_hash": tx,
        "event_id": event_id, "event_type": typ, "protocol": proto,
        "seq_index": seq, "roles": roles, "attrs": attrs,
        "vertices": vertices, "signature_key": signature,
        "signature_labels": labels,
    }
    return signature, vertices, json.dumps(edge, separators=(",", ":"), ensure_ascii=False)


def build_runtime(conn) -> tuple[int, int]:
    cursor = conn.execute("""
        SELECT event_id,closure_id,tx_hash,event_type,protocol,seq_index,roles_json,attrs_json
        FROM atom_stage ORDER BY closure_id,seq_index,event_type,event_id
    """)
    current_closure = None
    closure_ids, closure_types = [], []
    event_count = closure_count = 0
    event_batch, edge_batch, part_batch, post_batch = [], [], [], []

    def flush_closure():
        nonlocal closure_count
        if current_closure is not None:
            conn.execute(
                "INSERT INTO evidence_closures VALUES (?,?,?,?,?)",
                (current_closure, current_closure, len(closure_ids),
                 json.dumps(closure_ids, separators=(",", ":"), ensure_ascii=False),
                 json.dumps(closure_types, separators=(",", ":"), ensure_ascii=False)),
            )
            closure_count += 1

    def flush_batches():
        conn.executemany("INSERT INTO event_atoms VALUES (?,?,?,?,?,?,?,?)", event_batch)
        conn.executemany("INSERT INTO hg_hyperedges VALUES (?,?,?,?,?,?,?)", edge_batch)
        conn.executemany("INSERT INTO partition_stage VALUES (?,?)", part_batch)
        conn.executemany("INSERT INTO posting_stage VALUES (?,?,?)", post_batch)
        event_batch.clear(); edge_batch.clear(); part_batch.clear(); post_batch.clear()
        conn.commit()

    for row in cursor:
        event_count += 1
        if row[1] != current_closure:
            flush_closure()
            current_closure = row[1]
            closure_ids, closure_types = [], []
        closure_ids.append(row[0]); closure_types.append(row[3])
        signature, vertices, edge_json = edge_for(row)
        event_batch.append(row)
        edge_batch.append((row[0], row[1], row[2], signature, row[3], row[5], edge_json))
        part_batch.append((signature, row[0]))
        for vertex_id in sorted({v["vertex_id"] for v in vertices}):
            post_batch.append((signature, vertex_id, row[0]))
        if len(event_batch) >= 10_000:
            flush_batches()
            if event_count % 100_000 == 0:
                print(f"[hgmatch runtime] events={event_count}", flush=True)
    flush_closure()
    if event_batch:
        flush_batches()
    return event_count, closure_count


def grouped_lists(conn, query, insert_sql, labels=False):
    current_key = None
    values = []
    count = 0
    for row in conn.execute(query):
        key, value = tuple(row[:-1]), row[-1]
        if key != current_key and current_key is not None:
            label_json = json.dumps(current_key[0].split(SEP), separators=(",", ":"), ensure_ascii=False) if labels else None
            params = (*current_key, label_json, len(values), json.dumps(values, separators=(",", ":"), ensure_ascii=False)) if labels else (*current_key, len(values), json.dumps(values, separators=(",", ":"), ensure_ascii=False))
            conn.execute(insert_sql, params)
            count += 1
            values = []
        current_key = key
        values.append(value)
    if current_key is not None:
        label_json = json.dumps(current_key[0].split(SEP), separators=(",", ":"), ensure_ascii=False) if labels else None
        params = (*current_key, label_json, len(values), json.dumps(values, separators=(",", ":"), ensure_ascii=False)) if labels else (*current_key, len(values), json.dumps(values, separators=(",", ":"), ensure_ascii=False))
        conn.execute(insert_sql, params)
        count += 1
    conn.commit()
    return count


def main():
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
        atoms = stage_atoms(conn, args)
        t1 = time.perf_counter()
        create_final_schema(conn)
        events, closures = build_runtime(conn)
        t2 = time.perf_counter()
        conn.execute("CREATE INDEX idx_partition_stage ON partition_stage(signature_key,edge_id)")
        conn.execute("CREATE INDEX idx_posting_stage ON posting_stage(signature_key,vertex_id,edge_id)")
        conn.commit()
        partitions = grouped_lists(
            conn,
            "SELECT signature_key,edge_id FROM partition_stage ORDER BY signature_key,edge_id",
            "INSERT INTO hg_partitions VALUES (?,?,?,?)",
            labels=True,
        )
        postings = grouped_lists(
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
        "registry_version": "hgmatch_same_tx_rust_fair",
        "scope": "same_tx",
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
