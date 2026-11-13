import os
import ast
import copy
import hashlib
import json
import math
import sqlite3
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
RELEASE = ROOT / "FunnelQ_code_release/defi"
DB = Path(os.environ.get('FUNNELQ_DEFI_DB', str(ROOT / 'data/defi/anchored_episode_runtime_sidecar.db')))
FIELDS = {"flashloan": ["recipient", "asset", "pool"],
          "swap": ["taker", "token_sold", "token_bought", "pool"],
          "borrow": ["borrower", "asset", "pool"]}


def workloads():

    tree = ast.parse((RELEASE / "scripts/run_final_queryset_4scheme_once.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(x, ast.Name) and x.id in {"SAME_TX", "EPISODE"} for x in node.targets):
            entries = eval(compile(ast.Expression(node.value), "workload mapping", "eval"), {"ROOT": RELEASE, "__builtins__": {}})
            for label, name, path in entries:
                q = json.loads(path.read_text())
                for window in ([0] if q["scope"] == "same_tx" else [60, 300, 600]):
                    qq = copy.deepcopy(q)
                    if window:
                        qq["scope_params"]["forward_window_sec"] = window
                    yield label, window, path, qq


def sql(q, limit=None):
    es = q["events"]
    tables, cond, variables = [], [], {}
    for e in es:
        a, typ = e["id"], e["type"]
        assert a.isidentifier() and typ in FIELDS
        tables += [f"support_{typ} {a}", f"episode_tx_timeline t_{a}"]
        cond.append(f"{a}.global_tx_ord=t_{a}.global_tx_ord")
        for field, v in e["roles"].items():
            assert field in FIELDS[typ] and v.startswith("$")
            x = f"{a}.{field}_id"
            if v in variables:
                cond.append(f"{x}={variables[v]}")
            else:
                variables[v] = x
    for a, b in q["order"]:
        cond.append(f"{a}.global_event_ord<{b}.global_event_ord")
    for a, fa, b, fb in q.get("neq_constraints", []):
        cond.append(f"{a}.{fa}_id!={b}.{fb}_id")
    for f in q.get("attr_filters", []):
        assert f["field"] == "amount" and f["value"] == "0" and f["op"] in ("<", ">")
        cond.append(f"{f['event']}.amount_num{f['op']}0")
    anchor = es[0]["id"] if q["scope"] == "same_tx" else q["scope_params"]["semantic_anchor_event"]
    for i, e in enumerate(es):
        a = e["id"]
        for other in es[:i]:
            cond.append(f"{a}.global_event_ord!={other['id']}.global_event_ord")
        if q["scope"] == "same_tx":
            cond.append(f"{a}.global_tx_ord={anchor}.global_tx_ord")
        else:
            w = q["scope_params"]["forward_window_sec"]
            cond += [f"{a}.global_event_ord>={anchor}.global_event_ord",
                     f"{a}.global_tx_ord>={anchor}.global_tx_ord",
                     f"t_{a}.block_time_unix>=t_{anchor}.block_time_unix",
                     f"t_{a}.block_time_unix<=t_{anchor}.block_time_unix+{w}"]
    cols = ",".join(f"{e['id']}.global_event_ord" for e in es)
    return f"SELECT {cols} FROM {' CROSS JOIN '.join(tables)} WHERE {' AND '.join(cond)}" + (f" LIMIT {limit}" if limit else "")


def convert(row, typ, timeline):
    ts = timeline[row["global_tx_ord"]]
    fields = {k: {"kind": "int", "value": row[k + "_id"]} for k in FIELDS[typ]}
    if typ == "borrow":
        v = row["amount_num"]
        assert math.isfinite(v)
        fields["amount_sign"] = {"kind": "int", "value": int(v > 0) - int(v < 0)}
    return {"event_id": f"defi:{row['global_event_ord']}", "event_type": typ,
            "order_seq": row["global_event_ord"], "block_ts": ts["block_time_unix"],
            "tx_hash": f"txid:{ts['tx_hash_id']}", "fields": fields, "provenance": []}


def main():
    c = sqlite3.connect(DB.as_uri() + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA query_only=ON")
    catalog = {typ: c.execute(f"SELECT count(*) FROM support_{typ}").fetchone()[0] for typ in FIELDS}
    catalog["transactions"] = c.execute("SELECT count(*) FROM episode_tx_timeline").fetchone()[0]
    cases, audit = [], []
    for label, window, path, q in workloads():
        t0 = time.perf_counter()
        c.set_progress_handler(lambda: int(time.perf_counter() - t0 > 40), 10000)
        statement = sql(q, 1)
        try:
            found = c.execute(statement).fetchone()
        finally:
            c.set_progress_handler(None, 0)
        assert found is not None, f"no real witness: {label}/{window}"
        records = {}
        for atom, eid in zip(q["events"], found):
            typ = atom["type"]
            r = dict(c.execute(f"SELECT * FROM support_{typ} WHERE global_event_ord=?", (eid,)).fetchone())
            records[eid] = (typ, r)

        first_tx = min(r[1]["global_tx_ord"] for r in records.values())
        for typ in sorted({e["type"] for e in q["events"]}):
            for r in c.execute(f"SELECT * FROM support_{typ} WHERE global_tx_ord BETWEEN ? AND ? LIMIT 16", (first_tx, first_tx + 20)):
                records[r["global_event_ord"]] = (typ, dict(r))
        txs = {r[1]["global_tx_ord"] for r in records.values()}
        timeline = {i: dict(c.execute("SELECT * FROM episode_tx_timeline WHERE global_tx_ord=?", (i,)).fetchone()) for i in txs}
        mem = sqlite3.connect(":memory:")
        for table in [*("support_" + t for t in FIELDS), "episode_tx_timeline"]:
            ddl = c.execute("SELECT sql FROM sqlite_master WHERE name=?", (table,)).fetchone()[0]
            mem.execute(ddl)
        for typ, r in records.values():
            mem.execute(f"INSERT INTO support_{typ} VALUES ({','.join('?' for _ in r)})", list(r.values()))
        for r in timeline.values():
            mem.execute(f"INSERT INTO episode_tx_timeline VALUES ({','.join('?' for _ in r)})", list(r.values()))
        expected = sorted([list(x) for x in mem.execute(sql(q))])
        assert expected and len(expected) == len({tuple(x) for x in expected})
        events = [convert(r, typ, timeline) for _, (typ, r) in sorted(records.items())]
        cases.append({"label": label, "window": window, "query": q, "events": events, "expected": expected})
        audit.append({"label": label, "window": window, "source_query": str(path), "query_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                      "sample_rows": len(events), "sample_answers": len(expected), "source_witness": list(found),
                      "discovery_seconds": time.perf_counter() - t0, "source_sql": statement})
        print(label, window, len(events), len(expected), flush=True)
        mem.close()
    result = {"scope": "positive-enriched real-event subset functional tests, not full-epoch performance", "source_db": str(DB),
              "source_counts": catalog, "amount_semantics": "sign(amount_num), exact for stored non-NaN REAL comparisons against zero only", "cases": cases, "audit": audit}
    (OUT / "fixtures.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    c.close()


if __name__ == "__main__":
    main()
