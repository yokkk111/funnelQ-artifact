#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

FINAL5 = ["C1", "C2", "L3_RETURN", "L6_NEG_RETURN_SUPPORT", "H5_23"]


def run(cmd: list[str], cwd: Path) -> None:
    subprocess.run(cmd, cwd=str(cwd), check=True)


def load_payloads(path: Path) -> list[tuple[str, int, str, str]]:
    out = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                x = json.loads(line)
                out.append((str(x["event_id"]), int(x["order_seq"]), str(x["closure_id"]), str(x["event_type"])))
    return out


def instance_keys(path: Path) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as f:
        return [r["instance_key"] for r in csv.DictReader(f)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[1])
    ap.add_argument("--exe", type=Path, default=None)
    args = ap.parse_args()
    root = args.project_root.resolve()
    adapt = root / "systems" / "nft" / "mbft"
    exe = (args.exe or adapt / "persistent_build" / "mbft_nft_adapt").resolve()
    events = root / "validation" / "synthetic_nft_events.csv"
    if not exe.is_file():
        raise FileNotFoundError(exe)

    with tempfile.TemporaryDirectory(prefix="mbft_nft_shards_") as tmp_raw:
        tmp = Path(tmp_raw)
        stores = tmp / "stores"
        sharded_query = tmp / "sharded_query"
        mono_query = tmp / "mono_query"
        run([
            sys.executable, str(adapt / "build_mbft_nft_sharded_stores.py"),
            "--events", str(events), "--stores-dir", str(stores), "--exe", str(exe),
            "--shard-rows", "2",
        ], root)
        run([
            sys.executable, str(adapt / "query_mbft_nft_sharded_stores.py"),
            "--manifest", str(stores / "mbft_nft_shard_manifest.json"),
            "--exe", str(exe), "--outdir", str(sharded_query), "--merge-candidates",
        ], root)
        run([
            str(exe), "build-store", "--events", str(events),
            "--store", str(tmp / "mono.mbft"), "--metrics", str(tmp / "mono.build.json"),
        ], root)
        run([
            str(exe), "query-store", "--store", str(tmp / "mono.mbft"),
            "--outdir", str(mono_query), "--types", "nfttrade,nfttransfer",
        ], root)
        for kind in ("nfttrade", "nfttransfer"):
            mono = load_payloads(mono_query / "server_candidates" / f"{kind}.jsonl")
            shard = load_payloads(sharded_query / "server_candidates" / f"{kind}.jsonl")
            if mono != shard:
                raise AssertionError(f"atomic candidate mismatch for {kind}")

        mono_match = tmp / "mono_match"
        shard_match = tmp / "shard_match"
        run([
            sys.executable, str(adapt / "nft_semantic_matcher.py"), "--events-jsonl",
            str(mono_query / "server_candidates" / "nfttrade.jsonl"),
            str(mono_query / "server_candidates" / "nfttransfer.jsonl"),
            "--query-dir", str(adapt / "queries"), "--outdir", str(mono_match),
        ], root)
        run([
            sys.executable, str(adapt / "nft_semantic_matcher.py"), "--events-jsonl",
            str(sharded_query / "server_candidates" / "nfttrade.jsonl"),
            str(sharded_query / "server_candidates" / "nfttransfer.jsonl"),
            "--query-dir", str(adapt / "queries"), "--outdir", str(shard_match),
        ], root)
        for query in FINAL5:
            a = instance_keys(mono_match / query / "instances.csv")
            b = instance_keys(shard_match / query / "instances.csv")
            if a != b:
                raise AssertionError(f"semantic result mismatch for {query}")
    print("MBFT NFT sharded persistent equivalence: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
