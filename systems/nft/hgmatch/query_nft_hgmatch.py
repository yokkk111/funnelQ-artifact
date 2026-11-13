#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


def _path_supplied(path: object) -> bool:
    return str(path or "").strip() not in {"", "."}

from systems.nft.common.full_binding_spec import query_path
from systems.nft.common.platform_paths import cargo_binary


def main() -> int:
    ap = argparse.ArgumentParser(description="Run Rust NFT HGMatch full-event-binding query.")
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--query-json", default="", type=Path)
    ap.add_argument("--query-id", default="")
    ap.add_argument("--query-dir", default=str(ROOT / "queries" / "nft" / "final5"), type=Path)
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--max-results", type=int, default=0)
    ap.add_argument("--exe", default="", type=Path)
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--phase-marker", default="", type=Path)
    args = ap.parse_args()
    query_json = args.query_json if _path_supplied(args.query_json) else query_path(args.query_dir, args.query_id)
    crate = Path(__file__).resolve().parent / "rust_engine"
    exe = args.exe.resolve() if _path_supplied(args.exe) else cargo_binary(crate, "nft_hgmatch_full", build=True)
    args.outdir.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(exe), "--db", str(args.db.resolve()), "--query-json", str(query_json.resolve()),
        "--outdir", str(args.outdir.resolve()), "--threads", str(max(1, args.threads)),
        "--max-results", str(max(0, args.max_results)),
        "--warmup-runs", str(max(0, args.warmup_runs)),
    ]
    if _path_supplied(args.phase_marker):
        cmd += ["--phase-marker", str(args.phase_marker.resolve())]
    proc = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True)
    (args.outdir / "hgmatch.stdout.txt").write_text(proc.stdout, encoding="utf-8")
    (args.outdir / "hgmatch.stderr.txt").write_text(proc.stderr, encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"HGMatch Rust failed ({proc.returncode}): {proc.stderr[-3000:]}")
    report_path = args.outdir / "execution_report.json"
    if not report_path.exists():
        raise FileNotFoundError(report_path)
    print(json.dumps(json.loads(report_path.read_text(encoding="utf-8")), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
