#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

SAME_TX_QUERIES = [
    "flashloan_swap_borrow.json",
    "flashloan_swap_borrow_followup.json",
    "flashloan_2swaps_distinct_followup.json",
    "flashloan_3swaps_chain_reuse.json",
    "flashloan_3swaps_distinct_parallel.json",
]
EPISODE_QUERIES = [
    "flashloan_swap_borrow_episode_v1.json",
    "flashloan_swap_borrow_followup_episode_v1.json",
    "flashloan_2swaps_distinct_followup_episode_v1.json",
    "flashloan_3swaps_chain_reuse_episode_v1.json",
    "flashloan_3swaps_distinct_parallel_episode_v1.json",
]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--matcher", type=Path, default=Path(__file__).with_name("neutral_matcher.py"))
    p.add_argument("--query-dir", required=True, type=Path)
    p.add_argument("--scope", choices=["same_tx", "episode"], required=True)
    p.add_argument("--flashloan", required=True, type=Path)
    p.add_argument("--swap", required=True, type=Path)
    p.add_argument("--borrow", required=True, type=Path)
    p.add_argument("--outdir", required=True, type=Path)
    p.add_argument("--materialize-limit", type=int, default=0)
    args = p.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    names = SAME_TX_QUERIES if args.scope == "same_tx" else EPISODE_QUERIES
    summary = []
    for name in names:
        query_path = args.query_dir / name
        out = args.outdir / f"{query_path.stem}.json"
        cmd = [
            sys.executable, str(args.matcher),
            "--query", str(query_path),
            "--scope", args.scope,
            "--events", str(args.flashloan),
            "--events", str(args.swap),
            "--events", str(args.borrow),
            "--out", str(out),
            "--materialize-limit", str(args.materialize_limit),
        ]
        subprocess.run(cmd, check=True)
        result = json.loads(out.read_text(encoding="utf-8"))
        summary.append(result)
    (args.outdir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
