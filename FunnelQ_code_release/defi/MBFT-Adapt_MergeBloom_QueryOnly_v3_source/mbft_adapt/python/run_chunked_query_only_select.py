#!/usr/bin/env python3
"""Disk-backed, bounded-memory MBFT query-only atomic selection.

The canonical CSV is streamed into fixed-size shards.  Each shard builds and
queries an unmodified MBFT MergeBloom chain in a separate child process.  Its
atomic candidates are appended to final JSONL streams before the next shard is
processed, so only one chain is resident at a time.
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import time
from pathlib import Path
from typing import Any


TYPES = ("flashloan", "swap", "borrow")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mbft-adapt", required=True, type=Path)
    parser.add_argument("--events", required=True, type=Path)
    parser.add_argument("--outdir", required=True, type=Path)
    parser.add_argument("--chunk-rows", type=int, default=300_000)
    parser.add_argument("--combine", type=int, default=0)
    parser.add_argument("--fp-rate", type=float, default=0.001)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--tweak", type=int, default=0)
    args = parser.parse_args()

    outdir = args.outdir.resolve()
    shard_dir = outdir / "shards"
    atomic_dir = outdir / "atomic"
    shard_dir.mkdir(parents=True, exist_ok=True)
    atomic_dir.mkdir(parents=True, exist_ok=True)
    final_paths = {kind: atomic_dir / f"{kind}.query_only.jsonl" for kind in TYPES}
    final_handles = {kind: path.open("wb") for kind, path in final_paths.items()}
    aggregate: dict[str, Any] = {
        "format": "MBFT-Adapt-MergeBloom-query-only-v3-chunked",
        "mode": "query-only-no-vo",
        "chunk_rows": args.chunk_rows,
        "event_count": 0,
        "block_count_sum": 0,
        "csv_load_time_us": 0.0,
        "mbft_build_time_us": 0.0,
        "selections": {
            kind: {
                "unique_events": 0,
                "mbft_tree_query_time_us": 0.0,
                "candidate_write_time_us": 0.0,
                "candidate_file_bytes": 0,
                "candidate_file": str(final_paths[kind]),
                "false_positive_leaves": 0,
                "opened_leaves": 0,
                "pruned_nodes": 0,
                "raw_matched_leaves": 0,
            }
            for kind in TYPES
        },
        "shards": [],
    }

    started = time.perf_counter()
    try:
        with args.events.open("r", encoding="utf-8", newline="") as source:
            reader = csv.reader(source)
            header = next(reader)
            shard_index = 0
            while True:
                shard_index += 1
                csv_path = shard_dir / f"events_{shard_index:04d}.csv"
                rows = 0
                with csv_path.open("w", encoding="utf-8", newline="") as target:
                    writer = csv.writer(target, lineterminator="\n")
                    writer.writerow(header)
                    for row in reader:
                        writer.writerow(row)
                        rows += 1
                        if rows >= args.chunk_rows:
                            break
                if rows == 0:
                    csv_path.unlink()
                    break

                result_dir = shard_dir / f"result_{shard_index:04d}"
                cmd = [
                    str(args.mbft_adapt.resolve()), "query-only-select",
                    "--events", str(csv_path),
                    "--outdir", str(result_dir),
                    "--combine", str(args.combine),
                    "--fp-rate", str(args.fp_rate),
                    "--seed", str(args.seed),
                    "--tweak", str(args.tweak),
                    "--types", ",".join(TYPES),
                ]
                shard_start = time.perf_counter()
                completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
                shard_wall_s = time.perf_counter() - shard_start
                summary = json.loads((result_dir / "query_only_atomic_summary.json").read_text(encoding="utf-8"))

                aggregate["event_count"] += int(summary["event_count"])
                aggregate["block_count_sum"] += int(summary["block_count"])
                aggregate["csv_load_time_us"] += float(summary["csv_load_time_us"])
                aggregate["mbft_build_time_us"] += float(summary["mbft_build_time_us"])
                shard_record = {
                    "shard": shard_index,
                    "rows": rows,
                    "wall_s": shard_wall_s,
                    "summary": str(result_dir / "query_only_atomic_summary.json"),
                }
                aggregate["shards"].append(shard_record)

                for kind in TYPES:
                    metrics = summary["selections"][kind]
                    src = result_dir / f"{kind}.query_only.jsonl"
                    with src.open("rb") as stream:
                        while True:
                            block = stream.read(8 * 1024 * 1024)
                            if not block:
                                break
                            final_handles[kind].write(block)
                    dst = aggregate["selections"][kind]
                    for key in (
                        "unique_events", "false_positive_leaves", "opened_leaves",
                        "pruned_nodes", "raw_matched_leaves",
                    ):
                        dst[key] += int(metrics[key])
                    for key in ("mbft_tree_query_time_us", "candidate_write_time_us"):
                        dst[key] += float(metrics[key])
                print(json.dumps(shard_record), flush=True)
    finally:
        for handle in final_handles.values():
            handle.close()

    for kind in TYPES:
        aggregate["selections"][kind]["candidate_file_bytes"] = final_paths[kind].stat().st_size
    aggregate["wall_s"] = time.perf_counter() - started
    report = outdir / "chunked_query_only_atomic_summary.json"
    report.write_text(json.dumps(aggregate, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(aggregate, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
