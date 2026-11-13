#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import time
from pathlib import Path


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
    indexes = outdir / "indexes"
    reports = outdir / "shard_reports"
    temporary = outdir / "temporary_csv"
    for path in (indexes, reports, temporary):
        path.mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()
    shards: list[dict[str, object]] = []
    totals = {
        "event_count": 0,
        "block_count_sum": 0,
        "root_count": 0,
        "unique_node_count": 0,
        "bloom_bytes": 0,
        "payload_bytes": 0,
        "serialized_index_bytes": 0,
        "csv_load_s": 0.0,
        "mbft_build_s": 0.0,
        "serialize_s": 0.0,
        "child_complete_internal_s": 0.0,
    }

    with args.events.open("r", encoding="utf-8", newline="") as source:
        reader = csv.reader(source)
        header = next(reader)
        shard_index = 0
        while True:
            shard_index += 1
            csv_path = temporary / f"events_{shard_index:04d}.csv"
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

            index_path = indexes / f"mbft_shard_{shard_index:04d}.mbfti"
            report_path = reports / f"mbft_shard_{shard_index:04d}.json"
            cmd = [
                str(args.mbft_adapt.resolve()), "offline-build",
                "--events", str(csv_path), "--index", str(index_path),
                "--report", str(report_path), "--combine", str(args.combine),
                "--fp-rate", str(args.fp_rate), "--seed", str(args.seed),
                "--tweak", str(args.tweak),
            ]
            shard_started = time.perf_counter()
            subprocess.run(cmd, check=True)
            shard_wall_s = time.perf_counter() - shard_started
            csv_path.unlink()

            report = json.loads(report_path.read_text(encoding="utf-8"))
            timing = report["timing_s"]
            metrics = report["index_metrics"]
            record = {
                "shard": shard_index, "rows": rows,
                "wall_s": shard_wall_s, "index": str(index_path),
                "report": str(report_path), **metrics,
            }
            shards.append(record)
            totals["event_count"] += int(metrics["event_count"])
            totals["block_count_sum"] += int(metrics["block_count"])
            totals["root_count"] += int(metrics["root_count"])
            totals["unique_node_count"] += int(metrics["unique_node_count"])
            totals["bloom_bytes"] += int(metrics["bloom_bytes"])
            totals["payload_bytes"] += int(metrics["payload_bytes"])
            totals["serialized_index_bytes"] += int(metrics["index_file_bytes"])
            totals["csv_load_s"] += float(timing["csv_load"])
            totals["mbft_build_s"] += float(timing["mbft_build"])
            totals["serialize_s"] += float(timing["serialize"])
            totals["child_complete_internal_s"] += float(timing["complete_internal"])
            print(json.dumps(record, ensure_ascii=False), flush=True)

    complete_wall_s = time.perf_counter() - started
    manifest = {
        "format": "MBFT-Adapt-serialized-index-v1-chunked",
        "source_events": str(args.events.resolve()),
        "chunk_rows": args.chunk_rows,
        "shard_count": len(shards),
        "fp_rate": args.fp_rate, "seed": args.seed,
        "tweak": args.tweak, "combine_cycle": args.combine,
        "complete_wall_s": complete_wall_s,
        "totals": totals,
        "shards": shards,
    }
    manifest_path = outdir / "offline_build_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
