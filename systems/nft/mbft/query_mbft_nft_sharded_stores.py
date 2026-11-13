#!/usr/bin/env python3


from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

from sharded_store_common import atomic_write_json, run_monitored

BUILD_FORMAT = "MBFT-NFT-Sharded-Persistent-Store-v1"
QUERY_FORMAT = "MBFT-NFT-Sharded-Persistent-Query-v1"


def parse_types(raw: str) -> list[str]:
    out = [x.strip().lower() for x in raw.split(",") if x.strip()]
    if not out:
        raise ValueError("at least one event type is required")
    return out


def append_file(src: Path, dst_handle) -> int:
    copied = 0
    with src.open("rb") as stream:
        while True:
            block = stream.read(8 * 1024 * 1024)
            if not block:
                break
            dst_handle.write(block)
            copied += len(block)
    return copied


def main() -> int:
    ap = argparse.ArgumentParser(description="Query MBFT NFT persistent shards sequentially.")
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--exe", required=True, type=Path)
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--types", default="nfttrade,nfttransfer")
    ap.add_argument("--timeout-per-shard-sec", type=float, default=0.0)
    ap.add_argument("--max-rss-mb", type=float, default=0.0)
    ap.add_argument("--merge-candidates", action="store_true")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--delete-shard-results-after-merge", action="store_true")
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--phase-marker", default="", type=Path)
    args = ap.parse_args()

    manifest_path = args.manifest.resolve()
    exe = args.exe.resolve()
    outdir = args.outdir.resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    if not exe.is_file() or not os.access(exe, os.X_OK):
        raise FileNotFoundError(f"MBFT executable not found/executable: {exe}")
    build = json.loads(manifest_path.read_text(encoding="utf-8"))
    if build.get("format") != BUILD_FORMAT or build.get("status") != "success":
        raise ValueError("shard build manifest is not a successful supported manifest")
    stores_dir = manifest_path.parent
    types = parse_types(args.types)
    shard_out_root = outdir / "shards"
    shard_out_root.mkdir(parents=True, exist_ok=True)
    log_dir = outdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    partial_path = outdir / "sharded_query_progress.json"
    existing: dict[int, dict[str, Any]] = {}
    if partial_path.exists() and args.resume:
        partial = json.loads(partial_path.read_text(encoding="utf-8"))
        existing = {int(x["shard_id"]): x for x in partial.get("shards", [])}
    elif partial_path.exists() and not args.resume:
        raise FileExistsError(f"query progress already exists; use --resume: {partial_path}")

    started = time.perf_counter()
    records: list[dict[str, Any]] = []
    candidate_paths: dict[str, list[str]] = {kind: [] for kind in types}
    aggregate_types: dict[str, dict[str, Any]] = {
        kind: {
            "events": 0,
            "query_time_us": 0.0,
            "candidate_write_time_us": 0.0,
            "opened_leaves": 0,
            "false_positive_leaves": 0,
            "payload_bytes": 0,
            "paths": [],
        }
        for kind in types
    }

    for shard in build["shards"]:
        shard_id = int(shard["shard_id"])
        result_dir = shard_out_root / f"shard_{shard_id:05d}"
        summary_path = result_dir / "atomic_selection_summary.json"
        old = existing.get(shard_id)
        if old is not None and summary_path.is_file():
            proc = old.get("process", {})
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            status = "resume-skip"
        else:
            if result_dir.exists():
                shutil.rmtree(result_dir)
            result_dir.mkdir(parents=True, exist_ok=True)
            store_path = stores_dir / shard["store_relative"]
            cmd = [
                str(exe), "query-store",
                "--store", str(store_path),
                "--outdir", str(result_dir),
                "--types", ",".join(types),
                "--warmup-runs", str(max(0, args.warmup_runs)),
            ]
            if str(args.phase_marker).strip() not in {"", "."}:
                cmd += ["--phase-marker", str(args.phase_marker.resolve())]
            proc, stdout, stderr = run_monitored(
                cmd,
                cwd=exe.parent,
                timeout_s=args.timeout_per_shard_sec,
                max_rss_mb=args.max_rss_mb,
            )
            (log_dir / f"shard_{shard_id:05d}.stdout.txt").write_text(stdout, encoding="utf-8")
            (log_dir / f"shard_{shard_id:05d}.stderr.txt").write_text(stderr, encoding="utf-8")
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            status = "queried"

        persistent_tx = int(summary.get("build", {}).get("persistent", {}).get("transactions", -1))
        if persistent_tx != int(shard["rows"]):
            raise RuntimeError(
                f"shard {shard_id}: persistent transaction count {persistent_tx} != {shard['rows']}"
            )
        query_only = summary.get("query_only", {})
        for kind in types:
            if kind not in query_only:
                raise RuntimeError(f"shard {shard_id}: missing query result for {kind}")
            item = query_only[kind]
            path = Path(item["path"]).resolve()
            if not path.is_file():
                raise FileNotFoundError(path)
            candidate_paths[kind].append(str(path))
            dst = aggregate_types[kind]
            dst["events"] += int(item.get("events", 0))
            dst["query_time_us"] += float(item.get("query_time_us", 0.0))
            dst["candidate_write_time_us"] += float(item.get("candidate_write_time_us", 0.0))
            dst["opened_leaves"] += int(item.get("opened_leaves", 0))
            dst["false_positive_leaves"] += int(item.get("false_positive_leaves", 0))
            dst["payload_bytes"] += int(item.get("payload_bytes", 0))
            dst["paths"].append(str(path))

        record = {
            "shard_id": shard_id,
            "rows": int(shard["rows"]),
            "order_offset": int(shard["order_offset"]),
            "store_relative": shard["store_relative"],
            "result_dir": str(result_dir),
            "store_load_time_us": float(summary.get("build", {}).get("store_load_time_us", 0.0)),
            "query_time_us": sum(float(query_only[k].get("query_time_us", 0.0)) for k in types),
            "candidate_write_time_us": sum(float(query_only[k].get("candidate_write_time_us", 0.0)) for k in types),
            "process": proc,
        }
        records.append(record)
        progress = {
            "format": QUERY_FORMAT,
            "status": "running",
            "build_manifest": str(manifest_path),
            "types": types,
            "shards": records,
            "elapsed_s": time.perf_counter() - started,
        }
        atomic_write_json(partial_path, progress)
        print(json.dumps({"shard": shard_id, "status": status, "rows": shard["rows"],
                          "peak_rss_mb": proc.get("peak_tree_rss_mb", 0.0)}), flush=True)

    merged_paths: dict[str, str] = {}
    merged_bytes: dict[str, int] = {}
    if args.merge_candidates:
        merged_dir = outdir / "server_candidates"
        merged_dir.mkdir(parents=True, exist_ok=True)
        for kind in types:
            dst = merged_dir / f"{kind}.jsonl"
            copied = 0
            with dst.open("wb") as handle:
                for raw in candidate_paths[kind]:
                    copied += append_file(Path(raw), handle)
                handle.flush()
                os.fsync(handle.fileno())
            merged_paths[kind] = str(dst)
            merged_bytes[kind] = copied
            aggregate_types[kind]["path"] = str(dst)
            aggregate_types[kind]["candidate_file_bytes"] = copied
    else:
        for kind in types:
            aggregate_types[kind]["candidate_file_bytes"] = sum(Path(p).stat().st_size for p in candidate_paths[kind])

    store_load_sum = sum(float(x["store_load_time_us"]) for x in records)
    store_load_max = max((float(x["store_load_time_us"]) for x in records), default=0.0)
    query_sum = sum(float(x["query_time_us"]) for x in records)
    candidate_write_sum = sum(float(x["candidate_write_time_us"]) for x in records)
    peak_max = max((float(x["process"].get("peak_tree_rss_mb", 0.0)) for x in records), default=0.0)
    report = {
        "format": QUERY_FORMAT,
        "status": "success",
        "build_manifest": str(manifest_path),
        "executable": str(exe),
        "sequential_store_lifecycle": "load one shard -> query -> child exits/releases chain -> next shard",
        "shard_count": len(records),
        "source_event_count": int(build["event_count"]),
        "types": types,
        "query_only": aggregate_types,
        "candidate_layout": "merged-and-sharded" if args.merge_candidates else "sharded",
        "candidate_paths": candidate_paths,
        "merged_candidate_paths": merged_paths,
        "store_load_time_us_sum": store_load_sum,
        "store_load_time_us_max": store_load_max,
        "mbft_tree_query_time_us_sum": query_sum,
        "atomic_query_s": query_sum / 1_000_000.0,
        "candidate_write_s_sum": candidate_write_sum / 1_000_000.0,
        "store_load_s_sum": store_load_sum / 1_000_000.0,
        "store_load_s_max": store_load_max / 1_000_000.0,
        "peak_shard_process_rss_mb": peak_max,
        "process_wall_s_sum": sum(float(x["process"].get("wall_s", 0.0)) for x in records),
        "wall_s": time.perf_counter() - started,
        "timing_contract": {
            "formal_atomic_query_s": "sum of internal MBFT tree query times across all shards and requested types",
            "candidate_write_s_sum": "intermediate candidate JSONL materialization required by the adapter",
            "store_load_s_sum": "persistent store load reported separately and excluded by the unified formal runner",
            "warmup_runs_per_shard": max(0, args.warmup_runs),
            "wall_s": "includes store load, MBFT query, candidate JSONL materialization, and process startup",
            "offline_shard_build_excluded": True,
        },
        "shards": records,
    }
    total_candidates = sum(int(aggregate_types[k]["events"]) for k in types)
    report["candidate_events"] = total_candidates
    if set(types) == {"nfttrade", "nfttransfer"} and total_candidates != int(build["event_count"]):
        raise RuntimeError(
            f"candidate completeness mismatch: {total_candidates} != {build['event_count']}"
        )
    atomic_write_json(outdir / "atomic_selection_summary.json", report)
    atomic_write_json(outdir / "candidate_manifest.json", {
        "format": "MBFT-NFT-Sharded-Candidates-v1",
        "types": types,
        "event_count": total_candidates,
        "paths": candidate_paths,
        "merged_paths": merged_paths,
    })
    if args.delete_shard_results_after_merge:
        if not args.merge_candidates:
            raise ValueError("--delete-shard-results-after-merge requires --merge-candidates")
        shutil.rmtree(shard_out_root)
    partial_path.unlink(missing_ok=True)
    print(json.dumps({"status": "success", "summary": str(outdir / 'atomic_selection_summary.json'),
                      "shards": len(records), "candidate_events": total_candidates,
                      "atomic_query_s": report["atomic_query_s"],
                      "peak_shard_process_rss_mb": peak_max}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr, flush=True)
        raise
