#!/usr/bin/env python3


from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any, TextIO

from sharded_store_common import atomic_write_json, run_monitored, sha256_file

FORMAT = "MBFT-NFT-Sharded-Persistent-Store-v1"
MBFT_COLUMNS = (
    "event_id",
    "event_type",
    "block_time",
    "block_number",
    "tx_hash",
    "local_order",
    "nft",
    "seller",
    "buyer",
    "from_address",
    "to_address",
    "source_table",
    "source_id",
    "evt_index",
)

REQUIRED = set(MBFT_COLUMNS)


def int_loose(value: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(float(value or 0))


def order_key(row: list[str], col: dict[str, int]) -> tuple[Any, ...]:
    return (
        row[col["block_time"]],
        int_loose(row[col["block_number"]]),
        row[col["tx_hash"]].strip().lower(),
        int_loose(row[col["local_order"]]),
        row[col["event_id"]],
    )


def load_existing(path: Path, resume: bool) -> dict[str, Any] | None:
    if not path.exists():
        return None
    if not resume:
        raise FileExistsError(f"manifest already exists; use --resume: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("format") != FORMAT:
        raise ValueError(f"unsupported manifest format: {data.get('format')}")
    return data


def main() -> int:
    ap = argparse.ArgumentParser(description="Build persistent MBFT NFT shard stores offline.")
    ap.add_argument("--events", required=True, type=Path)
    ap.add_argument("--stores-dir", required=True, type=Path)
    ap.add_argument("--exe", required=True, type=Path)
    ap.add_argument("--manifest", type=Path, default=None)
    ap.add_argument("--shard-rows", type=int, default=300_000)
    ap.add_argument("--combine", type=int, default=0)
    ap.add_argument("--fp-rate", type=float, default=0.001)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--tweak", type=int, default=0)
    ap.add_argument("--timeout-per-shard-sec", type=float, default=0.0)
    ap.add_argument("--max-rss-mb", type=float, default=0.0)
    ap.add_argument("--keep-shard-csv", action="store_true")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--compute-source-sha256", action="store_true")
    args = ap.parse_args()

    if args.shard_rows <= 0:
        raise ValueError("--shard-rows must be positive")
    events = args.events.resolve()
    exe = args.exe.resolve()
    stores_dir = args.stores_dir.resolve()
    manifest_path = (args.manifest or stores_dir / "mbft_nft_shard_manifest.json").resolve()
    temp_dir = stores_dir / ".build_tmp"
    shard_store_dir = stores_dir / "stores"
    metrics_dir = stores_dir / "build_metrics"
    log_dir = stores_dir / "build_logs"
    for path in (stores_dir, temp_dir, shard_store_dir, metrics_dir, log_dir):
        path.mkdir(parents=True, exist_ok=True)
    if not events.is_file():
        raise FileNotFoundError(events)
    if not exe.is_file() or not os.access(exe, os.X_OK):
        raise FileNotFoundError(f"MBFT executable not found/executable: {exe}")

    existing = load_existing(manifest_path, args.resume)
    completed = {int(x["shard_id"]): x for x in (existing or {}).get("shards", [])}
    source_stat = events.stat()
    if existing:
        old = existing.get("source", {})
        if int(old.get("file_bytes", -1)) != source_stat.st_size:
            raise ValueError("source file size changed since partial build")
        if int(existing.get("shard_rows_target", -1)) != args.shard_rows:
            raise ValueError("--shard-rows differs from existing manifest")

    manifest: dict[str, Any] = {
        "format": FORMAT,
        "status": "building",
        "source": {
            "path": str(events),
            "file_bytes": source_stat.st_size,
            "mtime_ns": source_stat.st_mtime_ns,
            "sha256": (existing or {}).get("source", {}).get("sha256", ""),
        },
        "executable": str(exe),
        "shard_rows_target": args.shard_rows,
        "boundary": "do_not_split_ethereum_block_number",
        "global_order_contract": "order_seq=row offset in globally canonical source order",
        "parameters": {
            "combine": args.combine,
            "fp_rate": args.fp_rate,
            "seed": args.seed,
            "tweak": args.tweak,
        },
        "query_timing_contract": {
            "index_build_excluded": True,
            "atomic_query_time": "sum of internal MBFT tree query time across shards",
            "store_load_time": "reported separately as sum and max",
            "process_memory": "maximum resident tree of one sequential shard child",
        },
        "shards": [],
    }
    if args.compute_source_sha256 and not manifest["source"]["sha256"]:
        manifest["source"]["sha256"] = sha256_file(events)

    started = time.perf_counter()
    shard_records: list[dict[str, Any]] = []
    global_rows = 0
    previous_key: tuple[Any, ...] | None = None

    with events.open("r", encoding="utf-8", newline="") as source:
        reader = csv.reader(source)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("empty canonical NFT CSV")
        col = {name: i for i, name in enumerate(header)}
        missing = sorted(REQUIRED - set(col))
        if missing:
            raise ValueError(f"missing CSV columns: {missing}")

        shard_id = 0
        target: TextIO | None = None
        writer: csv.writer | None = None
        csv_path: Path | None = None
        rows = 0
        order_start = 0
        first_event = ""
        last_event = ""
        first_block = ""
        last_block = ""
        current_block = ""
        skip_existing = False

        def start_shard() -> None:
            nonlocal shard_id, target, writer, csv_path, rows, order_start
            nonlocal first_event, last_event, first_block, last_block, current_block, skip_existing
            shard_id += 1
            rows = 0
            order_start = global_rows
            first_event = last_event = first_block = last_block = current_block = ""
            skip_existing = shard_id in completed
            csv_path = temp_dir / f"events_{shard_id:05d}.csv"
            if skip_existing:
                target = None
                writer = None
            else:
                target = csv_path.open("w", encoding="utf-8", newline="")
                writer = csv.writer(target, lineterminator="\n")
                writer.writerow(MBFT_COLUMNS)

        def finish_shard() -> None:
            nonlocal target, writer, csv_path
            if rows == 0:
                if target:
                    target.close()
                if csv_path and csv_path.exists():
                    csv_path.unlink()
                return
            if target:
                target.flush()
                os.fsync(target.fileno())
                target.close()
            assert csv_path is not None
            base_record = {
                "shard_id": shard_id,
                "rows": rows,
                "row_start": order_start,
                "row_end_exclusive": order_start + rows,
                "order_offset": order_start,
                "first_event_id": first_event,
                "last_event_id": last_event,
                "first_block_number": first_block,
                "last_block_number": last_block,
            }
            old = completed.get(shard_id)
            if old is not None:
                for key, value in base_record.items():
                    if str(old.get(key)) != str(value):
                        raise ValueError(
                            f"resume manifest/source mismatch at shard {shard_id}, {key}: "
                            f"{old.get(key)!r} != {value!r}"
                        )
                store_path = stores_dir / old["store_relative"]
                metrics_path = stores_dir / old["metrics_relative"]
                if not store_path.is_file() or not metrics_path.is_file():
                    raise FileNotFoundError(f"completed shard artifact missing: {store_path}")
                record = old
                print(json.dumps({"shard": shard_id, "status": "resume-skip", "rows": rows}), flush=True)
            else:
                store_path = shard_store_dir / f"shard_{shard_id:05d}.mbft"
                metrics_path = metrics_dir / f"shard_{shard_id:05d}.build.json"
                cmd = [
                    str(exe), "build-store",
                    "--events", str(csv_path),
                    "--store", str(store_path),
                    "--metrics", str(metrics_path),
                    "--order-offset", str(order_start),
                    "--combine", str(args.combine),
                    "--fp-rate", str(args.fp_rate),
                    "--seed", str(args.seed),
                    "--tweak", str(args.tweak),
                ]
                proc, stdout, stderr = run_monitored(
                    cmd,
                    cwd=exe.parent,
                    timeout_s=args.timeout_per_shard_sec,
                    max_rss_mb=args.max_rss_mb,
                )
                (log_dir / f"shard_{shard_id:05d}.stdout.txt").write_text(stdout, encoding="utf-8")
                (log_dir / f"shard_{shard_id:05d}.stderr.txt").write_text(stderr, encoding="utf-8")
                metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
                built_rows = int(metrics.get("build", {}).get("event_count", -1))
                built_offset = int(metrics.get("build", {}).get("order_offset", -1))
                if built_rows != rows or built_offset != order_start:
                    raise RuntimeError(
                        f"shard build metrics mismatch: rows {built_rows}/{rows}, "
                        f"offset {built_offset}/{order_start}"
                    )
                record = {
                    **base_record,
                    "store_relative": str(store_path.relative_to(stores_dir)),
                    "metrics_relative": str(metrics_path.relative_to(stores_dir)),
                    "store_bytes": store_path.stat().st_size,
                    "store_sha256": sha256_file(store_path),
                    "build_process_wall_s": proc["wall_s"],
                    "build_peak_tree_rss_mb": proc["peak_tree_rss_mb"],
                    "csv_load_time_us": float(metrics["build"].get("csv_load_time_us", 0.0)),
                    "mbft_build_time_us": float(metrics["build"].get("mbft_build_time_us", 0.0)),
                    "store_save_time_us": float(metrics.get("store_save_time_us", 0.0)),
                    "persistent": metrics.get("persistent", {}),
                }
                print(json.dumps({"shard": shard_id, "status": "built", "rows": rows,
                                  "store_bytes": record["store_bytes"]}), flush=True)
            shard_records.append(record)
            manifest["shards"] = shard_records
            manifest["event_count"] = sum(int(x["rows"]) for x in shard_records)
            manifest["shard_count"] = len(shard_records)
            manifest["status"] = "building"
            manifest["elapsed_s"] = time.perf_counter() - started
            atomic_write_json(manifest_path, manifest)
            if csv_path.exists() and not args.keep_shard_csv:
                csv_path.unlink()
            target = None
            writer = None

        start_shard()
        for row in reader:
            if len(row) != len(header):
                raise ValueError(f"malformed CSV row near global row {global_rows}")
            key = order_key(row, col)
            if previous_key is not None and key < previous_key:
                raise ValueError(
                    "canonical CSV is not globally sorted by "
                    "(block_time, block_number, tx_hash, local_order, event_id)"
                )
            previous_key = key
            block = row[col["block_number"]]
            if rows >= args.shard_rows and block != current_block:
                finish_shard()
                start_shard()
            if writer is not None:
                writer.writerow([row[col[name]] for name in MBFT_COLUMNS])
            event_id = row[col["event_id"]]
            if rows == 0:
                first_event = event_id
                first_block = block
            last_event = event_id
            last_block = block
            current_block = block
            rows += 1
            global_rows += 1
        finish_shard()

    if temp_dir.exists() and not args.keep_shard_csv:
        shutil.rmtree(temp_dir, ignore_errors=True)
    manifest["status"] = "success"
    manifest["event_count"] = global_rows
    manifest["shard_count"] = len(shard_records)
    manifest["total_store_bytes"] = sum(int(x["store_bytes"]) for x in shard_records)
    manifest["build_process_wall_s_sum"] = sum(float(x.get("build_process_wall_s", 0.0)) for x in shard_records)
    manifest["build_peak_tree_rss_mb_max"] = max(
        (float(x.get("build_peak_tree_rss_mb", 0.0)) for x in shard_records), default=0.0
    )
    manifest["csv_load_time_us_sum"] = sum(float(x.get("csv_load_time_us", 0.0)) for x in shard_records)
    manifest["mbft_build_time_us_sum"] = sum(float(x.get("mbft_build_time_us", 0.0)) for x in shard_records)
    manifest["store_save_time_us_sum"] = sum(float(x.get("store_save_time_us", 0.0)) for x in shard_records)
    manifest["wall_s"] = time.perf_counter() - started
    atomic_write_json(manifest_path, manifest)
    print(json.dumps({"status": "success", "manifest": str(manifest_path),
                      "shards": len(shard_records), "events": global_rows,
                      "total_store_bytes": manifest["total_store_bytes"]}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr, flush=True)
        raise
