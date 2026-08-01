#!/usr/bin/env python3
"""Create chronological NFT CSV prefixes without splitting Ethereum blocks."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(16 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, type=Path)
    ap.add_argument("--out-root", required=True, type=Path)
    ap.add_argument("--source-rows", required=True, type=int)
    ap.add_argument("--scales", default="20,40,60,80,100")
    ap.add_argument("--expected-source-sha256", default="")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    source = args.source.resolve()
    out_root = args.out_root.resolve()
    scales = sorted({int(x) for x in args.scales.split(",") if x.strip()})
    if not scales or scales[-1] > 100 or scales[0] <= 0:
        raise ValueError("scales must be integers in 1..100")
    source_sha = sha256_file(source)
    if args.expected_source_sha256 and source_sha.lower() != args.expected_source_sha256.lower():
        raise ValueError("source SHA-256 mismatch")

    final_manifest = out_root / "scale_dataset_manifest.json"
    if args.resume and final_manifest.is_file():
        previous = json.loads(final_manifest.read_text(encoding="utf-8"))
        if previous.get("status") == "success" and previous.get("source_sha256") == source_sha:
            missing = [x["csv"] for x in previous["datasets"] if not Path(x["csv"]).is_file()]
            if not missing:
                print(json.dumps(previous, ensure_ascii=False, indent=2))
                return 0

    targets = {s: max(1, math.ceil(args.source_rows * s / 100.0)) for s in scales}
    out_root.mkdir(parents=True, exist_ok=True)
    handles: dict[int, object] = {}
    writers: dict[int, csv.writer] = {}
    records: dict[int, dict[str, object]] = {}
    for scale in scales:
        scale_dir = out_root / f"scale_{scale:03d}"
        scale_dir.mkdir(parents=True, exist_ok=True)
        csv_path = scale_dir / "event_atoms_nft.csv"
        handle = csv_path.open("w", encoding="utf-8", newline="")
        handles[scale] = handle
        writers[scale] = csv.writer(handle, lineterminator="\n")
        records[scale] = {
            "scale_percent": scale,
            "target_rows": targets[scale],
            "actual_rows": 0,
            "first_block_number": None,
            "last_block_number": None,
            "first_block_time": None,
            "last_block_time": None,
            "csv": str(csv_path),
        }

    source_rows = 0
    active = set(scales)
    with source.open("r", encoding="utf-8", newline="") as src:
        reader = csv.reader(src)
        header = next(reader)
        col = {name: i for i, name in enumerate(header)}
        for required in ("block_number", "block_time"):
            if required not in col:
                raise ValueError(f"missing source column: {required}")
        for writer in writers.values():
            writer.writerow(header)

        previous_block: str | None = None
        for row in reader:
            source_rows += 1
            block = row[col["block_number"]]
            block_time = row[col["block_time"]]
            if previous_block is not None and block != previous_block:
                to_close = [s for s in active if int(records[s]["actual_rows"]) >= targets[s]]
                for scale in to_close:
                    handles[scale].flush()
                    os.fsync(handles[scale].fileno())
                    handles[scale].close()
                    active.remove(scale)
            for scale in list(active):
                writers[scale].writerow(row)
                record = records[scale]
                record["actual_rows"] = int(record["actual_rows"]) + 1
                if record["first_block_number"] is None:
                    record["first_block_number"] = block
                    record["first_block_time"] = block_time
                record["last_block_number"] = block
                record["last_block_time"] = block_time
            previous_block = block

    for scale in list(active):
        handles[scale].flush()
        os.fsync(handles[scale].fileno())
        handles[scale].close()

    if source_rows != args.source_rows:
        raise ValueError(f"source row count changed: expected {args.source_rows}, got {source_rows}")
    datasets = []
    for scale in scales:
        record = records[scale]
        csv_path = Path(str(record["csv"]))
        record["actual_fraction"] = int(record["actual_rows"]) / source_rows
        record["file_bytes"] = csv_path.stat().st_size
        record["sha256"] = sha256_file(csv_path)
        datasets.append(record)
    manifest = {
        "status": "success",
        "policy": "chronological row-count prefix extended through the target Ethereum block",
        "source": str(source),
        "source_rows": source_rows,
        "source_sha256": source_sha,
        "datasets": datasets,
    }
    atomic_json(final_manifest, manifest)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
