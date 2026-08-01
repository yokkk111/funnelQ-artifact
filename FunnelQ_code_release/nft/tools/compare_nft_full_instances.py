#!/usr/bin/env python3
"""Streaming exact comparison for canonical NFT full-instance result sets."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Iterator

CANONICAL_COLUMNS = [
    "query_id",
    "branch_id",
    "closure_id",
    "anchor_order",
    "instance_key",
    "event_bindings_json",
    "event_orders_json",
    "variable_bindings_json",
]


def iter_rows(path: Path) -> Iterator[tuple[str, dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)

        missing = [
            column
            for column in CANONICAL_COLUMNS
            if column not in (reader.fieldnames or [])
        ]
        if missing:
            raise ValueError(f"{path} is missing columns: {missing}")

        previous_key: str | None = None

        for row_number, raw in enumerate(reader, start=2):
            row = {
                column: str(raw.get(column, ""))
                for column in CANONICAL_COLUMNS
            }
            key = row["instance_key"]

            if not key:
                raise ValueError(
                    f"{path}:{row_number} has an empty instance_key"
                )

            if previous_key is not None:
                if key == previous_key:
                    raise ValueError(
                        f"{path}:{row_number} contains duplicate key {key}"
                    )
                if key < previous_key:
                    raise ValueError(
                        f"{path}:{row_number} is not sorted by instance_key"
                    )

            previous_key = key
            yield key, row


def summarize(path: Path) -> dict:
    digest = hashlib.sha256()
    count = 0

    for key, _ in iter_rows(path):
        digest.update(key.encode("ascii"))
        digest.update(b"\n")
        count += 1

    return {
        "path": str(path),
        "result_count": count,
        "duplicate_count": 0,
        "result_digest_sha256": digest.hexdigest(),
    }


def compare_pair(
    reference_path: Path,
    backend_path: Path,
) -> dict:
    reference = iter(iter_rows(reference_path))
    backend = iter(iter_rows(backend_path))

    ref_item = next(reference, None)
    backend_item = next(backend, None)

    missing_count = 0
    unexpected_count = 0
    payload_mismatch_count = 0

    missing_sample: list[str] = []
    unexpected_sample: list[str] = []
    payload_mismatch_sample: list[dict] = []

    while ref_item is not None or backend_item is not None:
        if ref_item is None:
            key, _ = backend_item
            unexpected_count += 1
            if len(unexpected_sample) < 20:
                unexpected_sample.append(key)
            backend_item = next(backend, None)
            continue

        if backend_item is None:
            key, _ = ref_item
            missing_count += 1
            if len(missing_sample) < 20:
                missing_sample.append(key)
            ref_item = next(reference, None)
            continue

        ref_key, ref_row = ref_item
        backend_key, backend_row = backend_item

        if ref_key == backend_key:
            if ref_row != backend_row:
                payload_mismatch_count += 1
                if len(payload_mismatch_sample) < 20:
                    differing = {
                        column: {
                            "reference": ref_row[column],
                            "backend": backend_row[column],
                        }
                        for column in CANONICAL_COLUMNS
                        if ref_row[column] != backend_row[column]
                    }
                    payload_mismatch_sample.append({
                        "instance_key": ref_key,
                        "differing_columns": differing,
                    })

            ref_item = next(reference, None)
            backend_item = next(backend, None)

        elif ref_key < backend_key:
            missing_count += 1
            if len(missing_sample) < 20:
                missing_sample.append(ref_key)
            ref_item = next(reference, None)

        else:
            unexpected_count += 1
            if len(unexpected_sample) < 20:
                unexpected_sample.append(backend_key)
            backend_item = next(backend, None)

    return {
        "missing_count": missing_count,
        "unexpected_count": unexpected_count,
        "payload_mismatch_count": payload_mismatch_count,
        "missing_sample": missing_sample,
        "unexpected_sample": unexpected_sample,
        "payload_mismatch_sample": payload_mismatch_sample,
        "all_pass": (
            missing_count == 0
            and unexpected_count == 0
            and payload_mismatch_count == 0
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "inputs",
        nargs="+",
        help="backend=instances.csv",
    )
    args = parser.parse_args()

    paths: dict[str, Path] = {}

    for item in args.inputs:
        if "=" not in item:
            raise ValueError(f"expected backend=path: {item}")

        backend, raw_path = item.split("=", 1)
        paths[backend] = Path(raw_path)

    names = list(paths)

    if len(names) < 2:
        raise ValueError("at least two backends are required")

    print("[compare] scanning canonical summaries", flush=True)

    summaries = {
        backend: summarize(path)
        for backend, path in paths.items()
    }

    reference = names[0]
    comparisons = []
    all_pass = True

    for backend in names[1:]:
        print(
            f"[compare] reference={reference} backend={backend}",
            flush=True,
        )

        result = compare_pair(
            paths[reference],
            paths[backend],
        )
        result = {
            "reference": reference,
            "backend": backend,
            **result,
        }
        comparisons.append(result)
        all_pass = all_pass and result["all_pass"]

    report = {
        "query": args.query,
        "result_semantics": "full_event_binding_v1",
        "comparison_mode": "streaming_exact_sorted_canonical_rows",
        "backends": summaries,
        "comparisons": comparisons,
        "all_pass": all_pass,
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if all_pass else 2


if __name__ == "__main__":
    raise SystemExit(main())
