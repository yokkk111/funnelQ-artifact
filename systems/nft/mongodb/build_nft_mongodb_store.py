#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

from pymongo import ASCENDING, MongoClient

from systems.nft.common.nft_semantics import RUNTIME_COLUMNS, load_event_atoms


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the NFT MongoDB document baseline.")
    parser.add_argument("--event-atoms-csv", required=True, type=Path)
    parser.add_argument("--report-json", required=True, type=Path)
    parser.add_argument("--uri", default=os.getenv("MONGODB_URI", "mongodb://mongo:27017"))
    parser.add_argument("--database", default=os.getenv("MONGODB_DATABASE", "funnelq"))
    parser.add_argument("--batch-rows", type=int, default=25_000)
    args = parser.parse_args()

    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    wall0 = time.perf_counter()
    dataframe = load_event_atoms(args.event_atoms_csv)
    loaded_at = time.perf_counter()

    client = MongoClient(args.uri, connectTimeoutMS=30_000, serverSelectionTimeoutMS=30_000)
    database = client[args.database]
    collection_name = "nft_events"
    database.drop_collection(collection_name)
    collection = database.create_collection(collection_name)

    integer_columns = {"block_ts", "order_seq", "nft_pos"}
    batch: list[dict[str, Any]] = []
    inserted = 0
    insert0 = time.perf_counter()
    for values in dataframe[RUNTIME_COLUMNS].itertuples(index=False, name=None):
        document = {
            column: (int(value) if column in integer_columns else str(value))
            for column, value in zip(RUNTIME_COLUMNS, values)
        }
        batch.append(document)
        if len(batch) >= args.batch_rows:
            collection.insert_many(batch, ordered=False)
            inserted += len(batch)
            batch.clear()
    if batch:
        collection.insert_many(batch, ordered=False)
        inserted += len(batch)
    inserted_at = time.perf_counter()

    indexes = [
        ("idx_mongo_event_type", [("event_type", ASCENDING)]),
        (
            "idx_mongo_type_pair_collection_nft_time",
            [
                ("event_type", ASCENDING), ("move_from", ASCENDING), ("move_to", ASCENDING),
                ("collection_contract", ASCENDING), ("nft", ASCENDING),
                ("block_ts", ASCENDING), ("order_seq", ASCENDING),
            ],
        ),
        (
            "idx_mongo_type_from_collection_nft_time",
            [
                ("event_type", ASCENDING), ("move_from", ASCENDING),
                ("collection_contract", ASCENDING), ("nft", ASCENDING),
                ("block_ts", ASCENDING), ("order_seq", ASCENDING),
            ],
        ),
        (
            "idx_mongo_type_from_collection_time",
            [
                ("event_type", ASCENDING), ("move_from", ASCENDING),
                ("collection_contract", ASCENDING), ("block_ts", ASCENDING),
                ("order_seq", ASCENDING), ("nft", ASCENDING),
            ],
        ),
        (
            "idx_mongo_type_to_collection_nft_time",
            [
                ("event_type", ASCENDING), ("move_to", ASCENDING),
                ("collection_contract", ASCENDING), ("nft", ASCENDING),
                ("block_ts", ASCENDING), ("order_seq", ASCENDING),
            ],
        ),
        (
            "idx_mongo_type_to_collection_time",
            [
                ("event_type", ASCENDING), ("move_to", ASCENDING),
                ("collection_contract", ASCENDING), ("block_ts", ASCENDING),
                ("order_seq", ASCENDING), ("nft", ASCENDING),
            ],
        ),
    ]
    for name, keys in indexes:
        collection.create_index(keys, name=name)
    indexed_at = time.perf_counter()

    collection_stats = database.command("collStats", collection_name)
    database_stats = database.command("dbStats", 1)
    server_info = client.server_info()
    report = {
        "system": "MongoDB",
        "engine": "MongoDB aggregation-pipeline full-instance baseline",
        "server_version": server_info.get("version", ""),
        "event_atoms_csv": str(args.event_atoms_csv),
        "event_rows": int(inserted),
        "event_type_counts": {
            str(key): int(value) for key, value in dataframe["event_type"].value_counts().to_dict().items()
        },
        "collection": collection_name,
        "index_names": sorted(collection.index_information()),
        "collection_logical_size_bytes": int(collection_stats.get("size", 0)),
        "collection_storage_size_bytes": int(collection_stats.get("storageSize", 0)),
        "total_index_size_bytes": int(collection_stats.get("totalIndexSize", 0)),
        "database_storage_size_bytes": int(database_stats.get("storageSize", 0)),
        "database_index_size_bytes": int(database_stats.get("indexSize", 0)),
        "timing_s": {
            "load_event_atoms_s": loaded_at - wall0,
            "insert_s": inserted_at - insert0,
            "create_indexes_s": indexed_at - inserted_at,
            "total_s": indexed_at - wall0,
        },
        "build_settings": {"batch_rows": int(args.batch_rows)},
    }
    args.report_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
