from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping

from . import PROTOCOL_VERSION
from .memory import peak_rss_mb
from .merkle import layers_from_leaves, parent_hash
from .protocol import (
    SIGNATURE_FIELDS,
    anchor_directory_leaf_hash,
    anchor_leaf_hash,
    closure_domain_leaf_hash,
    epoch_root,
    event_leaf,
    file_sha256,
    event_leaf_hash,
    hash_obj,
    manifest_root,
    tx_leaf_hash,
)

ANCHOR_COLUMNS = (
    "position",
    "closure_id",
    "anchor_order_seq",
    "start_ts",
    "end_ts",
    "end_order_seq",
    "collection_id",
    "transition_count",
    "addr_count",
    "nft_count",
    "nft_ge2_count",
    "b_out_event_count",
    "root_event_type_id",
    *SIGNATURE_FIELDS,
)

CLOSURE_COLUMNS = ("closure_position", "is_anchor", *ANCHOR_COLUMNS[1:])


def closure_descriptor_dict(row: Mapping[str, Any]) -> dict[str, int]:
    return {key: int(row[key]) for key in CLOSURE_COLUMNS}


ANCHOR_AGG_FIELDS = (
    "count",
    "start_position",
    "end_position",
    "max_transition_count",
    "max_addr_count",
    "max_nft_count",
    "min_transition_count",
    "min_addr_count",
    "min_nft_count",
    *("min_" + field for field in SIGNATURE_FIELDS),
)


def descriptor_dict(row: Mapping[str, Any]) -> dict[str, int]:
    return {key: int(row[key]) for key in ANCHOR_COLUMNS}


def leaf_aggregate(row: Mapping[str, Any]) -> dict[str, int]:
    return {
        "count": 1,
        "start_position": int(row["position"]),
        "end_position": int(row["position"]),
        "max_transition_count": int(row["transition_count"]),
        "max_addr_count": int(row["addr_count"]),
        "max_nft_count": int(row["nft_count"]),
        "min_transition_count": int(row["transition_count"]),
        "min_addr_count": int(row["addr_count"]),
        "min_nft_count": int(row["nft_count"]),
        **{f"min_{field}": int(row[field]) for field in SIGNATURE_FIELDS},
    }


def parent_aggregate(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, int]:
    return {
        "count": int(left["count"]) + int(right["count"]),
        "start_position": int(left["start_position"]),
        "end_position": int(right["end_position"]),
        "max_transition_count": max(
            int(left["max_transition_count"]), int(right["max_transition_count"])
        ),
        "max_addr_count": max(int(left["max_addr_count"]), int(right["max_addr_count"])),
        "max_nft_count": max(int(left["max_nft_count"]), int(right["max_nft_count"])),
        "min_transition_count": min(
            int(left["min_transition_count"]), int(right["min_transition_count"])
        ),
        "min_addr_count": min(int(left["min_addr_count"]), int(right["min_addr_count"])),
        "min_nft_count": min(int(left["min_nft_count"]), int(right["min_nft_count"])),
        **{
            f"min_{field}": min(int(left[f"min_{field}"]), int(right[f"min_{field}"]))
            for field in SIGNATURE_FIELDS
        },
    }


def anchor_node_hash(
    level: int,
    index: int,
    aggregate: Mapping[str, Any],
    left_hash: str,
    right_hash: str,
) -> str:
    return hash_obj(
        "anchor-summary-node-v1",
        {
            "level": int(level),
            "index": int(index),
            "aggregate": {key: int(aggregate[key]) for key in ANCHOR_AGG_FIELDS},
            "left": left_hash,
            "right": right_hash,
        },
    )


def _create_index_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
        CREATE TABLE closures(
          closure_position INTEGER PRIMARY KEY,
          is_anchor INTEGER NOT NULL CHECK(is_anchor IN (0,1)),
          closure_id INTEGER NOT NULL UNIQUE,
          anchor_order_seq INTEGER NOT NULL UNIQUE,
          start_ts INTEGER NOT NULL,end_ts INTEGER NOT NULL,end_order_seq INTEGER NOT NULL,
          collection_id INTEGER NOT NULL,
          transition_count INTEGER NOT NULL,addr_count INTEGER NOT NULL,nft_count INTEGER NOT NULL,
          nft_ge2_count INTEGER NOT NULL,b_out_event_count INTEGER NOT NULL,
          root_event_type_id INTEGER NOT NULL,
          l2_ba_nft_equal_min_delta INTEGER NOT NULL,
          l2_ba_nft_different_min_delta INTEGER NOT NULL,
          l3_bcca_all_equal_min_delta INTEGER NOT NULL,
          l3_bcca_all_distinct_min_delta INTEGER NOT NULL,
          l3_bcca_root_back_equal_min_delta INTEGER NOT NULL,
          leaf_hash TEXT NOT NULL
        );
        CREATE TABLE closure_nodes(
          level INTEGER NOT NULL,node_index INTEGER NOT NULL,node_hash TEXT NOT NULL,
          PRIMARY KEY(level,node_index)
        ) WITHOUT ROWID;
        CREATE TABLE anchors(
          position INTEGER PRIMARY KEY,
          closure_id INTEGER NOT NULL UNIQUE,
          anchor_order_seq INTEGER NOT NULL UNIQUE,
          start_ts INTEGER NOT NULL,end_ts INTEGER NOT NULL,end_order_seq INTEGER NOT NULL,
          collection_id INTEGER NOT NULL,
          transition_count INTEGER NOT NULL,addr_count INTEGER NOT NULL,nft_count INTEGER NOT NULL,
          nft_ge2_count INTEGER NOT NULL,b_out_event_count INTEGER NOT NULL,
          root_event_type_id INTEGER NOT NULL,
          l2_ba_nft_equal_min_delta INTEGER NOT NULL,
          l2_ba_nft_different_min_delta INTEGER NOT NULL,
          l3_bcca_all_equal_min_delta INTEGER NOT NULL,
          l3_bcca_all_distinct_min_delta INTEGER NOT NULL,
          l3_bcca_root_back_equal_min_delta INTEGER NOT NULL,
          leaf_hash TEXT NOT NULL
        );
        CREATE TABLE anchor_nodes(
          level INTEGER NOT NULL,node_index INTEGER NOT NULL,
          count INTEGER NOT NULL,start_position INTEGER NOT NULL,end_position INTEGER NOT NULL,
          max_transition_count INTEGER NOT NULL,max_addr_count INTEGER NOT NULL,max_nft_count INTEGER NOT NULL,
          min_transition_count INTEGER NOT NULL,min_addr_count INTEGER NOT NULL,min_nft_count INTEGER NOT NULL,
          min_l2_ba_nft_equal_min_delta INTEGER NOT NULL,
          min_l2_ba_nft_different_min_delta INTEGER NOT NULL,
          min_l3_bcca_all_equal_min_delta INTEGER NOT NULL,
          min_l3_bcca_all_distinct_min_delta INTEGER NOT NULL,
          min_l3_bcca_root_back_equal_min_delta INTEGER NOT NULL,
          node_hash TEXT NOT NULL,
          PRIMARY KEY(level,node_index)
        ) WITHOUT ROWID;
        CREATE TABLE anchor_directory_nodes(
          level INTEGER NOT NULL,node_index INTEGER NOT NULL,node_hash TEXT NOT NULL,
          PRIMARY KEY(level,node_index)
        ) WITHOUT ROWID;
        CREATE TABLE event_catalog(
          collection_id INTEGER NOT NULL,position INTEGER NOT NULL,order_seq INTEGER NOT NULL UNIQUE,
          block_ts INTEGER NOT NULL,leaf_hash TEXT NOT NULL,PRIMARY KEY(collection_id,position)
        ) WITHOUT ROWID;
        CREATE INDEX idx_event_catalog_collection_time_order
          ON event_catalog(collection_id,block_ts,order_seq);
        CREATE TABLE event_nodes(
          collection_id INTEGER NOT NULL,level INTEGER NOT NULL,node_index INTEGER NOT NULL,node_hash TEXT NOT NULL,
          PRIMARY KEY(collection_id,level,node_index)
        ) WITHOUT ROWID;
        CREATE TABLE collection_directory(
          position INTEGER PRIMARY KEY,collection_id INTEGER NOT NULL UNIQUE,event_count INTEGER NOT NULL,
          global_start_position INTEGER NOT NULL,first_order INTEGER NOT NULL,last_order INTEGER NOT NULL,
          event_root TEXT NOT NULL,leaf_hash TEXT NOT NULL
        );
        CREATE TABLE collection_directory_nodes(
          level INTEGER NOT NULL,node_index INTEGER NOT NULL,node_hash TEXT NOT NULL,
          PRIMARY KEY(level,node_index)
        ) WITHOUT ROWID;
        CREATE TABLE tx_nodes(
          level INTEGER NOT NULL,node_index INTEGER NOT NULL,node_hash TEXT NOT NULL,
          PRIMARY KEY(level,node_index)
        ) WITHOUT ROWID;
        """
    )


def _insert_simple_layers(
    conn: sqlite3.Connection,
    table: str,
    leaves: list[str],
    domain: str,
    prefix: tuple[Any, ...] = (),
) -> str:
    layers = layers_from_leaves(leaves, domain)
    if prefix:
        sql = f"INSERT INTO {table} VALUES ({','.join('?' for _ in range(len(prefix) + 3))})"
        for level, hashes in enumerate(layers):
            conn.executemany(
                sql, (prefix + (level, index, value) for index, value in enumerate(hashes))
            )
    else:
        sql = f"INSERT INTO {table} VALUES (?,?,?)"
        for level, hashes in enumerate(layers):
            conn.executemany(sql, ((level, index, value) for index, value in enumerate(hashes)))
    return layers[-1][0]


def _finish_disk_merkle(
    conn: sqlite3.Connection,
    table: str,
    leaf_count: int,
    domain: str,
    *,
    where_prefix: tuple[Any, ...] = (),
) -> str:
    if leaf_count <= 0:
        return hash_obj(domain, {"empty": True})
    level = 0
    width = leaf_count
    prefix_where = ""
    prefix_values: tuple[Any, ...] = ()
    if where_prefix:
        prefix_where = "collection_id=? AND "
        prefix_values = where_prefix
    while width > 1:
        cursor = conn.execute(
            f"SELECT node_hash FROM {table} WHERE {prefix_where}level=? ORDER BY node_index",
            prefix_values + (level,),
        )
        node_index = 0
        while True:
            left = cursor.fetchone()
            if left is None:
                break
            right = cursor.fetchone() or left
            value = parent_hash(domain, str(left[0]), str(right[0]))
            if where_prefix:
                conn.execute(
                    f"INSERT INTO {table} VALUES (?,?,?,?)",
                    where_prefix + (level + 1, node_index, value),
                )
            else:
                conn.execute(
                    f"INSERT INTO {table} VALUES (?,?,?)",
                    (level + 1, node_index, value),
                )
            node_index += 1
        level += 1
        width = (width + 1) // 2
    return str(
        conn.execute(
            f"SELECT node_hash FROM {table} WHERE {prefix_where}level=? AND node_index=0",
            prefix_values + (level,),
        ).fetchone()[0]
    )


def _build_closure_tree(index: sqlite3.Connection, descriptors: Path) -> tuple[str, int]:
    source = sqlite3.connect(f"file:{Path(descriptors).as_posix()}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    leaf_count = 0
    for row in source.execute("SELECT * FROM formal_closure_descriptor ORDER BY closure_position"):
        item = closure_descriptor_dict(row)
        if item["closure_position"] != leaf_count:
            raise ValueError("closure descriptor positions are not contiguous")
        leaf_hash = closure_domain_leaf_hash(item)
        index.execute(
            "INSERT INTO closures VALUES (" + ",".join("?" for _ in range(len(CLOSURE_COLUMNS) + 1)) + ")",
            tuple(item[key] for key in CLOSURE_COLUMNS) + (leaf_hash,),
        )
        index.execute(
            "INSERT INTO closure_nodes VALUES (?,?,?)",
            (0, leaf_count, leaf_hash),
        )
        leaf_count += 1
    source.close()
    if leaf_count <= 0:
        raise ValueError("complete closure descriptor domain is empty")
    root = _finish_disk_merkle(
        index, "closure_nodes", leaf_count, "closure-domain-node-v1"
    )
    return root, leaf_count


def _build_anchor_trees(index: sqlite3.Connection, descriptors: Path) -> tuple[str, str, int]:
    source = sqlite3.connect(f"file:{Path(descriptors).as_posix()}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    leaf_count = 0
    for row in source.execute("SELECT * FROM formal_anchor_descriptor ORDER BY position"):
        item = descriptor_dict(row)
        if item["position"] != leaf_count:
            raise ValueError("descriptor positions are not a contiguous zero-based sequence")
        leaf_hash = anchor_leaf_hash(item)
        directory_hash = anchor_directory_leaf_hash(
            item["closure_id"], item["anchor_order_seq"]
        )
        index.execute(
            "INSERT INTO anchor_directory_nodes VALUES (?,?,?)",
            (0, item["position"], directory_hash),
        )
        index.execute(
            "INSERT INTO anchors VALUES (" + ",".join("?" for _ in range(19)) + ")",
            tuple(item[key] for key in ANCHOR_COLUMNS) + (leaf_hash,),
        )
        aggregate = leaf_aggregate(item)
        index.execute(
            "INSERT INTO anchor_nodes VALUES ("
            + ",".join("?" for _ in range(3 + len(ANCHOR_AGG_FIELDS)))
            + ")",
            (
                0,
                item["position"],
                *(aggregate[key] for key in ANCHOR_AGG_FIELDS),
                leaf_hash,
            ),
        )
        leaf_count += 1
    source.close()
    level = 0
    width = leaf_count
    while width > 1:
        cursor = index.execute(
            "SELECT * FROM anchor_nodes WHERE level=? ORDER BY node_index", (level,)
        )
        node_index = 0
        while True:
            left = cursor.fetchone()
            if left is None:
                break
            fetched_right = cursor.fetchone()
            right = fetched_right or left
            left_agg = {key: left[2 + i] for i, key in enumerate(ANCHOR_AGG_FIELDS)}
            right_agg = {key: right[2 + i] for i, key in enumerate(ANCHOR_AGG_FIELDS)}
            aggregate = (
                parent_aggregate(left_agg, right_agg)
                if fetched_right is not None
                else left_agg
            )
            node_hash = anchor_node_hash(
                level + 1, node_index, aggregate, str(left[-1]), str(right[-1])
            )
            index.execute(
                "INSERT INTO anchor_nodes VALUES ("
                + ",".join("?" for _ in range(3 + len(ANCHOR_AGG_FIELDS)))
                + ")",
                (
                    level + 1,
                    node_index,
                    *(aggregate[key] for key in ANCHOR_AGG_FIELDS),
                    node_hash,
                ),
            )
            node_index += 1
        level += 1
        width = (width + 1) // 2
    summary_root = str(
        index.execute(
            "SELECT node_hash FROM anchor_nodes ORDER BY level DESC,node_index LIMIT 1"
        ).fetchone()[0]
    )
    directory_root = _finish_disk_merkle(
        index,
        "anchor_directory_nodes",
        leaf_count,
        "anchor-directory-node-v1",
    )
    return directory_root, summary_root, leaf_count


def _event_query() -> str:
    return """
      SELECT e.order_seq,e.event_id,e.event_type_id,t.event_type,e.block_ts,
             e.src_addr_id,sa.value_text,e.dst_addr_id,da.value_text,
             e.nft_id,nv.value_text,e.collection_id,cv.value_text,
             x.tx_id,h.tx_hash
      FROM nft_role_transition_edges e
      JOIN nft_event_type_dict t USING(event_type_id)
      JOIN nft_role_value_dict sa ON sa.value_kind='address' AND sa.value_id=e.src_addr_id
      JOIN nft_role_value_dict da ON da.value_kind='address' AND da.value_id=e.dst_addr_id
      JOIN nft_role_value_dict nv ON nv.value_kind='nft' AND nv.value_id=e.nft_id
      JOIN nft_role_value_dict cv ON cv.value_kind='collection' AND cv.value_id=e.collection_id
      JOIN txdb.nft_event_tx_ids x USING(order_seq)
      JOIN txdb.nft_tx_hash_dict h USING(tx_id)
      ORDER BY e.collection_id,e.order_seq
    """


def _event_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    keys = (
        "order_seq",
        "event_id",
        "event_type_id",
        "event_type",
        "block_ts",
        "src_addr_id",
        "src_addr",
        "dst_addr_id",
        "dst_addr",
        "nft_id",
        "nft",
        "collection_id",
        "collection",
        "tx_id",
        "tx_hash",
    )
    return event_leaf(dict(zip(keys, row)))


def _build_event_and_tx_trees(
    index: sqlite3.Connection, main_db: Path, tx_db: Path
) -> tuple[str, str, int, int]:
    """Build EventStream/TxBinding trees with bounded per-collection memory.

    Event leaves are inserted directly into the disk-backed Merkle table.  The
    previous implementation retained every semantic event of the current
    collection in a Python list, which could become the verifier-index build's
    dominant memory cost for a very large collection.
    """
    source = sqlite3.connect(f"file:{Path(main_db).as_posix()}?mode=ro", uri=True)
    source.execute(
        "ATTACH DATABASE ? AS txdb",
        (f"file:{Path(tx_db).as_posix()}?mode=ro",),
    )
    try:
        source_count, unique_event_ids = source.execute(
            "SELECT COUNT(*),COUNT(DISTINCT event_id) FROM nft_role_transition_edges"
        ).fetchone()
        if int(source_count) <= 0:
            raise ValueError("compact-v4 event stream is empty")
        if int(source_count) != int(unique_event_ids):
            raise ValueError("event_id is not globally unique")
        malformed_time_order = source.execute(
            "SELECT COUNT(*) FROM ("
            " SELECT block_ts,LAG(block_ts) OVER(PARTITION BY collection_id ORDER BY order_seq) AS previous_ts"
            " FROM nft_role_transition_edges"
            ") WHERE previous_ts IS NOT NULL AND block_ts<previous_ts"
        ).fetchone()[0]
        if int(malformed_time_order):
            raise ValueError("collection event timestamps are not monotone")

        current_collection: int | None = None
        current_count = 0
        current_first_order = 0
        current_last_order = 0
        current_global_start = 0
        collection_leaves: list[str] = []
        collection_position = 0
        event_count = 0

        def flush_collection() -> None:
            nonlocal collection_position, current_count
            if current_collection is None or current_count <= 0:
                return
            event_root = _finish_disk_merkle(
                index,
                "event_nodes",
                current_count,
                "collection-event-node-v1",
                where_prefix=(current_collection,),
            )
            directory_leaf = hash_obj(
                "collection-directory-leaf-v2",
                {
                    "collection_id": current_collection,
                    "event_count": current_count,
                    "global_start_position": current_global_start,
                    "first_order": current_first_order,
                    "last_order": current_last_order,
                    "event_root": event_root,
                },
            )
            index.execute(
                "INSERT INTO collection_directory VALUES (?,?,?,?,?,?,?,?)",
                (
                    collection_position,
                    current_collection,
                    current_count,
                    current_global_start,
                    current_first_order,
                    current_last_order,
                    event_root,
                    directory_leaf,
                ),
            )
            collection_leaves.append(directory_leaf)
            collection_position += 1
            current_count = 0

        for raw in source.execute(_event_query()):
            event = _event_from_row(raw)
            collection_id = int(event["collection_id"])
            if current_collection is None or collection_id != current_collection:
                flush_collection()
                current_collection = collection_id
                current_global_start = event_count
                current_first_order = int(event["order_seq"])
                current_last_order = int(event["order_seq"])
            leaf_hash = event_leaf_hash(event)
            position = current_count
            index.execute(
                "INSERT INTO event_catalog VALUES (?,?,?,?,?)",
                (collection_id, position, int(event["order_seq"]), int(event["block_ts"]), leaf_hash),
            )
            index.execute(
                "INSERT INTO event_nodes VALUES (?,?,?,?)",
                (collection_id, 0, position, leaf_hash),
            )
            index.execute(
                "INSERT INTO tx_nodes VALUES (?,?,?)",
                (
                    0,
                    event_count,
                    tx_leaf_hash(
                        int(event["order_seq"]),
                        int(event["tx_id"]),
                        str(event["tx_hash"]),
                    ),
                ),
            )
            current_count += 1
            current_last_order = int(event["order_seq"])
            event_count += 1
        flush_collection()
        if event_count != int(source_count):
            raise ValueError("event/tx/dictionary join does not cover the complete event stream")
    finally:
        source.close()

    event_stream_root = _insert_simple_layers(
        index,
        "collection_directory_nodes",
        collection_leaves,
        "collection-directory-node-v2",
    )
    tx_root = _finish_disk_merkle(index, "tx_nodes", event_count, "tx-binding-node-v1")
    return event_stream_root, tx_root, event_count, len(collection_leaves)


def _load_and_validate_descriptor_meta(
    main_db: Path, descriptor_db: Path
) -> tuple[dict[str, str], str, str]:
    main_db = Path(main_db)
    descriptor_db = Path(descriptor_db)
    main_hash = file_sha256(main_db)
    descriptor_hash = file_sha256(descriptor_db)
    desc = sqlite3.connect(f"file:{descriptor_db.as_posix()}?mode=ro", uri=True)
    try:
        tables = {
            str(row[0])
            for row in desc.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        user_tables = {name for name in tables if not name.startswith("sqlite_")}
        expected_tables = {
            "formal_closure_descriptor",
            "formal_anchor_descriptor",
            "descriptor_meta",
        }
        if user_tables != expected_tables:
            raise ValueError(
                f"descriptor database has a non-canonical table set: {sorted(user_tables)}"
            )
        anchor_columns = [
            str(row[1]) for row in desc.execute("PRAGMA table_info(formal_anchor_descriptor)")
        ]
        closure_columns = [
            str(row[1]) for row in desc.execute("PRAGMA table_info(formal_closure_descriptor)")
        ]
        if anchor_columns != list(ANCHOR_COLUMNS):
            raise ValueError("Anchor descriptor columns do not match the formal vocabulary")
        if closure_columns != list(CLOSURE_COLUMNS):
            raise ValueError("closure descriptor columns do not match the formal vocabulary")
        meta = {
            str(key): str(value)
            for key, value in desc.execute("SELECT key,value FROM descriptor_meta")
        }
        required = {
            "complete",
            "descriptor_vocabulary",
            "anchor_count",
            "closure_count",
            "source_db_sha256",
            "source_db_bytes",
            "builder_version",
        }
        if set(meta) != required:
            raise ValueError(
                f"descriptor metadata is not canonical: missing={sorted(required-set(meta))} "
                f"unknown={sorted(set(meta)-required)}"
            )
        if meta["complete"] != "true":
            raise ValueError("descriptor database is not complete")
        if meta["descriptor_vocabulary"] != "generic_role_nft_relation_min_delta_v1":
            raise ValueError("descriptor vocabulary mismatch")
        if meta["builder_version"] != "generic_descriptor_builder_v3":
            raise ValueError("descriptor builder version mismatch")
        if meta["source_db_sha256"].lower() != main_hash:
            raise ValueError("descriptor database was built from a different main database")
        if int(meta["source_db_bytes"]) != main_db.stat().st_size:
            raise ValueError("descriptor source database size mismatch")
        anchor_count = int(
            desc.execute("SELECT COUNT(*) FROM formal_anchor_descriptor").fetchone()[0]
        )
        closure_count = int(
            desc.execute("SELECT COUNT(*) FROM formal_closure_descriptor").fetchone()[0]
        )
        if anchor_count != int(meta["anchor_count"]):
            raise ValueError("Anchor descriptor row count differs from metadata")
        if closure_count != int(meta["closure_count"]):
            raise ValueError("closure descriptor row count differs from metadata")
        if anchor_count > closure_count:
            raise ValueError("Anchor descriptor count exceeds closure descriptor count")
    finally:
        desc.close()

    source = sqlite3.connect(f"file:{main_db.as_posix()}?mode=ro", uri=True)
    desc = sqlite3.connect(f"file:{descriptor_db.as_posix()}?mode=ro", uri=True)
    try:
        anchor_ids = {
            int(row[0]) for row in source.execute("SELECT closure_id FROM nft_anchor_directory")
        }
        source_rows = source.execute(
            "SELECT s.closure_id,s.anchor_order_seq,s.start_ts,s.end_ts,s.end_order_seq,"
            "s.collection_id,s.transition_count,s.addr_count,s.nft_count,s.nft_ge2_count,"
            "s.b_out_event_count,COALESCE(e.event_type_id,0) "
            "FROM nft_closure_summary s "
            "LEFT JOIN nft_role_transition_edges e ON e.order_seq=s.anchor_order_seq "
            "ORDER BY s.anchor_order_seq,s.closure_id"
        )
        descriptor_rows = desc.execute(
            "SELECT closure_position,is_anchor,closure_id,anchor_order_seq,start_ts,end_ts,"
            "end_order_seq,collection_id,transition_count,addr_count,nft_count,nft_ge2_count,"
            "b_out_event_count,root_event_type_id "
            "FROM formal_closure_descriptor ORDER BY closure_position"
        )
        closure_count = 0
        anchor_position = 0
        for left, right in zip(source_rows, descriptor_rows):
            closure_id = int(left[0])
            expected = (
                closure_count,
                1 if closure_id in anchor_ids else 0,
                *tuple(int(value) for value in left),
            )
            actual = tuple(int(value) for value in right)
            if actual != expected:
                raise ValueError(
                    f"closure descriptor/source mismatch at position {closure_count}: "
                    f"expected={expected} actual={actual}"
                )
            if expected[1]:
                anchor_row = desc.execute(
                    "SELECT position,closure_id,anchor_order_seq,start_ts,end_ts,end_order_seq,"
                    "collection_id,transition_count,addr_count,nft_count,nft_ge2_count,"
                    "b_out_event_count,root_event_type_id "
                    "FROM formal_anchor_descriptor WHERE position=?",
                    (anchor_position,),
                ).fetchone()
                expected_anchor = (anchor_position, *tuple(int(value) for value in left))
                if anchor_row is None or tuple(int(value) for value in anchor_row) != expected_anchor:
                    raise ValueError("Anchor descriptor is not the ordered closure-domain subset")
                anchor_position += 1
            closure_count += 1
        if source_rows.fetchone() is not None or descriptor_rows.fetchone() is not None:
            raise ValueError("descriptor/source closure counts differ")
        if closure_count != int(meta["closure_count"]):
            raise ValueError("descriptor/source closure streamed count mismatch")
        if anchor_position != int(meta["anchor_count"]) or anchor_position != len(anchor_ids):
            raise ValueError("descriptor/source Anchor streamed count mismatch")
    finally:
        source.close()
        desc.close()
    return meta, main_hash, descriptor_hash


def build_index(
    main_db: Path,
    tx_db: Path,
    descriptor_db: Path,
    out_dir: Path,
    *,
    force: bool = False,
) -> dict[str, Any]:
    started = time.perf_counter()
    main_db = Path(main_db)
    tx_db = Path(tx_db)
    descriptor_db = Path(descriptor_db)
    descriptor_meta, main_db_sha256, descriptor_db_sha256 = (
        _load_and_validate_descriptor_meta(main_db, descriptor_db)
    )
    tx_db_sha256 = file_sha256(tx_db)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    index_path = out_dir / "authenticated_index.sqlite"
    if index_path.exists():
        if not force:
            raise FileExistsError(f"{index_path} exists; pass force=True")
        index_path.unlink()
    index = sqlite3.connect(index_path)
    _create_index_schema(index)
    closure_root, closure_count = _build_closure_tree(index, descriptor_db)
    directory_root, summary_root, anchor_count = _build_anchor_trees(index, descriptor_db)
    index.commit()
    event_root, tx_root, event_count, collection_count = _build_event_and_tx_trees(
        index, main_db, tx_db
    )
    index.commit()
    anchor_height = int(
        index.execute("SELECT MAX(level) FROM anchor_nodes").fetchone()[0]
    )
    bounds_conn = sqlite3.connect(
        f"file:{Path(main_db).as_posix()}?mode=ro", uri=True
    )
    try:
        event_bounds = bounds_conn.execute(
            "SELECT MIN(order_seq),MAX(order_seq),MIN(block_ts),MAX(block_ts) "
            "FROM nft_role_transition_edges"
        ).fetchone()
        horizon_row = bounds_conn.execute(
            "SELECT COUNT(*),MIN(end_ts-start_ts),MAX(end_ts-start_ts),"
            "SUM(CASE WHEN end_order_seq<anchor_order_seq OR end_ts<start_ts THEN 1 ELSE 0 END) "
            "FROM nft_closure_summary"
        ).fetchone()
    finally:
        bounds_conn.close()
    if not event_bounds or any(value is None for value in event_bounds):
        raise ValueError("compact-v4 event stream is empty")
    if not horizon_row or int(horizon_row[0]) <= 0:
        raise ValueError("compact-v4 closure summary is empty")
    if int(horizon_row[3] or 0):
        raise ValueError("compact-v4 closure summary contains invalid order/time bounds")
    if int(horizon_row[1]) <= 0 or int(horizon_row[1]) != int(horizon_row[2]):
        raise ValueError("compact-v4 closure horizon is not one positive fixed duration")
    closure_horizon_seconds = int(horizon_row[1])
    segment_root = hash_obj(
        "segment-directory-v1",
        {
            "summaryRoot": summary_root,
            "leafCount": anchor_count,
            "treeHeight": anchor_height,
            "aggregation": {
                "max": ["transition_count", "addr_count", "nft_count"],
                "min": ["transition_count", "addr_count", "nft_count", *SIGNATURE_FIELDS],
            },
        },
    )
    manifest: dict[str, Any] = {
        "protocolVersion": PROTOCOL_VERSION,
        "schemaVersion": "compact_full_binding_v4_defi_runtime",
        "eventLeafVersion": "semantic_event_with_tx_v2",
        "anchorLeafVersion": "anchor_summary_descriptor_v1",
        "closureLeafVersion": "closure_summary_descriptor_v1",
        "descriptorVocabularyVersion": "generic_role_nft_relation_min_delta_v1",
        "dataRange": {
            "startOrderSeq": int(event_bounds[0]),
            "endOrderSeq": int(event_bounds[1]),
            "startTimestamp": int(event_bounds[2]),
            "endTimestamp": int(event_bounds[3]),
        },
        "systemParameters": {
            "closureHorizonSeconds": closure_horizon_seconds,
            "anchorDirectorySemantics": "root_receiver_has_later_outgoing_event",
            "eventOrdering": "collection_id_then_order_seq",
            "collectionTimestampsMonotone": True,
            "eventIdGloballyUnique": True,
        },
        "counts": {
            "eventCount": event_count,
            "anchorCount": anchor_count,
            "closureCount": closure_count,
            "collectionCount": collection_count,
        },
        "roots": {
            "anchorDirectoryRoot": directory_root,
            "summaryRoot": summary_root,
            "closureDomainRoot": closure_root,
            "segmentDirectoryRoot": segment_root,
            "eventStreamRoot": event_root,
            "txBindingRoot": tx_root,
        },
        "crossStructureContract": {
            "trustedBuilder": "deterministic_query_independent_ingest_v3",
            "anchorAndSummaryShareOrderedClosureMapping": True,
            "closureDomainContainsEveryClosure": True,
            "anchorSubsetDerivedFromClosureDomain": True,
            "eventLeavesEmbedTxBinding": True,
            "eventStreamRootDefinition": "merkle_root_of_collection_directory_whose_leaves_bind_collection_event_roots_and_global_tx_offsets",
            "txBindingRootDefinition": "global_collection_order_tx_leaf_merkle_tree_aligned_with_event_ranges",
            "queryDependentInputsAccepted": [],
            "descriptorSourceMainDbSha256": descriptor_meta["source_db_sha256"],
            "sourceMainDbSha256": main_db_sha256,
            "sourceTxDbSha256": tx_db_sha256,
            "descriptorDbSha256": descriptor_db_sha256,
            "descriptorComplete": True,
            "descriptorAnchorCount": anchor_count,
            "descriptorClosureCount": closure_count,
            "descriptorBuilderVersion": descriptor_meta["builder_version"],
        },
    }
    manifest["manifestRoot"] = manifest_root(manifest)
    manifest["epochRoot"] = epoch_root(manifest["manifestRoot"])
    manifest_path = out_dir / "epoch_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    index.execute(
        "INSERT INTO metadata VALUES('manifest_json',?)", (json.dumps(manifest),)
    )
    index.commit()
    index.close()
    metrics = {
        "status": "BUILT",
        "indexPath": str(index_path),
        "manifestPath": str(manifest_path),
        "epochRoot": manifest["epochRoot"],
        "indexBuildSeconds": time.perf_counter() - started,
        "descriptorBuildSeconds": None,
        "totalAuthenticatedIndexBuildSeconds": None,
        "indexBytes": index_path.stat().st_size,
        "descriptorBytes": descriptor_db.stat().st_size,
        "anchorCount": anchor_count,
        "closureCount": closure_count,
        "eventCount": event_count,
        "collectionCount": collection_count,
        "queryDependentInputsRead": False,
        "indexPeakRssMiB": peak_rss_mb(),
        "descriptorPeakRssMiB": None,
        "totalAuthenticatedIndexPeakRssMiB": None,
    }
    descriptor_metrics_path = descriptor_db.with_suffix(".metrics.json")
    if descriptor_metrics_path.is_file():
        descriptor_metrics = json.loads(descriptor_metrics_path.read_text(encoding="utf-8"))
        descriptor_seconds = float(descriptor_metrics["descriptorBuildSeconds"])
        metrics["descriptorBuildSeconds"] = descriptor_seconds
        metrics["totalAuthenticatedIndexBuildSeconds"] = (
            descriptor_seconds + float(metrics["indexBuildSeconds"])
        )
        descriptor_peak = descriptor_metrics.get("descriptorPeakRssMiB")
        metrics["descriptorPeakRssMiB"] = descriptor_peak
        if descriptor_peak is not None:
            metrics["totalAuthenticatedIndexPeakRssMiB"] = max(
                float(descriptor_peak), float(metrics["indexPeakRssMiB"])
            )
    (out_dir / "index_metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    return metrics
