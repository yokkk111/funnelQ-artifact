from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .coverage import VARIANTS
from .index import ANCHOR_COLUMNS, CLOSURE_COLUMNS, _event_from_row
from .merkle import verify_open_range, verify_proof
from .protocol import event_leaf_hash, hash_obj, tx_leaf_hash
from .transcript import base_pass, refined_pass

_MANIFEST_FIELDS = {
    "payloadVersion",
    "variant",
    "eventStreamRoot",
    "txBindingRoot",
    "collectionCount",
    "eventCount",
    "rangeCount",
    "replayCount",
    "rangesFile",
    "rangesSha256",
    "deduplicationContract",
    "boundaryContract",
    "payloadDigest",
}
_RANGE_FIELDS = {
    "rangeId",
    "collection",
    "leftPosition",
    "rightPosition",
    "events",
    "rangeFrontier",
    "txLeftPosition",
    "txRightPosition",
    "txRangeFrontier",
    "txLeafHashes",
}
_COLLECTION_FIELDS = {
    "directoryPosition",
    "collectionId",
    "eventCount",
    "globalStartPosition",
    "firstOrderSeq",
    "lastOrderSeq",
    "eventRoot",
    "directoryLeafHash",
    "directoryPath",
}


def _require_exact_fields(name: str, value: Mapping[str, Any], expected: set[str]) -> None:
    actual = set(value)
    if actual != expected:
        raise ValueError(
            f"{name} fields mismatch: missing={sorted(expected-actual)} "
            f"unknown={sorted(actual-expected)}"
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _height(count: int) -> int:
    height, width = 0, int(count)
    while width > 1:
        height += 1
        width = (width + 1) // 2
    return height


def _tree_node_hash(
    conn: sqlite3.Connection,
    table: str,
    level: int,
    index: int,
    *,
    collection_id: int | None = None,
) -> str | None:
    if collection_id is None:
        row = conn.execute(
            f"SELECT node_hash FROM {table} WHERE level=? AND node_index=?",
            (int(level), int(index)),
        ).fetchone()
    else:
        row = conn.execute(
            f"SELECT node_hash FROM {table} "
            "WHERE collection_id=? AND level=? AND node_index=?",
            (int(collection_id), int(level), int(index)),
        ).fetchone()
    return None if row is None else str(row[0])


def _range_frontier_db(
    conn: sqlite3.Connection,
    table: str,
    total: int,
    left: int,
    right: int,
    *,
    collection_id: int | None = None,
) -> list[dict[str, Any]]:
    height = _height(total)
    out: list[dict[str, Any]] = []

    def visit(level: int, index: int, start: int, span: int) -> None:
        end = min(total, start + span) - 1
        if end < left or start > right:
            value = _tree_node_hash(
                conn, table, level, index, collection_id=collection_id
            )
            if value is not None:
                out.append({"level": level, "index": index, "hash": value})
            return
        if left <= start and end <= right:
            return
        if level == 0:
            return
        half = span // 2
        visit(level - 1, index * 2, start, half)
        if start + half < total:
            visit(level - 1, index * 2 + 1, start + half, half)

    visit(height, 0, 0, 1 << height)
    return out


def _simple_path(
    conn: sqlite3.Connection, table: str, position: int, leaf_count: int
) -> list[dict[str, Any]]:
    if leaf_count <= 0 or position < 0 or position >= leaf_count:
        raise ValueError("invalid Merkle directory position")
    out: list[dict[str, Any]] = []
    index, width, level = int(position), int(leaf_count), 0
    while width > 1:
        sibling = index ^ 1
        duplicated = sibling >= width
        sibling_index = index if duplicated else sibling
        value = conn.execute(
            f"SELECT node_hash FROM {table} WHERE level=? AND node_index=?",
            (level, sibling_index),
        ).fetchone()
        if value is None:
            raise ValueError("Merkle directory path is incomplete")
        out.append(
            {
                "sibling": str(value[0]),
                "siblingOnLeft": (not duplicated) and sibling < index,
                "duplicated": duplicated,
            }
        )
        index //= 2
        width = (width + 1) // 2
        level += 1
    return out


def _merge_intervals(rows: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[list[int]] = []
    for left, right in sorted(rows):
        if not merged or left > merged[-1][1] + 1:
            merged.append([left, right])
        else:
            merged[-1][1] = max(merged[-1][1], right)
    return [(left, right) for left, right in merged]


def _source_event_query() -> str:
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
      WHERE e.collection_id=? AND e.order_seq BETWEEN ? AND ?
      ORDER BY e.order_seq
    """


def _anchor_from_closure(row: Mapping[str, Any]) -> dict[str, int]:
    return {
        "position": int(row.get("closure_position", row.get("position", 0))),
        **{key: int(row[key]) for key in ANCHOR_COLUMNS if key != "position"},
    }


def _iter_replay_descriptors(
    index: sqlite3.Connection,
    transcript: Mapping[str, Any],
    variant: str,
) -> Iterable[dict[str, int]]:
    if variant in {"full", "no_refined"}:
        index.row_factory = sqlite3.Row
        rows = index.execute("SELECT * FROM anchors ORDER BY collection_id,anchor_order_seq")
        for row in rows:
            descriptor = {key: int(row[key]) for key in ANCHOR_COLUMNS}
            if not base_pass(descriptor, transcript):
                continue
            if variant == "full" and not refined_pass(descriptor, transcript):
                continue
            yield descriptor
    else:
        index.row_factory = sqlite3.Row
        rows = index.execute("SELECT * FROM closures ORDER BY collection_id,anchor_order_seq")
        for row in rows:
            closure = {key: int(row[key]) for key in CLOSURE_COLUMNS}
            descriptor = _anchor_from_closure(closure)
            if variant == "no_anchor":
                if not closure["is_anchor"]:
                    continue
                if not base_pass(descriptor, transcript) or not refined_pass(
                    descriptor, transcript
                ):
                    continue
            yield descriptor


def build_residual_payload(
    index_path: Path,
    main_db: Path,
    tx_db: Path,
    transcript: Mapping[str, Any],
    out_dir: Path,
    event_stream_root: str,
    tx_binding_root: str,
    *,
    variant: str = "full",
) -> dict[str, Any]:
    if variant not in VARIANTS:
        raise ValueError(f"unsupported verification ablation variant: {variant}")
    index = sqlite3.connect(f"file:{Path(index_path).as_posix()}?mode=ro", uri=True)
    source = sqlite3.connect(f"file:{Path(main_db).as_posix()}?mode=ro", uri=True)
    source.execute(
        "ATTACH DATABASE ? AS txdb",
        (f"file:{Path(tx_db).as_posix()}?mode=ro",),
    )
    delta = int(transcript["refined"]["maxCompletionDeltaSeconds"])
    collection_count = int(
        index.execute("SELECT COUNT(*) FROM collection_directory").fetchone()[0]
    )
    total_event_count = int(index.execute("SELECT COUNT(*) FROM event_catalog").fetchone()[0])
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ranges_path = out_dir / "ranges.jsonl.gz"
    range_count = 0
    replay_count = 0
    unique_opened = 0

    def load_collection(collection_id: int) -> dict[str, Any]:
        directory = index.execute(
            "SELECT * FROM collection_directory WHERE collection_id=?",
            (int(collection_id),),
        ).fetchone()
        if directory is None:
            raise ValueError("replay collection is not authenticated")
        (
            directory_position,
            _collection_id,
            event_count,
            global_start_position,
            first_order,
            last_order,
            event_root,
            directory_leaf,
        ) = directory
        catalog = list(
            index.execute(
                "SELECT position,order_seq,block_ts FROM event_catalog "
                "WHERE collection_id=? ORDER BY position",
                (int(collection_id),),
            )
        )
        positions = [int(row[0]) for row in catalog]
        orders = [int(row[1]) for row in catalog]
        timestamps = [int(row[2]) for row in catalog]
        if positions != list(range(len(catalog))) or len(catalog) != int(event_count):
            raise ValueError("authenticated collection catalog positions are not canonical")
        return {
            "collection_id": int(collection_id),
            "directory_position": int(directory_position),
            "event_count": int(event_count),
            "global_start_position": int(global_start_position),
            "first_order": int(first_order),
            "last_order": int(last_order),
            "event_root": str(event_root),
            "directory_leaf": str(directory_leaf),
            "orders": orders,
            "timestamps": timestamps,
            "order_to_position": {
                order: position for position, order in enumerate(orders)
            },
        }

    def descriptor_interval(
        context: Mapping[str, Any], descriptor: Mapping[str, Any]
    ) -> tuple[int, int]:
        anchor_order = int(descriptor["anchor_order_seq"])
        left = context["order_to_position"].get(anchor_order)
        if left is None:
            raise ValueError("replay anchor is absent from authenticated event catalog")
        timestamps = context["timestamps"]
        orders = context["orders"]
        if int(timestamps[left]) != int(descriptor["start_ts"]):
            raise ValueError("replay anchor timestamp differs from authenticated summary")
        deadline = min(int(descriptor["start_ts"]) + delta, int(descriptor["end_ts"]))
        by_time = bisect_right(timestamps, deadline) - 1
        by_order = bisect_right(orders, int(descriptor["end_order_seq"])) - 1
        match_right = min(by_time, by_order)
        if match_right < left:
            raise ValueError("query replay window does not include its own anchor")
        event_count = int(context["event_count"])
        open_right = match_right + 1 if match_right + 1 < event_count else match_right
        return int(left), int(open_right)

    def emit_collection(
        handle: Any,
        context: Mapping[str, Any],
        intervals: Sequence[tuple[int, int]],
    ) -> None:
        nonlocal range_count, unique_opened
        collection_id = int(context["collection_id"])
        event_count = int(context["event_count"])
        global_start_position = int(context["global_start_position"])
        orders = context["orders"]
        for merged_left, merged_right in intervals:
            left_order, right_order = orders[merged_left], orders[merged_right]
            events = [
                _event_from_row(row)
                for row in source.execute(
                    _source_event_query(), (collection_id, left_order, right_order)
                )
            ]
            catalog_rows = list(
                index.execute(
                    "SELECT position,order_seq,leaf_hash FROM event_catalog "
                    "WHERE collection_id=? AND position BETWEEN ? AND ? ORDER BY position",
                    (collection_id, merged_left, merged_right),
                )
            )
            if len(events) != len(catalog_rows):
                raise ValueError("source range and authenticated catalog length differ")
            opened: list[dict[str, Any]] = []
            tx_hashes: list[str] = []
            for event, (position, order_seq, expected_hash) in zip(events, catalog_rows):
                if (
                    int(event["order_seq"]) != int(order_seq)
                    or event_leaf_hash(event) != expected_hash
                ):
                    raise ValueError("source event differs from committed event leaf")
                opened.append({"position": int(position), "event": event})
                tx_hashes.append(
                    tx_leaf_hash(
                        int(event["order_seq"]),
                        int(event["tx_id"]),
                        str(event["tx_hash"]),
                    )
                )
            tx_left = global_start_position + merged_left
            tx_right = global_start_position + merged_right
            row = {
                "rangeId": range_count,
                "collection": {
                    "directoryPosition": int(context["directory_position"]),
                    "collectionId": collection_id,
                    "eventCount": event_count,
                    "globalStartPosition": global_start_position,
                    "firstOrderSeq": int(context["first_order"]),
                    "lastOrderSeq": int(context["last_order"]),
                    "eventRoot": str(context["event_root"]),
                    "directoryLeafHash": str(context["directory_leaf"]),
                    "directoryPath": _simple_path(
                        index,
                        "collection_directory_nodes",
                        int(context["directory_position"]),
                        collection_count,
                    ),
                },
                "leftPosition": merged_left,
                "rightPosition": merged_right,
                "events": opened,
                "rangeFrontier": _range_frontier_db(
                    index,
                    "event_nodes",
                    event_count,
                    merged_left,
                    merged_right,
                    collection_id=collection_id,
                ),
                "txLeftPosition": tx_left,
                "txRightPosition": tx_right,
                "txRangeFrontier": _range_frontier_db(
                    index,
                    "tx_nodes",
                    total_event_count,
                    tx_left,
                    tx_right,
                ),
                "txLeafHashes": tx_hashes,
            }
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            range_count += 1
            unique_opened += len(opened)

    current_context: dict[str, Any] | None = None
    current_intervals: list[tuple[int, int]] = []
    with gzip.open(
        ranges_path, "wt", encoding="utf-8", newline="\n", compresslevel=6
    ) as handle:
        for descriptor in _iter_replay_descriptors(index, transcript, variant):
            collection_id = int(descriptor["collection_id"])
            if current_context is None or collection_id != int(
                current_context["collection_id"]
            ):
                if current_context is not None:
                    emit_collection(handle, current_context, current_intervals)
                current_context = load_collection(collection_id)
                current_intervals = []
            left, right = descriptor_interval(current_context, descriptor)
            if not current_intervals or left > current_intervals[-1][1] + 1:
                current_intervals.append((left, right))
            else:
                old_left, old_right = current_intervals[-1]
                current_intervals[-1] = (old_left, max(old_right, right))
            replay_count += 1
        if current_context is not None:
            emit_collection(handle, current_context, current_intervals)

    manifest = {
        "payloadVersion": "derived_complete_collection_ranges_v3",
        "variant": variant,
        "eventStreamRoot": event_stream_root,
        "txBindingRoot": tx_binding_root,
        "collectionCount": collection_count,
        "eventCount": total_event_count,
        "rangeCount": range_count,
        "replayCount": replay_count,
        "rangesFile": ranges_path.name,
        "rangesSha256": _sha256_file(ranges_path),
        "deduplicationContract": "opened_collection_positions_are_disjoint_canonical_merged_ranges",
        "boundaryContract": "verifier_derives_each_replay_window_and_requires_immediate_successor_or_collection_end",
    }
    manifest["payloadDigest"] = hash_obj("residual-manifest-v3", manifest)
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    index.close()
    source.close()
    return {
        "rangeCount": range_count,
        "replayCount": replay_count,
        "uniqueOpenedEventCount": unique_opened,
        "bytes": manifest_path.stat().st_size + ranges_path.stat().st_size,
    }


def _read_ranges(path: Path) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)



@dataclass
class VerifiedResidualCatalog:
    candidates: Any
    ranges_by_collection: Mapping[int, Sequence[Mapping[str, Any]]]
    delta: int
    closure_by_anchor: Any
    manifest: Mapping[str, Any]

    def __post_init__(self) -> None:
        self._range_first_orders: dict[int, list[int]] = {}
        for collection_id, rows in self.ranges_by_collection.items():
            first_orders: list[int] = []
            for row in rows:
                events = row["events"]
                if not events:
                    raise ValueError("authenticated residual range is empty")
                first_orders.append(int(events[0]["event"]["order_seq"]))
            self._range_first_orders[int(collection_id)] = first_orders

    @staticmethod
    def _find_anchor_offset(
        opened: Sequence[Mapping[str, Any]], anchor: int
    ) -> int:
        left, right = 0, len(opened)
        while left < right:
            middle = (left + right) // 2
            order = int(opened[middle]["event"]["order_seq"])
            if order < anchor:
                left = middle + 1
            else:
                right = middle
        if left >= len(opened) or int(opened[left]["event"]["order_seq"]) != anchor:
            return -1
        return left

    @staticmethod
    def _derive_window(
        descriptor: Mapping[str, Any],
        collection_ranges: Sequence[Mapping[str, Any]],
        delta: int,
        range_first_orders: Sequence[int] | None = None,
    ) -> tuple[list[dict[str, Any]], tuple[int, int]]:
        anchor = int(descriptor["anchor_order_seq"])
        containing: Mapping[str, Any] | None = None
        anchor_offset = -1
        if range_first_orders:
            range_index = bisect_right(range_first_orders, anchor) - 1
            if range_index >= 0:
                row = collection_ranges[range_index]
                events = row["events"]
                if events and anchor <= int(events[-1]["event"]["order_seq"]):
                    offset = VerifiedResidualCatalog._find_anchor_offset(events, anchor)
                    if offset >= 0:
                        containing, anchor_offset = row, offset
        else:
            for row in collection_ranges:
                events = row["events"]
                if not events:
                    continue
                if (
                    int(events[0]["event"]["order_seq"])
                    <= anchor
                    <= int(events[-1]["event"]["order_seq"])
                ):
                    offset = VerifiedResidualCatalog._find_anchor_offset(events, anchor)
                    if offset >= 0:
                        containing, anchor_offset = row, offset
                        break
        if containing is None:
            raise ValueError("residual ranges omit an authenticated replay anchor")
        opened = containing["events"]
        anchor_event = opened[anchor_offset]["event"]
        if int(anchor_event["block_ts"]) != int(descriptor["start_ts"]):
            raise ValueError("residual anchor timestamp differs from summary")
        deadline = min(int(descriptor["start_ts"]) + delta, int(descriptor["end_ts"]))
        summary_end_order = int(descriptor["end_order_seq"])
        match_end = anchor_offset
        while match_end + 1 < len(opened):
            next_event = opened[match_end + 1]["event"]
            if (
                int(next_event["block_ts"]) <= deadline
                and int(next_event["order_seq"]) <= summary_end_order
            ):
                match_end += 1
            else:
                break
        match_right_position = int(opened[match_end]["position"])
        collection_last = int(containing["collection"]["eventCount"]) - 1
        if match_right_position == collection_last:
            open_right_position = match_right_position
        else:
            if match_end + 1 >= len(opened):
                raise ValueError("residual range omits immediate right boundary witness")
            successor = opened[match_end + 1]["event"]
            if (
                int(successor["block_ts"]) <= deadline
                and int(successor["order_seq"]) <= summary_end_order
            ):
                raise ValueError("residual payload stopped before complete query window")
            open_right_position = int(opened[match_end + 1]["position"])
            if open_right_position != match_right_position + 1:
                raise ValueError("right boundary witness is not immediate successor")
        left_position = int(opened[anchor_offset]["position"])
        events = [item["event"] for item in opened[anchor_offset : match_end + 1]]
        return events, (left_position, open_right_position)

    def derive_window(
        self, descriptor: Mapping[str, Any]
    ) -> tuple[list[dict[str, Any]], tuple[int, int]]:
        collection_id = int(descriptor["collection_id"])
        return self._derive_window(
            descriptor,
            self.ranges_by_collection.get(collection_id, []),
            self.delta,
            self._range_first_orders.get(collection_id, []),
        )

    def iter_windows(self) -> Iterable[tuple[Mapping[str, Any], list[dict[str, Any]]]]:
        for descriptor in self.candidates:
            events, _required = self.derive_window(descriptor)
            yield descriptor, events


def verify_residual_payload(
    residual_dir: Path,
    candidates: Any,
    transcript: Mapping[str, Any],
    expected_event_stream_root: str,
    expected_tx_binding_root: str,
    expected_collection_count: int,
    expected_event_count: int,
    *,
    expected_variant: str,
    materialize_windows: bool = True,
) -> tuple[dict[int, list[dict[str, Any]]], dict[int, int], dict[str, Any]] | VerifiedResidualCatalog:
    residual_dir = Path(residual_dir)
    manifest = json.loads((residual_dir / "manifest.json").read_text(encoding="utf-8"))
    _require_exact_fields("residual manifest", manifest, _MANIFEST_FIELDS)
    digest = str(manifest.pop("payloadDigest"))
    if hash_obj("residual-manifest-v3", manifest) != digest:
        raise ValueError("residual manifest digest mismatch")
    manifest["payloadDigest"] = digest
    if manifest["payloadVersion"] != "derived_complete_collection_ranges_v3":
        raise ValueError("unsupported residual payload version")
    if manifest["variant"] != expected_variant:
        raise ValueError("residual payload variant differs from coverage")
    if manifest["eventStreamRoot"] != expected_event_stream_root:
        raise ValueError("residual payload is not bound to EventStreamRoot")
    if manifest["txBindingRoot"] != expected_tx_binding_root:
        raise ValueError("residual payload is not bound to TxBindingRoot")
    if int(manifest["collectionCount"]) != int(expected_collection_count):
        raise ValueError("residual collection count differs from Epoch")
    if int(manifest["eventCount"]) != int(expected_event_count):
        raise ValueError("residual event count differs from Epoch")
    if int(manifest["replayCount"]) != len(candidates):
        raise ValueError("residual replay count differs from authenticated coverage")
    if manifest["rangesFile"] != "ranges.jsonl.gz":
        raise ValueError("residual ranges filename is not canonical")
    ranges_path = residual_dir / manifest["rangesFile"]
    if _sha256_file(ranges_path) != manifest["rangesSha256"]:
        raise ValueError("residual ranges transport digest mismatch")
    if manifest["deduplicationContract"] != (
        "opened_collection_positions_are_disjoint_canonical_merged_ranges"
    ):
        raise ValueError("residual deduplication contract mismatch")
    if manifest["boundaryContract"] != (
        "verifier_derives_each_replay_window_and_requires_immediate_successor_or_collection_end"
    ):
        raise ValueError("residual boundary contract mismatch")

    ranges_by_collection: dict[int, list[dict[str, Any]]] = defaultdict(list)
    previous_interval: tuple[int, int, int] | None = None
    range_count = 0
    for expected_range_id, row in enumerate(_read_ranges(ranges_path)):
        _require_exact_fields("residual range", row, _RANGE_FIELDS)
        if int(row["rangeId"]) != expected_range_id:
            raise ValueError("residual range ids must be contiguous and canonical")
        collection = row["collection"]
        _require_exact_fields("collection opening", collection, _COLLECTION_FIELDS)
        event_count = int(collection["eventCount"])
        global_start = int(collection["globalStartPosition"])
        collection_id = int(collection["collectionId"])
        if event_count <= 0 or global_start < 0 or global_start + event_count > expected_event_count:
            raise ValueError("collection metadata exceeds Epoch event domain")
        collection_leaf = hash_obj(
            "collection-directory-leaf-v2",
            {
                "collection_id": collection_id,
                "event_count": event_count,
                "global_start_position": global_start,
                "first_order": int(collection["firstOrderSeq"]),
                "last_order": int(collection["lastOrderSeq"]),
                "event_root": str(collection["eventRoot"]),
            },
        )
        if collection_leaf != collection["directoryLeafHash"] or not verify_proof(
            collection_leaf,
            int(collection["directoryPosition"]),
            collection["directoryPath"],
            expected_event_stream_root,
            "collection-directory-node-v2",
            expected_collection_count,
        ):
            raise ValueError("collection directory proof failed")
        opened = row["events"]
        if not isinstance(opened, list):
            raise ValueError("residual range events must be a list")
        positions = [int(item["position"]) for item in opened]
        left, right = int(row["leftPosition"]), int(row["rightPosition"])
        if left < 0 or right >= event_count or positions != list(range(left, right + 1)):
            raise ValueError("residual positions are not a contiguous range")
        current_interval = (collection_id, left, right)
        if previous_interval is not None:
            if current_interval <= previous_interval:
                raise ValueError("residual ranges are not in canonical collection/position order")
            if current_interval[0] == previous_interval[0] and current_interval[1] <= previous_interval[2] + 1:
                raise ValueError("residual ranges overlap or were not maximally merged")
        previous_interval = current_interval
        event_hashes: list[str] = []
        tx_hashes: list[str] = []
        previous_order: int | None = None
        previous_ts: int | None = None
        seen_event_ids: set[str] = set()
        for item in opened:
            if set(item) != {"position", "event"}:
                raise ValueError("opened event item fields are not canonical")
            event = item["event"]
            if int(event["collection_id"]) != collection_id:
                raise ValueError("opened event belongs to wrong collection")
            order = int(event["order_seq"])
            block_ts = int(event["block_ts"])
            event_id = str(event["event_id"])
            if previous_order is not None and order <= previous_order:
                raise ValueError("opened event orders are not strictly increasing")
            if previous_ts is not None and block_ts < previous_ts:
                raise ValueError("opened timestamps violate monotonicity")
            if event_id in seen_event_ids:
                raise ValueError("opened range repeats an event id")
            seen_event_ids.add(event_id)
            previous_order, previous_ts = order, block_ts
            event_hashes.append(event_leaf_hash(event))
            tx_hashes.append(tx_leaf_hash(order, int(event["tx_id"]), str(event["tx_hash"])))
        if not verify_open_range(
            event_count,
            left,
            event_hashes,
            row["rangeFrontier"],
            str(collection["eventRoot"]),
            "collection-event-node-v1",
        ):
            raise ValueError("complete residual event range proof failed")
        tx_left, tx_right = int(row["txLeftPosition"]), int(row["txRightPosition"])
        if tx_left != global_start + left or tx_right != global_start + right:
            raise ValueError("transaction range is not aligned with event range")
        if list(row["txLeafHashes"]) != tx_hashes:
            raise ValueError("transaction leaf list differs from opened events")
        if not verify_open_range(
            expected_event_count,
            tx_left,
            tx_hashes,
            row["txRangeFrontier"],
            expected_tx_binding_root,
            "tx-binding-node-v1",
        ):
            raise ValueError("complete transaction binding range proof failed")
        ranges_by_collection[collection_id].append(row)
        range_count += 1
    if range_count != int(manifest["rangeCount"]):
        raise ValueError("residual range count differs from manifest")

    if hasattr(candidates, "get"):
        closure_by_anchor: Any = candidates
    else:
        closure_by_anchor = {}
        for candidate in candidates:
            anchor = int(candidate["anchor_order_seq"])
            if anchor in closure_by_anchor:
                raise ValueError("authenticated replay set contains duplicate anchor orders")
            closure_by_anchor[anchor] = int(candidate["closure_id"])

    delta = int(transcript["refined"]["maxCompletionDeltaSeconds"])
    catalog = VerifiedResidualCatalog(
        candidates=candidates,
        ranges_by_collection=ranges_by_collection,
        delta=delta,
        closure_by_anchor=closure_by_anchor,
        manifest=manifest,
    )

    if hasattr(candidates, "iter_collection_order"):
        ordered_candidates = candidates.iter_collection_order()
    else:
        ordered_candidates = iter(
            sorted(
                candidates,
                key=lambda row: (
                    int(row["collection_id"]), int(row["anchor_order_seq"])
                ),
            )
        )

    expected_intervals: list[tuple[int, int, int]] = []
    current_collection: int | None = None
    merged_left = merged_right = -1

    def flush_interval() -> None:
        if current_collection is not None and merged_left >= 0:
            expected_intervals.append(
                (int(current_collection), int(merged_left), int(merged_right))
            )

    for descriptor in ordered_candidates:
        collection_id = int(descriptor["collection_id"])
        _events, (left, right) = catalog.derive_window(descriptor)
        if current_collection is None or collection_id != current_collection:
            flush_interval()
            current_collection = collection_id
            merged_left, merged_right = left, right
        elif left <= merged_right + 1:
            merged_right = max(merged_right, right)
        else:
            expected_intervals.append((collection_id, merged_left, merged_right))
            merged_left, merged_right = left, right
    flush_interval()
    actual_intervals = [
        (
            int(row["collection"]["collectionId"]),
            int(row["leftPosition"]),
            int(row["rightPosition"]),
        )
        for collection_id in sorted(ranges_by_collection)
        for row in ranges_by_collection[collection_id]
    ]
    if actual_intervals != expected_intervals:
        raise ValueError("residual ranges are not the canonical merge of replay windows")

    if not materialize_windows:
        return catalog
    events_by_anchor: dict[int, list[dict[str, Any]]] = {}
    for descriptor, events in catalog.iter_windows():
        events_by_anchor[int(descriptor["anchor_order_seq"])] = events
    if len(events_by_anchor) != len(candidates):
        raise ValueError("VOrep does not bind every authenticated replay closure")
    return events_by_anchor, closure_by_anchor, manifest
