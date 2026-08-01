from __future__ import annotations

import csv
import gzip
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from .index import (
    ANCHOR_AGG_FIELDS,
    ANCHOR_COLUMNS,
    CLOSURE_COLUMNS,
    anchor_node_hash,
    closure_descriptor_dict,
    leaf_aggregate,
    parent_aggregate,
)
from .merkle import parent_hash, root_from_leaves
from .protocol import (
    anchor_directory_leaf_hash,
    anchor_leaf_hash,
    closure_domain_leaf_hash,
    hash_obj,
)
from .transcript import base_pass, refined_pass

VARIANTS = ("full", "no_anchor", "no_refined", "no_pruning")

_COVERAGE_MANIFEST_FIELDS = {
    "coverageVersion",
    "variant",
    "summaryRoot",
    "anchorDirectoryRoot",
    "closureDomainRoot",
    "treeHeight",
    "anchorCount",
    "closureCount",
    "baseNegativeFrontierCount",
    "baseNegativeAnchorCount",
    "refinedNegativeFrontierCount",
    "refinedNegativeAnchorCount",
    "replayFrontierCount",
    "replayCount",
    "closureDomainRecordCount",
    "partitionContract",
    "files",
    "candidateHintUsedAsProof",
    "candidateHintEqual",
    "coverageDigest",
}
_COMMON_RECORD_FIELDS = {
    "level",
    "index",
    "aggregate",
    "nodeHash",
    "kind",
    "anchorDirectoryNodeHash",
}
_LEAF_RECORD_FIELDS = _COMMON_RECORD_FIELDS | {"descriptor"}
_INTERNAL_RECORD_FIELDS = _COMMON_RECORD_FIELDS | {
    "leftHash",
    "rightHash",
    "duplicated",
}
_CLOSURE_RECORD_FIELDS = {"descriptor", "leafHash"}
_FILES = {
    "base": "negative_base.jsonl.gz",
    "refined": "negative_refined.jsonl.gz",
    "replay": "replay_authenticated.jsonl.gz",
    "closureDomain": "closure_domain_authenticated.jsonl.gz",
}


def _require_exact_fields(name: str, value: Any, expected: set[str]) -> None:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    actual = set(value)
    if actual != expected:
        raise ValueError(
            f"{name} fields mismatch: missing={sorted(expected-actual)} "
            f"unknown={sorted(actual-expected)}"
        )


def _is_sha256(value: Any) -> bool:
    text = str(value).lower()
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


def _tree_height(leaf_count: int) -> int:
    if leaf_count <= 0:
        raise ValueError("coverage Anchor count must be positive")
    return (int(leaf_count) - 1).bit_length()


def _node(conn: sqlite3.Connection, level: int, index: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM anchor_nodes WHERE level=? AND node_index=?",
        (int(level), int(index)),
    ).fetchone()
    if row is None:
        return None
    return {
        "level": int(row[0]),
        "index": int(row[1]),
        "aggregate": {
            key: int(row[2 + offset]) for offset, key in enumerate(ANCHOR_AGG_FIELDS)
        },
        "nodeHash": str(row[-1]),
    }


def _base_negative(aggregate: Mapping[str, Any], transcript: Mapping[str, Any]) -> bool:
    base = dict(transcript["base"])
    return (
        int(aggregate["max_transition_count"]) < int(base["minTransitionCount"])
        or int(aggregate["max_addr_count"]) < int(base["minAddressCount"])
        or int(aggregate["max_nft_count"]) < int(base["minNftCount"])
    )


def _refined_negative(aggregate: Mapping[str, Any], transcript: Mapping[str, Any]) -> bool:
    refined = dict(transcript["refined"])
    delta = int(refined["maxCompletionDeltaSeconds"])
    return all(
        int(aggregate[f"min_{signature}"]) > delta
        for signature in refined["genericSignatures"]
    )


def _base_positive(aggregate: Mapping[str, Any], transcript: Mapping[str, Any]) -> bool:
    base = dict(transcript["base"])
    return (
        int(aggregate["min_transition_count"]) >= int(base["minTransitionCount"])
        and int(aggregate["min_addr_count"]) >= int(base["minAddressCount"])
        and int(aggregate["min_nft_count"]) >= int(base["minNftCount"])
    )


def _internal_record(
    conn: sqlite3.Connection, node: Mapping[str, Any], kind: str
) -> dict[str, Any]:
    level, index = int(node["level"]), int(node["index"])
    left = _node(conn, level - 1, index * 2)
    right = _node(conn, level - 1, index * 2 + 1)
    if left is None:
        raise ValueError("authenticated summary tree is missing a left child")
    directory = conn.execute(
        "SELECT node_hash FROM anchor_directory_nodes WHERE level=? AND node_index=?",
        (level, index),
    ).fetchone()
    if directory is None:
        raise ValueError("authenticated Anchor directory is missing a matching node")
    duplicated = right is None
    return {
        **node,
        "kind": kind,
        "leftHash": left["nodeHash"],
        "rightHash": left["nodeHash"] if duplicated else right["nodeHash"],
        "duplicated": duplicated,
        "anchorDirectoryNodeHash": str(directory[0]),
    }


def _leaf_record(
    conn: sqlite3.Connection, node: Mapping[str, Any], kind: str
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM anchors WHERE position=?", (int(node["index"]),)
    ).fetchone()
    if row is None:
        raise ValueError("summary leaf is missing its descriptor")
    descriptor = {key: int(row[offset]) for offset, key in enumerate(ANCHOR_COLUMNS)}
    return {
        **node,
        "kind": kind,
        "descriptor": descriptor,
        "anchorDirectoryNodeHash": anchor_directory_leaf_hash(
            descriptor["closure_id"], descriptor["anchor_order_seq"]
        ),
    }


def _write_jsonl_gz(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    count = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="\n", compresslevel=6) as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    return count


def _read_jsonl_gz(path: Path) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _read_hint(path: Path) -> set[tuple[int, int]]:
    with Path(path).open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"closure_id", "anchor_order"}
        if not required <= set(reader.fieldnames or []):
            raise ValueError("Cand2 hint is missing closure_id/anchor_order")
        rows = [(int(row["closure_id"]), int(row["anchor_order"])) for row in reader]
    if len(rows) != len(set(rows)):
        raise ValueError("untrusted Cand2 hint contains duplicate Anchor rows")
    return set(rows)


def _anchor_descriptor_from_closure(
    closure: Mapping[str, Any], anchor_position: int
) -> dict[str, int]:
    return {
        "position": int(anchor_position),
        **{key: int(closure[key]) for key in ANCHOR_COLUMNS if key != "position"},
    }


def _closure_row(row: Sequence[Any]) -> dict[str, int]:
    return {key: int(row[offset]) for offset, key in enumerate(CLOSURE_COLUMNS)}


def build_coverage(
    index_path: Path,
    transcript: Mapping[str, Any],
    out_dir: Path,
    *,
    variant: str = "full",
    candidate_hint: Path | None = None,
) -> dict[str, Any]:
    if variant not in VARIANTS:
        raise ValueError(f"unsupported verification ablation variant: {variant}")
    conn = sqlite3.connect(f"file:{Path(index_path).as_posix()}?mode=ro", uri=True)
    height = int(conn.execute("SELECT MAX(level) FROM anchor_nodes").fetchone()[0])
    root = _node(conn, height, 0)
    if root is None:
        raise ValueError("authenticated summary tree has no root")
    anchor_directory_root = str(
        conn.execute(
            "SELECT node_hash FROM anchor_directory_nodes ORDER BY level DESC,node_index LIMIT 1"
        ).fetchone()[0]
    )
    closure_domain_root = str(
        conn.execute(
            "SELECT node_hash FROM closure_nodes ORDER BY level DESC,node_index LIMIT 1"
        ).fetchone()[0]
    )
    closure_count = int(conn.execute("SELECT COUNT(*) FROM closures").fetchone()[0])
    base_rows: list[dict[str, Any]] = []
    refined_rows: list[dict[str, Any]] = []
    replay_rows: list[dict[str, Any]] = []
    closure_rows: list[dict[str, Any]] = []

    semantic_base_negative = 0
    semantic_refined_negative = 0
    semantic_replay = 0
    derived_hint: set[tuple[int, int]] = set()

    if variant in {"full", "no_refined"}:
        def visit(level: int, index: int) -> None:
            node = _node(conn, level, index)
            if node is None:
                return
            if _base_negative(node["aggregate"], transcript):
                target, kind = base_rows, "BASE_NEGATIVE"
            elif not _base_positive(node["aggregate"], transcript):
                if level == 0:
                    target, kind = base_rows, "BASE_NEGATIVE"
                else:
                    visit(level - 1, index * 2)
                    visit(level - 1, index * 2 + 1)
                    return
            elif variant == "full" and _refined_negative(node["aggregate"], transcript):
                target, kind = refined_rows, "REFINED_NEGATIVE"
            elif level == 0:
                target, kind = replay_rows, "REPLAY"
            else:
                visit(level - 1, index * 2)
                visit(level - 1, index * 2 + 1)
                return
            target.append(
                _leaf_record(conn, node, kind)
                if level == 0
                else _internal_record(conn, node, kind)
            )

        visit(height, 0)
        semantic_base_negative = sum(int(row["aggregate"]["count"]) for row in base_rows)
        semantic_refined_negative = sum(
            int(row["aggregate"]["count"]) for row in refined_rows
        )
        semantic_replay = len(replay_rows)
        if variant == "full":
            derived_hint = {
                (
                    int(row["descriptor"]["closure_id"]),
                    int(row["descriptor"]["anchor_order_seq"]),
                )
                for row in replay_rows
            }
    else:
        anchor_position = 0
        for row in conn.execute("SELECT * FROM closures ORDER BY closure_position"):
            descriptor = _closure_row(row)
            leaf_hash = str(row[-1])
            closure_rows.append({"descriptor": descriptor, "leafHash": leaf_hash})
            if variant == "no_pruning":
                semantic_replay += 1
                continue
            if not int(descriptor["is_anchor"]):
                continue
            anchor_descriptor = _anchor_descriptor_from_closure(descriptor, anchor_position)
            anchor_position += 1
            if not base_pass(anchor_descriptor, transcript):
                semantic_base_negative += 1
            elif not refined_pass(anchor_descriptor, transcript):
                semantic_refined_negative += 1
            else:
                semantic_replay += 1
                derived_hint.add(
                    (
                        int(anchor_descriptor["closure_id"]),
                        int(anchor_descriptor["anchor_order_seq"]),
                    )
                )
        if variant == "no_anchor" and anchor_position != int(root["aggregate"]["count"]):
            raise ValueError("closure-domain Anchor subset count differs from Anchor tree")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {key: out_dir / name for key, name in _FILES.items()}
    _write_jsonl_gz(paths["base"], base_rows)
    _write_jsonl_gz(paths["refined"], refined_rows)
    _write_jsonl_gz(paths["replay"], replay_rows)
    _write_jsonl_gz(paths["closureDomain"], closure_rows)

    hint_equal: bool | None = None
    if candidate_hint is not None and variant in {"full", "no_anchor"}:
        hint = _read_hint(candidate_hint)
        hint_equal = hint == derived_hint
        if not hint_equal:
            raise ValueError(
                "untrusted Cand2 hint differs from authenticated derivation: "
                f"derived_only={len(derived_hint-hint)} hint_only={len(hint-derived_hint)}"
            )

    partition = {
        "full": "AnchorDomain=BaseNeg_disjoint_union_RefinedNeg_disjoint_union_Replay",
        "no_refined": "AnchorDomain=BaseNeg_disjoint_union_Replay",
        "no_anchor": "ClosureDomainOpened;AnchorSubsetDerived;Replay=Anchor_and_Base_and_Refined",
        "no_pruning": "ClosureDomain=Replay",
    }[variant]
    manifest = {
        "coverageVersion": "complete_replay_frontier_v3",
        "variant": variant,
        "summaryRoot": root["nodeHash"],
        "anchorDirectoryRoot": anchor_directory_root,
        "closureDomainRoot": closure_domain_root,
        "treeHeight": height,
        "anchorCount": int(root["aggregate"]["count"]),
        "closureCount": closure_count,
        "baseNegativeFrontierCount": len(base_rows),
        "baseNegativeAnchorCount": semantic_base_negative,
        "refinedNegativeFrontierCount": len(refined_rows),
        "refinedNegativeAnchorCount": semantic_refined_negative,
        "replayFrontierCount": len(replay_rows),
        "replayCount": semantic_replay,
        "closureDomainRecordCount": len(closure_rows),
        "partitionContract": partition,
        "files": dict(_FILES),
        "candidateHintUsedAsProof": False,
        "candidateHintEqual": hint_equal,
    }
    manifest["coverageDigest"] = hash_obj("coverage-manifest-v3", manifest)
    (out_dir / "coverage.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    conn.close()
    return manifest


def _anchor_roots_from_descriptors(
    descriptors: Sequence[Mapping[str, Any]],
) -> tuple[str, str]:
    summary_nodes: list[tuple[str, dict[str, int]]] = []
    directory_hashes: list[str] = []
    for descriptor in descriptors:
        summary_nodes.append((anchor_leaf_hash(descriptor), leaf_aggregate(descriptor)))
        directory_hashes.append(
            anchor_directory_leaf_hash(
                int(descriptor["closure_id"]), int(descriptor["anchor_order_seq"])
            )
        )
    level = 0
    while len(summary_nodes) > 1:
        next_nodes: list[tuple[str, dict[str, int]]] = []
        for index in range(0, len(summary_nodes), 2):
            left_hash, left_agg = summary_nodes[index]
            if index + 1 < len(summary_nodes):
                right_hash, right_agg = summary_nodes[index + 1]
                aggregate = parent_aggregate(left_agg, right_agg)
            else:
                right_hash, right_agg = left_hash, left_agg
                aggregate = left_agg
            next_nodes.append(
                (
                    anchor_node_hash(
                        level + 1,
                        index // 2,
                        aggregate,
                        left_hash,
                        right_hash,
                    ),
                    aggregate,
                )
            )
        summary_nodes = next_nodes
        level += 1
    if not summary_nodes:
        raise ValueError("derived Anchor subset is empty")
    return summary_nodes[0][0], root_from_leaves(
        directory_hashes, "anchor-directory-node-v1"
    )


def verify_coverage(
    coverage_dir: Path,
    transcript: Mapping[str, Any],
    expected_summary_root: str,
    expected_anchor_directory_root: str,
    expected_closure_domain_root: str,
    expected_closure_count: int,
    timings: dict[str, float] | None = None,
    *,
    collect_candidates: bool = True,
    candidate_sink: Callable[[Mapping[str, Any]], None] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    stage_times = timings if timings is not None else {}
    coverage_dir = Path(coverage_dir)
    manifest = json.loads((coverage_dir / "coverage.json").read_text(encoding="utf-8"))
    _require_exact_fields("coverage manifest", manifest, _COVERAGE_MANIFEST_FIELDS)
    digest = str(manifest.pop("coverageDigest"))
    if not _is_sha256(digest) or hash_obj("coverage-manifest-v3", manifest) != digest:
        raise ValueError("coverage manifest digest mismatch")
    manifest["coverageDigest"] = digest
    variant = str(manifest["variant"])
    if variant not in VARIANTS:
        raise ValueError("unsupported verification ablation variant")
    if manifest["coverageVersion"] != "complete_replay_frontier_v3":
        raise ValueError("unsupported coverage version")
    if manifest["summaryRoot"] != expected_summary_root:
        raise ValueError("coverage is not bound to the Epoch summary root")
    if manifest["anchorDirectoryRoot"] != expected_anchor_directory_root:
        raise ValueError("coverage is not bound to the Epoch Anchor directory root")
    if manifest["closureDomainRoot"] != expected_closure_domain_root:
        raise ValueError("coverage is not bound to the Epoch closure-domain root")
    if int(manifest["closureCount"]) != int(expected_closure_count):
        raise ValueError("coverage closure count differs from the Epoch")
    if manifest["files"] != _FILES:
        raise ValueError("coverage file map is not canonical")
    if manifest["candidateHintUsedAsProof"] is not False:
        raise ValueError("untrusted candidate hints cannot be proof inputs")
    if manifest["candidateHintEqual"] not in (None, True, False):
        raise ValueError("coverage candidateHintEqual has an invalid type")

    candidates: list[dict[str, Any]] = []

    def emit_candidate(descriptor: Mapping[str, Any]) -> None:
        if collect_candidates:
            candidates.append({str(k): int(v) for k, v in descriptor.items()})
        if candidate_sink is not None:
            candidate_sink(descriptor)

    actual_base_frontier = actual_base_anchors = 0
    actual_refined_frontier = actual_refined_anchors = 0
    actual_replay_frontier = actual_replay = 0

    if variant in {"full", "no_refined"}:
        leaf_count = int(manifest["anchorCount"])
        height = int(manifest["treeHeight"])
        if leaf_count <= 0 or height != _tree_height(leaf_count):
            raise ValueError("coverage tree dimensions are not canonical")
        intervals: list[tuple[int, int]] = []
        frontier: dict[tuple[int, int], tuple[str, dict[str, int]]] = {}
        directory_frontier: dict[tuple[int, int], str] = {}
        file_specs = (
            ("base", "BASE_NEGATIVE", "voneg_base_ms"),
            ("refined", "REFINED_NEGATIVE", "voneg_refined_ms"),
            ("replay", "REPLAY", "vocov_ms"),
        )
        for file_key, expected_kind, stage_key in file_specs:
            stage_started = time.perf_counter()
            for record in _read_jsonl_gz(coverage_dir / _FILES[file_key]):
                level, index = int(record.get("level", -1)), int(record.get("index", -1))
                _require_exact_fields(
                    "coverage leaf record" if level == 0 else "coverage internal record",
                    record,
                    _LEAF_RECORD_FIELDS if level == 0 else _INTERNAL_RECORD_FIELDS,
                )
                if level < 0 or level > height or index < 0:
                    raise ValueError("coverage frontier node coordinates are invalid")
                span = 1 << level
                canonical_start = index * span
                if canonical_start >= leaf_count:
                    raise ValueError("coverage frontier node lies outside the Anchor tree")
                canonical_end = min(leaf_count, canonical_start + span) - 1
                aggregate = {
                    key: int(record["aggregate"][key]) for key in ANCHOR_AGG_FIELDS
                }
                if set(record["aggregate"]) != set(ANCHOR_AGG_FIELDS):
                    raise ValueError("coverage aggregate fields are not canonical")
                if (
                    aggregate["start_position"] != canonical_start
                    or aggregate["end_position"] != canonical_end
                    or aggregate["count"] != canonical_end - canonical_start + 1
                ):
                    raise ValueError("coverage aggregate range does not match tree coordinates")
                if level == 0:
                    if set(record["descriptor"]) != set(ANCHOR_COLUMNS):
                        raise ValueError("coverage descriptor fields are not canonical")
                    descriptor = {
                        key: int(record["descriptor"][key]) for key in ANCHOR_COLUMNS
                    }
                    if descriptor["position"] != index:
                        raise ValueError("coverage descriptor position differs from leaf index")
                    if aggregate != leaf_aggregate(descriptor):
                        raise ValueError("leaf aggregate does not match descriptor")
                    node_hash = anchor_leaf_hash(descriptor)
                else:
                    child_width = (leaf_count + (1 << (level - 1)) - 1) >> (level - 1)
                    expected_duplicated = index * 2 + 1 >= child_width
                    if record["duplicated"] is not expected_duplicated:
                        raise ValueError("coverage duplicate-child flag is not canonical")
                    if expected_duplicated and str(record["rightHash"]) != str(record["leftHash"]):
                        raise ValueError("duplicated node does not repeat its left child")
                    node_hash = anchor_node_hash(
                        level,
                        index,
                        aggregate,
                        str(record["leftHash"]),
                        str(record["rightHash"]),
                    )
                if node_hash != record["nodeHash"]:
                    raise ValueError("frontier node hash mismatch")
                if str(record["kind"]) != expected_kind:
                    raise ValueError("coverage frontier kind does not match its canonical file")
                if expected_kind == "BASE_NEGATIVE":
                    if not _base_negative(aggregate, transcript):
                        raise ValueError("unsafe Base-negative certificate")
                    actual_base_frontier += 1
                    actual_base_anchors += aggregate["count"]
                elif expected_kind == "REFINED_NEGATIVE":
                    if variant != "full":
                        raise ValueError("No-Refined coverage must not contain Refined certificates")
                    if not _base_positive(aggregate, transcript) or not _refined_negative(
                        aggregate, transcript
                    ):
                        raise ValueError("unsafe Refined-negative certificate")
                    actual_refined_frontier += 1
                    actual_refined_anchors += aggregate["count"]
                else:
                    if level != 0 or not base_pass(descriptor, transcript):
                        raise ValueError("invalid replay leaf")
                    if variant == "full" and not refined_pass(descriptor, transcript):
                        raise ValueError("Full replay leaf does not pass Refined support")
                    emit_candidate(descriptor)
                    actual_replay_frontier += 1
                    actual_replay += 1
                intervals.append((aggregate["start_position"], aggregate["end_position"]))
                key = (level, index)
                if key in frontier:
                    raise ValueError("duplicate coverage frontier node")
                frontier[key] = (node_hash, aggregate)
                directory_hash = str(record["anchorDirectoryNodeHash"])
                if not _is_sha256(directory_hash):
                    raise ValueError("Anchor directory frontier hash is invalid")
                if level == 0:
                    expected_directory = anchor_directory_leaf_hash(
                        descriptor["closure_id"], descriptor["anchor_order_seq"]
                    )
                    if directory_hash != expected_directory:
                        raise ValueError("Anchor directory leaf mapping mismatch")
                directory_frontier[key] = directory_hash
            stage_times[stage_key] = stage_times.get(stage_key, 0.0) + (
                time.perf_counter() - stage_started
            ) * 1000.0

        fold_started = time.perf_counter()
        intervals.sort()
        expected_start = 0
        for start, end in intervals:
            if start != expected_start or end < start:
                raise ValueError("coverage frontier has a gap or overlap")
            expected_start = end + 1
        if expected_start != leaf_count:
            raise ValueError("coverage frontier does not cover the complete Anchor range")

        def solve(level: int, index: int, start: int, width: int):
            existing = frontier.get((level, index))
            if existing is not None:
                return existing
            if level == 0:
                raise ValueError("coverage frontier omits a leaf")
            left = solve(level - 1, index * 2, start, width // 2)
            right_start = start + width // 2
            if right_start >= leaf_count:
                right, aggregate = left, left[1]
            else:
                right = solve(level - 1, index * 2 + 1, right_start, width // 2)
                aggregate = parent_aggregate(left[1], right[1])
            return (
                anchor_node_hash(level, index, aggregate, left[0], right[0]),
                aggregate,
            )

        def solve_directory(level: int, index: int, start: int, width: int) -> str:
            existing = directory_frontier.get((level, index))
            if existing is not None:
                return existing
            if level == 0:
                raise ValueError("Anchor directory frontier omits a leaf")
            left = solve_directory(level - 1, index * 2, start, width // 2)
            right_start = start + width // 2
            right = (
                left
                if right_start >= leaf_count
                else solve_directory(level - 1, index * 2 + 1, right_start, width // 2)
            )
            return parent_hash("anchor-directory-node-v1", left, right)

        reconstructed, root_aggregate = solve(height, 0, 0, 1 << height)
        if reconstructed != expected_summary_root or root_aggregate["count"] != leaf_count:
            raise ValueError("coverage frontier does not reconstruct SummaryRoot")
        if solve_directory(height, 0, 0, 1 << height) != expected_anchor_directory_root:
            raise ValueError("coverage frontier does not reconstruct AnchorDirectoryRoot")
        stage_times["vocov_ms"] = stage_times.get("vocov_ms", 0.0) + (
            time.perf_counter() - fold_started
        ) * 1000.0
        if any(_read_jsonl_gz(coverage_dir / _FILES["closureDomain"])):
            raise ValueError("Anchor-frontier variants must not include closure-domain leaves")
    else:
        closure_started = time.perf_counter()
        closure_hashes: list[str] = []
        anchor_descriptors: list[dict[str, int]] = []
        position = 0
        for record in _read_jsonl_gz(coverage_dir / _FILES["closureDomain"]):
            _require_exact_fields("closure-domain record", record, _CLOSURE_RECORD_FIELDS)
            if set(record["descriptor"]) != set(CLOSURE_COLUMNS):
                raise ValueError("closure-domain descriptor fields are not canonical")
            descriptor = {
                key: int(record["descriptor"][key]) for key in CLOSURE_COLUMNS
            }
            if descriptor["closure_position"] != position:
                raise ValueError("closure-domain positions are not contiguous")
            leaf_hash = closure_domain_leaf_hash(descriptor)
            if leaf_hash != str(record["leafHash"]):
                raise ValueError("closure-domain leaf hash mismatch")
            closure_hashes.append(leaf_hash)
            if variant == "no_pruning":
                emit_candidate(
                    _anchor_descriptor_from_closure(
                        descriptor, descriptor["closure_position"]
                    )
                )
                actual_replay += 1
            elif descriptor["is_anchor"]:
                anchor_descriptor = _anchor_descriptor_from_closure(
                    descriptor, len(anchor_descriptors)
                )
                anchor_descriptors.append(anchor_descriptor)
                if not base_pass(anchor_descriptor, transcript):
                    actual_base_anchors += 1
                elif not refined_pass(anchor_descriptor, transcript):
                    actual_refined_anchors += 1
                else:
                    emit_candidate(anchor_descriptor)
                    actual_replay += 1
            position += 1
        if position != int(expected_closure_count):
            raise ValueError("closure-domain record count differs from the Epoch")
        if root_from_leaves(closure_hashes, "closure-domain-node-v1") != expected_closure_domain_root:
            raise ValueError("closure-domain opening does not reconstruct its root")
        if variant == "no_anchor":
            if len(anchor_descriptors) != int(manifest["anchorCount"]):
                raise ValueError("derived Anchor subset count differs from manifest")
            summary_root, directory_root = _anchor_roots_from_descriptors(anchor_descriptors)
            if summary_root != expected_summary_root or directory_root != expected_anchor_directory_root:
                raise ValueError("closure-domain Anchor subset does not reconstruct Anchor roots")
        for key in ("base", "refined", "replay"):
            if any(_read_jsonl_gz(coverage_dir / _FILES[key])):
                raise ValueError("closure-domain variants must not include Anchor frontier records")
        stage_times["vocov_ms"] = stage_times.get("vocov_ms", 0.0) + (
            time.perf_counter() - closure_started
        ) * 1000.0

    declared = {
        "baseFrontier": int(manifest["baseNegativeFrontierCount"]),
        "baseAnchors": int(manifest["baseNegativeAnchorCount"]),
        "refinedFrontier": int(manifest["refinedNegativeFrontierCount"]),
        "refinedAnchors": int(manifest["refinedNegativeAnchorCount"]),
        "replayFrontier": int(manifest["replayFrontierCount"]),
        "replay": int(manifest["replayCount"]),
        "closureRecords": int(manifest["closureDomainRecordCount"]),
    }
    actual = {
        "baseFrontier": actual_base_frontier,
        "baseAnchors": actual_base_anchors,
        "refinedFrontier": actual_refined_frontier,
        "refinedAnchors": actual_refined_anchors,
        "replayFrontier": actual_replay_frontier,
        "replay": actual_replay,
        "closureRecords": int(expected_closure_count) if variant in {"no_anchor", "no_pruning"} else 0,
    }
    if declared != actual:
        raise ValueError(f"coverage manifest counts differ from verified evidence: {declared} != {actual}")
    if variant == "full":
        expected_partition = "AnchorDomain=BaseNeg_disjoint_union_RefinedNeg_disjoint_union_Replay"
        if actual_base_anchors + actual_refined_anchors + actual_replay != int(manifest["anchorCount"]):
            raise ValueError("Full coverage partition count mismatch")
    elif variant == "no_refined":
        expected_partition = "AnchorDomain=BaseNeg_disjoint_union_Replay"
        if actual_base_anchors + actual_replay != int(manifest["anchorCount"]):
            raise ValueError("No-Refined coverage partition count mismatch")
    elif variant == "no_anchor":
        expected_partition = "ClosureDomainOpened;AnchorSubsetDerived;Replay=Anchor_and_Base_and_Refined"
    else:
        expected_partition = "ClosureDomain=Replay"
        if actual_replay != int(expected_closure_count):
            raise ValueError("No-Pruning replay does not cover every closure")
    if manifest["partitionContract"] != expected_partition:
        raise ValueError("coverage partition contract mismatch")

    manifest["verifiedBaseNegativeFrontierCount"] = actual_base_frontier
    manifest["verifiedBaseNegativeAnchorCount"] = actual_base_anchors
    manifest["verifiedRefinedNegativeFrontierCount"] = actual_refined_frontier
    manifest["verifiedRefinedNegativeAnchorCount"] = actual_refined_anchors
    manifest["verifiedReplayCount"] = actual_replay
    return manifest, candidates
