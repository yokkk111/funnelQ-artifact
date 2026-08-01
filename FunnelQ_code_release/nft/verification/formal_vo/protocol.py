from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import PROTOCOL_VERSION

INF_DELTA = 2**63 - 1

SIGNATURE_FIELDS = (
    "l2_ba_nft_equal_min_delta",
    "l2_ba_nft_different_min_delta",
    "l3_bcca_all_equal_min_delta",
    "l3_bcca_all_distinct_min_delta",
    "l3_bcca_root_back_equal_min_delta",
)

EVENT_LEAF_FIELDS = (
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

MANIFEST_FIELDS = (
    "protocolVersion",
    "schemaVersion",
    "eventLeafVersion",
    "anchorLeafVersion",
    "closureLeafVersion",
    "descriptorVocabularyVersion",
    "dataRange",
    "systemParameters",
    "counts",
    "roots",
    "crossStructureContract",
)

DATA_RANGE_FIELDS = {
    "startOrderSeq",
    "endOrderSeq",
    "startTimestamp",
    "endTimestamp",
}
SYSTEM_PARAMETER_FIELDS = {
    "closureHorizonSeconds",
    "anchorDirectorySemantics",
    "eventOrdering",
    "collectionTimestampsMonotone",
    "eventIdGloballyUnique",
}
COUNT_FIELDS = {"eventCount", "anchorCount", "closureCount", "collectionCount"}
ROOT_FIELDS = {
    "anchorDirectoryRoot",
    "summaryRoot",
    "closureDomainRoot",
    "segmentDirectoryRoot",
    "eventStreamRoot",
    "txBindingRoot",
}
CROSS_STRUCTURE_FIELDS = {
    "trustedBuilder",
    "anchorAndSummaryShareOrderedClosureMapping",
    "closureDomainContainsEveryClosure",
    "anchorSubsetDerivedFromClosureDomain",
    "eventLeavesEmbedTxBinding",
    "eventStreamRootDefinition",
    "txBindingRootDefinition",
    "queryDependentInputsAccepted",
    "descriptorSourceMainDbSha256",
    "sourceMainDbSha256",
    "sourceTxDbSha256",
    "descriptorDbSha256",
    "descriptorComplete",
    "descriptorAnchorCount",
    "descriptorClosureCount",
    "descriptorBuilderVersion",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def hash_bytes(domain: str, payload: bytes) -> str:
    h = hashlib.sha256()
    h.update(PROTOCOL_VERSION.encode("ascii"))
    h.update(b"\0")
    h.update(domain.encode("ascii"))
    h.update(b"\0")
    h.update(len(payload).to_bytes(8, "big"))
    h.update(payload)
    return h.hexdigest()


def hash_obj(domain: str, value: Any) -> str:
    return hash_bytes(domain, canonical_json(value).encode("utf-8"))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_exact_keys(name: str, value: Any, expected: set[str]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"manifest {name} must be an object")
    actual = set(value)
    if actual != expected:
        raise ValueError(
            f"manifest {name} fields mismatch: missing={sorted(expected-actual)} "
            f"unknown={sorted(actual-expected)}"
        )
    return dict(value)


def _require_int(name: str, value: Any, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"manifest {name} must be an integer")
    result = int(value)
    if minimum is not None and result < minimum:
        raise ValueError(f"manifest {name} is below its minimum")
    return result


def _require_sha256(name: str, value: Any) -> str:
    text = str(value).lower()
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text):
        raise ValueError(f"manifest {name} must be a lowercase SHA-256 hex digest")
    return text


def strict_manifest_payload(manifest: Mapping[str, Any]) -> dict[str, Any]:
    unknown = set(manifest) - set(MANIFEST_FIELDS) - {"manifestRoot", "epochRoot"}
    missing = set(MANIFEST_FIELDS) - set(manifest)
    if unknown:
        raise ValueError(f"unknown manifest fields: {sorted(unknown)}")
    if missing:
        raise ValueError(f"missing manifest fields: {sorted(missing)}")
    payload = {key: manifest[key] for key in MANIFEST_FIELDS}
    if payload["protocolVersion"] != PROTOCOL_VERSION:
        raise ValueError("manifest protocol version mismatch")
    if payload["schemaVersion"] != "compact_full_binding_v4_defi_runtime":
        raise ValueError("manifest schema version mismatch")
    if payload["eventLeafVersion"] != "semantic_event_with_tx_v2":
        raise ValueError("manifest event leaf version mismatch")
    if payload["anchorLeafVersion"] != "anchor_summary_descriptor_v1":
        raise ValueError("manifest anchor leaf version mismatch")
    if payload["closureLeafVersion"] != "closure_summary_descriptor_v1":
        raise ValueError("manifest closure leaf version mismatch")
    if payload["descriptorVocabularyVersion"] != "generic_role_nft_relation_min_delta_v1":
        raise ValueError("manifest descriptor vocabulary mismatch")

    data_range = _require_exact_keys("dataRange", payload["dataRange"], DATA_RANGE_FIELDS)
    start_order = _require_int("dataRange.startOrderSeq", data_range["startOrderSeq"], minimum=0)
    end_order = _require_int("dataRange.endOrderSeq", data_range["endOrderSeq"], minimum=start_order)
    start_ts = _require_int("dataRange.startTimestamp", data_range["startTimestamp"], minimum=0)
    _require_int("dataRange.endTimestamp", data_range["endTimestamp"], minimum=start_ts)
    if end_order < start_order:
        raise ValueError("manifest data range is invalid")

    system = _require_exact_keys(
        "systemParameters", payload["systemParameters"], SYSTEM_PARAMETER_FIELDS
    )
    _require_int("systemParameters.closureHorizonSeconds", system["closureHorizonSeconds"], minimum=1)
    if system["anchorDirectorySemantics"] != "root_receiver_has_later_outgoing_event":
        raise ValueError("unexpected Anchor directory semantics")
    if system["eventOrdering"] != "collection_id_then_order_seq":
        raise ValueError("unexpected event ordering contract")
    if system["collectionTimestampsMonotone"] is not True:
        raise ValueError("collection timestamp monotonicity must be committed")
    if system["eventIdGloballyUnique"] is not True:
        raise ValueError("global event-id uniqueness must be committed")

    counts = _require_exact_keys("counts", payload["counts"], COUNT_FIELDS)
    for field in COUNT_FIELDS:
        _require_int(f"counts.{field}", counts[field], minimum=0)

    roots = _require_exact_keys("roots", payload["roots"], ROOT_FIELDS)
    for field in ROOT_FIELDS:
        _require_sha256(f"roots.{field}", roots[field])

    contract = _require_exact_keys(
        "crossStructureContract",
        payload["crossStructureContract"],
        CROSS_STRUCTURE_FIELDS,
    )
    if contract["trustedBuilder"] != "deterministic_query_independent_ingest_v3":
        raise ValueError("unexpected trusted builder contract")
    if contract["anchorAndSummaryShareOrderedClosureMapping"] is not True:
        raise ValueError("Anchor/Summary ordered mapping is not committed")
    if contract["closureDomainContainsEveryClosure"] is not True:
        raise ValueError("complete closure-domain commitment is not declared")
    if contract["anchorSubsetDerivedFromClosureDomain"] is not True:
        raise ValueError("Anchor subset derivation is not committed")
    if contract["eventLeavesEmbedTxBinding"] is not True:
        raise ValueError("event leaves must embed transaction bindings")
    if contract["eventStreamRootDefinition"] != (
        "merkle_root_of_collection_directory_whose_leaves_bind_"
        "collection_event_roots_and_global_tx_offsets"
    ):
        raise ValueError("unexpected EventStreamRoot definition")
    if contract["txBindingRootDefinition"] != (
        "global_collection_order_tx_leaf_merkle_tree_aligned_with_event_ranges"
    ):
        raise ValueError("unexpected TxBindingRoot definition")
    if contract["queryDependentInputsAccepted"] != []:
        raise ValueError("query-dependent inputs are forbidden in the epoch builder")
    for field in (
        "descriptorSourceMainDbSha256",
        "sourceMainDbSha256",
        "sourceTxDbSha256",
        "descriptorDbSha256",
    ):
        _require_sha256(f"crossStructureContract.{field}", contract[field])
    if contract["descriptorSourceMainDbSha256"] != contract["sourceMainDbSha256"]:
        raise ValueError("descriptor and authenticated index use different source databases")
    if contract["descriptorComplete"] is not True:
        raise ValueError("descriptor database is not marked complete")
    if _require_int(
        "crossStructureContract.descriptorAnchorCount",
        contract["descriptorAnchorCount"],
        minimum=0,
    ) != int(counts["anchorCount"]):
        raise ValueError("descriptor Anchor count differs from manifest Anchor count")
    if _require_int(
        "crossStructureContract.descriptorClosureCount",
        contract["descriptorClosureCount"],
        minimum=0,
    ) != int(counts["closureCount"]):
        raise ValueError("descriptor closure count differs from manifest closure count")
    if int(counts["anchorCount"]) > int(counts["closureCount"]):
        raise ValueError("manifest Anchor count exceeds closure count")
    if contract["descriptorBuilderVersion"] != "generic_descriptor_builder_v3":
        raise ValueError("descriptor builder version mismatch")
    return payload


def manifest_root(manifest: Mapping[str, Any]) -> str:
    return hash_obj("manifest", strict_manifest_payload(manifest))


def epoch_root(manifest_root_hex: str) -> str:
    return hash_obj(
        "epoch",
        {
            "manifestRoot": manifest_root_hex,
            "protocolVersion": PROTOCOL_VERSION,
        },
    )


def event_leaf(event: Mapping[str, Any]) -> dict[str, Any]:
    missing = [key for key in EVENT_LEAF_FIELDS if key not in event]
    if missing:
        raise ValueError(f"event leaf missing fields: {missing}")
    unknown = set(event) - set(EVENT_LEAF_FIELDS)
    if unknown:
        raise ValueError(f"event leaf has unknown fields: {sorted(unknown)}")
    leaf = {key: event[key] for key in EVENT_LEAF_FIELDS}
    for key in (
        "order_seq",
        "event_type_id",
        "block_ts",
        "src_addr_id",
        "dst_addr_id",
        "nft_id",
        "collection_id",
        "tx_id",
    ):
        leaf[key] = int(leaf[key])
    for key in (
        "event_id",
        "event_type",
        "src_addr",
        "dst_addr",
        "nft",
        "collection",
        "tx_hash",
    ):
        leaf[key] = str(leaf[key])
        if not leaf[key]:
            raise ValueError(f"event leaf has empty {key}")
    return leaf


def event_leaf_hash(event: Mapping[str, Any]) -> str:
    return hash_obj("event-leaf-v2", event_leaf(event))


def anchor_leaf_hash(row: Mapping[str, Any]) -> str:
    return hash_obj("anchor-descriptor-leaf-v1", dict(row))


def closure_domain_leaf_hash(row: Mapping[str, Any]) -> str:
    return hash_obj("closure-descriptor-leaf-v1", dict(row))


def anchor_directory_leaf_hash(closure_id: int, anchor_order: int) -> str:
    return hash_obj(
        "anchor-directory-leaf-v1",
        {"closure_id": int(closure_id), "anchor_order_seq": int(anchor_order)},
    )


def tx_leaf_hash(order_seq: int, tx_id: int, tx_hash: str) -> str:
    return hash_obj(
        "tx-binding-leaf-v1",
        {"order_seq": int(order_seq), "tx_id": int(tx_id), "tx_hash": str(tx_hash)},
    )


def length_prefixed_digest(domain: str, values: Iterable[bytes]) -> str:
    h = hashlib.sha256()
    h.update(PROTOCOL_VERSION.encode("ascii") + b"\0" + domain.encode("ascii") + b"\0")
    for value in values:
        h.update(len(value).to_bytes(8, "big"))
        h.update(value)
    return h.hexdigest()
