from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import sqlite3
import time
from collections import defaultdict
from bisect import bisect_left
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_hex_bytes(data: bytes) -> str:
    return "0x" + hashlib.sha256(data).hexdigest()


def sha256_hex_json(obj: Any) -> str:
    return sha256_hex_bytes(stable_json_dumps(obj).encode("utf-8"))


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, obj: Any, *, compact: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if compact:
        path.write_text(stable_json_dumps(obj), encoding="utf-8")
    else:
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(stable_json_dumps(row) + "\n")
            n += 1
    return n


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_gzip_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))


def write_gzip_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as f:
        for row in rows:
            f.write(stable_json_dumps(row) + "\n")
            n += 1
    return n


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def read_csv_rows_with_header(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    """Read a CSV without losing its schema when the data row set is empty."""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def write_csv_rows(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: Sequence[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames))
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})
            n += 1
    return n


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return "0x" + h.hexdigest()


def merkle_empty_hash(tag: str) -> str:
    return sha256_hex_json({"empty": f"merkle:{tag}"})


def merkle_root_hex(leaf_hashes: Sequence[str], tag: str) -> str:
    if not leaf_hashes:
        return merkle_empty_hash(tag)
    layer = list(leaf_hashes)
    while len(layer) > 1:
        nxt: List[str] = []
        for i in range(0, len(layer), 2):
            left = layer[i]
            right = layer[i + 1] if i + 1 < len(layer) else left
            nxt.append(sha256_hex_json({"tag": tag, "left": left, "right": right}))
        layer = nxt
    return layer[0]


def merkle_proof(leaf_hashes: Sequence[str], index: int, tag: str) -> List[Dict[str, str]]:
    if index < 0 or index >= len(leaf_hashes):
        raise IndexError(index)
    layer = list(leaf_hashes)
    idx = index
    out: List[Dict[str, str]] = []
    while len(layer) > 1:
        if idx % 2:
            out.append({"side": "left", "hash": layer[idx - 1]})
        else:
            sib = idx + 1 if idx + 1 < len(layer) else idx
            out.append({"side": "right", "hash": layer[sib]})
        nxt: List[str] = []
        for i in range(0, len(layer), 2):
            left = layer[i]
            right = layer[i + 1] if i + 1 < len(layer) else left
            nxt.append(sha256_hex_json({"tag": tag, "left": left, "right": right}))
        idx //= 2
        layer = nxt
    return out


def verify_merkle_proof(leaf_hash: str, proof: Sequence[Dict[str, str]], root: str, tag: str) -> bool:
    cur = leaf_hash
    for step in proof:
        side = step.get("side")
        h = step.get("hash")
        if side == "left":
            cur = sha256_hex_json({"tag": tag, "left": h, "right": cur})
        elif side == "right":
            cur = sha256_hex_json({"tag": tag, "left": cur, "right": h})
        else:
            return False
    return cur == root


def sqlite_connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def count_rows(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def now_ms() -> int:
    return int(time.time() * 1000)


def get_runtime_meta(conn: sqlite3.Connection) -> Dict[str, str]:
    if not table_exists(conn, "nft_runtime_meta"):
        return {}
    return {str(r["meta_key"]): str(r["meta_value"]) for r in conn.execute("SELECT meta_key, meta_value FROM nft_runtime_meta")}


def query_name_to_id(query_name: str) -> str:
    m = {
        "C1_collection_micro_community": "C1",
        "C2_dispersal_then_return_flow": "C2",
        "L3_return_to_seller_convergence": "L3_RETURN",
        "L6_negative_return_support_closure": "L6_NEG_RETURN_SUPPORT",
        "H5_23": "H5_23",
    }
    return m.get(query_name, query_name)


def canonical_edge(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "closure_id": int(row["closure_id"]),
        "order_seq": int(row["order_seq"]),
        "edge_id": int(row["edge_id"]),
        "event_type_id": int(row["event_type_id"]),
        "block_ts": int(row["block_ts"]),
        "src_addr_id": int(row["src_addr_id"]),
        "dst_addr_id": int(row["dst_addr_id"]),
        "nft_id": int(row["nft_id"]),
        "collection_id": int(row["collection_id"]),
    }


def edge_payload_hash(edge: Dict[str, Any]) -> str:
    return sha256_hex_json({"nft_edge_payload_v1": edge})


def closure_payload_root(edges: Sequence[Dict[str, Any]]) -> str:
    return merkle_root_hex([edge_payload_hash(e) for e in edges], "nft_closure_payload")


def build_closure_record_hash(record: Dict[str, Any]) -> str:
    keep = {
        "cid": record.get("cid"),
        "beta_c": record.get("beta_c"),
        "S_c_base": record.get("S_c_base"),
        "S_c_ref": record.get("S_c_ref"),
        "R_c_pay": record.get("R_c_pay"),
        "payloadEdgeCount": record.get("payloadEdgeCount"),
    }
    return sha256_hex_json({"nft_closure_record_v1": keep})


def build_segment_leaf_hash(seg: Dict[str, Any]) -> str:
    keep = {
        "gid": seg.get("gid"),
        "range_g": seg.get("range_g"),
        "R_g_post": seg.get("R_g_post"),
        "R_g_post_dict": seg.get("R_g_post_dict"),
        "R_g_clo": seg.get("R_g_clo"),
        "S_bar_g": seg.get("S_bar_g"),
    }
    return sha256_hex_json({"nft_segment_leaf_v1": keep})


def nft_dictionary_snapshot(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Return the compact dictionary sidecar that maps committed ids to semantics.

    The Full Recompute verifier uses committed manifest payload edges, whose event
    types and role values are integer ids.  This snapshot binds the SQLite
    dictionaries needed to interpret those ids under DictionaryRoot instead of
    trusting an uncommitted sidecar database.
    """
    event_types = []
    if table_exists(conn, "nft_event_type_dict"):
        event_types = [
            {"event_type_id": int(r["event_type_id"]), "event_type": str(r["event_type"])}
            for r in conn.execute("SELECT event_type_id,event_type FROM nft_event_type_dict ORDER BY event_type_id,event_type")
        ]
    address_canon = []
    if table_exists(conn, "nft_address_canon_dict"):
        address_canon = [
            {"addr_id": int(r["addr_id"]), "value_text": str(r["value_text"]).lower()}
            for r in conn.execute("SELECT addr_id,value_text FROM nft_address_canon_dict ORDER BY addr_id,value_text")
        ]
    role_values = []
    if table_exists(conn, "nft_role_value_dict"):
        role_values = [
            {"role_name": str(r["role_name"]), "value_id": int(r["value_id"]), "value_text": str(r["value_text"])}
            for r in conn.execute("SELECT role_name,value_id,value_text FROM nft_role_value_dict ORDER BY role_name,value_id,value_text")
        ]
    return {
        "dictionaryVersion": "nft_dictionary_v1",
        "eventTypes": event_types,
        "addressCanon": address_canon,
        "roleValues": role_values,
    }


def nft_dictionary_root_from_snapshot(snapshot: Dict[str, Any]) -> str:
    event_hashes = [sha256_hex_json({"nft_dictionary_event_type_v1": r}) for r in snapshot.get("eventTypes") or []]
    address_hashes = [sha256_hex_json({"nft_dictionary_address_canon_v1": r}) for r in snapshot.get("addressCanon") or []]
    role_hashes = [sha256_hex_json({"nft_dictionary_role_value_v1": r}) for r in snapshot.get("roleValues") or []]
    return sha256_hex_json({
        "nft_dictionary_root_v1": {
            "dictionaryVersion": snapshot.get("dictionaryVersion") or "nft_dictionary_v1",
            "eventTypeRoot": merkle_root_hex(event_hashes, "nft_dictionary:event_type"),
            "addressCanonRoot": merkle_root_hex(address_hashes, "nft_dictionary:address_canon"),
            "roleValueRoot": merkle_root_hex(role_hashes, "nft_dictionary:role_value"),
            "numEventTypes": len(event_hashes),
            "numAddressCanon": len(address_hashes),
            "numRoleValues": len(role_hashes),
        }
    })


def nft_dictionary_root(conn: sqlite3.Connection) -> str:
    return nft_dictionary_root_from_snapshot(nft_dictionary_snapshot(conn))


def manifest_root_from_manifest(manifest: Dict[str, Any]) -> str:
    roots = manifest.get("roots") or {}
    return sha256_hex_json({
        "nft_manifest_root_v4": {
            "manifestVersion": manifest.get("manifestVersion"),
            "manifestType": manifest.get("manifestType"),
            "scope": manifest.get("scope"),
            "scopeDefinition": manifest.get("scopeDefinition"),
            "formalSnapshotComplete": manifest.get("formalSnapshotComplete"),
            "epoch": manifest.get("epoch"),
            "roots": roots,
            "metrics": manifest.get("metrics"),
        }
    })


def formal_manifest_contract_issues(manifest: Dict[str, Any]) -> List[str]:
    """Return protocol-contract violations for a formal NFT episode snapshot."""
    issues: List[str] = []
    epoch = manifest.get("epoch") or {}
    ruleset = manifest.get("rulesetSnapshot") or {}
    metrics = manifest.get("metrics") or {}
    expected = {
        "manifestVersion": 4,
        "manifestType": "nft_episode_compact_manifest",
        "scope": "nft_episode",
        "scopeDefinition": "nft_role_normalizable_closures_v2",
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            issues.append(f"{field}_mismatch")
    if epoch.get("scope") != "nft_episode":
        issues.append("epoch_scope_mismatch")
    if ruleset.get("ruleset") != "nft_ruleset_v1":
        issues.append("ruleset_id_mismatch")
    if ruleset.get("scope") != "nft_episode":
        issues.append("ruleset_scope_mismatch")
    expected_coverage_map = {
        "root": "sig_id:1",
        "root_receive_or_buy": "sig_id:1",
        "role_transition": "sig_id:1",
        "community_transition": "sig_id:1",
    }
    if ruleset.get("coverageAnchorSignatureMap") != expected_coverage_map:
        issues.append("coverage_anchor_signature_map_mismatch")
    if manifest.get("formalSnapshotComplete") is not True:
        issues.append("formal_snapshot_incomplete")
    try:
        closure_limit = metrics.get("closureLimit")
        invalid_count = metrics.get("invalidClosureCount")
        if (
            isinstance(closure_limit, bool)
            or closure_limit is None
            or int(closure_limit) != 0
        ):
            issues.append("closure_limit_nonzero")
        if (
            isinstance(invalid_count, bool)
            or invalid_count is None
            or int(invalid_count) != 0
        ):
            issues.append("invalid_closure_count_nonzero")
    except (TypeError, ValueError):
        issues.append("formal_metric_type_invalid")
    if metrics.get("excludeInvalidClosures") is not False:
        issues.append("invalid_closure_exclusion_enabled")
    try:
        scope_count = int(metrics.get("scopeClosureCount"))
        in_scope_root_count = int(metrics.get("inScopeRootClosureCount"))
        sig1_unique_count = int(metrics.get("sig1UniqueClosureCount"))
        missing_sig1_count = int(metrics.get("missingFromSig1Count"))
        extra_sig1_count = int(metrics.get("extraInSig1Count"))
        duplicate_sig1_count = int(metrics.get("duplicateSig1Count"))
        if in_scope_root_count != scope_count:
            issues.append("in_scope_root_closure_count_mismatch")
        if sig1_unique_count != scope_count:
            issues.append("sig1_unique_closure_count_mismatch")
        if missing_sig1_count != 0:
            issues.append("sig1_root_closure_missing")
        if extra_sig1_count != 0:
            issues.append("sig1_root_closure_extra")
        if duplicate_sig1_count != 0:
            issues.append("sig1_root_closure_duplicate")
    except (TypeError, ValueError):
        issues.append("sig1_coverage_metric_invalid")
    return issues



VALID_NFT_EVENT_TYPES = {"NFTTrade", "NFTTransfer"}
VALID_NFT_PLAN_KINDS = {"aggregate_closure", "flow_convergence", "event_pattern", "variable_path"}
VALID_NFT_ANCHOR_KINDS = {
    "root", "root_receive_or_buy", "role_transition", "community_transition",
    "out_from_anchor_buyer", "out_from_anchor",
    "return_to_anchor_seller", "return_to_anchor", "community_candidate",
}
VALID_NFT_FIELDS = {
    "src", "move_from", "seller", "from_address",
    "dst", "move_to", "buyer", "to_address",
    "nft", "collection", "collection_id", "collection_contract",
    "event_type_id", "order_seq", "order", "block_ts",
}
VALID_NFT_CONSTRAINTS = {
    "same", "not_equal", "neq", "after", "order", "within",
    "pairwise_distinct", "min_distinct", "distinct_event_ids",
}
ADDRESS_ROLE_NAMES = {"move_from", "move_to", "seller", "buyer", "from_address", "to_address"}


def canonical_query_hash(spec: Dict[str, Any]) -> str:
    return sha256_hex_json({"nft_query_spec_v1": spec})


def compressed_result_digest(keys: Iterable[str]) -> str:
    return sha256_hex_json({"nft_compressed_result_keys_v1": sorted(set(str(k) for k in keys))})


def _is_positive_int_token(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return int(value) > 0
    except Exception:
        return False


def _validate_event_type_token(value: Any) -> bool:
    if value in VALID_NFT_EVENT_TYPES:
        return True
    return _is_positive_int_token(value)


def validate_query_spec(spec: Dict[str, Any]) -> List[str]:
    """Validate the public QuerySpec before any replay.

    The verifier must fail closed on unknown operators, malformed references,
    and misspelled constraints.  Returning an error list keeps callers able to
    report all defects at once while still treating any non-empty list as a hard
    verification failure.
    """
    errors: List[str] = []
    if not isinstance(spec, dict):
        return ["query_spec_not_object"]
    query_name = str(spec.get("query_name") or "").strip()
    if not query_name:
        errors.append("missing_query_name")

    atoms = list(((spec.get("anchor_requirements") or {}).get("atoms") or []))
    if not atoms:
        errors.append("missing_anchor_requirements_atoms")
    atom_names: set[str] = set()
    for i, atom in enumerate(atoms):
        if not isinstance(atom, dict):
            errors.append(f"anchor_atom_{i}_not_object")
            continue
        name = str(atom.get("name") or "").strip()
        if not name:
            errors.append(f"anchor_atom_{i}_missing_name")
        elif name in atom_names:
            errors.append(f"duplicate_anchor_atom:{name}")
        atom_names.add(name)
        kind = str(atom.get("kind") or "root")
        if kind not in VALID_NFT_ANCHOR_KINDS:
            errors.append(f"unknown_anchor_kind:{kind}")
        event_types = atom.get("event_type") or ["NFTTrade", "NFTTransfer"]
        if not isinstance(event_types, list) or not event_types:
            errors.append(f"anchor_atom_{name or i}_invalid_event_type_list")
        else:
            for et in event_types:
                if not _validate_event_type_token(et):
                    errors.append(f"anchor_atom_{name or i}_unknown_event_type:{et}")

    plan = spec.get("local_plan") or {}
    if not isinstance(plan, dict):
        return errors + ["local_plan_not_object"]
    kind = str(plan.get("kind") or "")
    if kind not in VALID_NFT_PLAN_KINDS:
        errors.append(f"unknown_local_plan_kind:{kind}")
    output_label = str(plan.get("output_label") or query_name).strip()
    if not output_label:
        errors.append("missing_output_label")

    patterns = list(plan.get("event_patterns") or [])
    pattern_ids: set[str] = set()
    variables: set[str] = set()
    if kind == "event_pattern" and not patterns:
        errors.append("event_pattern_missing_event_patterns")
    for i, pat in enumerate(patterns):
        if not isinstance(pat, dict):
            errors.append(f"event_pattern_{i}_not_object")
            continue
        pid = str(pat.get("id") or "").strip()
        if not pid:
            errors.append(f"event_pattern_{i}_missing_id")
        elif pid in pattern_ids:
            errors.append(f"duplicate_event_pattern_id:{pid}")
        pattern_ids.add(pid)
        event_types = pat.get("event_type") or []
        if event_types and not isinstance(event_types, list):
            errors.append(f"event_pattern_{pid or i}_invalid_event_type_list")
        for et in event_types if isinstance(event_types, list) else []:
            if not _validate_event_type_token(et):
                errors.append(f"event_pattern_{pid or i}_unknown_event_type:{et}")
        for field in ("src", "dst", "nft", "collection"):
            tok = pat.get(field)
            if tok in (None, ""):
                continue
            if isinstance(tok, str) and tok.startswith("$") and len(tok) > 1:
                variables.add(tok)
            elif not _is_positive_int_token(tok):
                errors.append(f"event_pattern_{pid or i}_invalid_{field}_token:{tok}")

    anchor_event = str(plan.get("anchor_event") or "root")
    if kind == "event_pattern" and anchor_event not in pattern_ids:
        errors.append(f"anchor_event_not_in_patterns:{anchor_event}")

    def valid_ref(ref: Any, where: str) -> None:
        if isinstance(ref, bool) or ref in (None, ""):
            errors.append(f"{where}:missing_reference")
            return
        if isinstance(ref, (int, float)):
            return
        if not isinstance(ref, str):
            errors.append(f"{where}:invalid_reference_type")
            return
        if ref.startswith("$"):
            if ref not in variables:
                errors.append(f"{where}:unknown_variable:{ref}")
            return
        if "." in ref:
            event_id, field = ref.split(".", 1)
            if event_id not in pattern_ids:
                errors.append(f"{where}:unknown_event:{event_id}")
            if field not in VALID_NFT_FIELDS:
                errors.append(f"{where}:unknown_field:{field}")
            return
        try:
            int(ref)
        except Exception:
            errors.append(f"{where}:invalid_literal:{ref}")

    for i, c in enumerate(plan.get("constraints") or []):
        if not isinstance(c, dict):
            errors.append(f"constraint_{i}_not_object")
            continue
        typ = str(c.get("type") or "")
        if typ not in VALID_NFT_CONSTRAINTS:
            errors.append(f"unknown_constraint_type:{typ}")
            continue
        prefix = f"constraint_{i}_{typ}"
        if typ in {"same", "not_equal", "neq"}:
            valid_ref(c.get("left"), prefix + "_left")
            valid_ref(c.get("right"), prefix + "_right")
        elif typ == "after":
            later, earlier = str(c.get("later") or ""), str(c.get("earlier") or "")
            if later not in pattern_ids:
                errors.append(f"{prefix}:unknown_later:{later}")
            if earlier not in pattern_ids:
                errors.append(f"{prefix}:unknown_earlier:{earlier}")
        elif typ == "order":
            seq = list(c.get("sequence") or [])
            if len(seq) < 2:
                errors.append(f"{prefix}:sequence_too_short")
            for eid in seq:
                if str(eid) not in pattern_ids:
                    errors.append(f"{prefix}:unknown_event:{eid}")
        elif typ == "within":
            later, earlier = str(c.get("later") or ""), str(c.get("earlier") or "")
            if later not in pattern_ids:
                errors.append(f"{prefix}:unknown_later:{later}")
            if earlier not in pattern_ids:
                errors.append(f"{prefix}:unknown_earlier:{earlier}")
            if norm_int(c.get("delta_seconds"), -1) < 0:
                errors.append(f"{prefix}:invalid_delta_seconds")
        elif typ in {"pairwise_distinct", "min_distinct"}:
            refs = list(c.get("values") or [])
            if len(refs) < 2:
                errors.append(f"{prefix}:too_few_values")
            for j, ref in enumerate(refs):
                valid_ref(ref, f"{prefix}_value_{j}")
            if typ == "min_distinct" and norm_int(c.get("min_count"), 0) <= 0:
                errors.append(f"{prefix}:invalid_min_count")
        elif typ == "distinct_event_ids":
            evs = list(c.get("events") or [])
            if len(evs) < 2:
                errors.append(f"{prefix}:too_few_events")
            for eid in evs:
                if str(eid) not in pattern_ids:
                    errors.append(f"{prefix}:unknown_event:{eid}")

    for i, anti in enumerate(plan.get("anti_conditions") or []):
        if not isinstance(anti, dict):
            errors.append(f"anti_condition_{i}_not_object")
            continue
        typ = str(anti.get("type") or "")
        if typ != "no_event":
            errors.append(f"unknown_anti_condition_type:{typ}")
            continue
        after = str(anti.get("after") or "")
        if after and after not in pattern_ids:
            errors.append(f"anti_condition_{i}:unknown_after:{after}")
        if norm_int(anti.get("within_seconds"), 0) < 0:
            errors.append(f"anti_condition_{i}:invalid_within_seconds")
        event_types = anti.get("event_type") or []
        if event_types and not isinstance(event_types, list):
            errors.append(f"anti_condition_{i}:invalid_event_type_list")
        for et in event_types if isinstance(event_types, list) else []:
            if not _validate_event_type_token(et):
                errors.append(f"anti_condition_{i}:unknown_event_type:{et}")
        for field in ("src", "dst", "nft", "collection"):
            tok = anti.get(field)
            if tok in (None, ""):
                continue
            valid_ref(tok, f"anti_condition_{i}_{field}")

    if kind == "aggregate_closure":
        min_addr = norm_int(plan.get("min_addresses"), 0)
        max_addr = norm_int(plan.get("max_addresses"), 0)
        if min_addr < 0 or max_addr < 0 or (max_addr and min_addr > max_addr):
            errors.append("aggregate_closure_invalid_address_bounds")
        if norm_int(plan.get("transition_threshold"), 0) <= 0:
            errors.append("aggregate_closure_invalid_transition_threshold")
        if norm_int(plan.get("min_nfts_with_transitions_ge"), 0) <= 0:
            errors.append("aggregate_closure_invalid_min_nfts")
    elif kind == "flow_convergence":
        if norm_int(plan.get("min_out_targets"), 0) <= 0:
            errors.append("flow_convergence_invalid_min_out_targets")
        if norm_int(plan.get("min_return_sources"), 0) <= 0:
            errors.append("flow_convergence_invalid_min_return_sources")
    elif kind == "variable_path":
        if norm_int(plan.get("max_depth"), 0) < 2:
            errors.append("variable_path_invalid_max_depth")

    return errors


def dictionary_invariant_issues(snapshot: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(snapshot, dict):
        return ["dictionary_snapshot_not_object"]
    event_id_to_name: Dict[int, str] = {}
    event_name_to_id: Dict[str, int] = {}
    for i, row in enumerate(snapshot.get("eventTypes") or []):
        try:
            eid = int(row.get("event_type_id"))
            name = str(row.get("event_type") or "")
        except Exception:
            errors.append(f"event_type_row_{i}_malformed")
            continue
        if eid <= 0 or not name:
            errors.append(f"event_type_row_{i}_invalid")
        if eid in event_id_to_name and event_id_to_name[eid] != name:
            errors.append(f"event_type_id_conflict:{eid}")
        if name in event_name_to_id and event_name_to_id[name] != eid:
            errors.append(f"event_type_name_conflict:{name}")
        event_id_to_name[eid] = name
        event_name_to_id[name] = eid
    for required in sorted(VALID_NFT_EVENT_TYPES):
        if required not in event_name_to_id:
            errors.append(f"missing_event_type:{required}")

    addr_id_to_text: Dict[int, str] = {}
    addr_text_to_id: Dict[str, int] = {}
    for i, row in enumerate(snapshot.get("addressCanon") or []):
        try:
            aid = int(row.get("addr_id"))
            raw_text = str(row.get("value_text") or "")
        except Exception:
            errors.append(f"address_row_{i}_malformed")
            continue
        text = raw_text.lower()
        if aid <= 0 or not text:
            errors.append(f"address_row_{i}_invalid")
        if raw_text != text:
            errors.append(f"address_not_lowercase:{raw_text}")
        if aid in addr_id_to_text and addr_id_to_text[aid] != text:
            errors.append(f"address_id_conflict:{aid}")
        if text in addr_text_to_id and addr_text_to_id[text] != aid:
            errors.append(f"address_text_conflict:{text}")
        addr_id_to_text[aid] = text
        addr_text_to_id[text] = aid

    role_id_seen: Dict[int, Tuple[str, str]] = {}
    role_key_seen: set[Tuple[str, str]] = set()
    for i, row in enumerate(snapshot.get("roleValues") or []):
        try:
            rid = int(row.get("value_id"))
            role = str(row.get("role_name") or "")
            raw_text = str(row.get("value_text") or "")
        except Exception:
            errors.append(f"role_value_row_{i}_malformed")
            continue
        if rid <= 0 or not role or not raw_text:
            errors.append(f"role_value_row_{i}_invalid")
        if rid in role_id_seen and role_id_seen[rid] != (role, raw_text):
            errors.append(f"role_value_id_conflict:{rid}")
        key = (role, raw_text)
        if key in role_key_seen:
            errors.append(f"duplicate_role_value:{role}:{raw_text}")
        role_id_seen[rid] = key
        role_key_seen.add(key)
        if role in ADDRESS_ROLE_NAMES:
            text = raw_text.lower()
            if raw_text != text:
                errors.append(f"role_address_not_lowercase:{role}:{raw_text}")
            if text not in addr_text_to_id:
                errors.append(f"role_address_missing_from_canon:{role}:{text}")
    return errors


def dictionary_runtime_maps(snapshot: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    errors = dictionary_invariant_issues(snapshot)
    event_type_by_id = {int(r["event_type_id"]): str(r["event_type"]) for r in snapshot.get("eventTypes") or [] if r.get("event_type_id") is not None}
    event_type_to_id = {v: k for k, v in event_type_by_id.items()}
    address_by_text = {str(r.get("value_text") or "").lower(): int(r.get("addr_id") or 0) for r in snapshot.get("addressCanon") or []}
    role_id_to_canon: Dict[int, int] = {}
    for r in snapshot.get("roleValues") or []:
        if str(r.get("role_name") or "") in ADDRESS_ROLE_NAMES:
            role_id_to_canon[int(r.get("value_id") or 0)] = address_by_text.get(str(r.get("value_text") or "").lower(), 0)
    return {
        "event_type_by_id": event_type_by_id,
        "event_type_to_id": event_type_to_id,
        "address_by_text": address_by_text,
        "role_id_to_canon": role_id_to_canon,
    }, errors


def canonicalize_committed_edge(raw: Dict[str, Any], dictionary_maps: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        e = {
            "closure_id": int(raw.get("closure_id") or 0),
            "order_seq": int(raw.get("order_seq") or 0),
            "edge_id": int(raw.get("edge_id") or 0),
            "event_type_id": int(raw.get("event_type_id") or 0),
            "block_ts": int(raw.get("block_ts") or 0),
            "src_addr_id": int(raw.get("src_addr_id") or 0),
            "dst_addr_id": int(raw.get("dst_addr_id") or 0),
            "nft_id": int(raw.get("nft_id") or 0),
            "collection_id": int(raw.get("collection_id") or 0),
        }
    except Exception:
        return None
    role_map = dictionary_maps.get("role_id_to_canon") or {}
    e["src_addr_id"] = int(role_map.get(e["src_addr_id"], 0))
    e["dst_addr_id"] = int(role_map.get(e["dst_addr_id"], 0))
    valid_event_ids = set((dictionary_maps.get("event_type_by_id") or {}).keys())
    if (
        e["order_seq"] <= 0 or e["edge_id"] <= 0 or e["event_type_id"] not in valid_event_ids
        or e["block_ts"] < 0 or e["src_addr_id"] <= 0 or e["dst_addr_id"] <= 0
        or e["nft_id"] <= 0 or e["collection_id"] <= 0
    ):
        return None
    return e


def index_root_from_manifest_roots(roots: Dict[str, Any]) -> str:
    return sha256_hex_json({
        "nft_index_root_v1": {
            "segmentRoot": roots.get("segmentRoot"),
            "closureRoot": roots.get("closureRoot"),
            "postingRoot": roots.get("postingRoot"),
            "collectionEdgeDictRoot": roots.get("collectionEdgeDictRoot"),
            "dictionaryRoot": roots.get("dictionaryRoot"),
            "statsRoot": roots.get("statsRoot"),
            "scopeMembershipRoot": roots.get("scopeMembershipRoot"),
        }
    })


def epoch_root_from_manifest(manifest: Dict[str, Any]) -> str:
    roots = manifest.get("roots") or {}
    epoch = manifest.get("epoch") or {}
    return sha256_hex_json({
        "nft_epoch_root_v1": {
            "scope": manifest.get("scope"),
            "epochId": int(epoch.get("epochId") or 0),
            "segmentSize": int(epoch.get("segmentSize") or 0),
            "manifestRoot": manifest.get("manifestRoot") or manifest_root_from_manifest(manifest),
            "payloadRoot": roots.get("globalEdgeRoot"),
            "indexRoot": index_root_from_manifest_roots(roots),
            "rulesetRoot": roots.get("rulesetRoot"),
        }
    })

def signature_for_anchor_atom(atom: Dict[str, Any]) -> List[str]:
    """Return generic NFT anchor signatures for an anchor atom.

    Current compact sidecar has three generic signature ids:
      1 universal root/role_transition coverage
      2 later transition from the root destination
      3 out_from_anchor_buyer / return_to_anchor_seller style structural atoms
    The mapping is intentionally generic by role-transition interface, not by
    final-five query id.
    """
    kind = str(atom.get("kind") or "root")
    if kind in {
        "root",
        "root_receive_or_buy",
        "role_transition",
        "community_transition",
    }:
        return ["sig_id:1"]
    return ["sig_id:3"]


def anchor_sig_text_to_id() -> Dict[str, int]:
    return {
        "sig_id:1": 1,
        "sig_id:2": 2,
        "sig_id:3": 3,
    }


def norm_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


class NftLocalVerifier:
    def __init__(
        self,
        conn: sqlite3.Connection | None,
        committed_payload_only: bool = False,
        *,
        dictionary_snapshot: Dict[str, Any] | None = None,
    ):
        self.conn = conn
        if dictionary_snapshot is None:
            if conn is None:
                raise ValueError("dictionary_snapshot or SQLite connection is required")
            dictionary_snapshot = nft_dictionary_snapshot(conn)
        self.dictionary_snapshot = dictionary_snapshot
        self.dictionary_maps, dictionary_errors = dictionary_runtime_maps(dictionary_snapshot)
        if dictionary_errors:
            raise ValueError("invalid committed NFT dictionary: " + ";".join(dictionary_errors[:16]))
        self.type_ids = dict(self.dictionary_maps["event_type_to_id"])
        self.trade_id = self.type_ids.get("NFTTrade", -1)
        self.transfer_id = self.type_ids.get("NFTTransfer", -2)
        self.role_id_to_canon = dict(self.dictionary_maps["role_id_to_canon"])
        self.root_postings: Dict[Tuple[int, int], sqlite3.Row] = {}
        self.root_posting_first: Dict[int, sqlite3.Row] = {}
        self.edges_by_closure: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        self.edges_by_collection: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        self.edge_by_order: Dict[int, Dict[str, Any]] = {}
        if not committed_payload_only:
            if conn is None:
                raise ValueError("SQLite connection required when committed_payload_only=False")
            for r in conn.execute("SELECT * FROM nft_root_anchor_postings ORDER BY closure_id, root_order_seq"):
                cid = int(r["closure_id"])
                self.root_postings[(cid, int(r["root_order_seq"]))] = r
                self.root_posting_first.setdefault(cid, r)
            for r in conn.execute("SELECT * FROM nft_role_transition_edges ORDER BY closure_id, collection_id, order_seq, edge_id"):
                e = self._runtime_edge(r)
                self.edges_by_closure[int(e["closure_id"])].append(e)
                self.edges_by_collection[int(e["collection_id"])].append(e)
                self.edge_by_order[int(e["order_seq"])] = e
        self.collection_orders: Dict[int, List[int]] = {
            cid: [int(e["order_seq"]) for e in edges]
            for cid, edges in self.edges_by_collection.items()
        }

    def _runtime_edge(self, row: sqlite3.Row) -> Dict[str, Any]:
        e = canonical_edge(row)
        e["src_addr_id"] = self.role_id_to_canon.get(int(e["src_addr_id"]), 0)
        e["dst_addr_id"] = self.role_id_to_canon.get(int(e["dst_addr_id"]), 0)
        return e

    def _runtime_edge_from_mapping(self, row: Dict[str, Any]) -> Dict[str, Any] | None:
        return canonicalize_committed_edge(row, self.dictionary_maps)

    def build_manifest_edge_source(self, global_edge_by_order: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
        """Build a replay edge source from committed manifest global edges.

        The returned structure has the same access shape as the compact DB edge
        arrays, but it is sourced from the manifest/global-edge sidecar rather
        than from mutable SQLite payload tables.
        """
        edges_by_collection: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        edge_by_order: Dict[int, Dict[str, Any]] = {}
        invalid_orders: List[int] = []
        for order in sorted(int(x) for x in global_edge_by_order):
            e = self._runtime_edge_from_mapping(global_edge_by_order[order])
            if e is None:
                invalid_orders.append(order)
                continue
            edges_by_collection[int(e["collection_id"])].append(e)
            edge_by_order[int(e["order_seq"])] = e
        collection_orders = {
            cid: [int(e["order_seq"]) for e in edges]
            for cid, edges in edges_by_collection.items()
        }
        return {
            "edges_by_collection": edges_by_collection,
            "collection_orders": collection_orders,
            "edge_by_order": edge_by_order,
            "invalid_raw_orders": invalid_orders,
        }

    def load_edges(self, closure_id: int, anchor_order: int = 0) -> List[Dict[str, Any]]:
        direct = self.edges_by_closure.get(int(closure_id), [])
        if direct:
            return list(direct)
        # Compact v22 stores the global role-transition edge array with
        # closure_id=0.  Per-closure exact replay is reconstructed from the
        # generic root anchor posting: collection + [root_order_seq, end_ts].
        prow = self.root_postings.get((int(closure_id), int(anchor_order))) if anchor_order else None
        if prow is None:
            prow = self.root_posting_first.get(int(closure_id))
        if prow is None:
            return []
        coll = int(prow["collection_id"])
        start_order = int(prow["root_order_seq"])
        end_ts = int(prow["end_ts"])
        edges = self.edges_by_collection.get(coll, [])
        orders = self.collection_orders.get(coll, [])
        i = bisect_left(orders, start_order)
        out: List[Dict[str, Any]] = []
        for e in edges[i:]:
            if int(e["block_ts"]) > end_ts:
                # block_ts/order are monotonic enough for this compact episode
                # workload; keep scanning only when timestamps tie oddly.
                if int(e["order_seq"]) > start_order:
                    break
            out.append(e)
        return out

    def load_edges_from_payload_binding(self, binding: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Reconstruct a compact closure window from a committed payload binding.

        Verification must not depend on mutable root posting rows once VOcov has
        opened an authenticated anchor entry.  The manifest closure record binds
        collectionId/rootOrderSeq/endTs; this method uses that binding directly.
        """
        coll = int(binding.get("collectionId") or -1)
        start_order = int(binding.get("rootOrderSeq") or 0)
        end_ts = int(binding.get("endTs") or -1)
        edges = self.edges_by_collection.get(coll, [])
        orders = self.collection_orders.get(coll, [])
        if coll < 0 or start_order <= 0 or end_ts < 0:
            return []
        i = bisect_left(orders, start_order)
        out: List[Dict[str, Any]] = []
        for e in edges[i:]:
            if int(e["block_ts"]) > end_ts:
                if int(e["order_seq"]) > start_order:
                    break
            out.append(e)
        return out

    def load_edges_from_payload_binding_source(self, binding: Dict[str, Any], source: Dict[str, Any]) -> List[Dict[str, Any]]:
        coll = int(binding.get("collectionId") or -1)
        start_order = int(binding.get("rootOrderSeq") or 0)
        end_ts = int(binding.get("endTs") or -1)
        edges = (source.get("edges_by_collection") or {}).get(coll, [])
        orders = (source.get("collection_orders") or {}).get(coll, [])
        if coll < 0 or start_order <= 0 or end_ts < 0:
            return []
        i = bisect_left(orders, start_order)
        out: List[Dict[str, Any]] = []
        for e in edges[i:]:
            if int(e["block_ts"]) > end_ts:
                if int(e["order_seq"]) > start_order:
                    break
            out.append(e)
        return out

    @staticmethod
    def _anchor_edge(edges: Sequence[Dict[str, Any]], anchor_order: int) -> Optional[Dict[str, Any]]:
        for e in edges:
            if int(e["order_seq"]) == int(anchor_order):
                return e
        return None

    def verify(self, spec: Dict[str, Any], closure_id: int, anchor_order: int) -> Tuple[bool, Dict[str, Any]]:
        edges = self.load_edges(closure_id, anchor_order)
        return self.verify_with_edges(spec, closure_id, anchor_order, edges)

    def verify_with_payload_binding(self, spec: Dict[str, Any], closure_id: int, anchor_order: int, binding: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        edges = self.load_edges_from_payload_binding(binding)
        return self.verify_with_edges(spec, closure_id, anchor_order, edges)

    def verify_with_manifest_payload_binding(self, spec: Dict[str, Any], closure_id: int, anchor_order: int, binding: Dict[str, Any], source: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        edges = self.load_edges_from_payload_binding_source(binding, source)
        end_ts = int(binding.get("endTs") or -1)
        return self.verify_with_edges(spec, closure_id, anchor_order, edges, end_ts=end_ts)

    def verify_with_edges(self, spec: Dict[str, Any], closure_id: int, anchor_order: int, edges: Sequence[Dict[str, Any]], end_ts: int | None = None) -> Tuple[bool, Dict[str, Any]]:
        if not edges:
            return False, {"reason": "empty_closure_edges"}
        plan = spec.get("local_plan") or {}
        kind = str(plan.get("kind") or "event_pattern")
        anchor = self._anchor_edge(edges, anchor_order)
        if anchor is None:
            return False, {"reason": "anchor_order_not_in_closure", "anchor_order": anchor_order}
        if kind == "aggregate_closure":
            return self._verify_aggregate(plan, edges)
        if kind == "flow_convergence":
            return self._verify_flow(plan, anchor, edges, end_ts=end_ts)
        if kind == "variable_path":
            return self._verify_variable_path(plan, anchor, edges, end_ts=end_ts)
        if kind == "event_pattern":
            return self._verify_event_pattern(plan, anchor, edges, end_ts=end_ts)
        return False, {"reason": f"unsupported_local_plan:{kind}"}

    def _verify_aggregate(self, plan: Dict[str, Any], edges: Sequence[Dict[str, Any]]) -> Tuple[bool, Dict[str, Any]]:
        threshold = max(1, norm_int(plan.get("transition_threshold"), 2))
        min_nfts = max(1, norm_int(plan.get("min_nfts_with_transitions_ge"), 1))
        min_addr = norm_int(plan.get("min_addresses"), 0)
        max_addr = norm_int(plan.get("max_addresses"), 0)
        nft_counts: Dict[int, int] = defaultdict(int)
        addrs = set()
        for e in edges:
            nft_id = int(e["nft_id"])
            src_id = int(e["src_addr_id"])
            dst_id = int(e["dst_addr_id"])
            if nft_id != 0:
                nft_counts[nft_id] += 1
            if src_id != 0:
                addrs.add(src_id)
            if dst_id != 0:
                addrs.add(dst_id)
        good_nfts = [n for n, c in nft_counts.items() if c >= threshold]
        ok = len(good_nfts) >= min_nfts and (min_addr == 0 or len(addrs) >= min_addr) and (max_addr == 0 or len(addrs) <= max_addr)
        return ok, {"operator": "aggregate_closure", "nft_support": len(good_nfts), "addr_count": len(addrs)}

    def _verify_flow(self, plan: Dict[str, Any], anchor: Dict[str, Any], edges: Sequence[Dict[str, Any]], end_ts: int | None = None) -> Tuple[bool, Dict[str, Any]]:
        b = int(anchor["dst_addr_id"])
        a = int(anchor["src_addr_id"])
        x = int(anchor["nft_id"])
        limit_ts = int(end_ts if end_ts is not None and end_ts >= 0 else int(anchor["block_ts"]) + 86400)
        min_out = max(1, norm_int(plan.get("min_out_targets"), 2))
        min_ret = max(2, norm_int(plan.get("min_return_sources"), 2))
        exclude_x = bool(plan.get("exclude_anchor_nft", True))
        out_targets = set()
        out_nfts = set()
        first_order_by_mid: Dict[int, int] = {}
        out_orders: List[int] = []
        for e in edges:
            if int(e["src_addr_id"]) != b or int(e["order_seq"]) <= int(anchor["order_seq"]) or int(e["block_ts"]) > limit_ts:
                continue
            if exclude_x and int(e["nft_id"]) == x:
                continue
            if int(e["dst_addr_id"]) == 0:
                continue
            mid = int(e["dst_addr_id"])
            out_targets.add(mid)
            out_nfts.add(int(e["nft_id"]))
            out_orders.append(int(e["order_seq"]))
            first_order_by_mid[mid] = min(first_order_by_mid.get(mid, 10**30), int(e["order_seq"]))
        if len(out_targets) < min_out:
            return False, {"operator": "flow_convergence", "reason": "too_few_out_targets", "out_targets": len(out_targets)}
        returns_by_target: Dict[int, List[Tuple[int, Dict[str, Any]]]] = defaultdict(list)
        for mid, lower in first_order_by_mid.items():
            for e in edges:
                if int(e["src_addr_id"]) != mid or int(e["order_seq"]) <= lower or int(e["block_ts"]) > limit_ts or int(e["dst_addr_id"]) == 0:
                    continue
                returns_by_target[int(e["dst_addr_id"])].append((mid, e))
        best_target, best_sources, best_rows = None, 0, []
        for target, rows in returns_by_target.items():
            sources = {s for s, _ in rows}
            effective_sources = len(sources) + (1 if bool(plan.get("allow_return_to_anchor_src", True)) and target == a and a not in sources else 0)
            ok = effective_sources >= min_ret
            if ok and (effective_sources > best_sources or (effective_sources == best_sources and len(rows) > len(best_rows))):
                best_target, best_sources, best_rows = target, effective_sources, rows
        if not best_rows:
            return False, {"operator": "flow_convergence", "reason": "no_return_convergence", "out_targets": len(out_targets)}
        orders = sorted(out_orders + [int(e["order_seq"]) for _, e in best_rows])
        return True, {"operator": "flow_convergence", "out_targets": len(out_targets), "best_return_target": best_target, "return_sources": best_sources, "orders": orders}

    def _verify_variable_path(self, plan: Dict[str, Any], anchor: Dict[str, Any], edges: Sequence[Dict[str, Any]], end_ts: int | None = None) -> Tuple[bool, Dict[str, Any]]:
        a = int(anchor["src_addr_id"])
        b = int(anchor["dst_addr_id"])
        x = int(anchor["nft_id"])
        max_depth = max(2, norm_int(plan.get("max_depth"), 3))
        limit_ts = int(end_ts if end_ts is not None and end_ts >= 0 else int(anchor["block_ts"]) + 86400)
        outgoing: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for e in edges:
            if int(e["order_seq"]) <= int(anchor["order_seq"]) or int(e["block_ts"]) > limit_ts:
                continue
            # Rust generic variable_path always excludes the anchor NFT.
            if int(e["nft_id"]) == x:
                continue
            outgoing[int(e["src_addr_id"])].append(e)
        stack = [(b, int(anchor["order_seq"]), 0, [])]
        while stack:
            cur, last_order, depth, orders = stack.pop()
            if 2 <= depth <= max_depth and cur == a:
                return True, {"operator": "variable_path", "path_length": depth, "orders": orders}
            if depth >= max_depth:
                continue
            for e in outgoing.get(cur, []):
                if int(e["order_seq"]) <= last_order:
                    continue
                stack.append((int(e["dst_addr_id"]), int(e["order_seq"]), depth + 1, orders + [int(e["order_seq"])]))
        return False, {"operator": "variable_path", "reason": "no_path"}

    def _verify_event_pattern(self, plan: Dict[str, Any], anchor: Dict[str, Any], edges: Sequence[Dict[str, Any]], end_ts: int | None = None) -> Tuple[bool, Dict[str, Any]]:
        patterns = list(plan.get("event_patterns") or [])
        if not patterns:
            return False, {"reason": "missing_event_patterns"}
        anchor_id = str(plan.get("anchor_event") or "root")
        by_id = {str(p.get("id")): p for p in patterns}
        if anchor_id not in by_id:
            return False, {"reason": "anchor_pattern_missing"}
        assignment: Dict[str, Dict[str, Any]] = {anchor_id: anchor}
        bindings: Dict[str, int] = {}
        if not self._bind_pattern(anchor, by_id[anchor_id], bindings):
            return False, {"reason": "anchor_pattern_mismatch"}
        rest = [p for p in patterns if str(p.get("id")) != anchor_id]
        final_assignment: Dict[str, Dict[str, Any]] = {}
        early_anti_indices: set[int] = set()
        for anti_idx, anti in enumerate(plan.get("anti_conditions") or []):
            after_id = str(anti.get("after") or "")
            if after_id not in ("", anchor_id):
                continue
            refs = [
                anti.get(name)
                for name in ("src", "dst", "nft", "collection")
                if anti.get(name) not in (None, "")
            ]
            if not all(self._resolve_ref(ref, assignment, bindings) is not None for ref in refs):
                continue
            early_anti_indices.add(anti_idx)
            if self._anti_fails(anti, anchor, edges, assignment, bindings, end_ts=end_ts):
                return False, {"operator": "event_pattern", "reason": "anti_condition_matched"}

        def rec(i: int, cur_bindings: Dict[str, int]) -> bool:
            if i == len(rest):
                if not self._constraints_ok(plan, assignment, cur_bindings, require_resolved=True):
                    return False
                for anti_idx, anti in enumerate(plan.get("anti_conditions") or []):
                    if anti_idx in early_anti_indices:
                        continue
                    if self._anti_fails(anti, anchor, edges, assignment, cur_bindings, end_ts=end_ts):
                        return False
                bindings.clear(); bindings.update(cur_bindings)
                final_assignment.clear(); final_assignment.update(assignment)
                return True
            p = rest[i]
            pid = str(p.get("id"))
            for e in edges:
                if any(int(e["order_seq"]) == int(v["order_seq"]) for v in assignment.values()):
                    continue
                next_bindings = dict(cur_bindings)
                if not self._bind_pattern(e, p, next_bindings):
                    continue
                assignment[pid] = e
                if self._constraints_ok(plan, assignment, next_bindings) and rec(i + 1, next_bindings):
                    return True
                assignment.pop(pid, None)
            return False

        ok = rec(0, dict(bindings))
        if not ok:
            return False, {"operator": "event_pattern", "reason": "no_assignment"}
        orders = sorted(int(e["order_seq"]) for e in final_assignment.values())
        return True, {"operator": "event_pattern", "assigned": sorted(final_assignment.keys()), "orders": orders}


    def _event_type_allowed(self, e: Dict[str, Any], p: Dict[str, Any]) -> bool:
        ets = p.get("event_type") or []
        if not ets:
            return True
        allowed = set()
        for et in ets:
            if et == "NFTTrade":
                allowed.add(self.trade_id)
            elif et == "NFTTransfer":
                allowed.add(self.transfer_id)
            else:
                try:
                    allowed.add(int(et))
                except Exception:
                    pass
        return int(e["event_type_id"]) in allowed

    def _bind_pattern(self, e: Dict[str, Any], p: Dict[str, Any], bindings: Dict[str, int]) -> bool:
        if not self._event_type_allowed(e, p):
            return False
        fields = {"src": "src_addr_id", "dst": "dst_addr_id", "nft": "nft_id", "collection": "collection_id"}
        for logical, col in fields.items():
            tok = p.get(logical)
            if tok in (None, ""):
                continue
            val = int(e[col])
            if isinstance(tok, str) and tok.startswith("$"):
                old = bindings.get(tok)
                if old is not None and old != val:
                    return False
                bindings[tok] = val
            else:
                try:
                    if int(tok) != val:
                        return False
                except Exception:
                    # Match Rust bind_or_check: unparsable non-variable tokens are wildcards.
                    continue
        return True

    @staticmethod
    def _edge_field(edge: Dict[str, Any], field: str) -> int:
        if field in {"src", "move_from", "seller", "from_address"}:
            return int(edge["src_addr_id"])
        if field in {"dst", "move_to", "buyer", "to_address"}:
            return int(edge["dst_addr_id"])
        if field == "nft":
            return int(edge["nft_id"])
        if field in {"collection", "collection_id", "collection_contract"}:
            return int(edge["collection_id"])
        if field == "event_type_id":
            return int(edge["event_type_id"])
        if field in {"order_seq", "order"}:
            return int(edge["order_seq"])
        if field == "block_ts":
            return int(edge["block_ts"])
        return 0

    def _resolve_ref(self, ref: Any, assignment: Dict[str, Dict[str, Any]], bindings: Dict[str, int]) -> Optional[int]:
        if ref in (None, ""):
            return None
        if isinstance(ref, (int, float)):
            return int(ref)
        if not isinstance(ref, str):
            return None
        if ref.startswith("$"):
            return bindings.get(ref)
        if "." in ref:
            event_id, field = ref.split(".", 1)
            e = assignment.get(event_id)
            if e is None:
                return None
            return self._edge_field(e, field)
        try:
            return int(ref)
        except Exception:
            return None

    def _constraints_ok(
        self,
        plan: Dict[str, Any],
        assignment: Dict[str, Dict[str, Any]],
        bindings: Dict[str, int],
        *,
        require_resolved: bool = False,
    ) -> bool:
        for c in plan.get("constraints") or []:
            typ = str(c.get("type") or "")
            if typ == "same":
                l = self._resolve_ref(c.get("left"), assignment, bindings)
                r = self._resolve_ref(c.get("right"), assignment, bindings)
                if require_resolved and (l is None or r is None):
                    return False
                if l is not None and r is not None and l != r:
                    return False
            elif typ in {"not_equal", "neq"}:
                l = self._resolve_ref(c.get("left"), assignment, bindings)
                r = self._resolve_ref(c.get("right"), assignment, bindings)
                if require_resolved and (l is None or r is None):
                    return False
                if l is not None and r is not None and l == r:
                    return False
            elif typ == "after":
                later, earlier = c.get("later"), c.get("earlier")
                if later in assignment and earlier in assignment:
                    if int(assignment[later]["order_seq"]) <= int(assignment[earlier]["order_seq"]):
                        return False
            elif typ == "order":
                seq = [x for x in c.get("sequence", []) if x in assignment]
                for left, right in zip(seq, seq[1:]):
                    if int(assignment[right]["order_seq"]) <= int(assignment[left]["order_seq"]):
                        return False
            elif typ == "within":
                earlier, later = c.get("earlier"), c.get("later")
                if earlier in assignment and later in assignment:
                    e0 = int(assignment[earlier]["block_ts"])
                    e1 = int(assignment[later]["block_ts"])
                    delta = norm_int(c.get("delta_seconds"), 86400)
                    if e1 < e0 or e1 > e0 + delta:
                        return False
            elif typ == "pairwise_distinct":
                refs = list(c.get("values") or [])
                vals = [self._resolve_ref(v, assignment, bindings) for v in refs]
                if require_resolved and any(v is None for v in vals):
                    return False
                if all(v is not None for v in vals) and len(set(vals)) != len(vals):
                    return False
            elif typ == "min_distinct":
                refs = list(c.get("values") or [])
                vals = [self._resolve_ref(v, assignment, bindings) for v in refs]
                if require_resolved and any(v is None for v in vals):
                    return False
                if all(v is not None for v in vals):
                    if len(set(vals)) < norm_int(c.get("min_count"), 1):
                        return False
            elif typ == "distinct_event_ids":
                evs = [int(assignment[x]["order_seq"]) for x in c.get("events", []) if x in assignment]
                if len(evs) == len(c.get("events", []) or []) and len(evs) != len(set(evs)):
                    return False
            else:
                return False
        return True

    def _anti_fails(
        self,
        anti: Dict[str, Any],
        anchor: Dict[str, Any],
        edges: Sequence[Dict[str, Any]],
        assignment: Dict[str, Dict[str, Any]],
        bindings: Dict[str, int],
        end_ts: int | None = None,
    ) -> bool:
        if anti.get("type") != "no_event":
            return True
        after_id = str(anti.get("after") or "")
        after_edge = assignment.get(after_id) if after_id else anchor
        after_order = int(after_edge["order_seq"]) if after_edge is not None else 0
        after_ts = int(after_edge["block_ts"]) if after_edge is not None else 0
        window_end = int(end_ts if end_ts is not None and end_ts >= 0 else after_ts + norm_int(anti.get("within_seconds"), 86400))
        if norm_int(anti.get("within_seconds"), 0) > 0 and after_ts > 0:
            window_end = min(window_end, after_ts + norm_int(anti.get("within_seconds"), 86400))
        pat = {
            "event_type": anti.get("event_type") or [],
            "src": "",
            "dst": "",
            "nft": "",
            "collection": "",
        }
        checks = []
        for logical, col in (("src", "src_addr_id"), ("dst", "dst_addr_id"), ("nft", "nft_id"), ("collection", "collection_id")):
            tok = anti.get(logical)
            if tok in (None, ""):
                continue
            checks.append((tok, col))
        for e in edges:
            if int(e["order_seq"]) <= after_order or int(e["block_ts"]) > window_end:
                continue
            if not self._event_type_allowed(e, pat):
                continue
            matched = True
            for tok, col in checks:
                expected = self._resolve_ref(tok, assignment, bindings)
                if expected is None:
                    matched = False
                    break
                if int(e[col]) != expected:
                    matched = False
                    break
            if matched:
                return True
        return False
