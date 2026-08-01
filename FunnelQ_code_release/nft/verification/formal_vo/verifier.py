from __future__ import annotations

import json
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Mapping

from .candidate_store import CandidateStore
from .chain import read_epoch_root, resolve_epoch_id
from .coverage import VARIANTS, verify_coverage
from .exact_match import enumerate_instances
from .memory import peak_rss_mb
from .package import sha256_file
from .protocol import SIGNATURE_FIELDS, epoch_root, hash_obj, manifest_root
from .residual import verify_residual_payload
from .result_store import ResultStore
from .transcript import compile_transcript_file


PACKAGE_VERSION = "formal_vo_package_v3"
_REQUIRED_PATHS = {
    "epoch_manifest.json",
    "transcript.json",
    "coverage/coverage.json",
    "coverage/negative_base.jsonl.gz",
    "coverage/negative_refined.jsonl.gz",
    "coverage/replay_authenticated.jsonl.gz",
    "coverage/closure_domain_authenticated.jsonl.gz",
    "residual/manifest.json",
    "residual/ranges.jsonl.gz",
}


def _verify_exact_python(
    query: Mapping[str, Any],
    transcript: Mapping[str, Any],
    residual_catalog: Any,
    instances_csv: Path,
    work_path: Path,
) -> tuple[int, int, dict[str, Any], float, float]:
    """Verifier-owned exact replay and disk-backed result-set comparison."""
    exact_started = time.perf_counter()
    store = ResultStore(work_path / "result_compare.sqlite")
    claimed_count = 0
    generated_count = 0
    comparison: dict[str, Any]
    try:
        for descriptor, events in residual_catalog.iter_windows():
            anchor_order = int(descriptor["anchor_order_seq"])
            rows = enumerate_instances(
                query,
                int(descriptor["closure_id"]),
                anchor_order,
                events,
            )
            generated_count += store.add_generated(rows)
        store.conn.commit()
        exact_ms = (time.perf_counter() - exact_started) * 1000.0

        compare_started = time.perf_counter()
        claimed_count = store.load_claimed(
            instances_csv,
            query_id=str(transcript["queryId"]),
            closure_map=residual_catalog.closure_by_anchor,
        )
        comparison = store.compare()
        compare_ms = (time.perf_counter() - compare_started) * 1000.0
    finally:
        store.close()
        (work_path / "result_compare.sqlite").unlink(missing_ok=True)
    return claimed_count, generated_count, comparison, exact_ms, compare_ms


def verify_package(
    package_dir: Path,
    query_spec: Path,
    instances_csv: Path,
    expected_epoch_root: str | None = None,
    *,
    work_dir: Path | None = None,
    chain_config: Mapping[str, Any] | None = None,
    engine: str = "auto",
) -> dict[str, Any]:
    """Verify a v3 package.

    ``engine=auto`` uses the bundled Rust verifier core when it is available
    and otherwise falls back to the independent Python verifier.  The Python
    path remains a complete reference implementation and is also used by the
    regression suite.
    """
    if engine not in {"auto", "rust", "python"}:
        raise ValueError("engine must be one of: auto, rust, python")

    wall_started = time.perf_counter()
    package_dir = Path(package_dir)
    stage: dict[str, float] = {
        "package_integrity_ms": 0.0,
        "manifest_verification_ms": 0.0,
        "chain_read_ms": 0.0,
        "tauq_ms": 0.0,
        "vocov_ms": 0.0,
        "voneg_base_ms": 0.0,
        "voneg_refined_ms": 0.0,
        "vorep_ms": 0.0,
        "exact_match_ms": 0.0,
        "result_compare_ms": 0.0,
    }

    inventory_started = time.perf_counter()
    package_index = json.loads(
        (package_dir / "package_index.json").read_text(encoding="utf-8")
    )
    expected_index_fields = {
        "packageVersion",
        "variant",
        "queryId",
        "epochRoot",
        "transcriptRoot",
        "requiredFiles",
        "instancesCsvIncluded",
        "candidateHintIncluded",
    }
    if set(package_index) != expected_index_fields:
        raise ValueError("package index fields do not match the strict v3 schema")
    if package_index["packageVersion"] != PACKAGE_VERSION:
        raise ValueError("unsupported formal VO package version")
    variant = str(package_index["variant"])
    if variant not in VARIANTS:
        raise ValueError("unsupported verification ablation variant")
    if package_index["instancesCsvIncluded"] is not False or package_index[
        "candidateHintIncluded"
    ] is not False:
        raise ValueError("untrusted results or candidate hints must not be packaged as proofs")
    required_files = package_index["requiredFiles"]
    if not isinstance(required_files, list):
        raise ValueError("package requiredFiles must be a list")
    listed_paths = [str(item["path"]) for item in required_files]
    if set(listed_paths) != _REQUIRED_PATHS or len(listed_paths) != len(_REQUIRED_PATHS):
        raise ValueError("package inventory does not contain the strict required-file set")
    for item in required_files:
        if set(item) != {"path", "bytes", "sha256"}:
            raise ValueError("package inventory entry has unknown fields")
        path = package_dir / str(item["path"])
        if not path.is_file():
            raise ValueError(f"package file is missing: {item['path']}")
        if path.stat().st_size != int(item["bytes"]) or sha256_file(path) != item["sha256"]:
            raise ValueError(f"package inventory mismatch: {item['path']}")
    stage["package_integrity_ms"] = (time.perf_counter() - inventory_started) * 1000.0

    manifest_started = time.perf_counter()
    manifest = json.loads((package_dir / "epoch_manifest.json").read_text(encoding="utf-8"))
    if manifest_root(manifest) != manifest["manifestRoot"]:
        raise ValueError("ManifestRoot mismatch")
    if epoch_root(manifest["manifestRoot"]) != manifest["epochRoot"]:
        raise ValueError("EpochRoot derivation mismatch")
    if package_index["epochRoot"] != manifest["epochRoot"]:
        raise ValueError("package index is bound to a different EpochRoot")
    stage["manifest_verification_ms"] = (time.perf_counter() - manifest_started) * 1000.0

    chain_started = time.perf_counter()
    normalize_root = lambda value: str(value).lower().removeprefix("0x")
    chain_evidence: dict[str, Any]
    if chain_config is not None:
        required_chain_fields = {"rpc_url", "contract_address"}
        missing_chain_fields = required_chain_fields - set(chain_config)
        if missing_chain_fields:
            raise ValueError(
                f"chain configuration is missing fields: {sorted(missing_chain_fields)}"
            )
        epoch_id = resolve_epoch_id(chain_config.get("epoch_id"), manifest)
        expected_chain_id_raw = chain_config.get("expected_chain_id")
        expected_chain_id = (
            None if expected_chain_id_raw in (None, "") else int(expected_chain_id_raw)
        )
        chain_evidence = read_epoch_root(
            rpc_url=str(chain_config["rpc_url"]),
            contract_address=str(chain_config["contract_address"]),
            epoch_id=epoch_id,
            read_signature=str(chain_config.get("read_signature", "getEpochRoot(bytes32)")),
            block_tag=chain_config.get("block_tag", "latest"),
            expected_chain_id=expected_chain_id,
            timeout_seconds=float(chain_config.get("timeout_seconds", 15.0)),
        )
        independent_root = chain_evidence["epochRoot"]
        chain_read_mode = chain_evidence["mode"]
    else:
        if expected_epoch_root is None or not str(expected_epoch_root).strip():
            raise ValueError(
                "verification requires either an independently supplied EpochRoot "
                "or an Ethereum JSON-RPC chain configuration"
            )
        independent_root = str(expected_epoch_root)
        chain_read_mode = "external_expected_epoch_root_binding_not_rpc"
        chain_evidence = {
            "mode": chain_read_mode,
            "epochRoot": normalize_root(independent_root),
        }
    if normalize_root(independent_root) != normalize_root(manifest["epochRoot"]):
        raise ValueError("independent EpochRoot does not match the VO")
    stage["chain_read_ms"] = (time.perf_counter() - chain_started) * 1000.0

    tau_started = time.perf_counter()
    query, transcript = compile_transcript_file(query_spec)
    supplied_transcript = json.loads((package_dir / "transcript.json").read_text(encoding="utf-8"))
    if supplied_transcript != transcript:
        raise ValueError("server transcript differs from client compilation")
    if package_index["queryId"] != transcript["queryId"]:
        raise ValueError("package index query id differs from the client transcript")
    if package_index["transcriptRoot"] != transcript["transcriptRoot"]:
        raise ValueError("package index transcript root differs from the client transcript")
    if int(transcript["refined"]["maxCompletionDeltaSeconds"]) > int(
        manifest["systemParameters"]["closureHorizonSeconds"]
    ):
        raise ValueError(
            "query window exceeds the authenticated closure horizon; Refined/VOrep would be incomplete"
        )
    stage["tauq_ms"] = (time.perf_counter() - tau_started) * 1000.0

    direct_shared_range_replay = False
    temporary_replay_bytes = None
    unique_opened_event_count = None
    residual_range_count = None
    authenticated_candidate_db_bytes = None

    with ExitStack() as stack:
        verifier_engine = "python-reference"
        if work_dir is None:
            work_path = Path(
                stack.enter_context(tempfile.TemporaryDirectory(prefix="formal_vo_verify_"))
            )
        else:
            work_path = Path(work_dir)
            work_path.mkdir(parents=True, exist_ok=True)

        # SegmentDirectoryRoot is a deterministic consequence of the committed
        # SummaryRoot and public aggregate vocabulary.  This inexpensive check
        # stays in the Python orchestration layer for both verifier engines.
        coverage_header = json.loads(
            (package_dir / "coverage" / "coverage.json").read_text(encoding="utf-8")
        )
        segment_started = time.perf_counter()
        expected_segment_root = hash_obj(
            "segment-directory-v1",
            {
                "summaryRoot": manifest["roots"]["summaryRoot"],
                "leafCount": int(coverage_header["anchorCount"]),
                "treeHeight": int(coverage_header["treeHeight"]),
                "aggregation": {
                    "max": ["transition_count", "addr_count", "nft_count"],
                    "min": [
                        "transition_count",
                        "addr_count",
                        "nft_count",
                        *SIGNATURE_FIELDS,
                    ],
                },
            },
        )
        if expected_segment_root != manifest["roots"]["segmentDirectoryRoot"]:
            raise ValueError(
                "SegmentDirectoryRoot is inconsistent with the committed SummaryRoot"
            )
        stage["vocov_ms"] += (time.perf_counter() - segment_started) * 1000.0

        rust_result = None
        if engine in {"auto", "rust"}:
            from .rust_core import verify_package_with_rust

            rust_result = verify_package_with_rust(
                package_dir=package_dir,
                query_spec=Path(query_spec),
                instances_csv=Path(instances_csv),
                work_dir=work_path,
                require=(engine == "rust"),
            )

        if rust_result is not None:
            if str(rust_result["variant"]) != variant:
                raise ValueError("Rust verifier variant differs from package index")
            claimed_count = int(rust_result["resultCount"])
            generated_count = int(rust_result["generatedResultCount"])
            comparison = dict(rust_result["comparison"])
            for key in (
                "vocov_ms",
                "voneg_base_ms",
                "voneg_refined_ms",
                "vorep_ms",
                "exact_match_ms",
                "result_compare_ms",
            ):
                stage[key] += float(rust_result.get(key, 0.0))
            verifier_engine = str(rust_result["verifierEngine"])
            cand0_count = int(rust_result["cand0Count"])
            cand1_count = int(rust_result["cand1Count"])
            replay_count = int(rust_result["replayCount"])
            direct_shared_range_replay = bool(
                rust_result.get("directSharedRangeReplay", False)
            )
            temporary_replay_bytes = int(rust_result.get("temporaryReplayBytes", -1))
            unique_opened_event_count = int(
                rust_result.get("uniqueOpenedEventCount", 0)
            )
            residual_range_count = int(rust_result.get("residualRangeCount", 0))
            authenticated_candidate_db_bytes = int(
                rust_result.get("authenticatedCandidateDbBytes", 0)
            )
        else:
            candidate_path = work_path / "authenticated_candidates.sqlite"
            stack.callback(candidate_path.unlink, missing_ok=True)
            candidate_store = stack.enter_context(CandidateStore(candidate_path))
            coverage_manifest, _unused_candidates = verify_coverage(
                package_dir / "coverage",
                transcript,
                manifest["roots"]["summaryRoot"],
                manifest["roots"]["anchorDirectoryRoot"],
                manifest["roots"]["closureDomainRoot"],
                int(manifest["counts"]["closureCount"]),
                stage,
                collect_candidates=False,
                candidate_sink=candidate_store.add,
            )
            spill_commit_started = time.perf_counter()
            candidate_store.commit()
            stage["vocov_ms"] += (
                time.perf_counter() - spill_commit_started
            ) * 1000.0
            if len(candidate_store) != int(coverage_manifest["verifiedReplayCount"]):
                raise ValueError(
                    "disk-backed replay set count differs from authenticated coverage"
                )
            if str(coverage_manifest["variant"]) != variant:
                raise ValueError("coverage variant differs from package index")

            vorep_started = time.perf_counter()
            residual_catalog = verify_residual_payload(
                package_dir / "residual",
                candidate_store,
                transcript,
                manifest["roots"]["eventStreamRoot"],
                manifest["roots"]["txBindingRoot"],
                int(manifest["counts"]["collectionCount"]),
                int(manifest["counts"]["eventCount"]),
                expected_variant=variant,
                materialize_windows=False,
            )
            residual_manifest = residual_catalog.manifest
            if int(residual_manifest["replayCount"]) != int(
                coverage_manifest["verifiedReplayCount"]
            ):
                raise ValueError("coverage and residual replay counts differ")
            stage["vorep_ms"] = (time.perf_counter() - vorep_started) * 1000.0

            (
                claimed_count,
                generated_count,
                comparison,
                stage["exact_match_ms"],
                stage["result_compare_ms"],
            ) = _verify_exact_python(
                query,
                transcript,
                residual_catalog,
                Path(instances_csv),
                work_path,
            )
            cand0_count = (
                int(manifest["counts"]["closureCount"])
                if variant == "no_pruning"
                else int(manifest["counts"]["anchorCount"])
            )
            base_negative = int(
                coverage_manifest["verifiedBaseNegativeAnchorCount"]
            )
            cand1_count = (
                cand0_count if variant == "no_pruning" else cand0_count - base_negative
            )
            replay_count = int(coverage_manifest["verifiedReplayCount"])

        if not comparison["equal"]:
            raise ValueError(
                f"claimed result set differs from authenticated replay: {comparison}"
            )

    wall_ms = (time.perf_counter() - wall_started) * 1000.0
    attributed_total = sum(stage.values())
    raw_unattributed = wall_ms - attributed_total
    if raw_unattributed < -1.0:
        raise RuntimeError(
            f"verification stage timers overlap wall time by {-raw_unattributed:.3f} ms"
        )
    unattributed = max(0.0, raw_unattributed)
    verification_total = attributed_total + unattributed
    timing_error = verification_total - wall_ms
    return {
        "status": "VERIFIED",
        "variant": variant,
        "verifierEngine": verifier_engine,
        "queryId": transcript["queryId"],
        "epochRoot": manifest["epochRoot"],
        "cand0Count": cand0_count,
        "cand1Count": cand1_count,
        "replayCount": replay_count,
        "cand2Count": replay_count if variant in {"full", "no_anchor"} else None,
        "resultCount": claimed_count,
        "generatedResultCount": generated_count,
        **stage,
        "attributed_total_ms": attributed_total,
        "unattributed_ms": unattributed,
        "verification_total_ms": verification_total,
        "wall_ms": wall_ms,
        "timing_error_ms": timing_error,
        "timingSumExact": abs(timing_error) < 1e-6,
        "timingCoversWall": abs(timing_error) < 1e-6,
        "chainReadMode": chain_read_mode,
        "chainReadEvidence": chain_evidence,
        "verificationDatabaseUsed": False,
        "verifier_peak_rss_mb": peak_rss_mb(),
        "instancesReadOnlyInResultCompare": True,
        "directSharedRangeReplay": direct_shared_range_replay,
        "temporaryReplayBytes": temporary_replay_bytes,
        "uniqueOpenedEventCount": unique_opened_event_count,
        "residualRangeCount": residual_range_count,
        "authenticatedCandidateDbBytes": authenticated_candidate_db_bytes,
        "comparison": comparison,
    }
