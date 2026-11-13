#!/usr/bin/env python3


from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

from sharded_store_common import atomic_write_json, run_monitored, sha256_file

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
STORE_FORMAT = "MBFT-NFT-Sharded-Persistent-Store-v1"
SELECTED5 = [
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
]


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_json(value: Any) -> str:
    return "0x" + hashlib.sha256(compact_json(value).encode("utf-8")).hexdigest()


def parse_json_stdout(stdout: str) -> dict[str, Any]:
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start < 0 or end <= start:
        raise RuntimeError(f"could not parse JSON output:\n{stdout[-4000:]}")
    value = json.loads(stdout[start : end + 1])
    if not isinstance(value, dict):
        raise ValueError("command output is not a JSON object")
    return value


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def compare_sets(qid: str, server_csv: Path, client_csv: Path, out: Path) -> tuple[dict[str, Any], float]:
    out.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    proc = run_monitored(
        [
            sys.executable,
            str(ROOT / "tools" / "compare_nft_full_instances.py"),
            "--query",
            qid,
            "--out",
            str(out),
            f"server={server_csv}",
            f"client={client_csv}",
        ],
        cwd=ROOT,
    )[0]
    elapsed = time.perf_counter() - started
    report = json.loads(out.read_text(encoding="utf-8"))
    if int(proc["return_code"]) != 0 or not bool(report.get("all_pass")):
        raise RuntimeError(f"exact full-binding comparison failed for {qid}: {compact_json(report)}")
    return report, elapsed


def run_matcher(
    *,
    candidate_paths: list[Path],
    query_dir: Path,
    qids: list[str],
    outdir: Path,
    warmup_runs: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    cmd = [
        sys.executable,
        str(HERE / "nft_semantic_matcher.py"),
        "--events-jsonl",
        *(str(path) for path in candidate_paths),
        "--query-dir",
        str(query_dir),
        "--queries",
        ",".join(qids),
        "--outdir",
        str(outdir),
        "--warmup-runs",
        str(max(0, warmup_runs)),
    ]
    process, stdout, stderr = run_monitored(cmd, cwd=HERE)
    (outdir / "runner.stdout.txt").write_text(stdout, encoding="utf-8")
    (outdir / "runner.stderr.txt").write_text(stderr, encoding="utf-8")
    atomic_write_json(outdir / "runner.process.json", process)
    report = json.loads((outdir / "matcher_report.json").read_text(encoding="utf-8"))
    return process, report


def make_commitment_manifest(
    *,
    build_manifest_path: Path,
    build: dict[str, Any],
    server_shards: list[dict[str, Any]],
) -> dict[str, Any]:
    body = {
        "format": "MBFT-NFT-Sharded-Root-Manifest-v1",
        "securityBoundary": (
            "The on-chain SHA-256 manifest root binds every shard's native MBFT root schedule "
            "and parameters. Inner MBFT roots remain the source implementation's 32-bit MurmurHash3."
        ),
        "atomicPredicate": {
            "lowerBound": 0,
            "upperBound": 4294967295,
            "keywordsCNF": [["type=nfttrade"]],
        },
        "sourceBuildManifest": {
            "path": str(build_manifest_path),
            "format": build["format"],
            "eventCount": int(build["event_count"]),
            "shardCount": int(build["shard_count"]),
            "shardRowsTarget": int(build["shard_rows_target"]),
            "source": build.get("source", {}),
            "parameters": build.get("parameters", {}),
        },
        "shards": [
            {
                "shardId": int(item["shard_id"]),
                "rows": int(item["rows"]),
                "orderOffset": int(item["order_offset"]),
                "storeRelative": item["store_relative"],
                "storeSha256": item["store_sha256"],
                "commitmentsSha256": item["commitments_sha256"],
                "commitments": item["commitments"],
            }
            for item in server_shards
        ],
    }
    return {**body, "manifestRoot": sha256_json({"mbft_nft_root_manifest_v1": body})}


def validate_commitment_manifest(
    manifest: dict[str, Any],
    server_shards: list[dict[str, Any]],
) -> None:
    body = {key: value for key, value in manifest.items() if key != "manifestRoot"}
    expected_root = sha256_json({"mbft_nft_root_manifest_v1": body})
    if str(manifest.get("manifestRoot", "")).lower() != expected_root:
        raise RuntimeError("MBFT commitment manifest root mismatch")
    by_id = {int(item["shard_id"]): item for item in server_shards}
    for committed in manifest["shards"]:
        shard = by_id[int(committed["shardId"])]
        actual = sha256_file(Path(shard["commitments_path"]))
        if actual != str(committed["commitmentsSha256"]):
            raise RuntimeError(f"shard {committed['shardId']} commitment file is not chain-bound")


def commit_onchain(
    *,
    manifest: dict[str, Any],
    outdir: Path,
    eth_project_root: Path,
    epoch_id: int,
    start_local_node_flag: bool,
    read_reps: int,
    hardhat_network: str,
) -> dict[str, Any]:
    verification_root = ROOT / "NFT_verification_fixed_complete" / "NFT_verification_fixed"
    if str(verification_root) not in sys.path:
        sys.path.insert(0, str(verification_root))
    from run_nft_chain_commitment_bench import (
        TS_HELPER,
        run_helper,
        start_local_node,
        verify_chain,
    )

    first_block = min(int(item.get("first_block_number") or 0) for item in manifest["sourceBuildManifest"].get("sourceShards", []) or [{"first_block_number": 0}])
    last_block = max(int(item.get("last_block_number") or 0) for item in manifest["sourceBuildManifest"].get("sourceShards", []) or [{"last_block_number": 0}])
    epoch = {
        "format": "MBFT-NFT-Onchain-Epoch-v1",
        "epochId": int(epoch_id),
        "startBlock": first_block,
        "endBlock": last_block,
        "segmentSize": int(manifest["sourceBuildManifest"]["shardRowsTarget"]),
        "epochRoot": manifest["manifestRoot"],
    }
    epoch_path = outdir / "mbft_epoch_commitment.json"
    atomic_write_json(epoch_path, epoch)
    helper = eth_project_root / "scripts" / "_mbft_nft_chain_tmp.ts"
    node = None
    try:
        if start_local_node_flag:
            node = start_local_node(eth_project_root)
            time.sleep(6.0)
        helper.write_text(TS_HELPER, encoding="utf-8")
        chain = run_helper(
            eth_project_root,
            helper,
            epoch_path,
            "new",
            "",
            max(1, read_reps),
            hardhat_network,
        )
    finally:
        helper.unlink(missing_ok=True)
        if node is not None and node.poll() is None:
            node.terminate()
            try:
                node.wait(timeout=5)
            except Exception:
                node.kill()
    checks = verify_chain(epoch, chain.get("chainEpoch") or {})
    report = {
        "format": "MBFT-NFT-Onchain-Readback-v1",
        "localEpoch": epoch,
        "chainEpoch": chain.get("chainEpoch"),
        "registryAddress": chain.get("registryAddress"),
        "network": chain.get("network"),
        "commitGas": chain.get("commitGas"),
        "commitTimeMs": chain.get("commitTimeMs"),
        "readTimeMsMean": chain.get("readTimeMsMean"),
        "readTimeMsValues": chain.get("readTimeMsValues"),
        "checks": checks,
    }
    atomic_write_json(outdir / "mbft_onchain_readback.json", report)
    if not checks["allPass"]:
        raise RuntimeError("on-chain MBFT manifest-root read-back mismatch")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Sharded MBFT Hint-VO verification for NFT selected5.")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--exe", required=True, type=Path)
    parser.add_argument("--outdir", required=True, type=Path)
    parser.add_argument("--queries", default=",".join(SELECTED5))
    parser.add_argument("--query-dir", default=str(ROOT / "queries" / "nft" / "selected5"), type=Path)
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--resume-server", action="store_true")
    parser.add_argument("--commit-onchain", action="store_true")
    parser.add_argument("--eth-project-root", default=r"chain", type=Path)
    parser.add_argument("--start-local-node", action="store_true")
    parser.add_argument("--hardhat-network", default="ganache")
    parser.add_argument("--chain-read-reps", type=int, default=20)
    parser.add_argument("--epoch-id", type=int, default=830001)
    args = parser.parse_args()

    build_manifest_path = args.manifest.resolve()
    exe = args.exe.resolve()
    outdir = args.outdir.resolve()
    query_dir = args.query_dir.resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    build = json.loads(build_manifest_path.read_text(encoding="utf-8"))
    if build.get("format") != STORE_FORMAT or build.get("status") != "success":
        raise ValueError("unsupported or incomplete MBFT shard build manifest")
    if not exe.is_file() or not os.access(exe, os.X_OK):
        raise FileNotFoundError(exe)
    qids = [item.strip().upper() for item in args.queries.split(",") if item.strip()]
    unknown = [qid for qid in qids if qid not in SELECTED5]
    if unknown:
        raise ValueError(f"this formal runner accepts selected5 only: {unknown}")

    server_root = outdir / "server"
    server_root.mkdir(parents=True, exist_ok=True)
    stores_dir = build_manifest_path.parent
    server_shards: list[dict[str, Any]] = []
    proof_generation_s = 0.0
    proof_write_s = 0.0
    server_candidate_paths: list[Path] = []
    proof_bytes = 0
    server_peak_mb = 0.0

    for shard in build["shards"]:
        shard_id = int(shard["shard_id"])
        shard_dir = server_root / "shards" / f"shard_{shard_id:05d}"
        summary_path = shard_dir / "atomic_selection_summary.json"
        if not (args.resume_server and summary_path.is_file()):
            if shard_dir.exists():
                shutil.rmtree(shard_dir)
            shard_dir.mkdir(parents=True, exist_ok=True)
            process, stdout, stderr = run_monitored(
                [
                    str(exe),
                    "prove-store",
                    "--store",
                    str(stores_dir / shard["store_relative"]),
                    "--outdir",
                    str(shard_dir),
                    "--types",
                    "nfttrade",
                    "--warmup-runs",
                    str(max(0, args.warmup_runs)),
                ],
                cwd=exe.parent,
            )
            (shard_dir / "prove.stdout.txt").write_text(stdout, encoding="utf-8")
            (shard_dir / "prove.stderr.txt").write_text(stderr, encoding="utf-8")
            atomic_write_json(shard_dir / "prove_process.json", process)
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        process = json.loads((shard_dir / "prove_process.json").read_text(encoding="utf-8"))
        proof = Path(summary["proofs"]["nfttrade"]["proof"]).resolve()
        commitments_path = shard_dir / "commitments.json"
        candidate = Path(summary["query_only"]["nfttrade"]["path"]).resolve()
        commitments = json.loads(commitments_path.read_text(encoding="utf-8"))
        if int(summary["build"]["persistent"]["transactions"]) != int(shard["rows"]):
            raise RuntimeError(f"shard {shard_id} store row count mismatch")
        proof_generation_s += float(summary["proofs"]["nfttrade"]["vo_generation_time_us"]) / 1e6
        proof_write_s += float(summary["proofs"]["nfttrade"]["proof_write_time_us"]) / 1e6
        proof_bytes += proof.stat().st_size
        server_peak_mb = max(server_peak_mb, float(process["peak_tree_rss_mb"]))
        server_candidate_paths.append(candidate)
        server_shards.append(
            {
                **shard,
                "server_dir": str(shard_dir),
                "proof_path": str(proof),
                "proof_bytes": proof.stat().st_size,
                "commitments_path": str(commitments_path),
                "commitments_sha256": sha256_file(commitments_path),
                "commitments": commitments,
                "candidate_path": str(candidate),
            }
        )
        atomic_write_json(
            outdir / "progress.json",
            {"status": "server_proofs", "completedShards": len(server_shards), "shardCount": len(build["shards"])},
        )

    manifest = make_commitment_manifest(
        build_manifest_path=build_manifest_path,
        build={**build, "sourceShards": build["shards"]},
        server_shards=server_shards,
    )

    manifest["sourceBuildManifest"]["sourceShards"] = [
        {
            "shard_id": int(item["shard_id"]),
            "first_block_number": item.get("first_block_number"),
            "last_block_number": item.get("last_block_number"),
        }
        for item in build["shards"]
    ]
    body = {key: value for key, value in manifest.items() if key != "manifestRoot"}
    manifest["manifestRoot"] = sha256_json({"mbft_nft_root_manifest_v1": body})
    atomic_write_json(outdir / "mbft_root_manifest.json", manifest)

    if not args.commit_onchain:
        raise RuntimeError("formal MBFT verification requires --commit-onchain")
    onchain = commit_onchain(
        manifest=manifest,
        outdir=outdir,
        eth_project_root=args.eth_project_root.resolve(),
        epoch_id=args.epoch_id,
        start_local_node_flag=args.start_local_node,
        read_reps=args.chain_read_reps,
        hardhat_network=args.hardhat_network,
    )
    validate_commitment_manifest(manifest, server_shards)
    chain_read_s = float(onchain.get("readTimeMsMean") or 0.0) / 1000.0

    server_claims_dir = server_root / "claims"
    server_match_report_path = server_claims_dir / "matcher_report.json"
    if args.resume_server and server_match_report_path.is_file():
        server_match = json.loads(server_match_report_path.read_text(encoding="utf-8"))
        process_path = server_claims_dir / "runner.process.json"
        server_match_process = (
            json.loads(process_path.read_text(encoding="utf-8"))
            if process_path.is_file()
            else {"peak_tree_rss_mb": 0.0, "reused_existing_server_claims": True}
        )
    else:
        server_match_process, server_match = run_matcher(
            candidate_paths=server_candidate_paths,
            query_dir=query_dir,
            qids=qids,
            outdir=server_claims_dir,
            warmup_runs=max(0, args.warmup_runs),
        )
    server_by_query = {item["query"]: item for item in server_match["queries"]}

    rows: list[dict[str, Any]] = []
    for qid in qids:
        validate_started = time.perf_counter()
        validate_commitment_manifest(manifest, server_shards)
        manifest_check_s = time.perf_counter() - validate_started
        client_root = outdir / "client" / qid
        verified_paths: list[Path] = []
        proof_load_s = 0.0
        commitment_load_s = 0.0
        mbft_core_s = 0.0
        candidate_write_s = 0.0
        verify_process_wall_s = 0.0
        verify_peak_mb = 0.0
        verified_events = 0
        for shard in server_shards:
            shard_id = int(shard["shard_id"])
            shard_client = client_root / "verified_shards" / f"shard_{shard_id:05d}"
            shard_client.mkdir(parents=True, exist_ok=True)
            candidate_path = shard_client / "nfttrade.jsonl"
            metrics_path = shard_client / "verify_metrics.json"
            process, stdout, stderr = run_monitored(
                [
                    str(exe),
                    "verify",
                    "--proof",
                    shard["proof_path"],
                    "--commitments",
                    shard["commitments_path"],
                    "--out",
                    str(candidate_path),
                    "--metrics",
                    str(metrics_path),
                    "--event-type",
                    "nfttrade",
                ],
                cwd=exe.parent,
            )
            (shard_client / "verify.stdout.txt").write_text(stdout, encoding="utf-8")
            (shard_client / "verify.stderr.txt").write_text(stderr, encoding="utf-8")
            phase = parse_json_stdout(stdout)
            if not bool(phase.get("valid")):
                raise RuntimeError(f"shard {shard_id} MBFT verification failed")
            proof_load_s += float(phase["proof_load_time_us"]) / 1e6
            commitment_load_s += float(phase["commitment_load_time_us"]) / 1e6
            mbft_core_s += float(phase["verify_time_us"]) / 1e6
            candidate_write_s += float(phase["candidate_write_time_us"]) / 1e6
            verify_process_wall_s += float(process["wall_s"])
            verify_peak_mb = max(verify_peak_mb, float(process["peak_tree_rss_mb"]))
            verified_events += int(phase["events"])
            verified_paths.append(candidate_path)

        client_match_process, client_match = run_matcher(
            candidate_paths=verified_paths,
            query_dir=query_dir,
            qids=[qid],
            outdir=client_root / "recompute",
            warmup_runs=max(0, args.warmup_runs),
        )
        client_result = client_match["queries"][0]
        server_result = server_by_query[qid]
        comparison, compare_s = compare_sets(
            qid,
            Path(server_result["instances_csv"]),
            Path(client_result["instances_csv"]),
            client_root / "exact_comparison.json",
        )
        replay_algorithm_s = (
            float(client_result["match_s"])
            + float(client_result.get("result_canonicalize_s", 0.0))
            + compare_s
        )
        verification_algorithm_s = (
            chain_read_s + manifest_check_s + mbft_core_s + replay_algorithm_s
        )
        verification_formal_s = (
            chain_read_s
            + manifest_check_s
            + proof_load_s
            + commitment_load_s
            + mbft_core_s
            + candidate_write_s
            + float(client_match.get("candidate_load_s", 0.0))
            + float(client_match.get("prepare_s", 0.0))
            + replay_algorithm_s
        )
        verification_e2e_s = (
            chain_read_s
            + manifest_check_s
            + verify_process_wall_s
            + float(client_match_process["wall_s"])
            + compare_s
        )
        row = {
            "system": "MBFT-Adapt",
            "query": qid,
            "result_semantics": "full_event_binding_v1",
            "result_count": int(client_result["result_count"]),
            "all_pass": bool(comparison["all_pass"]),
            "chain_commitment_ok": bool(onchain["checks"]["allPass"]),
            "authenticated_atomic_events": verified_events,
            "vo_size_bytes": proof_bytes,
            "vo_size_mib": proof_bytes / (1024 * 1024),
            "chain_read_s": chain_read_s,
            "manifest_check_s": manifest_check_s,
            "proof_load_s": proof_load_s,
            "commitment_load_s": commitment_load_s,
            "mbft_core_verify_s": mbft_core_s,
            "authenticated_candidate_write_s": candidate_write_s,
            "client_candidate_load_s": float(client_match.get("candidate_load_s", 0.0)),
            "client_prepare_s": float(client_match.get("prepare_s", 0.0)),
            "client_query_replay_s": float(client_result["match_s"]),
            "client_result_canonicalize_s": float(client_result.get("result_canonicalize_s", 0.0)),
            "exact_result_compare_s": compare_s,
            "verification_algorithm_s": verification_algorithm_s,
            "verification_formal_s": verification_formal_s,
            "verification_e2e_s": verification_e2e_s,
            "client_peak_rss_mb": max(verify_peak_mb, float(client_match_process["peak_tree_rss_mb"])),
        }
        rows.append(row)
        atomic_write_json(
            outdir / "progress.json",
            {"status": "client_verification", "completedQueries": len(rows), "queryCount": len(qids), "latest": row},
        )

    write_csv(outdir / "mbft_nft_verification_summary.csv", rows)
    report = {
        "format": "MBFT-NFT-Sharded-Full-Binding-Verification-v1",
        "resultSemantics": "full_event_binding_v1",
        "allPass": bool(rows and all(bool(row["all_pass"]) and bool(row["chain_commitment_ok"]) for row in rows)),
        "proofReusePolicy": (
            "Standalone per-query accounting: every query is charged the full sum of all shard VO bytes "
            "and independently reruns all shard proof verification."
        ),
        "timingContract": {
            "verification_algorithm_s": "chain read + manifest binding check + native MBFT core verification + client query replay/canonicalization/exact comparison",
            "verification_formal_s": "algorithm time plus proof/commitment parsing and required authenticated-candidate transport/preparation",
            "verification_e2e_s": "complete measured client subprocess wall time plus chain read and exact comparison",
            "server_vo_generation_excluded_from_client_verification": True,
            "offline_store_build_excluded": True,
            "warmupRuns": max(0, args.warmup_runs),
        },
        "server": {
            "proofGenerationS": proof_generation_s,
            "proofWriteS": proof_write_s,
            "proofBytes": proof_bytes,
            "peakShardRssMb": server_peak_mb,
            "semanticMatcherPeakRssMb": server_match_process["peak_tree_rss_mb"],
        },
        "onchain": onchain,
        "rootManifest": str(outdir / "mbft_root_manifest.json"),
        "rows": rows,
        "implementationBoundary": (
            "Native MBFT proves completeness and soundness of the atomic NFTTrade relation. "
            "The independent MBFT adapter replays each multi-record query and compares exact canonical instances. "
            "No FunnelQ verifier is imported."
        ),
        "artifactLimitation": (
            "The original MBFT source uses MurmurHash3_x86_32 internally. The SHA-256 on-chain wrapper "
            "authenticates the root manifest but does not upgrade the collision resistance of inner MBFT nodes."
        ),
    }
    atomic_write_json(outdir / "mbft_nft_verification_report.json", report)
    atomic_write_json(outdir / "progress.json", {"status": "success", "allPass": report["allPass"]})
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0 if report["allPass"] else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr, flush=True)
        raise
