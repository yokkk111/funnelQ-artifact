#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

QUERY_IDS = [
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--artifact-dir", type=Path)
    parser.add_argument("--result-root", type=Path)
    parser.add_argument("--reports-dir", type=Path)
    parser.add_argument("--packages-dir", type=Path)
    parser.add_argument("--out-csv", required=True, type=Path)
    args = parser.parse_args()
    if not args.run_root and not args.artifact_dir and not (args.reports_dir and args.packages_dir):
        parser.error("use --run-root, --artifact-dir, or both --reports-dir and --packages-dir")
    if bool(args.reports_dir) != bool(args.packages_dir):
        parser.error("--reports-dir and --packages-dir must be supplied together")
    rows = []
    for query_id in QUERY_IDS:
        if args.artifact_dir:
            verify = json.loads((args.artifact_dir / f"{query_id}.verify.json").read_text())
            build = json.loads((args.artifact_dir / f"{query_id}.build.json").read_text())
        elif args.reports_dir and args.packages_dir:
            verify = json.loads((args.reports_dir / f"{query_id}.json").read_text())
            build = json.loads(
                (args.packages_dir / query_id / "build_metrics.json").read_text()
            )
        else:
            verify = json.loads(
                (args.run_root / "reports" / f"{query_id}.json").read_text()
            )
            build = json.loads(
                (args.run_root / "packages" / query_id / "build_metrics.json").read_text()
            )
        result_path = (
            args.result_root / query_id / "instances.csv"
            if args.result_root
            else Path("")
        )
        row = {
            "query_id": query_id,
            "variant": verify.get("variant", build.get("variant", "full")),
            "verifier_engine": verify.get("verifierEngine", ""),
            "cand0_count": verify["cand0Count"],
            "cand1_count": verify["cand1Count"],
            "cand2_count": verify.get("cand2Count", ""),
            "replay_count": verify.get("replayCount", verify.get("cand2Count", "")),
            "result_count": verify["resultCount"],
            "package_integrity_ms": verify.get("package_integrity_ms", 0.0),
            "manifest_verification_ms": verify.get("manifest_verification_ms", 0.0),
            "chain_read_ms": verify["chain_read_ms"],
            "tauq_ms": verify["tauq_ms"],
            "vocov_ms": verify["vocov_ms"],
            "voneg_base_ms": verify["voneg_base_ms"],
            "voneg_refined_ms": verify["voneg_refined_ms"],
            "voneg_ms": verify["voneg_base_ms"] + verify["voneg_refined_ms"],
            "vorep_ms": verify["vorep_ms"],
            "exact_match_ms": verify["exact_match_ms"],
            "result_compare_ms": verify["result_compare_ms"],
            "unattributed_ms": verify.get("unattributed_ms", 0.0),
            "verification_total_ms": verify["verification_total_ms"],
            "wall_ms": verify.get("wall_ms", verify["verification_total_ms"]),
            "timing_error_ms": verify.get("timing_error_ms", ""),
            "chain_read_mode": verify.get("chainReadMode", ""),
            "chain_id": verify.get("chainReadEvidence", {}).get("chainId", ""),
            "chain_block_number": verify.get("chainReadEvidence", {}).get("blockNumber", ""),
            "chain_eth_call_ms": verify.get("chainReadEvidence", {}).get("ethCallMs", ""),
            "chain_contract_address": verify.get("chainReadEvidence", {}).get("contractAddress", ""),
            "chain_epoch_id": verify.get("chainReadEvidence", {}).get("epochId", ""),
            "verifier_peak_rss_mb": verify.get("verifier_peak_rss_mb", ""),
            "tauq_bytes": build["tauqBytes"],
            "vocov_bytes": build["vocovBytes"],
            "voneg_base_bytes": build["vonegBaseBytes"],
            "voneg_refined_bytes": build["vonegRefinedBytes"],
            "voneg_bytes": build["vonegBaseBytes"] + build["vonegRefinedBytes"],
            "vorep_bytes": build["vorepBytes"],
            "shared_epoch_bytes": build.get("sharedEpochBytes", ""),
            "query_specific_vo_bytes": build.get("querySpecificVoBytes", ""),
            "vo_total_bytes": build["voTotalBytes"],
            "unique_opened_event_count": build.get("uniqueOpenedEventCount", ""),
            "residual_range_count": build.get("residualRangeCount", ""),
            "result_bytes": result_path.stat().st_size if result_path.exists() else "",
            "status": verify["status"],
        }
        rows.append(row)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(args.out_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
