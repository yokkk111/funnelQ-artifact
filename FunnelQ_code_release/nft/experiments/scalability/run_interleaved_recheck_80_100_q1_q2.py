#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
FORMAL_TOOL = Path(__file__).resolve().parents[2] / "verification"
OUT = ROOT / "diagnostics" / "interleaved_80_100_q1_q2_10x"
TRIAL_ROOT = ROOT / "ri"
QUERIES = {
    "Q1": "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2": "Q2_CROSS_NFT_RECIPROCAL",
}
CHAIN = {
    80: {
        "epoch_id": "82c1824205d4487302534b3618281740b16f15bb34b3f8bd509616bf140cca58",
        "epoch_root": "904556f489267ebc2d00392d6803711cd447a5ab61d146a5ff0740f8940f9d1c",
        "block_tag": "0x24",
    },
    100: {
        "epoch_id": "5da777a1f095722dc1cdf9a9eb1cbcb12291eaf8b2db647948ea4536a88a4477",
        "epoch_root": "32c0a63baaad53834d5bf944a77ce389ab76a223e87720d57990a292574f17a5",
        "block_tag": "0x25",
    },
}
BASE_ORDER = [(80, "Q1"), (100, "Q1"), (80, "Q2"), (100, "Q2")]


def trial_order(repeat: int) -> list[tuple[int, str]]:
    shift = (repeat - 1) % len(BASE_ORDER)
    order = BASE_ORDER[shift:] + BASE_ORDER[:shift]
    if ((repeat - 1) // len(BASE_ORDER)) % 2:
        order = list(reversed(order))
    return order


def valid_report(path: Path, scale: int) -> bool:
    if not path.exists():
        return False
    report = json.loads(path.read_text(encoding="utf-8"))
    return all(report.get(k) is True for k in (
        "completed", "chain_bound", "chain_context_consistent", "all_results_equal_to_claims"
    )) and report.get("chain_read_mode") == "ethereum_json_rpc_eth_call" \
        and report.get("fixed_chain_block_tag") == CHAIN[scale]["block_tag"]


def run_trial(repeat: int, position: int, scale: int, short_query: str) -> None:
    query = QUERIES[short_query]
    trial = TRIAL_ROOT / f"r{repeat:02d}p{position}_{scale}_{short_query}"
    report_path = trial / "verification_ablation_report.json"
    if valid_report(report_path, scale):
        print(f"[skip] repeat={repeat} position={position} scale={scale} query={short_query}", flush=True)
        return
    trial.mkdir(parents=True, exist_ok=True)
    store = ROOT / "stores" / f"scale_{scale:03d}" / "funnelq"
    cmd = [
        sys.executable, str(FORMAL_TOOL / "run_verification_ablation.py"),
        "--db", str(store / "nft_hvceq_episode_sidecar.db"),
        "--tx-db", str(store / "nft_tx_sidecar.db"),
        "--claims-root", str(ROOT / "claims" / f"scale_{scale:03d}"),
        "--out-root", str(trial),
        "--runs", "1",
        "--query-ids", query,
        "--variants", "full",
        "--engine", "rust",
        "--verify-timeout-sec", "7200",
        "--build-timeout-sec", "7200",
        "--max-rss-mb", "18000",
        "--reuse-packages-from", str(ROOT / "vo_packages" / f"scale_{scale:03d}"),
        "--rpc-url", "http://127.0.0.1:8545",
        "--registry-address", "0xe6e340d132b5f46d1e472debcd681b2abc16e57e",
        "--epoch-id", CHAIN[scale]["epoch_id"],
        "--chain-read-signature", "getEpochRoot(bytes32)",
        "--chain-block-tag", CHAIN[scale]["block_tag"],
        "--expected-chain-id", "31337",
    ]
    print(f"[run] repeat={repeat} position={position} scale={scale} query={short_query}", flush=True)
    with (trial / "driver.stdout.log").open("w", encoding="utf-8") as stdout, \
            (trial / "driver.stderr.log").open("w", encoding="utf-8") as stderr:
        result = subprocess.run(cmd, cwd=FORMAL_TOOL, stdout=stdout, stderr=stderr)
    if result.returncode != 0 or not valid_report(report_path, scale):
        raise RuntimeError(f"trial failed: repeat={repeat} position={position} scale={scale} query={short_query}")


def aggregate() -> None:
    rows: list[dict[str, object]] = []
    for repeat in range(1, 11):
        for position, (scale, short_query) in enumerate(trial_order(repeat), 1):
            trial = TRIAL_ROOT / f"r{repeat:02d}p{position}_{scale}_{short_query}"
            source = trial / "verification_ablation_all_runs.csv"
            with source.open("r", encoding="utf-8-sig", newline="") as f:
                record = next(csv.DictReader(f))
            record = dict(record)
            record.update({"interleaved_repeat": repeat, "interleaved_position": position, "scale_percent": scale})
            rows.append(record)
    all_csv = OUT / "interleaved_all_40_runs.csv"
    with all_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    numeric = [
        "verification_total_ms", "verification_without_chain_ms", "chain_read_ms",
        "vocov_ms", "voneg_base_ms", "voneg_refined_ms", "vorep_ms",
        "exact_match_ms", "result_compare_ms", "vo_total_bytes", "resultCount",
    ]
    summary: list[dict[str, object]] = []
    for scale in (80, 100):
        for short_query, query in QUERIES.items():
            group = [r for r in rows if int(r["scale_percent"]) == scale and r["query_id"] == query]
            out: dict[str, object] = {"scale_percent": scale, "query": short_query, "runs": len(group)}
            for key in numeric:
                values = [float(r[key]) for r in group]
                mean = sum(values) / len(values)
                variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
                out[f"mean_{key}"] = mean
                out[f"std_{key}"] = variance ** 0.5
                out[f"min_{key}"] = min(values)
                out[f"max_{key}"] = max(values)
            summary.append(out)
    summary_csv = OUT / "interleaved_summary.csv"
    with summary_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)

    comparison: list[dict[str, object]] = []
    for item in summary:
        scale = int(item["scale_percent"])
        query = QUERIES[str(item["query"])]
        original_csv = ROOT / "runs" / f"scale_{scale:03d}" / "verification" / "verification_ablation_all_runs.csv"
        with original_csv.open("r", encoding="utf-8-sig", newline="") as f:
            original_rows = [
                row for row in csv.DictReader(f)
                if row.get("variant") == "full" and row.get("query_id") == query
            ]
        original_values = [float(row["verification_total_ms"]) for row in original_rows]
        original_mean = sum(original_values) / len(original_values)
        rerun_mean = float(item["mean_verification_total_ms"])
        rerun_values = sorted(
            float(row["verification_total_ms"]) / 1000.0
            for row in rows
            if int(row["scale_percent"]) == scale and row["query_id"] == query
        )
        middle = len(rerun_values) // 2
        rerun_median = (rerun_values[middle - 1] + rerun_values[middle]) / 2.0
        comparison.append({
            "scale_percent": scale,
            "query": item["query"],
            "original_runs": len(original_values),
            "original_mean_verification_s": original_mean / 1000.0,
            "interleaved_runs": item["runs"],
            "interleaved_mean_verification_s": rerun_mean / 1000.0,
            "interleaved_median_verification_s": rerun_median,
            "change_vs_original_percent": (rerun_mean / original_mean - 1.0) * 100.0,
        })
    comparison_csv = OUT / "original_vs_interleaved_comparison.csv"
    with comparison_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(comparison[0]))
        writer.writeheader()
        writer.writerows(comparison)
    reports = []
    for repeat in range(1, 11):
        for position, (scale, short_query) in enumerate(trial_order(repeat), 1):
            trial = TRIAL_ROOT / f"r{repeat:02d}p{position}_{scale}_{short_query}"
            reports.append(json.loads((trial / "verification_ablation_report.json").read_text(encoding="utf-8")))
    (OUT / "completion.json").write_text(json.dumps({
        "completed": True,
        "completed_at": datetime.now().astimezone().isoformat(),
        "trials": len(rows),
        "chain_bound": all(str(r.get("chain_bound", "")).lower() == "true" for r in rows),
        "chain_context_consistent": all(r.get("chain_context_consistent") is True for r in reports),
        "all_verified": all(r.get("verification_status") == "VERIFIED" for r in rows),
        "all_results_equal_to_claims": all(r.get("all_results_equal_to_claims") is True for r in reports),
    }, indent=2), encoding="utf-8")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    for repeat in range(1, 11):
        for position, (scale, short_query) in enumerate(trial_order(repeat), 1):
            run_trial(repeat, position, scale, short_query)
    aggregate()
    print(f"[complete] {OUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
