#!/usr/bin/env python3
"""Run the final FunnelQ ablations on the selected five 3-month NFT queries.

This driver invokes the compact three-month FunnelQ implementation.  Each
query/variant/run is
isolated in one monitored process.  Runtime loading and warm-up are excluded
from query latency, while query-phase Job commit is sampled through a marker.
All successful variants are compared by canonical full-instance digest and by
an exact streaming CSV comparison.
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path
from statistics import mean, pstdev
from typing import Any


QUERIES = [
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
]
VARIANTS = [
    "full",
    "no_anchor",
    "no_refined",
    "no_pruning",
]
DEFAULT_ENGINE_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Selected-five, three-month NFT FunnelQ ablation."
    )
    ap.add_argument("--engine-root", type=Path, default=DEFAULT_ENGINE_ROOT)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--tx-sidecar-db", type=Path, required=True)
    ap.add_argument("--query-dir", type=Path, default=Path("queries/nft/selected5"))
    ap.add_argument("--outdir", type=Path, required=True)
    ap.add_argument("--queries", default=",".join(QUERIES))
    ap.add_argument("--variants", default=",".join(VARIANTS))
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--timeout-sec", type=float, default=1200.0)
    ap.add_argument("--job-memory-limit-mb", type=float, default=18000.0)
    ap.add_argument("--sample-interval-ms", type=int, default=50)
    ap.add_argument("--candidate-limit", type=int, default=0)
    ap.add_argument("--anchor-limit", type=int, default=0)
    return ap.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def resolve_under(root: Path, value: Path) -> Path:
    return value.resolve() if value.is_absolute() else (root / value).resolve()


def query_json(query_dir: Path, query: str) -> Path:
    path = query_dir / f"{query}.json"
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def execute_one(
    args: argparse.Namespace,
    engine_root: Path,
    query_dir: Path,
    query: str,
    variant: str,
    run_id: int,
    outdir: Path,
) -> dict[str, Any]:
    tag = f"run_{run_id:02d}_{query.lower()}_{variant}"
    run_out = outdir / "query_outputs" / tag
    monitor_out = outdir / "monitors" / tag
    marker = run_out / "query_phase.marker"
    report_path = run_out / "execution_report.json"
    run_out.mkdir(parents=True, exist_ok=True)
    monitor_out.mkdir(parents=True, exist_ok=True)

    exe = (
        engine_root
        / "systems"
        / "nft"
        / "hvceq"
        / "local_executor_rust"
        / "target"
        / "release"
        / "nft_hvceq_local_executor.exe"
    )
    inner = [
        sys.executable,
        str(engine_root / "systems" / "nft" / "hvceq" / "query_nft_hvceq_full.py"),
        "--db",
        str(args.db.resolve()),
        "--tx-sidecar-db",
        str(args.tx_sidecar_db.resolve()),
        "--query-json",
        str(query_json(query_dir, query)),
        "--outdir",
        str(run_out),
        "--ablation",
        variant,
        "--threads",
        str(max(1, args.threads)),
        "--warmup-runs",
        str(max(0, args.warmup_runs)),
        "--phase-marker",
        str(marker),
        "--candidate-limit",
        str(max(0, args.candidate_limit)),
        "--anchor-limit",
        str(max(0, args.anchor_limit)),
        "--exe",
        str(exe),
    ]
    monitored = [
        sys.executable,
        str(engine_root / "common" / "monitor_process_windows.py"),
        "--outdir",
        str(monitor_out),
        "--job-memory-limit-mb",
        str(args.job_memory_limit_mb),
        "--timeout-sec",
        str(args.timeout_sec),
        "--sample-interval-ms",
        str(args.sample_interval_ms),
        "--phase-marker",
        str(marker),
        "--",
        *inner,
    ]
    proc = subprocess.run(monitored, cwd=str(engine_root), text=True, capture_output=True)
    (run_out / "driver.stdout.txt").write_text(proc.stdout, encoding="utf-8")
    (run_out / "driver.stderr.txt").write_text(proc.stderr, encoding="utf-8")
    monitor_path = monitor_out / "monitor_report.json"
    monitor = read_json(monitor_path) if monitor_path.exists() else {}
    monitor_status = monitor.get("status")
    if not monitor_status and monitor_path.exists():
        monitor_status = "ok" if int(monitor.get("return_code", 1)) == 0 else "failed"
    stop_reason = str(monitor.get("stop_reason") or "")
    killed_for_timeout = (
        monitor.get("killed_for_timeout")
        if monitor.get("killed_for_timeout") is not None
        else stop_reason == "timeout"
    )
    killed_for_memory = (
        monitor.get("killed_for_memory")
        if monitor.get("killed_for_memory") is not None
        else stop_reason in {
            "target_failed_or_job_memory_limit",
            "system_available_below_floor",
        }
    )
    report = read_json(report_path) if report_path.exists() else {}
    timing = report.get("timing") or {}
    candidate = report.get("candidate_metrics") or {}
    phase = monitor.get("query_phase") or {}
    # The primary latency contract now separates query-dependent search from
    # common final-result materialization.  search_only_s is a direct Rust
    # timer spanning anchor localization, Base/Refined processing, and exact
    # local match enumeration for every variant, including no_anchor.
    required_timing = (
        "search_only_s",
        "output_materialization_s",
        "query_latency_s",
        "candidate_query_s",
        "exact_match_s",
        "candidate_csv_write_s",
        "candidate_csv_read_s",
        "closure_input_load_s",
        "result_projection_load_s",
        "result_materialize_s",
        "result_canonicalize_s",
    )
    missing_latency_components = [
        key for key in required_timing if key not in timing
    ]
    if report and not missing_latency_components:
        search_only_s = float(timing["search_only_s"])
        output_materialization_s = float(timing["output_materialization_s"])
        reported_latency = float(timing["query_latency_s"])
        direct_timer_ok = bool(timing.get("search_only_direct_timer", False))
        latency_contract_ok = bool(
            direct_timer_ok
            and abs(search_only_s + output_materialization_s - reported_latency) <= 1e-6
        )
        if not latency_contract_ok:
            raise RuntimeError(
                "FunnelQ ablation latency contract mismatch: "
                f"search_only={search_only_s:.9f}s "
                f"output_materialization={output_materialization_s:.9f}s "
                f"reported={reported_latency:.9f}s "
                f"direct_timer={direct_timer_ok}"
            )
        output_component_sum = sum(
            float(timing[key])
            for key in (
                "candidate_csv_write_s",
                "candidate_csv_read_s",
                "closure_input_load_s",
                "result_projection_load_s",
                "result_materialize_s",
                "result_canonicalize_s",
            )
        )
        if abs(output_component_sum - output_materialization_s) > 1e-6:
            raise RuntimeError(
                "FunnelQ output-materialization contract mismatch: "
                f"components={output_component_sum:.9f}s "
                f"reported={output_materialization_s:.9f}s"
            )
        search_only_timing_contract_ok: bool | str = True
    else:
        latency_contract_ok = ""
        search_only_timing_contract_ok = ""

    expected_exact_engine = (
        "refined_posting_fused"
        if variant in {"full", "no_anchor"}
        else "generic_closure_scan"
    )
    expected_uses_refined_posting = variant in {"full", "no_anchor"}
    actual_exact_engine = str(
        report.get("exact_engine")
        or timing.get("exact_engine")
        or candidate.get("exact_engine")
        or ""
    )
    actual_uses_refined_posting = bool(
        report.get(
            "uses_refined_posting",
            timing.get(
                "uses_refined_posting",
                candidate.get("uses_refined_posting", False),
            ),
        )
    )
    exact_engine_isolation_contract_ok = bool(
        report
        and actual_exact_engine == expected_exact_engine
        and actual_uses_refined_posting == expected_uses_refined_posting
        and bool(report.get("exact_engine_isolation_contract_ok", False))
    )

    if variant == "no_anchor":
        expected_anchor_engine = "generic_anchor_scan"
        expected_uses_anchor_directory = False
    elif variant == "no_pruning":
        expected_anchor_engine = "disabled"
        expected_uses_anchor_directory = False
    else:
        expected_anchor_engine = "anchor_directory"
        expected_uses_anchor_directory = True
    actual_anchor_engine = str(
        report.get("anchor_engine")
        or timing.get("anchor_engine")
        or candidate.get("anchor_engine")
        or ""
    )
    actual_uses_anchor_directory = bool(
        report.get(
            "uses_anchor_directory",
            timing.get(
                "uses_anchor_directory",
                candidate.get("uses_anchor_directory", False),
            ),
        )
    )
    anchor_engine_isolation_contract_ok = bool(
        report
        and actual_anchor_engine == expected_anchor_engine
        and actual_uses_anchor_directory == expected_uses_anchor_directory
        and bool(report.get("anchor_engine_isolation_contract_ok", False))
    )

    row: dict[str, Any] = {
        "run_id": run_id,
        "query": query,
        "variant": variant,
        "process_return_code": proc.returncode,
        "monitor_status": monitor_status or "missing",
        "killed_for_timeout": killed_for_timeout,
        "killed_for_memory": killed_for_memory,
        "query_latency_s": timing.get("query_latency_s", ""),
        "search_only_s": timing.get("search_only_s", ""),
        "output_materialization_s": timing.get("output_materialization_s", ""),
        "search_only_direct_timer": timing.get("search_only_direct_timer", ""),
        "search_component_sum_s": timing.get("search_component_sum_s", ""),
        "search_timer_overhead_s": timing.get("search_timer_overhead_s", ""),
        "search_only_timing_contract_ok": search_only_timing_contract_ok,
        "peak_job_commit_mb": phase.get("peak_job_commit_mb", ""),
        "peak_tree_working_set_mb": phase.get("peak_tree_working_set_mb", ""),
        "result_count": report.get("result_count", ""),
        "duplicate_count": report.get("duplicate_count", ""),
        "result_digest_sha256": report.get("result_digest_sha256", ""),
        "instances_csv": report.get("instances_csv", ""),
        "closure_universe": candidate.get("closure_universe", ""),
        "cand0_anchor_hits": candidate.get("cand0_anchor_hits", ""),
        "cand1_base_hits": candidate.get("cand1_base_hits", ""),
        "cand2_refined_hits": candidate.get("cand2_refined_hits", ""),
        "exact_engine": actual_exact_engine,
        "uses_refined_posting": actual_uses_refined_posting,
        "fused_refined_exact": candidate.get("fused_refined_exact", ""),
        "exact_candidate_closures": candidate.get("exact_candidate_closures", ""),
        "refined_posting_closures": candidate.get("refined_posting_closures", ""),
        "generic_scan_closures": candidate.get("generic_scan_closures", ""),
        "local_edges_examined": candidate.get("local_edges_examined", ""),
        "exact_engine_isolation_contract_ok": exact_engine_isolation_contract_ok,
        "anchor_engine": actual_anchor_engine,
        "uses_anchor_directory": actual_uses_anchor_directory,
        "anchor_scan_closures": candidate.get("anchor_scan_closures", ""),
        "anchor_directory_entries_read": candidate.get(
            "anchor_directory_entries_read", ""
        ),
        "closure_summary_rows_scanned": candidate.get(
            "closure_summary_rows_scanned", ""
        ),
        "anchor_engine_isolation_contract_ok": anchor_engine_isolation_contract_ok,
        "candidate_query_s": timing.get("candidate_query_s", ""),
        "candidate_csv_write_s": timing.get("candidate_csv_write_s", ""),
        "candidate_csv_read_s": timing.get("candidate_csv_read_s", ""),
        "closure_input_load_s": timing.get("closure_input_load_s", ""),
        "exact_match_s": timing.get("exact_match_s", ""),
        "exact_order_csv_write_s": timing.get("exact_order_csv_write_s", ""),
        "exact_order_csv_read_s": timing.get("exact_order_csv_read_s", ""),
        "result_projection_load_s": timing.get("result_projection_load_s", ""),
        "result_materialize_s": timing.get("result_materialize_s", ""),
        "closure_reloading_eliminated": timing.get("closure_reloading_eliminated", ""),
        "matched_projection_event_count": report.get("matched_projection_event_count", ""),
        "result_canonicalize_s": timing.get("result_canonicalize_s", ""),
        "runtime_load_excluded": timing.get("runtime_load_excluded_from_query_latency", ""),
        "query_timeout_exceeded": (
            float(timing.get("query_latency_s", 0.0)) > float(args.timeout_sec)
        ),
        "query_latency_contract_ok": latency_contract_ok,
        "execution_report": str(report_path) if report_path.exists() else "",
        "monitor_report": str(monitor_path) if monitor_path.exists() else "",
    }
    ok = (
        proc.returncode == 0
        and monitor_status == "ok"
        and report.get("result_semantics") == "full_event_binding_v1"
        and int(report.get("duplicate_count", -1)) == 0
        and Path(str(report.get("instances_csv") or "")).is_file()
        and exact_engine_isolation_contract_ok
        and anchor_engine_isolation_contract_ok
        and latency_contract_ok is True
        and search_only_timing_contract_ok is True
    )
    row["status"] = "success" if ok else "failed"
    if not ok:
        row["error_tail"] = (proc.stderr or proc.stdout)[-3000:]
    return row


def validate_query(
    engine_root: Path,
    query: str,
    rows: list[dict[str, Any]],
    outdir: Path,
    run_id: int,
) -> dict[str, Any]:
    successful = [row for row in rows if row["status"] == "success"]
    digests = sorted({str(row["result_digest_sha256"]) for row in successful})
    report_path = outdir / "result_set_checks" / f"run_{run_id:02d}_{query}.json"
    if len(successful) != len(rows) or len(digests) != 1:
        result = {
            "query": query,
            "run_id": run_id,
            "status": "failed",
            "successful_variants": [row["variant"] for row in successful],
            "digests": {
                row["variant"]: row.get("result_digest_sha256", "") for row in rows
            },
        }
        write_json(report_path, result)
        return result

    by_variant = {str(row["variant"]): row for row in successful}
    anchor_stage_contract: dict[str, Any] = {
        "checked": False,
        "passed": True,
    }
    if "full" in by_variant and "no_anchor" in by_variant:
        full = by_variant["full"]
        no_anchor = by_variant["no_anchor"]
        stage_keys = (
            "cand0_anchor_hits",
            "cand1_base_hits",
            "cand2_refined_hits",
        )
        equal_stage_counts = all(
            str(full.get(key, "")) == str(no_anchor.get(key, ""))
            for key in stage_keys
        )
        no_anchor_scans_universe = (
            str(no_anchor.get("anchor_scan_closures", ""))
            == str(no_anchor.get("closure_universe", ""))
        )
        engine_signature_ok = bool(
            full.get("anchor_engine") == "anchor_directory"
            and bool(full.get("uses_anchor_directory"))
            and no_anchor.get("anchor_engine") == "generic_anchor_scan"
            and not bool(no_anchor.get("uses_anchor_directory"))
        )
        anchor_stage_contract = {
            "checked": True,
            "passed": bool(
                equal_stage_counts
                and no_anchor_scans_universe
                and engine_signature_ok
            ),
            "equal_stage_counts": equal_stage_counts,
            "no_anchor_scans_universe": no_anchor_scans_universe,
            "engine_signature_ok": engine_signature_ok,
            "full": {key: full.get(key, "") for key in stage_keys},
            "no_anchor": {key: no_anchor.get(key, "") for key in stage_keys},
            "closure_universe": no_anchor.get("closure_universe", ""),
            "anchor_scan_closures": no_anchor.get("anchor_scan_closures", ""),
        }
        if not anchor_stage_contract["passed"]:
            result = {
                "query": query,
                "run_id": run_id,
                "status": "failed",
                "reason": "full/no_anchor anchor-stage contract mismatch",
                "anchor_stage_contract": anchor_stage_contract,
            }
            write_json(report_path, result)
            return result

    if len(successful) == 1:
        result = {
            "query": query,
            "run_id": run_id,
            "status": "passed",
            "note": "single successful variant; digest recorded but no cross-variant comparison",
            "digest": successful[0]["result_digest_sha256"],
        }
        write_json(report_path, result)
        return result
    cmd = [
        sys.executable,
        str(engine_root / "tools" / "compare_nft_full_instances.py"),
        "--query",
        query,
        "--out",
        str(report_path),
        *[
            f"{row['variant']}={row['instances_csv']}"
            for row in successful
        ],
    ]
    proc = subprocess.run(cmd, cwd=str(engine_root), text=True, capture_output=True)
    if proc.returncode != 0:
        return {
            "query": query,
            "run_id": run_id,
            "status": "failed",
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
    result = read_json(report_path)
    result["status"] = "passed"
    result["anchor_stage_contract"] = anchor_stage_contract
    write_json(report_path, result)
    return result


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for query in QUERIES:
        for variant in VARIANTS:
            group = [
                row for row in rows
                if row["query"] == query and row["variant"] == variant
            ]
            ok = [row for row in group if row["status"] == "success"]

            def average(key: str) -> float | str:
                values = [
                    float(row[key])
                    for row in ok
                    if row.get(key) not in ("", None)
                ]
                return mean(values) if values else ""

            def deviation(key: str) -> float | str:
                values = [
                    float(row[key])
                    for row in ok
                    if row.get(key) not in ("", None)
                ]
                return pstdev(values) if len(values) > 1 else (0.0 if values else "")

            output.append({
                "query": query,
                "variant": variant,
                "successful_runs": len(ok),
                "total_runs": len(group),
                "result_count": ok[0]["result_count"] if ok else "",
                "mean_query_latency_s": average("query_latency_s"),
                "std_query_latency_s": deviation("query_latency_s"),
                "mean_search_only_s": average("search_only_s"),
                "std_search_only_s": deviation("search_only_s"),
                "mean_output_materialization_s": average("output_materialization_s"),
                "std_output_materialization_s": deviation("output_materialization_s"),
                "mean_search_timer_overhead_s": average("search_timer_overhead_s"),
                "all_search_only_timing_contracts_ok": all(
                    bool(row.get("search_only_timing_contract_ok")) for row in ok
                ) if ok else False,
                "mean_peak_job_commit_mb": average("peak_job_commit_mb"),
                "std_peak_job_commit_mb": deviation("peak_job_commit_mb"),
                "closure_universe": ok[0]["closure_universe"] if ok else "",
                "cand0_anchor_hits": ok[0]["cand0_anchor_hits"] if ok else "",
                "cand1_base_hits": ok[0]["cand1_base_hits"] if ok else "",
                "cand2_refined_hits": ok[0]["cand2_refined_hits"] if ok else "",
                "exact_engine": ok[0]["exact_engine"] if ok else "",
                "uses_refined_posting": ok[0]["uses_refined_posting"] if ok else "",
                "exact_candidate_closures": ok[0]["exact_candidate_closures"] if ok else "",
                "refined_posting_closures": ok[0]["refined_posting_closures"] if ok else "",
                "generic_scan_closures": ok[0]["generic_scan_closures"] if ok else "",
                "mean_local_edges_examined": average("local_edges_examined"),
                "all_exact_engine_isolation_contracts_ok": all(
                    bool(row.get("exact_engine_isolation_contract_ok")) for row in ok
                ) if ok else False,
                "anchor_engine": ok[0]["anchor_engine"] if ok else "",
                "uses_anchor_directory": ok[0]["uses_anchor_directory"] if ok else "",
                "anchor_scan_closures": ok[0]["anchor_scan_closures"] if ok else "",
                "anchor_directory_entries_read": (
                    ok[0]["anchor_directory_entries_read"] if ok else ""
                ),
                "closure_summary_rows_scanned": (
                    ok[0]["closure_summary_rows_scanned"] if ok else ""
                ),
                "all_anchor_engine_isolation_contracts_ok": all(
                    bool(row.get("anchor_engine_isolation_contract_ok")) for row in ok
                ) if ok else False,
                "mean_candidate_query_s": average("candidate_query_s"),
                "mean_candidate_csv_write_s": average("candidate_csv_write_s"),
                "mean_candidate_csv_read_s": average("candidate_csv_read_s"),
                "mean_closure_input_load_s": average("closure_input_load_s"),
                "mean_exact_match_s": average("exact_match_s"),
                "mean_exact_order_csv_write_s": average("exact_order_csv_write_s"),
                "mean_exact_order_csv_read_s": average("exact_order_csv_read_s"),
                "mean_result_projection_load_s": average("result_projection_load_s"),
                "mean_result_materialize_s": average("result_materialize_s"),
                "mean_result_canonicalize_s": average("result_canonicalize_s"),
                "all_latency_contracts_ok": all(
                    bool(row.get("query_latency_contract_ok")) for row in ok
                ) if ok else False,
            })
    return output


def main() -> int:
    args = parse_args()
    engine_root = args.engine_root.resolve()
    query_dir = resolve_under(engine_root, args.query_dir)
    outdir = args.outdir.resolve()
    queries = [value.strip().upper() for value in args.queries.split(",") if value.strip()]
    variants = [value.strip().lower() for value in args.variants.split(",") if value.strip()]
    unknown_queries = sorted(set(queries) - set(QUERIES))
    unknown_variants = sorted(set(variants) - set(VARIANTS))
    if unknown_queries or unknown_variants:
        raise ValueError(
            f"unknown queries={unknown_queries}, variants={unknown_variants}"
        )
    for required in [
        engine_root / "systems" / "nft" / "hvceq" / "query_nft_hvceq_full.py",
        engine_root / "common" / "monitor_process_windows.py",
        args.db,
        args.tx_sidecar_db,
    ]:
        if not required.is_file():
            raise FileNotFoundError(required)
    for query in queries:
        query_json(query_dir, query)

    outdir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "format": "NFT-selected5-3month-HVCEQ-ablation-v3-four-variant-search-only",
        "engine_root": str(engine_root),
        "db": str(args.db.resolve()),
        "tx_sidecar_db": str(args.tx_sidecar_db.resolve()),
        "query_dir": str(query_dir),
        "queries": queries,
        "variants": variants,
        "runs": max(1, args.runs),
        "variant_order": "balanced cyclic rotation across runs and queries",
        "threads": max(1, args.threads),
        "warmup_runs": max(0, args.warmup_runs),
        "timing_contract": (
            "same-runtime warmup; runtime/store loading excluded. search_only_s is "
            "a direct Rust timer covering anchor directory lookup or generic all-summary "
            "anchor scan, Base/Refined processing, and exact local enumeration. "
            "output_materialization_s covers exact-order CSV transfer, matched-event "
            "projection, result materialization, and canonicalization. query_latency_s "
            "equals search_only_s + output_materialization_s. Final instances CSV write "
            "remains excluded."
        ),
        "memory_contract": (
            "query-phase Windows Job commit sampled across candidate retrieval, "
            "exact execution, exact-order transfer, and matched-event projection; "
            "runtime load, warmup, and final instances CSV write excluded"
        ),
    }
    write_json(outdir / "ablation_manifest.json", manifest)

    rows: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    for run_id in range(1, max(1, args.runs) + 1):
        for query_index, query in enumerate(queries):
            query_rows: list[dict[str, Any]] = []
            # Rotate the four variants across runs and queries so no variant is
            # permanently assigned the coldest or warmest process position.
            offset = ((run_id - 1) + query_index) % len(variants)
            run_variants = variants[offset:] + variants[:offset]
            for variant in run_variants:
                print(
                    f"[ablation] run={run_id} query={query} variant={variant}",
                    flush=True,
                )
                row = execute_one(
                    args, engine_root, query_dir, query, variant, run_id, outdir
                )
                rows.append(row)
                query_rows.append(row)
                write_csv(outdir / "ablation_all_runs.csv", rows)
                print(
                    f"[done] status={row['status']} time={row['query_latency_s']} "
                    f"peak_mb={row['peak_job_commit_mb']} results={row['result_count']} "
                    f"cand2={row['cand2_refined_hits']}",
                    flush=True,
                )
            check = validate_query(engine_root, query, query_rows, outdir, run_id)
            checks.append(check)
            print(f"[validate] query={query} status={check['status']}", flush=True)

    summary = summarize(rows)
    write_csv(outdir / "ablation_summary.csv", summary)
    effective_signatures: dict[str, dict[str, list[str]]] = {}
    for query in queries:
        effective_signatures[query] = {}
        for row in [r for r in rows if r["query"] == query and r["run_id"] == 1]:
            signature = "|".join(str(row.get(key, "")) for key in (
                "cand0_anchor_hits", "cand1_base_hits", "cand2_refined_hits",
            ))
            effective_signatures[query].setdefault(signature, []).append(row["variant"])
    failures = [row for row in rows if row["status"] != "success"]
    failed_checks = [check for check in checks if check["status"] != "passed"]
    final_report = {
        **manifest,
        "completed": not failures and not failed_checks,
        "successful_runs": len(rows) - len(failures),
        "total_runs": len(rows),
        "result_set_checks": checks,
        "effective_candidate_stage_signatures": effective_signatures,
        "exact_engine_isolation": {
            "full": {
                "exact_engine": "refined_posting_fused",
                "uses_refined_posting": True,
            },
            "no_anchor": {
                "exact_engine": "refined_posting_fused",
                "uses_refined_posting": True,
            },
            "no_refined": {
                "exact_engine": "generic_closure_scan",
                "uses_refined_posting": False,
            },
            "no_pruning": {
                "exact_engine": "generic_closure_scan",
                "uses_refined_posting": False,
            },
        },
        "anchor_engine_isolation": {
            "full": {
                "anchor_engine": "anchor_directory",
                "uses_anchor_directory": True,
            },
            "no_anchor": {
                "anchor_engine": "generic_anchor_scan",
                "uses_anchor_directory": False,
            },
            "no_refined": {
                "anchor_engine": "anchor_directory",
                "uses_anchor_directory": True,
            },
            "no_pruning": {
                "anchor_engine": "disabled",
                "uses_anchor_directory": False,
            },
        },
        "all_exact_engine_isolation_contracts_ok": all(
            bool(row.get("exact_engine_isolation_contract_ok")) for row in rows
        ),
        "all_anchor_engine_isolation_contracts_ok": all(
            bool(row.get("anchor_engine_isolation_contract_ok")) for row in rows
        ),
        "failures": failures,
        "summary_csv": str(outdir / "ablation_summary.csv"),
    }
    write_json(outdir / "ablation_report.json", final_report)
    print(outdir, flush=True)
    return 0 if final_report["completed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
