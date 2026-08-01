#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, List

from fair_cost_runner_lib import (
    ROOT,
    SAME_TX_QUERIES,
    WORK_ROOT,
    latest_stores_dir,
    memory_from_monitor,
    next_numbered_dir,
    read_json,
    run_monitored,
    write_csv,
    write_json,
)


VARIANTS = ["full", "no_refined", "no_synopsis"]
VARIANT_LABELS = {
    "full": "Full",
    "no_refined": "No Refined Support",
    "no_synopsis": "No Synopsis/Support",
}


def as_float(v: Any) -> float:
    try:
        if v == "" or v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def as_int(v: Any) -> int:
    try:
        if v == "" or v is None:
            return 0
        return int(float(v))
    except Exception:
        return 0


def parse_query_result(outdir: Path) -> Dict[str, Any]:
    p = outdir / "query_result.json"
    if not p.exists():
        return {}
    rep = read_json(p)
    metrics = rep.get("metrics", {})
    stages = rep.get("stage_times_s", {})
    return {
        "query_name_reported": rep.get("query_name", ""),
        "ablation_variant_reported": rep.get("ablation_variant", ""),
        "num_matches": rep.get("num_matches", ""),
        "num_result_units": rep.get("num_result_units", ""),
        "reported_query_time_s": metrics.get("query_latency_s", ""),
        "aux_total_or_wall_time_s": metrics.get("query_wall_time_s", ""),
        "num_cand0": rep.get("num_cand0", ""),
        "num_cand1": rep.get("num_cand1", ""),
        "num_cand2": rep.get("num_cand2", ""),
        "base_synopsis_enabled": rep.get("base_synopsis_enabled", ""),
        "refined_synopsis_enabled": rep.get("refined_synopsis_enabled", ""),
        "stage_candidate_generation_s": stages.get("candidate_generation", ""),
        "stage_base_filter_s": stages.get("base_filter", ""),
        "stage_refined_filter_s": stages.get("refined_filter", ""),
        "stage_exact_match_s": stages.get("exact_match", ""),
        "exact_backend": rep.get("exact_backend", ""),
        "exact_policy": rep.get("exact_policy", ""),
        "num_candidate_event_bindings_examined": rep.get("num_candidate_event_bindings_examined", ""),
        "num_refined_support_bindings_examined": rep.get("num_refined_support_bindings_examined", ""),
        "candidate_frontier_commitments": json.dumps(rep.get("candidate_frontier_commitments", {}), ensure_ascii=False),
        "plan_order": json.dumps(rep.get("plan_order", []), ensure_ascii=False),
    }


def stats(vals: List[float], prefix: str) -> Dict[str, float]:
    if not vals:
        return {
            f"{prefix}_mean": 0.0,
            f"{prefix}_median": 0.0,
            f"{prefix}_std": 0.0,
        }
    return {
        f"{prefix}_mean": statistics.mean(vals),
        f"{prefix}_median": statistics.median(vals),
        f"{prefix}_std": statistics.stdev(vals) if len(vals) >= 2 else 0.0,
    }


def aggregate(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for q in SAME_TX_QUERIES:
        for variant in VARIANTS:
            group = [
                r for r in rows
                if r.get("query_name") == q and r.get("ablation_variant") == variant and r.get("status") == "ok"
            ]
            all_runs = [r for r in rows if r.get("query_name") == q and r.get("ablation_variant") == variant]
            row = {
                "mode": "same_tx",
                "query_name": q,
                "ablation_variant": variant,
                "ablation_label": VARIANT_LABELS[variant],
                "runs": len(all_runs),
                "ok_runs": len(group),
            }
            row.update(stats([as_float(r.get("reported_query_time_s")) for r in group], "reported_query_time_s"))
            row.update(stats([as_float(r.get("stage_exact_match_s")) for r in group], "stage_exact_match_s"))
            row.update(stats([as_float(r.get("peak_rss_mb")) for r in group], "peak_rss_mb"))
            for col in ["num_matches", "num_cand0", "num_cand1", "num_cand2"]:
                vals = [as_int(r.get(col)) for r in group]
                row[f"{col}_mean"] = statistics.mean(vals) if vals else 0.0
            out.append(row)
    return out


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run layered same_tx HVCEQ ablation without changing the fair-cost runner.")
    ap.add_argument("--stores-dir", default="")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--timeout-sec", type=int, default=None)
    ap.add_argument("--variants", default=",".join(VARIANTS), help="Comma-separated subset of full,no_refined,no_synopsis.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    stores_dir = Path(args.stores_dir).resolve() if args.stores_dir else latest_stores_dir()
    db = stores_dir / "hvceq_same_tx/same_tx_runtime.db"
    variants = [v.strip() for v in str(args.variants).split(",") if v.strip()]
    bad = [v for v in variants if v not in VARIANTS]
    if bad:
        raise ValueError(f"Unsupported variants: {bad}")

    run_dir = next_numbered_dir(WORK_ROOT, "hvceq_same_tx_layered_ablation_v3")
    rows: List[Dict[str, Any]] = []
    for run_id in range(1, max(1, int(args.runs)) + 1):
        for qname in SAME_TX_QUERIES:
            for variant in variants:
                outdir = run_dir / f"run_{run_id:02d}" / variant / qname
                cmd = [
                    sys.executable,
                    str(ROOT / "systems/hvceq/same_tx/query_same_tx_layered_v3.py"),
                    "--db",
                    str(db),
                    "--query-json",
                    str(ROOT / "queries/same_tx" / f"{qname}.json"),
                    "--outdir",
                    str(outdir),
                    "--sample-limit",
                    "0",
                    "--ablation",
                    variant,
                ]
                print(f"[hvceq-sametx-layered-v3][run {run_id}/{args.runs}][{variant}][{qname}] start", flush=True)
                meta = run_monitored(cmd, outdir, timeout_sec=args.timeout_sec)
                row = {
                    "cost_scope": "hvceq_same_tx_layered_ablation_query_cost",
                    "mode": "same_tx",
                    "system": "HVCEQ-SameTx-Layered-v3",
                    "system_key": "hvceq",
                    "query_name": qname,
                    "ablation_variant": variant,
                    "ablation_label": VARIANT_LABELS[variant],
                    "run_id": run_id,
                    "stores_dir": str(stores_dir),
                    "status": meta.get("status", ""),
                    "return_code": meta.get("return_code", ""),
                    "wall_time_s": meta.get("wall_time_s", ""),
                    "stdout_path": meta.get("stdout_path", ""),
                    "stderr_path": meta.get("stderr_path", ""),
                    "monitor_report_path": meta.get("monitor_report_path", ""),
                    "command_path": meta.get("command_path", ""),
                    "output_dir": str(outdir),
                    "report_path": str(outdir / "query_result.json"),
                    **parse_query_result(outdir),
                    **memory_from_monitor(meta.get("monitor_report_path", "")),
                }
                rows.append(row)
                print(
                    f"[hvceq-sametx-layered-v3][run {run_id}/{args.runs}][{variant}][{qname}] "
                    f"done status={row['status']} matches={row.get('num_matches','')} "
                    f"time={row.get('reported_query_time_s','')} rss={row.get('peak_rss_mb','')}",
                    flush=True,
                )

    write_csv(run_dir / "hvceq_same_tx_layered_ablation_v3_all_runs.csv", rows)
    write_csv(run_dir / "hvceq_same_tx_layered_ablation_v3_stats_by_query_variant.csv", aggregate(rows))
    write_json(run_dir / "hvceq_same_tx_layered_ablation_v3_report.json", {
        "stores_dir": str(stores_dir),
        "run_dir": str(run_dir),
        "db": str(db),
        "variants": variants,
        "note": "Layered same_tx HVCEQ runner. It exposes cand0/cand1/cand2 and stage times for verification-aware ablation.",
    })
    print(f"[hvceq-sametx-layered-v3] done out={run_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
