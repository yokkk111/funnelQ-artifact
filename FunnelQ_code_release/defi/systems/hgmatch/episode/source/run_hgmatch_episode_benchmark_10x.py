#!/usr/bin/env python3


from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Any


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_monitor_report(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def mean_or_zero(vals: List[float]) -> float:
    return statistics.mean(vals) if vals else 0.0


def median_or_zero(vals: List[float]) -> float:
    return statistics.median(vals) if vals else 0.0


def stdev_or_zero(vals: List[float]) -> float:
    return statistics.stdev(vals) if len(vals) >= 2 else 0.0


def query_name_from_json(path: Path) -> str:
    obj = read_json(path)
    qn = str(obj.get("query_name", "")).strip()
    return qn if qn else path.stem


def build_monitored_cmd(
    exe_path: Path,
    db: Path,
    query_json: Path,
    outdir: Path,
    threads: int,
) -> List[str]:
    return [
        str(exe_path),
        "--db",
        str(db),
        "--query-json",
        str(query_json),
        "--outdir",
        str(outdir),
        "--threads",
        str(threads),
    ]


def run_once(
    monitor_script: Path,
    monitored_cmd: List[str],
    monitor_outdir: Path,
    cwd: Path,
    sample_interval_ms: int,
) -> int:
    cmd = [
        sys.executable,
        str(monitor_script),
        "--outdir",
        str(monitor_outdir),
        "--sample-interval-ms",
        str(sample_interval_ms),
        "--",
        *monitored_cmd,
    ]
    proc = subprocess.run(cmd, cwd=str(cwd))
    return int(proc.returncode)


def collect_summary(query_outdir: Path) -> Dict[str, Any]:
    report_path = query_outdir / "execution_report.json"
    if not report_path.exists():
        report_path = query_outdir / "query_summary.json"
    if not report_path.exists():
        return {}
    return read_json(report_path)


def flatten_run_row(
    query_name: str,
    query_json: Path,
    repeat_idx: int,
    query_outdir: Path,
    monitor_outdir: Path,
    summary: Dict[str, Any],
    monitor_report: Dict[str, Any],
) -> Dict[str, Any]:
    stage_times = summary.get("stage_times_s", {}) if isinstance(summary, dict) else {}
    metrics = summary.get("metrics", {}) if isinstance(summary, dict) else {}
    structural = summary.get("structural_metrics", {}) if isinstance(summary, dict) else {}
    memory = monitor_report.get("memory", {}) if isinstance(monitor_report, dict) else {}
    cpu = monitor_report.get("cpu", {}) if isinstance(monitor_report, dict) else {}
    io = monitor_report.get("io", {}) if isinstance(monitor_report, dict) else {}

    return {
        "query_name": query_name,
        "query_json": str(query_json),
        "repeat_idx": repeat_idx,
        "query_outdir": str(query_outdir),
        "monitor_outdir": str(monitor_outdir),
        "return_code": monitor_report.get("return_code", ""),
        "scope": summary.get("scope", ""),
        "matching_order": json.dumps(summary.get("matching_order", []), ensure_ascii=False),
        "num_seed_edges": structural.get("num_seed_edges", ""),
        "num_seed_closures": structural.get("num_seed_closures", ""),
        "num_closures_scanned": structural.get("num_closures_scanned", ""),
        "num_closures_with_nonempty_local_candidates": structural.get("num_closures_with_nonempty_local_candidates", ""),
        "num_local_candidate_edges_examined": structural.get("num_local_candidate_edges_examined", ""),
        "num_structural_embeddings_raw": structural.get("num_structural_embeddings_raw", ""),
        "num_structural_embeddings_dedup": structural.get("num_structural_embeddings_dedup", ""),
        "num_candidate_closures": structural.get("num_candidate_closures", ""),
        "num_cand1_closures": summary.get("num_cand1_closures", ""),
        "num_cand2_closures": summary.get("num_cand2_closures", ""),
        "num_matches": summary.get("num_matches", ""),
        "num_residual_closures": summary.get("num_residual_closures", ""),
        "query_latency_s": metrics.get("query_latency_s", ""),
        "query_wall_time_s_reported": metrics.get("query_wall_time_s", ""),
        "compile_query_plan_s": stage_times.get("compile_query_plan", ""),
        "global_seed_selection_s": stage_times.get("global_seed_selection", ""),
        "hgmatch_structural_match_s": stage_times.get("hgmatch_structural_match", ""),
        "exact_semantic_verify_s": stage_times.get("exact_semantic_verify", ""),
        "load_store_from_sqlite_s": stage_times.get("load_store_from_sqlite", ""),
        "monitor_wall_s": monitor_report.get("wall_s", ""),
        "peak_rss_mb": memory.get("peak_rss_mb", ""),
        "peak_wset_mb": memory.get("peak_wset_mb", ""),
        "peak_private_mb": memory.get("peak_private_mb", ""),
        "cpu_total_s": cpu.get("total_s", ""),
        "io_read_mb": io.get("read_bytes_delta_mb", ""),
        "io_write_mb": io.get("write_bytes_delta_mb", ""),
    }


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["query_name"])
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def aggregate_stats(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_query: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_query.setdefault(str(r["query_name"]), []).append(r)

    out: List[Dict[str, Any]] = []
    numeric_cols = [
        "num_seed_edges",
        "num_seed_closures",
        "num_closures_scanned",
        "num_closures_with_nonempty_local_candidates",
        "num_local_candidate_edges_examined",
        "num_structural_embeddings_raw",
        "num_structural_embeddings_dedup",
        "num_candidate_closures",
        "num_cand1_closures",
        "num_cand2_closures",
        "num_matches",
        "num_residual_closures",
        "query_latency_s",
        "query_wall_time_s_reported",
        "compile_query_plan_s",
        "global_seed_selection_s",
        "hgmatch_structural_match_s",
        "exact_semantic_verify_s",
        "load_store_from_sqlite_s",
        "monitor_wall_s",
        "peak_rss_mb",
        "peak_wset_mb",
        "peak_private_mb",
        "cpu_total_s",
        "io_read_mb",
        "io_write_mb",
    ]

    for query_name, grp in sorted(by_query.items()):
        row: Dict[str, Any] = {
            "query_name": query_name,
            "runs": len(grp),
            "matching_order": grp[0].get("matching_order", ""),
        }
        for col in numeric_cols:
            vals = [safe_float(r.get(col, "")) for r in grp if str(r.get(col, "")).strip() != ""]
            row[f"{col}_mean"] = mean_or_zero(vals)
            row[f"{col}_median"] = median_or_zero(vals)
            row[f"{col}_stdev"] = stdev_or_zero(vals)
        out.append(row)
    return out


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run 5 HGMatch anchored-episode queries for N repeats with peak-memory monitoring.")
    ap.add_argument("--project-root", required=True, help="HGMatch Rust project root")
    ap.add_argument("--monitor-script", required=True, help="Path to monitor_process_windows.py")
    ap.add_argument("--exe", default="", help="Path to query_hgmatch_same_tx_rust_fair.exe; default is <project-root>\\target\\release\\query_hgmatch_same_tx_rust_fair.exe")
    ap.add_argument("--db", required=True, help="HGMatch episode sqlite db")
    ap.add_argument("--outdir", required=True, help="Benchmark output directory")
    ap.add_argument("--queries", nargs="+", required=True, help="List of 5 query json paths")
    ap.add_argument("--repeats", type=int, default=10)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--sample-interval-ms", type=int, default=5)
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    project_root = Path(args.project_root).resolve()
    monitor_script = Path(args.monitor_script).resolve()
    db = Path(args.db).resolve()
    outdir = Path(args.outdir).resolve()
    query_paths = [Path(q).resolve() for q in args.queries]

    exe_path = Path(args.exe).resolve() if args.exe else (project_root / "target" / "release" / "query_hgmatch_same_tx_rust_fair.exe").resolve()
    if not exe_path.exists():
        raise FileNotFoundError(f"query exe not found: {exe_path}. Please build first with cargo build --release --bin query_hgmatch_same_tx_rust_fair")
    if not monitor_script.exists():
        raise FileNotFoundError(f"monitor script not found: {monitor_script}")
    if not db.exists():
        raise FileNotFoundError(f"db not found: {db}")

    ensure_dir(outdir)
    run_rows: List[Dict[str, Any]] = []

    for query_json in query_paths:
        query_name = query_name_from_json(query_json)
        for repeat_idx in range(1, args.repeats + 1):
            query_outdir = outdir / query_name / f"run_{repeat_idx:02d}" / "query_outputs"
            monitor_outdir = outdir / query_name / f"run_{repeat_idx:02d}" / "monitor"
            ensure_dir(query_outdir)
            ensure_dir(monitor_outdir)

            cmd = build_monitored_cmd(
                exe_path=exe_path,
                db=db,
                query_json=query_json,
                outdir=query_outdir,
                threads=args.threads,
            )
            rc = run_once(
                monitor_script=monitor_script,
                monitored_cmd=cmd,
                monitor_outdir=monitor_outdir,
                cwd=project_root,
                sample_interval_ms=args.sample_interval_ms,
            )
            if rc != 0:
                raise RuntimeError(f"Run failed for {query_name} repeat {repeat_idx}: return_code={rc}")

            summary = collect_summary(query_outdir)
            monitor_report = read_monitor_report(monitor_outdir / "monitor_report.json")
            row = flatten_run_row(
                query_name=query_name,
                query_json=query_json,
                repeat_idx=repeat_idx,
                query_outdir=query_outdir,
                monitor_outdir=monitor_outdir,
                summary=summary,
                monitor_report=monitor_report,
            )
            run_rows.append(row)
            print(f"[done] {query_name} run_{repeat_idx:02d}")

    write_csv(outdir / "all_runs.csv", run_rows)
    write_csv(outdir / "stats_by_query.csv", aggregate_stats(run_rows))
    print(f"Wrote: {outdir / 'all_runs.csv'}")
    print(f"Wrote: {outdir / 'stats_by_query.csv'}")


if __name__ == "__main__":
    main()
