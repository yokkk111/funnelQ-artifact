#!/usr/bin/env python3
"""Run the final five FunnelQ queries in FULL mode with process-tree memory metrics.

Expected monitor metrics:
  - peak_job_commit_mb
  - peak_tree_working_set_mb
  - peak_tree_private_bytes_mb

Place this script under the project ``scripts`` directory, or run it from any
location while keeping the project files intact. The project root is discovered
from the script location.
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Iterable

FINAL5 = [
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
]
QUERY_FILES = {query: f"{query}.json" for query in FINAL5}
MEMORY_FIELDS = (
    "peak_job_commit_mb",
    "peak_tree_working_set_mb",
    "peak_tree_private_bytes_mb",
)


def _find_root() -> Path:
    here = Path(__file__).resolve()
    candidates = [here.parent, *here.parents]
    for root in candidates:
        if (
            (root / "systems" / "nft" / "hvceq" / "query_nft_hvceq_full.py").is_file()
            and (root / "common" / "monitor_process_windows.py").is_file()
        ):
            return root
    raise RuntimeError(
        "Cannot locate the project root. Put this script under the project scripts/ "
        "directory or another directory inside the project."
    )


ROOT = _find_root()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Run the final five FunnelQ FULL queries and record three memory metrics."
    )
    ap.add_argument("--stores-dir", required=True, type=Path)
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--queries", default=",".join(FINAL5))
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument(
        "--timeout-sec",
        type=float,
        default=600.0,
        help="Measured query-latency threshold; this is not the whole-process wall timeout.",
    )
    ap.add_argument(
        "--process-timeout-sec",
        type=float,
        default=0.0,
        help=(
            "Whole monitored-process wall timeout. 0 uses "
            "timeout_sec * (2 when warmup is enabled, else 1) + 300 seconds."
        ),
    )
    ap.add_argument(
        "--job-memory-limit-mb",
        "--max-rss-mb",
        dest="job_memory_limit_mb",
        type=float,
        default=18432.0,
        help="Windows Job committed-memory hard limit; --max-rss-mb is a legacy alias.",
    )
    ap.add_argument(
        "--min-system-available-mb",
        type=float,
        default=2048.0,
        help="Stop the monitored Job if system available memory falls below this floor.",
    )
    ap.add_argument("--sample-interval-ms", type=int, default=50)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--progress-interval-sec", type=float, default=10.0)
    ap.add_argument(
        "--query-dir",
        default=str(ROOT / "queries" / "nft" / "selected5"),
        type=Path,
    )
    ap.add_argument("--hvceq-db", default="", type=Path)
    ap.add_argument("--tx-sidecar-db", default="", type=Path)
    ap.add_argument("--hvceq-exe", default="", type=Path)
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument(
        "--allow-missing-memory",
        action="store_true",
        help="Do not fail when the monitor report lacks one of the three requested metrics.",
    )
    return ap.parse_args()


def _abs(path: Path) -> Path:
    return path.resolve() if path.is_absolute() else (ROOT / path).resolve()


def _path_supplied(path: object) -> bool:
    return str(path or "").strip() not in {"", "."}


def _resolve_db(args: argparse.Namespace) -> Path:
    if _path_supplied(args.hvceq_db):
        path = _abs(args.hvceq_db)
        if not path.is_file():
            raise FileNotFoundError(f"FunnelQ database not found: {path}")
        return path

    stores = _abs(args.stores_dir)
    candidates = (
        stores / "funnelq" / "nft_hvceq_episode_sidecar.db",
        stores / "hvceq" / "nft_hvceq_episode_sidecar.db",
    )
    for path in candidates:
        if path.is_file():
            return path
    raise FileNotFoundError(f"FunnelQ sidecar not found under stores-dir: {stores}")


def _resolve_tx_db(args: argparse.Namespace) -> Path:
    if _path_supplied(args.tx_sidecar_db):
        path = _abs(args.tx_sidecar_db)
        if not path.is_file():
            raise FileNotFoundError(f"Transaction sidecar not found: {path}")
        return path

    stores = _abs(args.stores_dir)
    candidates = (
        stores / "funnelq" / "nft_tx_sidecar.db",
        stores / "hvceq" / "nft_tx_sidecar.db",
        stores / "nft_tx_sidecar.db",
    )
    for path in candidates:
        if path.is_file():
            return path
    raise FileNotFoundError(f"FunnelQ transaction sidecar not found under stores-dir: {stores}")


def _resolve_hvceq_exe(args: argparse.Namespace) -> Path:
    if _path_supplied(args.hvceq_exe):
        path = _abs(args.hvceq_exe)
    else:
        path = (
            ROOT
            / "systems"
            / "nft"
            / "hvceq"
            / "local_executor_rust"
            / "target"
            / "release"
            / "nft_hvceq_local_executor.exe"
        )
    if not path.is_file():
        raise FileNotFoundError(
            "Compiled FunnelQ executor not found: "
            f"{path}. Build it once with cargo build --release before running the benchmark."
        )
    return path.resolve()


def _active_conflicting_processes() -> list[str]:
    """Return active verification/heavy benchmark command lines on Windows.

    Query memory measurements are invalid while the verification ablation or
    another monitored FunnelQ benchmark is still running.  The check is best
    effort and deliberately does not fail when PowerShell/CIM is unavailable.
    """
    if sys.platform != "win32":
        return []
    ps = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.ProcessId -ne $PID -and $_.CommandLine -match "
        "'run_verification_ablation.py|run_formal_vo.py verify|"
        "rust_full_verifier|rust_verifier_core' } | "
        "Select-Object -ExpandProperty CommandLine"
    )
    try:
        proc = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", ps],
            text=True,
            capture_output=True,
            timeout=15,
        )
    except Exception:
        return []
    if proc.returncode != 0:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _find_exact_key(obj: Any, key: str) -> Any:
    """Find an exact metric key anywhere in a JSON-compatible object."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for value in obj.values():
            found = _find_exact_key(value, key)
            if found not in (None, ""):
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _find_exact_key(value, key)
            if found not in (None, ""):
                return found
    return ""


def _number_or_blank(value: Any) -> float | int | str:
    if value in (None, ""):
        return ""
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def _numeric_values(rows: Iterable[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    return values


def _stats(rows: list[dict[str, Any]], key: str) -> dict[str, float | str]:
    values = _numeric_values(rows, key)
    if not values:
        return {
            f"mean_{key}": "",
            f"std_{key}": "",
            f"min_{key}": "",
            f"max_{key}": "",
        }
    return {
        f"mean_{key}": mean(values),
        f"std_{key}": stdev(values) if len(values) > 1 else 0.0,
        f"min_{key}": min(values),
        f"max_{key}": max(values),
    }


def run_one(
    args: argparse.Namespace,
    db: Path,
    tx_db: Path,
    hvceq_exe: Path,
    outdir: Path,
    run_id: int,
    query: str,
) -> dict[str, Any]:
    variant = "full"
    run_out = outdir / "query_outputs" / f"run_{run_id:02d}" / query / variant
    mon_out = outdir / "monitors" / f"run_{run_id:02d}" / query / variant
    phase_marker = run_out / "query_phase.marker"
    for path in (run_out, mon_out):
        if path.exists():
            shutil.rmtree(path)

    qjson = _abs(args.query_dir) / QUERY_FILES[query]
    if not qjson.is_file():
        raise FileNotFoundError(f"Query JSON not found: {qjson}")

    inner = [
        sys.executable,
        str(ROOT / "systems" / "nft" / "hvceq" / "query_nft_hvceq_full.py"),
        "--db",
        str(db),
        "--tx-sidecar-db",
        str(tx_db),
        "--query-json",
        str(qjson),
        "--outdir",
        str(run_out),
        "--ablation",
        variant,
        "--candidate-limit",
        str(10000 if args.smoke else 0),
        "--anchor-limit",
        str(10000 if args.smoke else 0),
        "--threads",
        str(max(1, args.threads)),
        "--warmup-runs",
        str(max(0, args.warmup_runs)),
        "--phase-marker",
        str(phase_marker),
        "--exe",
        str(hvceq_exe),
    ]

    process_timeout_sec = (
        float(args.process_timeout_sec)
        if float(args.process_timeout_sec) > 0
        else float(args.timeout_sec) * (1 if int(args.warmup_runs) == 0 else 2) + 300.0
    )

    cmd = [
        sys.executable,
        str(ROOT / "common" / "monitor_process_windows.py"),
        "--outdir",
        str(mon_out),
        "--job-memory-limit-mb",
        str(args.job_memory_limit_mb),
        "--min-system-available-mb",
        str(args.min_system_available_mb),
        "--timeout-sec",
        str(process_timeout_sec),
        "--sample-interval-ms",
        str(args.sample_interval_ms),
        "--phase-marker",
        str(phase_marker),
        "--",
        *inner,
    ]

    run_out.mkdir(parents=True, exist_ok=True)
    mon_out.mkdir(parents=True, exist_ok=True)
    stdout_path = run_out / "runner.stdout.txt"
    stderr_path = run_out / "runner.stderr.txt"
    (run_out / "command.json").write_text(
        json.dumps({"monitor_command": cmd, "query_command": inner, "process_timeout_sec": process_timeout_sec, "query_timeout_sec": args.timeout_sec}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    started = time.monotonic()
    last_progress = started
    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_handle:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            text=True,
            stdout=stdout_handle,
            stderr=stderr_handle,
        )
        while proc.poll() is None:
            time.sleep(0.5)
            now = time.monotonic()
            if now - last_progress >= max(1.0, args.progress_interval_sec):
                print(
                    f"[running] run={run_id}/{args.runs} query={query} "
                    f"elapsed_s={now - started:.1f}",
                    flush=True,
                )
                last_progress = now
        return_code = int(proc.returncode or 0)

    report_path = run_out / "execution_report.json"
    monitor_path = mon_out / "monitor_report.json"
    report = _read_json(report_path) if report_path.exists() else {}
    monitor = _read_json(monitor_path) if monitor_path.exists() else {"status": "missing"}
    if "status" not in monitor and monitor_path.exists():
        monitor["status"] = "ok" if int(monitor.get("return_code", 1)) == 0 else "failed"
    timing = report.get("timing") or {}
    candidate = report.get("candidate_metrics") or {}
    query_phase = monitor.get("query_phase") or {}

    memory_metrics = {
        field: _number_or_blank(query_phase.get(field, ""))
        for field in MEMORY_FIELDS
    }
    missing_memory = [field for field, value in memory_metrics.items() if value in (None, "")]
    query_phase_samples = int(query_phase.get("sample_count", 0) or 0)

    row: dict[str, Any] = {
        "run_id": run_id,
        "query": query,
        "variant": variant,
        "process_return_code": return_code,
        "monitor_status": monitor.get("status", "missing"),
        "wall_s": monitor.get("wall_s_external_monitor", monitor.get("wall_s", "")),
        "monitor_wall_time_s": monitor.get(
            "wall_s_external_monitor", monitor.get("wall_s", "")
        ),
        "process_timeout_sec": process_timeout_sec,
        "query_timeout_sec": args.timeout_sec,
        **memory_metrics,
        "minimum_system_available_mb": _number_or_blank(
            query_phase.get("minimum_system_available_mb", "")
        ),
        "query_phase_monitor_samples": query_phase_samples,
        "lifetime_peak_job_commit_mb": _number_or_blank(
            (monitor.get("job_commit") or {}).get("peak_job_commit_mb", "")
        ),
        # Retain the legacy monitor field when available for diagnostics only.
        "peak_rss_mb": _number_or_blank(_find_exact_key(monitor, "peak_rss_mb")),
        "query_latency_s": timing.get("query_latency_s", ""),
        "search_only_s": timing.get("search_only_s", ""),
        "output_materialization_s": timing.get("output_materialization_s", ""),
        "candidate_query_s": timing.get("candidate_query_s", ""),
        "closure_input_load_s": timing.get("closure_input_load_s", ""),
        "exact_match_s": timing.get("exact_match_s", ""),
        "result_count": report.get("result_count", ""),
        "duplicate_count": report.get("duplicate_count", ""),
        "result_digest_sha256": report.get("result_digest_sha256", ""),
        "instances_csv": report.get("instances_csv", ""),
        "closure_universe": candidate.get("closure_universe", ""),
        "cand0_anchor_hits": candidate.get("cand0_anchor_hits", ""),
        "cand1_base_hits": candidate.get("cand1_base_hits", ""),
        "cand2_refined_hits": candidate.get("cand2_refined_hits", ""),
        "exact_engine": report.get("exact_engine", candidate.get("exact_engine", "")),
        "execution_report": str(report_path),
        "monitor_report": str(monitor_path),
    }

    if return_code != 0 or monitor.get("status") != "ok" or not report_path.exists():
        stderr_tail = stderr_path.read_text(encoding="utf-8", errors="replace")[-5000:]
        stdout_tail = stdout_path.read_text(encoding="utf-8", errors="replace")[-5000:]
        row["error"] = stderr_tail or stdout_tail
        raise RuntimeError(json.dumps(row, ensure_ascii=False, indent=2))

    if int(report.get("duplicate_count", -1)) != 0:
        raise RuntimeError(f"{query}/full emitted duplicate instances")

    if (missing_memory or query_phase_samples < 1) and not args.allow_missing_memory:
        row["error"] = (
            "monitor_report.json has no valid query-phase memory measurement: "
            f"missing_metrics={missing_memory}, sample_count={query_phase_samples}"
        )
        raise RuntimeError(json.dumps(row, ensure_ascii=False, indent=2))

    expected_contract = {
        "output_mode": "query",
        "exact_engine": "refined_posting_fused",
        "uses_refined_posting": True,
        "fused_refined_exact": True,
        "anchor_engine": "anchor_directory",
        "uses_anchor_directory": True,
        "search_only_direct_timer": True,
    }
    actual_contract = {
        key: candidate.get(key) for key in expected_contract
    }
    contract_mismatches = {
        key: {"expected": expected, "actual": actual_contract.get(key)}
        for key, expected in expected_contract.items()
        if actual_contract.get(key) != expected
    }
    if contract_mismatches:
        row["error"] = {
            "message": "FULL query did not use the selected-five fused execution contract",
            "mismatches": contract_mismatches,
        }
        raise RuntimeError(json.dumps(row, ensure_ascii=False, indent=2))

    return row


def validate_consistency(rows: list[dict[str, Any]], outdir: Path) -> None:
    problems: list[dict[str, Any]] = []
    for query in FINAL5:
        group = [row for row in rows if row["query"] == query]
        if not group:
            continue
        counts = sorted({str(row.get("result_count", "")) for row in group})
        digests = sorted({str(row.get("result_digest_sha256", "")) for row in group})
        duplicates = [int(row.get("duplicate_count", -1)) for row in group]
        if len(counts) != 1 or len(digests) != 1 or max(duplicates) != 0:
            problems.append(
                {
                    "query": query,
                    "result_counts": counts,
                    "result_digests": digests,
                    "max_duplicate_count": max(duplicates),
                }
            )
    if problems:
        path = outdir / "FULL_RESULT_CONSISTENCY_FAILURE.json"
        path.write_text(json.dumps(problems, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"FULL runs produced inconsistent results; see {path}")


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    metric_keys = [
        "query_latency_s",
        "search_only_s",
        "output_materialization_s",
        "wall_s",
        "monitor_wall_time_s",
        "peak_job_commit_mb",
        "peak_tree_working_set_mb",
        "peak_tree_private_bytes_mb",
        "minimum_system_available_mb",
        "query_phase_monitor_samples",
        "lifetime_peak_job_commit_mb",
        "peak_rss_mb",
    ]
    for query in FINAL5:
        group = [row for row in rows if row["query"] == query]
        if not group:
            continue
        summary: dict[str, Any] = {
            "query": query,
            "variant": "full",
            "successful_runs": len(group),
            "result_count": group[0].get("result_count", ""),
            "result_digest_sha256": group[0].get("result_digest_sha256", ""),
            "closure_universe": group[0].get("closure_universe", ""),
            "cand0_anchor_hits": group[0].get("cand0_anchor_hits", ""),
            "cand1_base_hits": group[0].get("cand1_base_hits", ""),
            "cand2_refined_hits": group[0].get("cand2_refined_hits", ""),
        }
        for key in metric_keys:
            summary.update(_stats(group, key))
        summaries.append(summary)
    return summaries


def main() -> int:
    args = parse_args()
    if args.runs < 1:
        raise ValueError("--runs must be at least 1")
    if args.sample_interval_ms < 1:
        raise ValueError("--sample-interval-ms must be positive")
    if args.warmup_runs < 0:
        raise ValueError("--warmup-runs must be non-negative")
    if args.progress_interval_sec <= 0:
        raise ValueError("--progress-interval-sec must be positive")
    if args.process_timeout_sec < 0:
        raise ValueError("--process-timeout-sec must be non-negative")
    if args.job_memory_limit_mb <= 0:
        raise ValueError("--job-memory-limit-mb must be positive")
    if args.min_system_available_mb < 0:
        raise ValueError("--min-system-available-mb must be non-negative")

    conflicts = _active_conflicting_processes()
    if conflicts:
        raise RuntimeError(
            "Conflicting verification/benchmark processes are still running; "
            "stop them before measuring query memory:\n" + "\n".join(conflicts)
        )

    db = _resolve_db(args)
    tx_db = _resolve_tx_db(args)
    hvceq_exe = _resolve_hvceq_exe(args)
    outdir = _abs(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    queries = [query.strip().upper() for query in args.queries.split(",") if query.strip()]
    if not queries:
        raise ValueError("--queries must contain at least one query")
    if len(queries) != len(set(queries)):
        raise ValueError("--queries must not contain duplicates")
    unknown = sorted(set(queries) - set(FINAL5))
    if unknown:
        raise ValueError(f"Unknown queries: {unknown}")

    rows: list[dict[str, Any]] = []
    all_runs_path = outdir / "funnelq_full_all_runs.csv"

    for run_id in range(1, args.runs + 1):
        for query in queries:
            print(f"[full-memory] run={run_id}/{args.runs} query={query}", flush=True)
            row = run_one(args, db, tx_db, hvceq_exe, outdir, run_id, query)
            rows.append(row)
            _write_csv(all_runs_path, rows)
            print(
                "[done] "
                f"latency={row.get('query_latency_s')}s "
                f"job_commit={row.get('peak_job_commit_mb')}MiB "
                f"working_set={row.get('peak_tree_working_set_mb')}MiB "
                f"private_bytes={row.get('peak_tree_private_bytes_mb')}MiB",
                flush=True,
            )

    validate_consistency(rows, outdir)
    summaries = summarize(rows)
    _write_csv(outdir / "funnelq_full_summary.csv", summaries)

    report = {
        "experiment": "funnelq_final5_full_query_memory",
        "result_semantics": "full_event_binding_v1",
        "queries": queries,
        "variant": "full",
        "runs": args.runs,
        "all_run_result_sets_consistent": True,
        "measurement_policy": "one_warmup_then_one_measured_query_phase",
        "memory_primary_metric": "peak_job_commit_mb",
        "memory_metrics": list(MEMORY_FIELDS),
        "memory_contract": (
            "Same as the DeFi formal experiment: query-phase "
            "Windows Job commit, summed process-tree working set, and summed "
            "process-tree private bytes; runtime load, warmup, and final result "
            "file writing are excluded by the phase marker."
        ),
        "monitor": str(ROOT / "common" / "monitor_process_windows.py"),
        "sample_interval_ms": args.sample_interval_ms,
        "job_memory_limit_mb": args.job_memory_limit_mb,
        "min_system_available_mb": args.min_system_available_mb,
        "query_timeout_sec": args.timeout_sec,
        "process_timeout_sec": (args.process_timeout_sec if args.process_timeout_sec > 0 else args.timeout_sec * (1 if args.warmup_runs == 0 else 2) + 300.0),
        "threads": args.threads,
        "warmup_runs": args.warmup_runs,
        "hvceq_exe": str(hvceq_exe),
        "hvceq_db": str(db),
        "tx_sidecar_db": str(tx_db),
        "all_runs_csv": str(all_runs_path),
        "summary_csv": str(outdir / "funnelq_full_summary.csv"),
    }
    (outdir / "funnelq_full_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Completed: {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
