#!/usr/bin/env python3
"""Unified DeFi-aligned four-backend NFT full-binding experiment runner.

Each backend loads its persistent runtime once, performs one unmeasured warm-up
on that same runtime, and then marks exactly one measured query phase.  Runtime
loading, offline construction, warm-up, and final CSV writing are excluded;
query-dependent retrieval, matching, canonicalization, and complete NFT result
materialization are included.
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
from statistics import mean, pstdev
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FINAL5 = ["C1", "C2", "L3_RETURN", "L6_NEG_RETURN_SUPPORT", "H5_23"]
SELECTED5 = [
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
]
ALL_BACKENDS = ["hvceq", "sql", "hgmatch", "mbft"]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run and strongly validate four NFT full-binding backends.")
    ap.add_argument("--stores-dir", required=True, type=Path)
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--queries", default=",".join(SELECTED5))
    ap.add_argument("--backends", default=",".join(ALL_BACKENDS))
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument(
        "--timeout-sec",
        type=float,
        default=600.0,
        help="Reported query-latency threshold and MBFT inner-stage timeout.",
    )
    ap.add_argument(
        "--process-timeout-sec",
        type=float,
        default=0.0,
        help="Whole-process wall timeout; 0 uses --timeout-sec.",
    )
    ap.add_argument(
        "--job-memory-limit-mb",
        "--max-rss-mb",
        dest="job_memory_limit_mb",
        type=float,
        default=18000.0,
        help="Windows Job commit hard limit; --max-rss-mb is a legacy alias.",
    )
    ap.add_argument("--sample-interval-ms", type=int, default=50)
    ap.add_argument(
        "--no-warmup",
        action="store_true",
        help="Development only. Formal DeFi-aligned runs require one same-runtime warm-up.",
    )
    ap.add_argument("--query-dir", default=str(ROOT / "queries" / "nft" / "selected5"), type=Path)
    ap.add_argument("--hvceq-db", default="", type=Path)
    ap.add_argument("--tx-sidecar-db", default="", type=Path)
    ap.add_argument("--sql-db", default="", type=Path)
    ap.add_argument("--hgmatch-db", default="", type=Path)
    ap.add_argument("--mbft-manifest", default="", type=Path)
    ap.add_argument("--hvceq-exe", default="", type=Path)
    ap.add_argument("--hgmatch-exe", default="", type=Path)
    ap.add_argument("--mbft-exe", default="", type=Path)
    ap.add_argument("--candidate-limit", type=int, default=0, help="Development only; formal runs must keep 0.")
    ap.add_argument("--anchor-limit", type=int, default=0, help="Development only; formal runs must keep 0.")
    return ap.parse_args()


def _abs(path: Path) -> Path:
    return path.resolve() if path.is_absolute() else (ROOT / path).resolve()


def _path_supplied(path: object) -> bool:
    return str(path or "").strip() not in {"", "."}


def _first_existing(candidates: list[Path], label: str) -> Path:
    for path in candidates:
        if path.is_file():
            return path.resolve()
    raise FileNotFoundError(f"{label} not found; checked: " + ", ".join(str(x) for x in candidates))


def resolve_inputs(args: argparse.Namespace, backends: list[str]) -> dict[str, Path]:
    stores = _abs(args.stores_dir)
    result: dict[str, Path] = {}
    if "hvceq" in backends:
        result["hvceq_db"] = _abs(args.hvceq_db) if _path_supplied(args.hvceq_db) else _first_existing([
            stores / "funnelq" / "nft_hvceq_episode_sidecar.db",
            stores / "hvceq" / "nft_hvceq_episode_sidecar.db",
            stores / "nft_hvceq_episode_sidecar.db",
        ], "FunnelQ store")
        sidecar_candidates = [
            stores / "funnelq" / "nft_tx_sidecar.db",
            stores / "hvceq" / "nft_tx_sidecar.db",
            stores / "nft_tx_sidecar.db",
        ]
        if _path_supplied(args.tx_sidecar_db):
            result["tx_sidecar_db"] = _abs(args.tx_sidecar_db)
        else:
            for candidate in sidecar_candidates:
                if candidate.is_file():
                    result["tx_sidecar_db"] = candidate.resolve()
                    break
        if _path_supplied(args.hvceq_exe): result["hvceq_exe"] = _abs(args.hvceq_exe)
    if "sql" in backends:
        result["sql_db"] = _abs(args.sql_db) if _path_supplied(args.sql_db) else _first_existing([
            stores / "sql" / "nft_sql_store.db", stores / "nft_sql_store.db",
        ], "SQL store")
    if "hgmatch" in backends:
        result["hgmatch_db"] = _abs(args.hgmatch_db) if _path_supplied(args.hgmatch_db) else _first_existing([
            stores / "hgmatch" / "nft_hgmatch_store.db", stores / "nft_hgmatch_store.db",
        ], "HGMatch store")
        if _path_supplied(args.hgmatch_exe): result["hgmatch_exe"] = _abs(args.hgmatch_exe)
    if "mbft" in backends:
        result["mbft_manifest"] = _abs(args.mbft_manifest) if _path_supplied(args.mbft_manifest) else _first_existing([
            stores / "mbft_sharded_300k" / "mbft_nft_shard_manifest.json",
            stores / "mbft" / "mbft_nft_shard_manifest.json",
            stores / "mbft_nft_shard_manifest.json",
        ], "MBFT shard manifest")
        if _path_supplied(args.mbft_exe):
            result["mbft_exe"] = _abs(args.mbft_exe)
        else:
            base = ROOT / "systems" / "nft" / "mbft" / "persistent_build"
            result["mbft_exe"] = _first_existing([
                base / "mbft_nft_adapt", base / "mbft_nft_adapt.exe",
                base / "Release" / "mbft_nft_adapt.exe", base / "Release" / "mbft_nft_adapt",
            ], "MBFT executable")
    return result


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
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


def _query_json(query_dir: Path, query: str) -> Path:
    mapping = {
        "C1": "C1.json", "C2": "C2.json", "L3_RETURN": "L3_RETURN.json",
        "L6_NEG_RETURN_SUPPORT": "L6_NEG_RETURN_SUPPORT.json", "H5_23": "H5_23.json",
    }
    path = query_dir / mapping.get(query, f"{query}.json")
    if not path.is_file():
        raise FileNotFoundError(f"query JSON not found: {path}")
    return path.resolve()


def backend_command(
    args: argparse.Namespace,
    paths: dict[str, Path],
    backend: str,
    query: str,
    outdir: Path,
    phase_marker: Path,
) -> list[str]:
    qjson = _query_json(_abs(args.query_dir), query)
    warm_args = [
        "--warmup-runs", "0" if args.no_warmup else "1",
        "--phase-marker", str(phase_marker),
    ]
    if backend == "hvceq":
        cmd = [
            sys.executable, str(ROOT / "systems" / "nft" / "hvceq" / "query_nft_hvceq_full.py"),
            "--db", str(paths["hvceq_db"]), "--query-json", str(qjson), "--outdir", str(outdir),
            "--ablation", "full", "--candidate-limit", str(max(0, args.candidate_limit)),
            "--anchor-limit", str(max(0, args.anchor_limit)),
            "--threads", str(max(1, args.threads)),
            *warm_args,
        ]
        if "hvceq_exe" in paths:
            cmd += ["--exe", str(paths["hvceq_exe"])]
        if "tx_sidecar_db" in paths:
            cmd += ["--tx-sidecar-db", str(paths["tx_sidecar_db"])]
        return cmd
    if backend == "sql":
        return [
            sys.executable, str(ROOT / "systems" / "nft" / "sql" / "query_nft_sql_full.py"),
            "--db", str(paths["sql_db"]), "--query-json", str(qjson), "--outdir", str(outdir),
            *warm_args,
        ]
    if backend == "hgmatch":
        cmd = [
            sys.executable, str(ROOT / "systems" / "nft" / "hgmatch" / "query_nft_hgmatch.py"),
            "--db", str(paths["hgmatch_db"]), "--query-json", str(qjson), "--outdir", str(outdir),
            "--threads", str(max(1, args.threads)), "--max-results", "0",
            *warm_args,
        ]
        if "hgmatch_exe" in paths:
            cmd += ["--exe", str(paths["hgmatch_exe"])]
        return cmd
    if backend == "mbft":
        return [
            sys.executable, str(ROOT / "systems" / "nft" / "mbft" / "run_mbft_nft_sharded_query.py"),
            "--manifest", str(paths["mbft_manifest"]), "--exe", str(paths["mbft_exe"]),
            "--outdir", str(outdir), "--queries", query, "--query-dir", str(_abs(args.query_dir)),
            "--timeout-per-shard-sec", str(args.timeout_sec), "--max-shard-rss-mb", str(args.job_memory_limit_mb),
            "--matcher-timeout-sec", str(args.timeout_sec), "--max-matcher-rss-mb", str(args.job_memory_limit_mb),
            *warm_args,
        ]
    raise ValueError(f"unsupported backend: {backend}")


def monitored_run(
    args: argparse.Namespace,
    cmd: list[str],
    run_out: Path,
    monitor_out: Path,
    phase_marker: Path,
) -> tuple[int, dict[str, Any], str, str]:
    process_timeout_sec = (
        args.process_timeout_sec
        if args.process_timeout_sec > 0
        else args.timeout_sec * (1 if args.no_warmup else 2) + 300.0
    )
    monitor_cmd = [
        sys.executable, str(ROOT / "common" / "monitor_process_windows.py"),
        "--outdir", str(monitor_out), "--job-memory-limit-mb", str(args.job_memory_limit_mb),
        "--timeout-sec", str(process_timeout_sec),
        "--sample-interval-ms", str(args.sample_interval_ms),
        "--phase-marker", str(phase_marker),
        "--", *cmd,
    ]
    proc = subprocess.run(monitor_cmd, cwd=str(ROOT), text=True, capture_output=True)
    run_out.mkdir(parents=True, exist_ok=True)
    (run_out / "runner.stdout.txt").write_text(proc.stdout, encoding="utf-8")
    (run_out / "runner.stderr.txt").write_text(proc.stderr, encoding="utf-8")
    mon_path = monitor_out / "monitor_report.json"
    monitor = _read_json(mon_path) if mon_path.exists() else {"status": "missing"}
    if "status" not in monitor and mon_path.exists():
        monitor["status"] = "ok" if int(monitor.get("return_code", 1)) == 0 else "failed"
        monitor["killed_for_timeout"] = monitor.get("stop_reason") == "timeout"
        monitor["killed_for_memory"] = monitor.get("stop_reason") in {
            "target_failed_or_job_memory_limit",
            "system_available_below_floor",
        }
    return proc.returncode, monitor, proc.stdout, proc.stderr


def execute_one(
    args: argparse.Namespace,
    paths: dict[str, Path],
    backend: str,
    query: str,
    run_id: int,
    root_out: Path,
) -> dict[str, Any]:
    phase = f"run_{run_id:02d}"
    run_out = root_out / "query_outputs" / phase / query / backend
    mon_out = root_out / "monitors" / phase / query / backend
    if run_out.exists():
        shutil.rmtree(run_out)
    if mon_out.exists():
        shutil.rmtree(mon_out)
    phase_marker = run_out / "query_phase.marker"
    phase_marker.unlink(missing_ok=True)
    cmd = backend_command(args, paths, backend, query, run_out, phase_marker)
    rc, monitor, stdout, stderr = monitored_run(
        args, cmd, run_out, mon_out, phase_marker
    )
    report_path = run_out / "execution_report.json"
    report = _read_json(report_path) if report_path.exists() else {}
    timing = report.get("timing") or {}
    hvceq_components = (
        (
            "candidate_query_s",
            "candidate_csv_write_s",
            "candidate_csv_read_s",
            "closure_input_load_s",
            "exact_match_s",
            "result_projection_load_s",
            "result_materialize_s",
            "result_canonicalize_s",
        )
        if timing.get("closure_reloading_eliminated")
        else (
            "candidate_query_s",
            "candidate_csv_write_s",
            "candidate_csv_read_s",
            "closure_input_load_s",
            "exact_match_s",
            "result_canonicalize_s",
        )
    )
    required_latency_components = {
        "hvceq": hvceq_components,
        "mbft": (
            "atomic_retrieval_s",
            "candidate_jsonl_write_s",
            "candidate_jsonl_load_s",
            "adapter_prepare_s",
            "adapter_match_s",
            "result_canonicalize_s",
        ),
    }
    latency_contract_ok = True
    latency_contract_error = ""
    if backend in required_latency_components and report_path.exists():
        components = required_latency_components[backend]
        missing_components = [
            name for name in components if timing.get(name) in ("", None)
        ]
        if missing_components:
            latency_contract_ok = False
            latency_contract_error = (
                f"missing query latency components: {missing_components}"
            )
        else:
            component_sum = sum(float(timing[name]) for name in components)
            reported = float(timing.get("query_latency_s", -1.0))
            if abs(component_sum - reported) > 1e-6:
                latency_contract_ok = False
                latency_contract_error = (
                    f"query_latency_s={reported} component_sum={component_sum}"
                )
    row = {
        "phase": phase, "run_id": run_id, "query": query, "backend": backend,
        "process_return_code": rc, "monitor_status": monitor.get("status", "missing"),
        "killed_for_timeout": monitor.get("killed_for_timeout", False),
        "killed_for_memory": monitor.get("killed_for_memory", False),
        "wall_s": monitor.get("wall_s_external_monitor", ""),
        "peak_job_commit_mb": (monitor.get("query_phase") or {}).get("peak_job_commit_mb", ""),
        "peak_tree_working_set_mb": (monitor.get("query_phase") or {}).get("peak_tree_working_set_mb", ""),
        "peak_tree_private_bytes_mb": (monitor.get("query_phase") or {}).get("peak_tree_private_bytes_mb", ""),
        "minimum_system_available_mb": (monitor.get("query_phase") or {}).get("minimum_system_available_mb", ""),
        "lifetime_peak_job_commit_mb": (monitor.get("job_commit") or {}).get("peak_job_commit_mb", ""),
        "query_latency_s": timing.get("query_latency_s", ""),
        "query_latency_contract_ok": latency_contract_ok,
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
        "atomic_retrieval_s": timing.get("atomic_retrieval_s", ""),
        "candidate_jsonl_write_s": timing.get("candidate_jsonl_write_s", ""),
        "candidate_jsonl_load_s": timing.get("candidate_jsonl_load_s", ""),
        "adapter_prepare_s": timing.get("adapter_prepare_s", ""),
        "adapter_match_s": timing.get("adapter_match_s", ""),
        "result_canonicalize_s": timing.get("result_canonicalize_s", ""),
        "result_count": report.get("result_count", ""),
        "duplicate_count": report.get("duplicate_count", ""),
        "result_digest_sha256": report.get("result_digest_sha256", ""),
        "instances_csv": report.get("instances_csv", ""),
        "execution_report": str(report_path) if report_path.exists() else "",
        "monitor_report": str(mon_out / "monitor_report.json"),
    }
    if rc != 0 or monitor.get("status") != "ok" or not report_path.exists():
        row["error"] = (stderr or stdout)[-4000:]
        raise RuntimeError(json.dumps(row, ensure_ascii=False, indent=2))
    if not latency_contract_ok:
        raise RuntimeError(
            f"{backend}/{query} incomplete query latency accounting: "
            f"{latency_contract_error}"
        )
    if str(report.get("result_semantics")) != "full_event_binding_v1":
        raise RuntimeError(f"{backend}/{query} returned wrong semantics: {report.get('result_semantics')}")
    if int(report.get("duplicate_count", -1)) != 0:
        raise RuntimeError(f"{backend}/{query} emitted duplicate full instances: {report.get('duplicate_count')}")
    instances = Path(str(report.get("instances_csv") or ""))
    if not instances.is_absolute():
        instances = (ROOT / instances).resolve()
    if not instances.is_file():
        raise FileNotFoundError(f"{backend}/{query} missing canonical instances CSV: {instances}")
    row["instances_csv"] = str(instances)

    try:
        row["query_timeout_exceeded"] = (
            float(row["query_latency_s"]) > float(args.timeout_sec)
        )
    except (TypeError, ValueError):
        row["query_timeout_exceeded"] = False

    print(
        "[done] "
        f"phase={phase} "
        f"query={query} "
        f"backend={backend} "
        f"query_latency_s={row['query_latency_s']} "
        f"wall_s={row['wall_s']} "
        f"peak_job_commit_mb={row['peak_job_commit_mb']} "
        f"results={row['result_count']} "
        f"query_timeout_exceeded={row['query_timeout_exceeded']}",
        flush=True,
    )
    return row


def strong_compare(query: str, run_id: int, rows: list[dict[str, Any]], outdir: Path) -> dict[str, Any]:
    report_path = outdir / "result_set_checks" / f"run_{run_id:02d}_{query}.json"
    cmd = [
        sys.executable, str(ROOT / "tools" / "compare_nft_full_instances.py"),
        "--query", query, "--out", str(report_path),
        *[f"{r['backend']}={r['instances_csv']}" for r in rows],
    ]
    proc = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True)
    if proc.returncode != 0:
        mismatch = outdir / "MATCH_SET_MISMATCH.json"
        mismatch.write_text(report_path.read_text(encoding="utf-8") if report_path.exists() else json.dumps({
            "query": query, "run_id": run_id, "stdout": proc.stdout, "stderr": proc.stderr,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"full-instance result mismatch for run={run_id} query={query}; see {mismatch}")
    return _read_json(report_path)


def main() -> int:
    args = parse_args()
    if args.candidate_limit or args.anchor_limit:
        print("[warning] candidate/anchor limits are nonzero; this is not a formal completeness run", file=sys.stderr)
    queries = [q.strip().upper() for q in args.queries.split(",") if q.strip()]
    for query in queries:
        _query_json(_abs(args.query_dir), query)
    backends = [b.strip().lower() for b in args.backends.split(",") if b.strip()]
    unknown_b = sorted(set(backends) - set(ALL_BACKENDS))
    if unknown_b:
        raise ValueError(f"unknown backends: {unknown_b}")
    if len(backends) < 2:
        raise ValueError("strong comparison requires at least two backends")
    paths = resolve_inputs(args, backends)
    outdir = _abs(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    meta = {
        "format": "NFT-full-binding-four-backend-experiment-v3-defi-aligned",
        "result_semantics": "full_event_binding_v1",
        "queries": queries, "backends": backends, "runs": max(1, args.runs),
        "warmup": not args.no_warmup, "threads": max(1, args.threads),
        "timeout_sec": args.timeout_sec,
        "process_timeout_sec": (
            args.process_timeout_sec
            if args.process_timeout_sec > 0
            else args.timeout_sec * (1 if args.no_warmup else 2) + 300.0
        ),
        "job_memory_limit_mb": args.job_memory_limit_mb,
        "measurement_policy": (
            "single_cold_or_ambient_run"
            if args.no_warmup
            else "one_warmup_then_one_measured_query_phase"
        ),
        "timing_contract": (
            "same-runtime warmup; runtime/store loading and offline build excluded; "
            "query-dependent retrieval, matching, canonicalization, and full NFT "
            "result materialization included; final CSV write excluded"
        ),
        "memory_contract": (
            "Windows Job commit and process-tree memory sampled only while the "
            "backend query-phase marker exists; lifetime peak retained separately"
        ),
        "resolved_inputs": {k: str(v) for k, v in paths.items()},
    }
    (outdir / "experiment_manifest.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    measured_rows: list[dict[str, Any]] = []
    for run_id in range(1, max(1, args.runs) + 1):
        for query in queries:
            query_rows: list[dict[str, Any]] = []
            for backend in backends:
                print(f"[measure] run={run_id} query={query} backend={backend}", flush=True)
                row = execute_one(args, paths, backend, query, run_id, outdir)
                measured_rows.append(row)
                query_rows.append(row)
                _write_csv(outdir / "comparison_all_runs.csv", measured_rows)
            print(
                f"[compare] run={run_id} query={query} "
                "streaming exact full-instance comparison",
                flush=True,
            )
            strong_compare(query, run_id, query_rows, outdir)
            print(f"[validated] run={run_id} query={query} exact result sets are identical", flush=True)

    metric_names = (
        "query_latency_s",
        "peak_job_commit_mb",
        "peak_tree_working_set_mb",
        "candidate_query_s",
        "candidate_csv_write_s",
        "candidate_csv_read_s",
        "closure_input_load_s",
        "exact_match_s",
        "exact_order_csv_write_s",
        "exact_order_csv_read_s",
        "result_projection_load_s",
        "result_materialize_s",
        "atomic_retrieval_s",
        "candidate_jsonl_write_s",
        "candidate_jsonl_load_s",
        "adapter_prepare_s",
        "adapter_match_s",
        "result_canonicalize_s",
    )
    aggregate_rows: list[dict[str, Any]] = []
    for query in queries:
        for backend in backends:
            group = [
                row
                for row in measured_rows
                if row["query"] == query and row["backend"] == backend
            ]
            aggregate: dict[str, Any] = {
                "query": query,
                "backend": backend,
                "runs": len(group),
                "all_latency_contracts_ok": all(
                    bool(row.get("query_latency_contract_ok")) for row in group
                ),
                "all_query_timeouts_clear": not any(
                    bool(row.get("query_timeout_exceeded")) for row in group
                ),
                "result_count": group[0].get("result_count", "") if group else "",
                "result_digest_sha256": (
                    group[0].get("result_digest_sha256", "") if group else ""
                ),
            }
            for metric in metric_names:
                values = [
                    float(row[metric])
                    for row in group
                    if row.get(metric) not in ("", None)
                ]
                aggregate[f"mean_{metric}"] = mean(values) if values else ""
                aggregate[f"std_{metric}"] = (
                    pstdev(values) if len(values) > 1 else (0.0 if values else "")
                )
            aggregate_rows.append(aggregate)
    _write_csv(outdir / "comparison_summary_10x.csv", aggregate_rows)

    summary = {
        **meta,
        "completed": True,
        "measured_row_count": len(measured_rows),
        "all_result_set_checks_passed": True,
        "summary_csv": str(outdir / "comparison_all_runs.csv"),
        "aggregate_summary_csv": str(outdir / "comparison_summary_10x.csv"),
    }
    (outdir / "comparison_report.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(outdir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
