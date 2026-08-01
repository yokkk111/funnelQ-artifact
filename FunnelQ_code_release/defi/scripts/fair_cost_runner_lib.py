from __future__ import annotations

import csv
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parents[1]


def _resolve_config_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


DATASET_CONFIG_PATH = os.environ.get("HVCEQ_DATASET_CONFIG", "").strip()
if DATASET_CONFIG_PATH:
    _dataset_config = json.loads(_resolve_config_path(DATASET_CONFIG_PATH).read_text(encoding="utf-8"))
    DATASET_NAME = str(_dataset_config["dataset_name"])
    WORK_ROOT = _resolve_config_path(_dataset_config["work_root"])
    COMBINED_EVENT_ATOMS = _resolve_config_path(_dataset_config["event_atoms_csv"])
    RAW_FILES = {
        key: _resolve_config_path(_dataset_config["raw_files"][key])
        for key in ("flashloans", "borrow", "dex")
    }
    COMBINED_RAW = RAW_FILES["flashloans"].parent
else:
    DATASET_NAME = "twoweek"
    WORK_ROOT = ROOT / "data/week2_build_work/fair_cost_twoweek"
    COMBINED_EVENT_ATOMS = ROOT / "data/week2_build_work/combined_2weeks/event_atoms.csv"
    COMBINED_RAW = ROOT / "data/week2_build_work/combined_raw_mapped"
    RAW_FILES = {
        "flashloans": COMBINED_RAW / "flashloans.csv",
        "borrow": COMBINED_RAW / "borrow.csv",
        "dex": COMBINED_RAW / "dex.csv",
    }

SAME_TX_QUERIES = [
    "flashloan_swap_borrow",
    "flashloan_swap_borrow_followup",
    "flashloan_2swaps_distinct_followup",
    "flashloan_3swaps_chain_reuse",
    "flashloan_3swaps_distinct_parallel",
]
EPISODE_QUERIES = [
    "flashloan_swap_borrow_episode_v1",
    "flashloan_swap_borrow_followup_episode_v1",
    "flashloan_2swaps_distinct_followup_episode_v1",
    "flashloan_3swaps_chain_reuse_episode_v1",
    "flashloan_3swaps_distinct_parallel_episode_v1",
]
EPISODE_HGMATCH_QUERIES = {
    "flashloan_swap_borrow_episode_v1": "flashloan_swap_borrow_anchored_episode_hgmatch",
    "flashloan_swap_borrow_followup_episode_v1": "flashloan_swap_borrow_followup_anchored_episode_hgmatch",
    "flashloan_2swaps_distinct_followup_episode_v1": "flashloan_2swaps_distinct_followup_anchored_episode_hgmatch",
    "flashloan_3swaps_chain_reuse_episode_v1": "flashloan_3swaps_chain_reuse_anchored_episode_hgmatch",
    "flashloan_3swaps_distinct_parallel_episode_v1": "flashloan_3swaps_distinct_parallel_anchored_episode_hgmatch",
}


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def first_existing(paths: Iterable[Path]) -> Path:
    checked: List[str] = []
    for path in paths:
        checked.append(str(path))
        if path.exists():
            return path
    raise FileNotFoundError("Could not find any of these paths:\n" + "\n".join(checked))


def find_monitor() -> Path:
    return first_existing([
        ROOT / "common/monitor_process_windows.py",
        ROOT / "monitor_process_windows.py",
        ROOT / "scripts/monitor_process_windows.py",
    ])


def next_numbered_dir(parent: Path, prefix: str) -> Path:
    ensure_dir(parent)
    for i in range(1, 1000):
        path = parent / f"{prefix}_{i:03d}"
        if not path.exists() or (path.is_dir() and not any(path.iterdir())):
            return path
    raise RuntimeError(f"No available directory for {parent / (prefix + '_XXX')}")


def latest_stores_dir() -> Path:
    marker = WORK_ROOT / "LATEST_STORES_DIR.txt"
    if marker.exists():
        p = Path(marker.read_text(encoding="utf-8").strip())
        if p.exists():
            return p
    candidates = list(WORK_ROOT.glob("stores_*"))
    runs_dir = ROOT / "runs"
    if runs_dir.exists():
        candidates.extend(runs_dir.glob("stores_*"))
    candidates = sorted(candidates)
    if not candidates:
        raise FileNotFoundError("No fair-cost stores found. Run prepare_twoweek_fair_stores.py first.")
    return candidates[-1]


def store_paths(stores_dir: Path) -> Dict[str, Path]:
    return {
        "hvceq_same_tx_db": stores_dir / "hvceq_same_tx/same_tx_runtime.db",
        "hvceq_episode_db": stores_dir / "hvceq_episode/anchored_episode_runtime_sidecar.db",
        "sql_same_tx_db": stores_dir / "sql_same_tx/same_tx_relational_flat.db",
        "sql_episode_db": stores_dir / "sql_episode/anchored_episode_relational_flat.db",
        "hgmatch_same_tx_db": stores_dir / "hgmatch_same_tx/hgmatch_same_tx_rust_fair.db",
        "hgmatch_episode_db": stores_dir / "hgmatch_episode/hgmatch_episode_rust_fair.db",
    }


def run_monitored(
    child_cmd: List[str],
    outdir: Path,
    cwd: Path = ROOT,
    timeout_sec: Optional[int] = None,
    max_rss_mb: Optional[float] = None,
    phase_marker_path: Optional[Path] = None,
) -> Dict[str, Any]:
    ensure_dir(outdir)
    stdout_path = outdir / "stdout.txt"
    stderr_path = outdir / "stderr.txt"
    monitor_report_path = outdir / "monitor_report.json"
    command_path = outdir / "command.json"
    monitor = find_monitor()
    cmd = [
        sys.executable,
        str(monitor),
        "--outdir",
        str(outdir),
    ]
    if max_rss_mb is not None:
        cmd.extend(["--job-memory-limit-mb", str(max_rss_mb)])
    system_floor_mb = float(os.environ.get("HVCEQ_MIN_SYSTEM_AVAILABLE_MB", "4096"))
    cmd.extend(["--min-system-available-mb", str(system_floor_mb)])
    if timeout_sec is not None:
        cmd.extend(["--timeout-sec", str(timeout_sec)])
    if phase_marker_path is not None:
        cmd.extend(["--phase-marker", str(phase_marker_path)])
    cmd.extend([
        "--",
        *child_cmd,
    ])
    command = {
        "cmd": child_cmd,
        "monitor_cmd": cmd,
        "monitor_script": str(monitor),
        "cwd": str(cwd),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "monitor_report_path": str(monitor_report_path),
    }
    write_json(command_path, command)
    t0 = time.perf_counter()
    try:
        # The monitor owns the Job handle and enforces timeout by terminating
        # the complete Job. Do not externally kill only the monitor process.
        proc = subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True)
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        stdout_path.write_text(exc.stdout or "", encoding="utf-8")
        stderr_path.write_text((exc.stderr or "") + f"\nTIMEOUT after {timeout_sec}s\n", encoding="utf-8")
        return {
            "status": "timeout",
            "return_code": "timeout",
            "wall_time_s": time.perf_counter() - t0,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "monitor_report_path": str(monitor_report_path),
            "command_path": str(command_path),
        }
    stdout_path.write_text(proc.stdout or "", encoding="utf-8")
    stderr_path.write_text(proc.stderr or "", encoding="utf-8")
    wall = time.perf_counter() - t0
    monitor = read_json(monitor_report_path) if monitor_report_path.exists() else {}
    return_code = monitor.get("return_code", proc.returncode)
    stop_reason = str(monitor.get("stop_reason", ""))
    memory_stop = stop_reason in {
        "system_available_below_floor",
        "target_failed_or_job_memory_limit",
    }
    status = (
        "timeout" if stop_reason == "timeout"
        else "memory_limit" if memory_stop
        else "ok" if int(return_code) == 0
        else "failed"
    )
    return {
        "status": status,
        "return_code": return_code,
        "job_commit_hard_limit_mb": max_rss_mb if max_rss_mb is not None else "",
        "system_available_floor_mb": system_floor_mb,
        "monitor_stop_reason": stop_reason,
        "wall_time_s": monitor.get("wall_s_external_monitor", wall),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "monitor_report_path": str(monitor_report_path),
        "command_path": str(command_path),
    }


def memory_from_monitor(
    path: Path | str, query_phase_only: bool = False
) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {
            "peak_job_commit_mb": "",
            "peak_tree_working_set_mb": "",
            "peak_tree_private_bytes_mb": "",
            "minimum_system_available_mb": "",
            "monitor_wall_time_s": "",
        }
    report = read_json(p)
    if query_phase_only:
        phase = report.get("query_phase", {})
        return {
            "peak_job_commit_mb": phase.get("peak_job_commit_mb", ""),
            "peak_tree_working_set_mb": phase.get(
                "peak_tree_working_set_mb", ""
            ),
            "peak_tree_private_bytes_mb": phase.get(
                "peak_tree_private_bytes_mb", ""
            ),
            "minimum_system_available_mb": phase.get(
                "minimum_system_available_mb", ""
            ),
            "monitor_wall_time_s": report.get("wall_s_external_monitor", ""),
            "query_phase_monitor_samples": phase.get("sample_count", ""),
            "lifetime_peak_job_commit_mb": report.get("job_commit", {}).get(
                "peak_job_commit_mb", ""
            ),
        }
    return {
        "peak_job_commit_mb": report.get("job_commit", {}).get("peak_job_commit_mb", ""),
        "peak_tree_working_set_mb": report.get("process_tree_working_set", {}).get("peak_tree_working_set_mb", ""),
        "peak_tree_private_bytes_mb": report.get("process_tree_private_bytes", {}).get("peak_tree_private_bytes_mb", ""),
        "minimum_system_available_mb": report.get("system_memory", {}).get("minimum_available_mb", ""),
        "monitor_wall_time_s": report.get("wall_s_external_monitor", ""),
    }


def query_json(mode: str, system: str, qname: str) -> Path:
    if mode == "same_tx":
        return first_existing([
            ROOT / "queries/same_tx" / f"{qname}.json",
            ROOT / "queries" / f"{qname}.json",
        ])
    if system == "hgmatch":
        mapped = EPISODE_HGMATCH_QUERIES.get(qname, qname)
        return first_existing([
            ROOT / "hyper_episode/queries" / f"{mapped}.json",
            ROOT / "queries/episode/hgmatch" / f"{mapped}.json",
            ROOT / "queries/episode/hvceq_sql" / f"{qname}.json",
            ROOT / "queries/episode" / f"{qname}.json",
        ])
    return first_existing([
        ROOT / "queries/episode/hvceq_sql" / f"{qname}.json",
        ROOT / "queries/episode" / f"{qname}.json",
        ROOT / "hyper_episode/queries" / f"{qname}.json",
    ])


def materialize_hgmatch_episode_query_json(source: Path, outdir: Path) -> Path:
    # Current HGMatch runner accepts the same anchored-episode JSON used by
    # HVCEQ/SQL. Keep this hook so run_episode_window_sensitivity.py can call a
    # stable adapter even if a future HGMatch-specific query name is needed.
    return source


def query_script_cmd(mode: str, system: str, stores_dir: Path, qname: str, outdir: Path, runs: int = 1, threads: int = 8) -> List[str]:
    paths = store_paths(stores_dir)
    qpath = query_json(mode, system, qname)
    if mode == "same_tx" and system == "hvceq":
        return [
            sys.executable,
            str(ROOT / "systems/hvceq/same_tx/query_same_tx_layered_v3.py"),
            "--db",
            str(paths["hvceq_same_tx_db"]),
            "--query-json",
            str(qpath),
            "--outdir",
            str(outdir),
            "--sample-limit",
            "0",
            "--ablation",
            "full",
        ]
    if mode == "same_tx" and system == "sql":
        return [
            sys.executable,
            str(ROOT / "systems/sql/same_tx/query_same_tx_summary_reuse_indexes.py"),
            "--db",
            str(paths["sql_same_tx_db"]),
            "--query-json",
            str(qpath),
            "--outdir",
            str(outdir),
            "--runs",
            str(runs),
            "--reuse-prebuilt-indexes",
            "--sample-limit",
            "0",
        ]
    if mode == "same_tx" and system == "hgmatch":
        return [
            str(first_existing([
                ROOT / "systems/hgmatch/same_tx/source/target/release/query_hgmatch_same_tx_rust_fair.exe",
                ROOT / "systems/hgmatch/same_tx/bin/query_hgmatch_same_tx_rust_fair.exe",
                ROOT / "hgmatch_same_tx_rust_fair_taskflow_fixed/target/release/query_hgmatch_same_tx_rust_fair.exe",
            ])),
            "--db",
            str(paths["hgmatch_same_tx_db"]),
            "--query-json",
            str(qpath),
            "--outdir",
            str(outdir),
            "--threads",
            str(threads),
            "--summary-only",
        ]
    if mode == "episode" and system == "hvceq":
        return [
            sys.executable,
            str(ROOT / "systems/hvceq/episode/query_episode_layered_full.py"),
            "--episode-sidecar-db",
            str(paths["hvceq_episode_db"]),
            "--query-json",
            str(qpath),
            "--outdir",
            str(outdir),
        ]
    if mode == "episode" and system == "sql":
        return [
            sys.executable,
            str(ROOT / "systems/sql/episode/query_episode_summary_reuse_indexes.py"),
            "--db",
            str(paths["sql_episode_db"]),
            "--query-json",
            str(qpath),
            "--outdir",
            str(outdir),
            "--runs",
            str(runs),
            "--reuse-prebuilt-indexes",
            "--sample-limit",
            "0",
        ]
    if mode == "episode" and system == "hgmatch":
        return [
            str(first_existing([
                ROOT / "systems/hgmatch/episode/source/target/release/query_hgmatch_same_tx_rust_fair.exe",
                ROOT / "systems/hgmatch/episode/bin/query_hgmatch_same_tx_rust_fair.exe",
                ROOT / "hyper_episode/hgmatch_same_tx_rust_fair_taskflow_fixed/target/release/query_hgmatch_same_tx_rust_fair.exe",
            ])),
            "--db",
            str(paths["hgmatch_episode_db"]),
            "--query-json",
            str(qpath),
            "--outdir",
            str(outdir),
            "--threads",
            str(threads),
            "--summary-only",
        ]
    raise ValueError(f"Unsupported mode/system: {mode}/{system}")


def parse_query_output(mode: str, system: str, outdir: Path) -> Dict[str, Any]:
    if system == "hvceq":
        report_path = outdir / "query_result.json"
    else:
        report_path = outdir / "execution_report.json"
    report = read_json(report_path) if report_path.exists() else {}
    if system == "sql":
        timing = report.get("timing_s", {})
        reported = timing.get("mean_main_query_s", timing.get("main_count_query_s", ""))
        total = timing.get("mean_total_s", timing.get("total_s", ""))
        residual_time = timing.get("mean_residual_count_s", "")
    else:
        reported = report.get("metrics", {}).get("query_latency_s", "")
        total = report.get("metrics", {}).get("query_wall_time_s", "")
        residual_time = ""
    if mode == "episode":
        residual = report.get("num_residual_episodes", report.get("num_result_episodes", report.get("num_residual_closures", "")))
    else:
        residual = report.get("num_residual_closures", "")
    return {
        "report_path": str(report_path),
        "query_name_reported": report.get("query_name", ""),
        "num_matches": report.get("num_matches", ""),
        "num_result_units": report.get("num_result_units", report.get("num_result_txs", report.get("num_result_episodes", ""))),
        "num_residual_units": residual,
        "diagnostic_residual_units": residual,
        "reported_query_time_s": reported,
        "aux_total_or_wall_time_s": total,
        "sql_residual_count_time_s": residual_time,
        "num_cand0": report.get("num_cand0", ""),
        "num_cand1": report.get("num_cand1", report.get("num_cand1_closures", "")),
        "num_cand2": report.get("num_cand2", report.get("num_cand2_closures", "")),
    }


def metric_stats(values: List[float], prefix: str) -> Dict[str, Any]:
    if not values:
        return {
            f"{prefix}_mean": "",
            f"{prefix}_std": "",
            f"{prefix}_min": "",
            f"{prefix}_max": "",
            f"{prefix}_median": "",
        }
    return {
        f"{prefix}_mean": statistics.mean(values),
        f"{prefix}_std": statistics.stdev(values) if len(values) >= 2 else 0.0,
        f"{prefix}_min": min(values),
        f"{prefix}_max": max(values),
        f"{prefix}_median": statistics.median(values),
    }


def to_float(value: Any) -> Optional[float]:
    try:
        if value == "" or value is None:
            return None
        return float(value)
    except Exception:
        return None


def aggregate_query_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keys = sorted({(r["mode"], r["system"], r["query_name"]) for r in rows})
    out: List[Dict[str, Any]] = []
    for mode, system, query_name in keys:
        group = [r for r in rows if (r["mode"], r["system"], r["query_name"]) == (mode, system, query_name)]
        ok = [r for r in group if r.get("status") == "ok"]
        lat = [x for x in (to_float(r.get("reported_query_time_s")) for r in ok) if x is not None]
        rss = [x for x in (to_float(r.get("peak_rss_mb")) for r in group) if x is not None]
        row = {
            "mode": mode,
            "system": system,
            "query_name": query_name,
            "runs": len(group),
            "successful_runs": len(ok),
            "num_matches_consistent": len({r.get("num_matches") for r in ok}) == 1 if ok else False,
            "num_residual_units_consistent": len({r.get("num_residual_units") for r in ok}) == 1 if ok else False,
        }
        row.update(metric_stats(lat, "reported_query_time_s"))
        row.update(metric_stats(rss, "peak_rss_mb"))
        out.append(row)
    return out


def build_alignment(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for mode, qnames in [("same_tx", SAME_TX_QUERIES), ("episode", EPISODE_QUERIES)]:
        for qname in qnames:
            group = [r for r in rows if r["mode"] == mode and r["query_name"] == qname and r.get("status") == "ok"]
            systems = sorted({r["system"] for r in group})
            matches_by_system = {s: sorted({str(r.get("num_matches")) for r in group if r["system"] == s}) for s in systems}
            residual_by_system = {s: sorted({str(r.get("num_residual_units")) for r in group if r["system"] == s}) for s in systems}
            out.append({
                "mode": mode,
                "query_name": qname,
                "systems_completed": ",".join(systems),
                "num_matches_by_system": json.dumps(matches_by_system, ensure_ascii=False),
                "num_residual_units_by_system": json.dumps(residual_by_system, ensure_ascii=False),
                "matches_aligned": len(systems) == 3 and len({tuple(v) for v in matches_by_system.values()}) == 1,
                "residual_units_aligned": len(systems) == 3 and len({tuple(v) for v in residual_by_system.values()}) == 1,
            })
    return out


def copy_if_needed(src: Path, dst: Path) -> None:
    ensure_dir(dst.parent)
    if dst.exists():
        raise FileExistsError(f"Refusing to overwrite existing file: {dst}")
    shutil.copy2(src, dst)
