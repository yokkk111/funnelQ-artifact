#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parents[1]
WORK_ROOT = ROOT / "data" / "week2_build_work" / "fair_cost_twoweek"

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

SAME_TX_COLUMNS = [
    "cost_scope",
    "mode",
    "system",
    "system_key",
    "query_name",
    "run_id",
    "stores_reused",
    "query_specific_indexes_reused",
    "full_result_materialized",
    "sample_limit",
    "status",
    "return_code",
    "wall_time_s",
    "stdout_path",
    "stderr_path",
    "monitor_report_path",
    "command_path",
    "output_dir",
    "report_path",
    "query_name_reported",
    "num_matches",
    "num_result_units",
    "diagnostic_residual_units",
    "reported_query_time_s",
    "aux_total_or_wall_time_s",
    "sql_residual_count_time_s",
    "num_cand0",
    "num_cand1",
    "num_cand2",
    "peak_rss_mb",
    "peak_wset_mb",
    "peak_private_mb",
    "monitor_wall_time_s",
]

EPISODE_COLUMNS = [
    "cost_scope",
    "window_sec",
    "mode",
    "system",
    "system_key",
    "query_name",
    "run_id",
    "stores_reused",
    "query_specific_indexes_reused",
    "full_result_materialized",
    "sample_limit",
    "status",
    "return_code",
    "wall_time_s",
    "stdout_path",
    "stderr_path",
    "monitor_report_path",
    "command_path",
    "output_dir",
    "report_path",
    "query_name_reported",
    "num_matches",
    "num_result_units",
    "diagnostic_residual_units",
    "reported_query_time_s",
    "aux_total_or_wall_time_s",
    "sql_residual_count_time_s",
    "num_cand0",
    "num_cand1",
    "num_cand2",
    "peak_rss_mb",
    "peak_wset_mb",
    "peak_private_mb",
    "monitor_wall_time_s",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: List[Dict[str, Any]], columns: List[str]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in columns})


def find_monitor() -> Optional[Path]:
    candidates = [
        ROOT / "common" / "monitor_process_windows.py",
        ROOT / "monitor_process_windows.py",
        ROOT / "scripts" / "monitor_process_windows.py",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def stderr_tail(path: str | Path, max_chars: int = 1600) -> str:
    p = Path(path)
    if not p.exists():
        return ""
    text = p.read_text(encoding="utf-8", errors="replace")
    return text[-max_chars:].strip()


def next_numbered_dir(parent: Path, prefix: str) -> Path:
    ensure_dir(parent)
    for i in range(1, 10000):
        path = parent / f"{prefix}_{i:03d}"
        if not path.exists():
            return path
    raise RuntimeError(f"No available output directory under {parent}")


def parse_windows(value: str) -> List[int]:
    out: List[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        window = int(part)
        if window <= 0:
            raise ValueError("Window values must be positive.")
        out.append(window)
    if not out:
        raise ValueError("At least one window is required.")
    return out


def _has_hvceq_stores(path: Path) -> bool:
    return (
        (path / "hvceq_same_tx" / "same_tx_runtime.db").exists()
        and (path / "hvceq_episode" / "anchored_episode_runtime_sidecar.db").exists()
    )


def latest_stores_dir() -> Path:
    marker = WORK_ROOT / "LATEST_STORES_DIR.txt"
    if marker.exists():
        candidate = Path(marker.read_text(encoding="utf-8").strip())
        if _has_hvceq_stores(candidate):
            return candidate

    candidates: List[Path] = []
    search_roots = [WORK_ROOT, ROOT / "runs", ROOT / "data"]
    for base in search_roots:
        if not base.exists():
            continue
        for db in base.rglob("hvceq_same_tx/same_tx_runtime.db"):
            parent = db.parents[1]
            if _has_hvceq_stores(parent):
                candidates.append(parent)

    if not candidates:
        raise FileNotFoundError(
            "Could not find HVCEQ stores. Pass --stores-dir, or run the store preparation step first."
        )
    candidates = sorted(set(candidates), key=lambda p: p.stat().st_mtime)
    return candidates[-1]


def validate_stores(stores_dir: Path) -> Dict[str, Path]:
    paths = {
        "same_tx_db": stores_dir / "hvceq_same_tx" / "same_tx_runtime.db",
        "episode_db": stores_dir / "hvceq_episode" / "anchored_episode_runtime_sidecar.db",
    }
    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        raise FileNotFoundError("Missing required HVCEQ store files:\n" + "\n".join(missing))
    return paths


def first_existing(candidates: Iterable[Path]) -> Path:
    checked: List[str] = []
    for path in candidates:
        checked.append(str(path))
        if path.exists():
            return path
    raise FileNotFoundError("Could not find query JSON. Checked:\n" + "\n".join(checked))


def same_tx_query_json(qname: str) -> Path:
    return first_existing([
        ROOT / "queries" / "same_tx" / f"{qname}.json",
        ROOT / "queries" / f"{qname}.json",
    ])


def episode_query_json(qname: str) -> Path:
    return first_existing([
        ROOT / "queries" / "episode" / "hvceq_sql" / f"{qname}.json",
        ROOT / "queries" / "episode" / f"{qname}.json",
        ROOT / "hyper_episode" / "queries" / f"{qname}.json",
    ])


def materialize_episode_query(qname: str, window_sec: int, outdir: Path) -> Path:
    source = episode_query_json(qname)
    query = read_json(source)
    scope_params = dict(query.get("scope_params", {}))
    scope_params["forward_window_sec"] = int(window_sec)
    query["scope_params"] = scope_params
    effective = outdir / "effective_query.json"
    write_json(effective, query)
    return effective


def run_monitored(child_cmd: List[str], outdir: Path, timeout_sec: Optional[int]) -> Dict[str, Any]:
    ensure_dir(outdir)
    stdout_path = outdir / "stdout.txt"
    stderr_path = outdir / "stderr.txt"
    monitor_report_path = outdir / "monitor_report.json"
    command_path = outdir / "command.json"

    monitor = find_monitor()
    cmd = child_cmd if monitor is None else [
        sys.executable,
        str(monitor),
        "--outdir",
        str(outdir),
        "--",
        *child_cmd,
    ]
    write_json(command_path, {
        "cmd": child_cmd,
        "monitor_cmd": cmd,
        "monitor_script": str(monitor) if monitor is not None else "",
        "cwd": str(ROOT),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "monitor_report_path": str(monitor_report_path),
    })

    t0 = time.perf_counter()
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired as exc:
        stdout_path.write_text(exc.stdout or "", encoding="utf-8")
        stderr_path.write_text((exc.stderr or "") + f"\nTIMEOUT after {timeout_sec}s\n", encoding="utf-8")
        return {
            "status": "timeout",
            "return_code": "timeout",
            "wall_time_s": round(time.perf_counter() - t0, 6),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "monitor_report_path": str(monitor_report_path),
            "command_path": str(command_path),
        }

    stdout_path.write_text(proc.stdout or "", encoding="utf-8")
    stderr_path.write_text(proc.stderr or "", encoding="utf-8")
    wall = time.perf_counter() - t0
    if monitor is None:
        write_json(monitor_report_path, {
            "return_code": proc.returncode,
            "wall_s": round(wall, 6),
            "memory": {
                "peak_rss_mb": "",
                "peak_wset_mb": "",
                "peak_private_mb": "",
            },
            "note": "monitor_process_windows.py not found; memory fields are unavailable.",
        })
    monitor = read_json(monitor_report_path) if monitor_report_path.exists() else {}
    return_code = monitor.get("return_code", proc.returncode)
    try:
        ok = int(return_code) == 0
    except Exception:
        ok = False
    return {
        "status": "ok" if ok else "failed",
        "return_code": return_code,
        "wall_time_s": monitor.get("wall_s", monitor.get("wall_time_s", round(wall, 6))),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "monitor_report_path": str(monitor_report_path),
        "command_path": str(command_path),
    }


def memory_from_monitor(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {
            "peak_rss_mb": "",
            "peak_wset_mb": "",
            "peak_private_mb": "",
            "monitor_wall_time_s": "",
        }
    report = read_json(p)
    memory = report.get("memory", {})
    if not isinstance(memory, dict):
        memory = {}
    return {
        "peak_rss_mb": memory.get("peak_rss_mb", report.get("peak_rss_mb", "")),
        "peak_wset_mb": memory.get("peak_wset_mb", report.get("peak_wset_mb", "")),
        "peak_private_mb": memory.get("peak_private_mb", report.get("peak_private_mb", "")),
        "monitor_wall_time_s": report.get("wall_s", report.get("wall_time_s", "")),
    }


def parse_hvceq_report(outdir: Path, mode: str) -> Dict[str, Any]:
    report_path = outdir / "query_result.json"
    report = read_json(report_path) if report_path.exists() else {}
    metrics = report.get("metrics", {})
    if not isinstance(metrics, dict):
        metrics = {}
    if mode == "episode":
        diagnostic_residual = report.get(
            "num_residual_episodes",
            report.get("num_residual_closures", ""),
        )
    else:
        diagnostic_residual = report.get("num_residual_closures", "")
    return {
        "report_path": str(report_path),
        "query_name_reported": report.get("query_name", ""),
        "num_matches": report.get("num_matches", ""),
        "num_result_units": report.get(
            "num_result_units",
            report.get("num_result_episodes", report.get("num_result_txs", "")),
        ),
        "diagnostic_residual_units": diagnostic_residual,
        "reported_query_time_s": metrics.get("query_latency_s", ""),
        "aux_total_or_wall_time_s": metrics.get("query_wall_time_s", ""),
        "sql_residual_count_time_s": "",
        "num_cand0": report.get("num_cand0", ""),
        "num_cand1": report.get("num_cand1", report.get("num_cand1_closures", "")),
        "num_cand2": report.get("num_cand2", report.get("num_cand2_closures", "")),
    }


def same_tx_cmd(stores: Dict[str, Path], qname: str, outdir: Path) -> List[str]:
    return [
        sys.executable,
        str(ROOT / "systems" / "hvceq" / "same_tx" / "query_same_tx_layered_v3.py"),
        "--db",
        str(stores["same_tx_db"]),
        "--query-json",
        str(same_tx_query_json(qname)),
        "--outdir",
        str(outdir),
        "--sample-limit",
        "0",
        "--ablation",
        "full",
    ]


def episode_cmd(stores: Dict[str, Path], qname: str, window_sec: int, outdir: Path) -> List[str]:
    effective_query = materialize_episode_query(qname, window_sec, outdir)
    return [
        sys.executable,
        str(ROOT / "systems" / "hvceq" / "episode" / "query_episode_layered_full.py"),
        "--episode-sidecar-db",
        str(stores["episode_db"]),
        "--query-json",
        str(effective_query),
        "--outdir",
        str(outdir),
    ]


def common_row(
    *,
    mode: str,
    qname: str,
    run_id: int,
    outdir: Path,
    meta: Dict[str, Any],
    report: Dict[str, Any],
) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "cost_scope": "same_tx_fair_cost_query_cost" if mode == "same_tx" else "episode_window_sensitivity_query_cost",
        "mode": mode,
        "system": "HVCEQ",
        "system_key": "hvceq",
        "query_name": qname,
        "run_id": run_id,
        "stores_reused": True,
        "query_specific_indexes_reused": True,
        "full_result_materialized": False,
        "sample_limit": 0,
        "output_dir": str(outdir),
    }
    row.update(meta)
    row.update(report)
    row.update(memory_from_monitor(meta.get("monitor_report_path", "")))
    return row


def run_same_tx(stores: Dict[str, Path], run_dir: Path, runs: int, timeout_sec: Optional[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for run_id in range(1, runs + 1):
        for qname in SAME_TX_QUERIES:
            outdir = run_dir / "same_tx" / f"run_{run_id:02d}" / qname
            print(f"[hvceq-layered-full][same_tx][run {run_id}/{runs}][{qname}] start", flush=True)
            meta = run_monitored(same_tx_cmd(stores, qname, outdir), outdir, timeout_sec)
            report = parse_hvceq_report(outdir, "same_tx")
            rows.append(common_row(mode="same_tx", qname=qname, run_id=run_id, outdir=outdir, meta=meta, report=report))
            print(
                f"[hvceq-layered-full][same_tx][run {run_id}/{runs}][{qname}] "
                f"done status={meta.get('status')} matches={report.get('num_matches')} "
                f"time={report.get('reported_query_time_s')}",
                flush=True,
            )
            if meta.get("status") != "ok":
                tail = stderr_tail(meta.get("stderr_path", ""))
                if tail:
                    print(f"[hvceq-layered-full][same_tx][{qname}] stderr tail:\n{tail}", flush=True)
    return rows


def run_episode(
    stores: Dict[str, Path],
    run_dir: Path,
    runs: int,
    windows: List[int],
    timeout_sec: Optional[int],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for run_id in range(1, runs + 1):
        for window_sec in windows:
            for qname in EPISODE_QUERIES:
                outdir = run_dir / "episode" / f"window_{window_sec}" / f"run_{run_id:02d}" / qname
                print(
                    f"[hvceq-layered-full][episode][window {window_sec}s][run {run_id}/{runs}][{qname}] start",
                    flush=True,
                )
                meta = run_monitored(episode_cmd(stores, qname, window_sec, outdir), outdir, timeout_sec)
                report = parse_hvceq_report(outdir, "episode")
                row = common_row(mode="episode", qname=qname, run_id=run_id, outdir=outdir, meta=meta, report=report)
                row["window_sec"] = window_sec
                rows.append(row)
                print(
                    f"[hvceq-layered-full][episode][window {window_sec}s][run {run_id}/{runs}][{qname}] "
                    f"done status={meta.get('status')} matches={report.get('num_matches')} "
                    f"time={report.get('reported_query_time_s')}",
                    flush=True,
                )
                if meta.get("status") != "ok":
                    tail = stderr_tail(meta.get("stderr_path", ""))
                    if tail:
                        print(f"[hvceq-layered-full][episode][{qname}] stderr tail:\n{tail}", flush=True)
    return rows


def copy_to_plot_if_requested(run_dir: Path, same_csv: Path, episode_csv: Path, enabled: bool) -> None:
    if not enabled:
        return
    plot_dir = ROOT / "plot"
    ensure_dir(plot_dir)
    shutil.copy2(same_csv, plot_dir / same_csv.name)
    shutil.copy2(episode_csv, plot_dir / episode_csv.name)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Run only HVCEQ layered Full same_tx and episode queries, then write CSVs matching the final all-runs schemas."
    )
    ap.add_argument("--stores-dir", default="", help="Prepared stores directory. Default: auto-detect latest.")
    ap.add_argument("--outdir", default="", help="Output directory. Default: runs/hvceq_layered_full_only_XXX.")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--windows", default="300", help="Comma-separated episode windows. Use 30,60,120,180,300,600 for full sensitivity.")
    ap.add_argument("--skip-same-tx", action="store_true")
    ap.add_argument("--skip-episode", action="store_true")
    ap.add_argument("--timeout-sec", type=int, default=None)
    ap.add_argument("--copy-to-plot", action="store_true", help="Also copy the two output CSVs to plot/.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.skip_same_tx and args.skip_episode:
        raise SystemExit("Both --skip-same-tx and --skip-episode were set; nothing to run.")

    stores_dir = Path(args.stores_dir).resolve() if args.stores_dir else latest_stores_dir()
    stores = validate_stores(stores_dir)
    run_dir = Path(args.outdir).resolve() if args.outdir else next_numbered_dir(ROOT / "runs", "hvceq_layered_full_only")
    ensure_dir(run_dir)

    runs = max(1, int(args.runs))
    windows = parse_windows(args.windows)
    print(f"[hvceq-layered-full] stores={stores_dir}", flush=True)
    print(f"[hvceq-layered-full] outdir={run_dir}", flush=True)

    same_rows: List[Dict[str, Any]] = []
    episode_rows: List[Dict[str, Any]] = []
    if not args.skip_same_tx:
        same_rows = run_same_tx(stores, run_dir, runs, args.timeout_sec)
    if not args.skip_episode:
        episode_rows = run_episode(stores, run_dir, runs, windows, args.timeout_sec)

    same_csv = run_dir / "hvceq_layered_full_same_tx_all_runs.csv"
    episode_csv = run_dir / "hvceq_layered_full_episode_window_sensitivity_all_runs.csv"
    write_csv(same_csv, same_rows, SAME_TX_COLUMNS)
    write_csv(episode_csv, episode_rows, EPISODE_COLUMNS)
    write_json(run_dir / "hvceq_layered_full_only_summary.json", {
        "stores_dir": str(stores_dir),
        "runs": runs,
        "windows": windows,
        "same_tx_csv": str(same_csv),
        "episode_csv": str(episode_csv),
        "same_tx_rows": len(same_rows),
        "episode_rows": len(episode_rows),
    })
    copy_to_plot_if_requested(run_dir, same_csv, episode_csv, args.copy_to_plot)

    print(f"[hvceq-layered-full] wrote same_tx csv: {same_csv}", flush=True)
    print(f"[hvceq-layered-full] wrote episode csv: {episode_csv}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
