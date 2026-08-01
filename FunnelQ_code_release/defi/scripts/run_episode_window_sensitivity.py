#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from fair_cost_runner_lib import (
    EPISODE_QUERIES,
    ROOT,
    WORK_ROOT,
    first_existing,
    latest_stores_dir,
    materialize_hgmatch_episode_query_json,
    memory_from_monitor,
    next_numbered_dir,
    parse_query_output,
    query_json,
    read_json,
    run_monitored,
    store_paths,
    write_csv,
    write_json,
)


SYSTEMS = ["hvceq", "sql", "hgmatch"]
SYSTEM_LABELS = {
    "hvceq": "HVCEQ",
    "sql": "Generic SQL",
    "hgmatch": "HGMatch",
}


def parse_windows(value: str) -> List[int]:
    out: List[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        window = int(part)
        if window <= 0:
            raise ValueError("Episode window values must be positive.")
        out.append(window)
    if not out:
        raise ValueError("At least one episode window is required.")
    return out


def effective_episode_query_json(qname: str, window_sec: int, outdir: Path, system: str) -> Path:
    source = query_json("episode", system, qname)
    query = read_json(source)
    scope_params = dict(query.get("scope_params", {}))
    scope_params["forward_window_sec"] = int(window_sec)
    query["scope_params"] = scope_params
    effective = outdir / f"{qname}_window_{window_sec}.json"
    write_json(effective, query)
    if system == "hgmatch":
        return materialize_hgmatch_episode_query_json(effective, outdir)
    return effective


def episode_query_cmd(system: str, stores_dir: Path, qname: str, window_sec: int, outdir: Path, threads: int) -> List[str]:
    paths = store_paths(stores_dir)
    qpath = effective_episode_query_json(qname, window_sec, outdir, system)
    if system == "hvceq":
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
    if system == "sql":
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
            "1",
            "--reuse-prebuilt-indexes",
            "--sample-limit",
            "0",
        ]
    if system == "hgmatch":
        return [
            str(first_existing([
                ROOT / "systems/hgmatch/episode/source/target/release/query_hgmatch_same_tx_rust_fair.exe",
                ROOT / "systems/hgmatch/episode/bin/query_hgmatch_same_tx_rust_fair.exe",
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
    raise ValueError(f"Unsupported system: {system}")


def to_float(value: Any) -> Optional[float]:
    try:
        if value == "" or value is None:
            return None
        return float(value)
    except Exception:
        return None


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


def aggregate(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keys = sorted({(r["window_sec"], r["query_name"], r["system"]) for r in rows})
    out: List[Dict[str, Any]] = []
    for window_sec, qname, system in keys:
        group = [r for r in rows if (r["window_sec"], r["query_name"], r["system"]) == (window_sec, qname, system)]
        ok = [r for r in group if r.get("status") == "ok"]
        lat = [x for x in (to_float(r.get("reported_query_time_s")) for r in ok) if x is not None]
        rss = [x for x in (to_float(r.get("peak_rss_mb")) for r in group) if x is not None]
        out_row = {
            "window_sec": window_sec,
            "mode": "episode",
            "query_name": qname,
            "system": system,
            "runs": len(group),
            "successful_runs": len(ok),
            "num_matches_consistent": len({r.get("num_matches") for r in ok}) == 1 if ok else False,
            "num_matches": next(iter({r.get("num_matches") for r in ok}), "") if ok else "",
            "num_result_units": next(iter({r.get("num_result_units") for r in ok}), "") if ok else "",
        }
        out_row.update(metric_stats(lat, "reported_query_time_s"))
        out_row.update(metric_stats(rss, "peak_rss_mb"))
        out.append(out_row)
    return out


def alignment(rows: List[Dict[str, Any]], windows: List[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for window_sec in windows:
        for qname in EPISODE_QUERIES:
            group = [
                r for r in rows
                if r.get("window_sec") == window_sec and r.get("query_name") == qname and r.get("status") == "ok"
            ]
            systems = sorted({r["system"] for r in group})
            matches_by_system = {s: sorted({str(r.get("num_matches")) for r in group if r["system"] == s}) for s in systems}
            out.append({
                "window_sec": window_sec,
                "mode": "episode",
                "query_name": qname,
                "systems_completed": ",".join(systems),
                "num_matches_by_system": json.dumps(matches_by_system, ensure_ascii=False),
                "matches_aligned": len(systems) == 3 and len({tuple(v) for v in matches_by_system.values()}) == 1,
                "alignment_basis": "num_matches_only",
            })
    return out


def _fmt_float(value: Any, digits: int = 6) -> str:
    try:
        if value == "" or value is None:
            return "-"
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value) if value not in ("", None) else "-"


def _fmt_int(value: Any) -> str:
    try:
        if value == "" or value is None:
            return "-"
        return str(int(float(value)))
    except Exception:
        return str(value) if value not in ("", None) else "-"


def print_final_summary(stats: List[Dict[str, Any]], rows: List[Dict[str, Any]], windows: List[int], runs: int) -> None:
    failures: Dict[tuple, int] = {}
    for row in rows:
        if row.get("status") != "ok":
            key = (row.get("window_sec"), row.get("query_name"), row.get("system"))
            failures[key] = failures.get(key, 0) + 1
    by_key = {(r["window_sec"], r["query_name"], r["system"]): r for r in stats}
    print(f"\nFinal episode window sensitivity mean results over {max(1, int(runs))} run(s)")
    print("window_sec,query,system,ok/runs,mean_query_s,median_query_s,mean_peak_rss_mb,num_matches,failed")
    for window_sec in windows:
        for qname in EPISODE_QUERIES:
            for system in [SYSTEM_LABELS[s] for s in SYSTEMS]:
                stat = by_key.get((window_sec, qname, system), {})
                key = (window_sec, qname, system)
                print(",".join([
                    str(window_sec),
                    qname,
                    system,
                    f"{int(stat.get('successful_runs') or 0)}/{max(1, int(runs))}",
                    _fmt_float(stat.get("reported_query_time_s_mean")),
                    _fmt_float(stat.get("reported_query_time_s_median")),
                    _fmt_float(stat.get("peak_rss_mb_mean"), digits=3),
                    _fmt_int(stat.get("num_matches")),
                    str(failures.get(key, 0)),
                ]))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run episode window sensitivity over HVCEQ, Generic SQL, and HGMatch.")
    ap.add_argument("--stores-dir", default="", help="Prepared stores_XXX directory. Defaults to latest.")
    ap.add_argument("--windows", default="30,60,120,180,300,600")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--timeout-sec", type=int, default=None)
    ap.add_argument("--max-rss-mb", type=float, default=None)
    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    windows = parse_windows(args.windows)
    stores_dir = Path(args.stores_dir).resolve() if args.stores_dir else latest_stores_dir()
    run_dir = next_numbered_dir(WORK_ROOT, "episode_window_sensitivity")
    rows: List[Dict[str, Any]] = []

    for run_id in range(1, max(1, int(args.runs)) + 1):
        for window_sec in windows:
            for qname in EPISODE_QUERIES:
                for system_key in SYSTEMS:
                    system = SYSTEM_LABELS[system_key]
                    outdir = run_dir / f"window_{window_sec}" / f"run_{run_id:02d}" / system_key / qname
                    if args.verbose:
                        print(f"[window][{window_sec}s][run {run_id}/{args.runs}][{system}][{qname}] start", flush=True)
                    cmd = episode_query_cmd(system_key, stores_dir, qname, window_sec, outdir, args.threads)
                    meta = run_monitored(cmd, outdir, timeout_sec=args.timeout_sec, max_rss_mb=args.max_rss_mb)
                    row: Dict[str, Any] = {
                        "cost_scope": "episode_window_sensitivity_query_cost",
                        "window_sec": window_sec,
                        "mode": "episode",
                        "system": system,
                        "system_key": system_key,
                        "query_name": qname,
                        "run_id": run_id,
                        "stores_reused": True,
                        "index_mode": "generic",
                        "query_specific_indexes_reused": False,
                        "generic_indexes_reused": True,
                        "full_result_materialized": False,
                        "sample_limit": 0,
                        **meta,
                        "output_dir": str(outdir),
                    }
                    if meta["status"] == "ok":
                        row.update(parse_query_output("episode", system_key if system_key != "sql" else "sql", outdir))
                    row.update(memory_from_monitor(meta["monitor_report_path"]))
                    rows.append(row)
                    if args.verbose:
                        print(
                            f"[window][{window_sec}s][run {run_id}/{args.runs}][{system}][{qname}] "
                            f"done status={row['status']} matches={row.get('num_matches','')} "
                            f"time={row.get('reported_query_time_s','')} rss={row.get('peak_rss_mb','')}",
                            flush=True,
                        )

    stats = aggregate(rows)
    align_rows = alignment(rows, windows)
    write_csv(run_dir / "episode_window_sensitivity_all_runs.csv", rows)
    write_csv(run_dir / "episode_window_sensitivity_stats_by_query.csv", stats)
    write_csv(run_dir / "episode_window_sensitivity_alignment_summary.csv", align_rows)
    write_json(run_dir / "episode_window_sensitivity_report.json", {
        "cost_scope": "episode_window_sensitivity_fair_cost",
        "stores_dir": str(stores_dir),
        "run_dir": str(run_dir),
        "windows": windows,
        "systems": [SYSTEM_LABELS[s] for s in SYSTEMS],
        "queries": EPISODE_QUERIES,
        "notes": [
            "Each episode query is run with a uniform forward_window_sec set by the runner.",
            "The original query JSON files are not modified.",
            "HVCEQ requires the requested windows to have been precomputed by prepare_twoweek_fair_stores.py.",
        ],
    })
    print_final_summary(stats, rows, windows, args.runs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
