from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from fair_cost_runner_lib import (  # noqa: E402
    EPISODE_QUERIES,
    SAME_TX_QUERIES,
    memory_from_monitor,
    parse_query_output,
    query_script_cmd,
    run_monitored,
)

STORES = ROOT / "data/month3_build_work/fair_cost_month3/stores_002"
HVCEQ_EPISODE_DB = (
    ROOT
    / "data/month3_build_work/new_query_pattern_stores/hvceq_episode"
    / "anchored_episode_runtime_sidecar.db"
)
OUT = ROOT / "data/month3_build_work/final_queryset_4scheme_once"
WARM_QUERY_MODE = False
MBFT_ROOT = ROOT / "MBFT-Adapt_MergeBloom_QueryOnly_v3_source"
MBFT_ATOMIC = (
    ROOT / "data/month3_build_work/mbft_adapt_month3_chunked/selection/atomic"
)
MBFT_REPORT = (
    ROOT
    / "data/month3_build_work/mbft_adapt_month3_chunked/selection"
    / "chunked_query_only_atomic_summary.json"
)

SYSTEMS = ("hvceq", "sql", "hgmatch", "mbft")
WINDOWS = (60, 300, 600)
SAME_TX = (
    (
        "S1",
        "flashloan_then_debt_reduction_same_tx",
        ROOT
        / "queries/replacement_trial/same_tx"
        / "flashloan_then_debt_reduction_same_tx.json",
    ),
    (
        "S2",
        "borrow_then_reduce_same_market_debt_same_tx",
        ROOT
        / "queries/replacement_trial/same_tx"
        / "borrow_then_reduce_same_market_debt_same_tx.json",
    ),
    (
        "S3",
        "flashloan_roundtrip_swaps",
        ROOT / "queries/new_patterns/same_tx/flashloan_roundtrip_swaps.json",
    ),
    (
        "S4",
        "borrow_then_swap_borrowed_asset",
        ROOT / "queries/new_patterns/same_tx/borrow_then_swap_borrowed_asset.json",
    ),
    (
        "S5",
        "debt_reduction_then_reborrow_other_pool",
        ROOT
        / "queries/new_patterns/same_tx"
        / "debt_reduction_then_reborrow_other_pool.json",
    ),
)
EPISODE = (
    (
        "E1",
        "borrow_refinance_other_pool",
        ROOT / "queries/new_patterns/episode/borrow_refinance_other_pool.json",
    ),
    (
        "E2",
        "borrow_swap_repay_same_market",
        ROOT
        / "queries/replacement_trial/episode"
        / "borrow_swap_repay_same_market.json",
    ),
    (
        "E3",
        "swap_then_borrow_bought_asset",
        ROOT / "queries/new_patterns/episode/swap_then_borrow_bought_asset.json",
    ),
    (
        "E4",
        "flashloan_then_debt_reduction",
        ROOT / "queries/new_patterns/episode/flashloan_then_debt_reduction.json",
    ),
    (
        "E5",
        "borrow_swap_reduce_bought_asset_debt",
        ROOT
        / "queries/new_patterns/episode"
        / "borrow_swap_reduce_bought_asset_debt.json",
    ),
)
TIMEOUT_SEC = 3600
JOB_LIMIT_MB = 18432


def materialize(
    mode: str, query_path: Path, query: str, window: int, outdir: Path
) -> Path:
    if mode == "same_tx":
        return query_path
    spec = json.loads(query_path.read_text(encoding="utf-8"))
    spec["scope_params"]["forward_window_sec"] = window
    path = outdir / f"{query}_w{window}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def mbft_command(
    mode: str, query_path: Path, outdir: Path
) -> tuple[list[str], Path, list[str]]:
    spec = json.loads(query_path.read_text(encoding="utf-8"))
    types = sorted({str(event["type"]).lower() for event in spec["events"]})
    result = outdir / "matcher_result.json"
    cmd = [
        sys.executable,
        str(MBFT_ROOT / "mbft_adapt/python/neutral_matcher.py"),
        "--query",
        str(query_path),
        "--scope",
        mode,
        "--out",
        str(result),
        "--materialize-limit",
        "0",
    ]
    for event_type in types:
        cmd += ["--events", str(MBFT_ATOMIC / f"{event_type}.query_only.jsonl")]
    if WARM_QUERY_MODE:
        cmd += ["--warmup-runs", "1", "--phase-marker", str(outdir / "query_phase.marker")]
    return cmd, result, types


def parse_mbft(result_path: Path, types: list[str]) -> dict:
    result = json.loads(result_path.read_text(encoding="utf-8"))
    atomic = json.loads(MBFT_REPORT.read_text(encoding="utf-8"))
    tree_s = sum(
        float(atomic["selections"][event_type]["mbft_tree_query_time_us"])
        for event_type in types
    ) / 1e6
    load_s = float(result["client_candidate_load_ms"]) / 1000
    index_s = float(result["client_temporary_index_build_ms"]) / 1000
    match_s = float(result["client_complex_match_ms"]) / 1000
    reported = tree_s + match_s if WARM_QUERY_MODE else tree_s + index_s + match_s
    return {
        "report_path": str(result_path),
        "num_matches": result.get("result_count", ""),
        "num_result_units": result.get(
            "matching_episode_count", result.get("matching_closure_count", "")
        ),
        "reported_query_time_s": reported,
        "mbft_candidate_load_s_excluded": load_s,
        "aux_total_or_wall_time_s": tree_s + load_s + index_s + match_s,
    }


def command_for(
    mode: str, system: str, query_path: Path, outdir: Path
) -> tuple[list[str], Path | None, list[str]]:
    if system == "mbft":
        return mbft_command(mode, query_path, outdir)
    dummy = SAME_TX_QUERIES[0] if mode == "same_tx" else EPISODE_QUERIES[0]
    cmd = query_script_cmd(mode, system, STORES, dummy, outdir, runs=1, threads=8)
    cmd[cmd.index("--query-json") + 1] = str(query_path)
    if mode == "episode" and system == "hvceq":
        cmd[cmd.index("--episode-sidecar-db") + 1] = str(HVCEQ_EPISODE_DB)
    if WARM_QUERY_MODE:
        cmd += ["--warmup-runs", "1", "--phase-marker", str(outdir / "query_phase.marker")]
    return cmd, None, []


def jobs() -> list[tuple[str, int, str, str, str, Path]]:
    result = [
        ("same_tx", 0, system, label, query, path)
        for label, query, path in SAME_TX
        for system in SYSTEMS
    ]
    result.extend(
        ("episode", window, system, label, query, path)
        for window in WINDOWS
        for label, query, path in EPISODE
        for system in SYSTEMS
    )
    return result


def write_csv(path: Path, rows: list[dict]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    global OUT, WARM_QUERY_MODE
    ap = argparse.ArgumentParser()
    ap.add_argument("--warm-query-once", action="store_true")
    ap.add_argument("--max-new-jobs", type=int, default=0)
    ap.add_argument("--systems", default="")
    ap.add_argument("--labels", default="")
    ap.add_argument("--windows", default="")
    ap.add_argument("--out-dir")
    args = ap.parse_args()
    WARM_QUERY_MODE = bool(args.warm_query_once)
    if WARM_QUERY_MODE:
        OUT = ROOT / "data/month3_build_work/final_queryset_4scheme_queryphase_once"
    if args.out_dir:
        OUT = Path(args.out_dir).resolve()
    OUT.mkdir(parents=True, exist_ok=True)
    os.environ["HVCEQ_MIN_SYSTEM_AVAILABLE_MB"] = "2048"
    progress = OUT / "progress.json"
    rows = json.loads(progress.read_text(encoding="utf-8")) if progress.exists() else []
    done = {
        (r["mode"], int(r["window_sec"]), r["system"], r["query"])
        for r in rows
        if r.get("status") == "ok"
    }
    selected_systems = {
        item.strip().lower() for item in args.systems.split(",") if item.strip()
    }
    selected_labels = {
        item.strip().upper() for item in args.labels.split(",") if item.strip()
    }
    selected_windows = {
        int(item.strip()) for item in args.windows.split(",") if item.strip()
    }
    all_jobs = [
        job
        for job in jobs()
        if (not selected_systems or job[2] in selected_systems)
        and (not selected_labels or job[3] in selected_labels)
        and (
            not selected_windows
            or (job[0] == "same_tx" and 0 in selected_windows)
            or (job[0] == "episode" and job[1] in selected_windows)
        )
    ]
    new_jobs_run = 0
    for order, (mode, window, system, label, query, source) in enumerate(all_jobs, 1):
        key = (mode, window, system, query)
        if key in done:
            continue
        if args.max_new_jobs > 0 and new_jobs_run >= args.max_new_jobs:
            break
        window_label = "same_tx" if mode == "same_tx" else f"w{window}"
        outdir = OUT / window_label / system / query
        query_path = materialize(mode, source, query, window, outdir)
        print(
            f"[{order}/{len(all_jobs)}] {label} {mode} w={window} "
            f"{system}/{query}",
            flush=True,
        )
        cmd, result_path, types = command_for(mode, system, query_path, outdir)
        phase_marker = outdir / "query_phase.marker" if WARM_QUERY_MODE else None
        if phase_marker is not None:
            phase_marker.unlink(missing_ok=True)
        meta = run_monitored(
            cmd,
            outdir,
            timeout_sec=TIMEOUT_SEC,
            max_rss_mb=JOB_LIMIT_MB,
            phase_marker_path=phase_marker,
        )
        row = {
            "order": order,
            "label": label,
            "mode": mode,
            "window_sec": window,
            "system": system,
            "query": query,
            "measurement_policy": (
                "one_warmup_then_one_measured_query_phase"
                if WARM_QUERY_MODE
                else "single_cold_or_ambient_run"
            ),
            **meta,
            **memory_from_monitor(
                meta["monitor_report_path"], query_phase_only=WARM_QUERY_MODE
            ),
        }
        if meta["status"] == "ok":
            row.update(
                parse_mbft(result_path, types)
                if system == "mbft"
                else parse_query_output(mode, system, outdir)
            )
        rows = [
            old
            for old in rows
            if not (
                old["mode"] == mode
                and int(old["window_sec"]) == window
                and old["system"] == system
                and old["query"] == query
            )
        ]
        rows.append(row)
        new_jobs_run += 1
        rows.sort(key=lambda item: int(item["order"]))
        progress.write_text(
            json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        write_csv(OUT / "results.csv", rows)
        print(
            f"[{order}/{len(all_jobs)}] status={row['status']} "
            f"matches={row.get('num_matches', '')} "
            f"query_s={row.get('reported_query_time_s', '')} "
            f"commit_mb={row.get('peak_job_commit_mb', '')}",
            flush=True,
        )

        successful_counts = {
            str(item.get("num_matches"))
            for item in rows
            if item["mode"] == mode
            and int(item["window_sec"]) == window
            and item["query"] == query
            and item.get("status") == "ok"
        }
        if len(successful_counts) > 1:
            (OUT / "MATCH_COUNT_MISMATCH.json").write_text(
                json.dumps(
                    {
                        "label": label,
                        "mode": mode,
                        "window_sec": window,
                        "query": query,
                        "counts": sorted(successful_counts),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
