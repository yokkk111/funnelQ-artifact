#!/usr/bin/env python3


from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

SAME_TX_QUERIES = [
    "flashloan_swap_borrow.json",
    "flashloan_swap_borrow_followup.json",
    "flashloan_2swaps_distinct_followup.json",
    "flashloan_3swaps_chain_reuse.json",
    "flashloan_3swaps_distinct_parallel.json",
]
EPISODE_QUERIES = [
    "flashloan_swap_borrow_episode_v1.json",
    "flashloan_swap_borrow_followup_episode_v1.json",
    "flashloan_2swaps_distinct_followup_episode_v1.json",
    "flashloan_3swaps_chain_reuse_episode_v1.json",
    "flashloan_3swaps_distinct_parallel_episode_v1.json",
]


def required_types(query: dict[str, Any]) -> list[str]:
    return sorted({str(item["type"]).strip().lower() for item in query["events"]})


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mbft-adapt", required=True, type=Path)
    p.add_argument("--events", required=True, type=Path)
    p.add_argument("--same-tx-query-dir", required=True, type=Path)
    p.add_argument("--episode-query-dir", required=True, type=Path)
    p.add_argument("--outdir", required=True, type=Path)
    p.add_argument("--matcher", type=Path, default=Path(__file__).with_name("neutral_matcher.py"))
    p.add_argument("--combine", type=int, default=0)
    p.add_argument("--fp-rate", type=float, default=0.001)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--tweak", type=int, default=0)
    p.add_argument("--windows", default="60,300,600")
    p.add_argument("--timeout-sec", type=int, default=600)
    p.add_argument("--skip-same-tx", action="store_true")
    p.add_argument("--skip-episode", action="store_true")
    args = p.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    atomic_dir = args.outdir / "atomic"
    atomic_dir.mkdir(exist_ok=True)

    atomic_cmd = [
        str(args.mbft_adapt), "query-only-select",
        "--events", str(args.events),
        "--outdir", str(atomic_dir),
        "--combine", str(args.combine),
        "--fp-rate", str(args.fp_rate),
        "--seed", str(args.seed),
        "--tweak", str(args.tweak),
        "--types", "flashloan,swap,borrow",
    ]
    atomic_wall_start = time.perf_counter()
    with (args.outdir / "atomic_stdout.json").open("w", encoding="utf-8") as stdout, \
         (args.outdir / "atomic_stderr.log").open("w", encoding="utf-8") as stderr:
        subprocess.run(atomic_cmd, check=True, stdout=stdout, stderr=stderr)
    atomic_wall_ms = (time.perf_counter() - atomic_wall_start) * 1000.0
    atomic_summary = load_json(atomic_dir / "query_only_atomic_summary.json")

    all_results: list[dict[str, Any]] = []

    def run_one(query_path: Path, scope: str, window: int | None = None) -> None:
        query = load_json(query_path)
        if window is not None:
            query.setdefault("scope_params", {})["forward_window_sec"] = window
            query_run_path = args.outdir / "queries" / f"{query_path.stem}_w{window}.json"
            query_run_path.parent.mkdir(exist_ok=True)
            query_run_path.write_text(json.dumps(query, indent=2) + "\n", encoding="utf-8")
            run_name = f"{query_path.stem}_w{window}"
        else:
            query_run_path = query_path
            run_name = query_path.stem

        types = required_types(query)
        event_paths = [atomic_dir / f"{event_type}.query_only.jsonl" for event_type in types]
        output_path = args.outdir / scope / f"{run_name}.json"
        output_path.parent.mkdir(exist_ok=True)
        stdout_path = output_path.with_suffix(".stdout.json")
        stderr_path = output_path.with_suffix(".stderr.log")
        cmd = [
            sys.executable, str(args.matcher),
            "--query", str(query_run_path),
            "--scope", "same_tx" if scope == "same_tx" else "episode",
            "--out", str(output_path),
            "--materialize-limit", "0",
        ]
        for path in event_paths:
            cmd += ["--events", str(path)]

        status = "ok"
        started = time.perf_counter()
        try:
            with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
                subprocess.run(cmd, check=True, stdout=stdout, stderr=stderr, timeout=args.timeout_sec)
        except subprocess.TimeoutExpired:
            status = "timeout"
        except subprocess.CalledProcessError:
            status = "error"
        wall_ms = (time.perf_counter() - started) * 1000.0

        atomic_metrics = [atomic_summary["selections"][event_type] for event_type in types]
        atomic_tree_ms = sum(float(item["mbft_tree_query_time_us"]) for item in atomic_metrics) / 1000.0
        atomic_write_ms = sum(float(item["candidate_write_time_us"]) for item in atomic_metrics) / 1000.0
        atomic_candidates = sum(int(item["unique_events"]) for item in atomic_metrics)
        atomic_bytes = sum(int(item["candidate_file_bytes"]) for item in atomic_metrics)

        if status == "ok":
            result = load_json(output_path)
            result.update({
                "system": "MBFT-Query",
                "mode": "query-only-no-vo",
                "status": status,
                "window_sec": window,
                "required_event_types": types,
                "mbft_atomic_tree_query_ms": atomic_tree_ms,
                "candidate_bridge_write_ms": atomic_write_ms,
                "atomic_candidate_events": atomic_candidates,
                "atomic_candidate_file_bytes": atomic_bytes,
                "server_candidate_load_ms": result.pop("client_candidate_load_ms"),
                "server_temporary_index_build_ms": result.pop("client_temporary_index_build_ms"),
                "server_complex_match_ms": result.pop("client_complex_match_ms"),
                "server_match_pipeline_ms": result.pop("client_match_total_ms"),
                "server_peak_rss_mb": result.pop("client_peak_rss_mb"),
                "observed_matcher_wall_ms": wall_ms,
            })
            result["server_query_core_ms"] = (
                result["mbft_atomic_tree_query_ms"]
                + result["server_temporary_index_build_ms"]
                + result["server_complex_match_ms"]
            )
            result["server_query_observed_ms"] = (
                result["mbft_atomic_tree_query_ms"]
                + result["candidate_bridge_write_ms"]
                + result["server_match_pipeline_ms"]
            )
            output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        else:
            result = {
                "system": "MBFT-Query",
                "mode": "query-only-no-vo",
                "status": status,
                "query_name": query.get("query_name", run_name),
                "scope": scope,
                "window_sec": window,
                "required_event_types": types,
                "mbft_atomic_tree_query_ms": atomic_tree_ms,
                "candidate_bridge_write_ms": atomic_write_ms,
                "atomic_candidate_events": atomic_candidates,
                "atomic_candidate_file_bytes": atomic_bytes,
                "observed_matcher_wall_ms": wall_ms,
            }
            output_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        all_results.append(result)
        print(json.dumps(result, ensure_ascii=False), flush=True)

    if not args.skip_same_tx:
        for name in SAME_TX_QUERIES:
            run_one(args.same_tx_query_dir / name, "same_tx")

    if not args.skip_episode:
        windows = [int(item) for item in args.windows.split(",") if item.strip()]
        for window in windows:
            for name in EPISODE_QUERIES:
                run_one(args.episode_query_dir / name, "episode", window)

    summary = {
        "system": "MBFT-Query",
        "mode": "query-only-no-vo",
        "description": "Original MBFT MergeBloom tree traversal plus server-side complex-event count matcher; no VO generation or verification.",
        "atomic_preparation_wall_ms": atomic_wall_ms,
        "atomic_summary": atomic_summary,
        "results": all_results,
    }
    (args.outdir / "query_only_workload_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
