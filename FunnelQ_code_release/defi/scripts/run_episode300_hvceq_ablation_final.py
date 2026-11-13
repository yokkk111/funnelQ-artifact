#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from fair_cost_runner_lib import (
    EPISODE_QUERIES,
    WORK_ROOT,
    latest_stores_dir,
    memory_from_monitor,
    next_numbered_dir,
    query_json,
    read_json,
    run_monitored,
    store_paths,
    write_csv,
    write_json,
)

from systems.hvceq.episode.query_episode_generic import (
    EpisodeAccessor,
    EncodedRuntime,
    base_filter,
    digest_ids,
    event_matches_spec,
    generic_match,
    load_query,
    load_value_ids,
    refined_filter,
    specs,
)


VARIANTS = ["full", "no_anchor", "no_refined", "no_synopsis"]
VARIANT_LABELS = {
    "full": "Full",
    "no_anchor": "No Anchor",
    "no_refined": "No Refined",
    "no_synopsis": "No Synopsis",
}


def parse_list(value: str, allowed: Sequence[str], kind: str) -> List[str]:
    out = [x.strip() for x in value.split(",") if x.strip()]
    bad = [x for x in out if x not in allowed]
    if bad:
        raise ValueError(f"Unknown {kind}: {bad}. Allowed: {list(allowed)}")
    return out or list(allowed)


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


def effective_episode_query_json(qname: str, outdir: Path) -> Path:
    source = query_json("episode", "hvceq", qname)
    query = read_json(source)
    scope_params = dict(query.get("scope_params", {}))
    scope_params["forward_window_sec"] = 300
    query["scope_params"] = scope_params
    effective = outdir / f"{qname}_window_300.json"
    write_json(effective, query)
    return effective


def build_cand0(acc: EpisodeAccessor, query: dict, semantic_qid: str, anchor_enabled: bool) -> List[Tuple[int, int, int]]:


    sp = specs(query)
    sem_spec = sp[semantic_qid]
    cand0: List[Tuple[int, int, int]] = []
    for ai in range(len(acc.rt.a_event_ord)):
        if anchor_enabled:
            ok, _new = event_matches_spec(acc, sem_spec, ai, {})
            if not ok:
                continue
        end_tx = acc.rt.window_end[acc.rt.a_tx_ord[ai]]
        hi_ord = acc.rt.tx_max_event_ord[end_tx]
        cand0.append((ai, int(acc.rt.a_event_ord[ai]), int(hi_ord)))
    return cand0


def run_variant(acc: EpisodeAccessor, query: dict, outdir: Path, variant: str) -> Dict[str, Any]:
    semantic_qid = str(query.get("scope_params", {}).get("semantic_anchor_event", ""))
    if not semantic_qid:
        raise ValueError("anchored_episode query requires scope_params.semantic_anchor_event")
    sem_spec = specs(query)[semantic_qid]
    if sem_spec.get("type") != "flashloan":
        raise ValueError("current encoded episode sidecar supports flashloan semantic anchors")
    window_sec = int(query["scope_params"]["forward_window_sec"])
    if window_sec != 300:
        raise ValueError(f"This ablation runner is fixed to episode window=300s, got {window_sec}.")

    anchor_enabled = variant != "no_anchor"
    base_enabled = variant != "no_synopsis"
    refined_enabled = variant not in {"no_refined", "no_synopsis"}

    t0 = time.perf_counter()
    cand0 = build_cand0(acc, query, semantic_qid, anchor_enabled=anchor_enabled)
    t1 = time.perf_counter()

    if base_enabled:
        cand1 = base_filter(acc, query, cand0)
    else:
        cand1 = list(cand0)
    t2 = time.perf_counter()

    if refined_enabled:
        cand2 = refined_filter(acc, query, cand1, semantic_qid)
    else:
        cand2 = list(cand1)
    t3 = time.perf_counter()

    exact = generic_match(acc, query, cand2, semantic_qid, outdir, write_bindings=False)
    t4 = time.perf_counter()

    return {
        **exact,
        "semantic_anchor_qid": semantic_qid,
        "anchor_enabled": bool(anchor_enabled),
        "anchor_mode": "anchor_signature_recall" if anchor_enabled else "disabled_all_episode_closures",
        "base_synopsis_enabled": bool(base_enabled),
        "refined_synopsis_enabled": bool(refined_enabled),
        "forward_window_sec": window_sec,
        "num_cand0": len(cand0),
        "num_cand1": len(cand1),
        "num_cand2": len(cand2),
        "candidate_frontier_commitments": {
            "cand0_sha256": digest_ids([x[0] for x in cand0]),
            "cand1_sha256": digest_ids([x[0] for x in cand1]),
            "cand2_sha256": digest_ids([x[0] for x in cand2]),
        },
        "stage_times_s": {
            "candidate_generation": round(t1 - t0, 6),
            "base_filter": round(t2 - t1, 6),
            "refined_filter": round(t3 - t2, 6),
            "exact_match": round(t4 - t3, 6),
        },
        "query_latency_s": round(t4 - t0, 6),
    }


def worker_main(args: argparse.Namespace) -> int:
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    query = load_query(Path(args.query_json))
    setup0 = time.perf_counter()
    conn = sqlite3.connect(args.episode_sidecar_db)
    try:
        rt = EncodedRuntime(
            conn,
            int(query["scope_params"]["forward_window_sec"]),
            enable_terminal_complete_dirs=True,
            enable_terminal_anti_join_dirs=True,
            terminal_direct_scan_limit=int(args.terminal_direct_scan_limit),
        )
        value_to_id = load_value_ids(conn)
    finally:
        conn.close()
    setup1 = time.perf_counter()

    acc = EpisodeAccessor(rt, value_to_id)
    result = run_variant(acc, query, outdir, args.ablation)
    done = time.perf_counter()

    report = {
        "query_name": query["query_name"],
        "scope": "anchored_episode",
        "system": "HVCEQ",
        "execution_profile": "generic_json_compiled_episode300_ablation",
        "ablation_variant": args.ablation,
        "ablation_label": VARIANT_LABELS[args.ablation],
        "forward_window_sec": result["forward_window_sec"],
        "anchor_enabled": result["anchor_enabled"],
        "anchor_mode": result["anchor_mode"],
        "base_synopsis_enabled": result["base_synopsis_enabled"],
        "refined_synopsis_enabled": result["refined_synopsis_enabled"],
        "semantic_anchor_qid": result["semantic_anchor_qid"],
        "compiled_match_order": result["compiled_match_order"],
        "summary_count_only": True,
        "matched_instances_csv_written": False,
        "matched_bindings_csv_written": False,
        "terminal_complete_dirs_enabled": bool(getattr(acc.rt, "enable_terminal_complete_dirs", False)),
        "terminal_anti_join_dirs_lazy_enabled": bool(getattr(acc.rt, "enable_terminal_anti_join_dirs", False)),
        "terminal_anti_join_dirs_built": bool(getattr(acc.rt, "swap_terminal_anti_join_dirs_built", False)),
        "terminal_direct_scan_limit": int(getattr(acc.rt, "terminal_direct_scan_limit", 0)),
        "num_cand0": result["num_cand0"],
        "num_cand1": result["num_cand1"],
        "num_cand2": result["num_cand2"],
        "num_matches": result["num_matches"],
        "num_result_units": result["num_result_episodes"],
        "num_result_episodes": result["num_result_episodes"],
        "num_residual_episodes": max(0, result["num_cand2"] - result["num_result_episodes"]),
        "num_candidate_event_bindings_examined": result["num_candidate_event_bindings_examined"],
        "num_terminal_aggregate_calls": result["num_terminal_aggregate_calls"],
        "num_terminal_candidates_aggregated": result["num_terminal_candidates_aggregated"],
        "candidate_frontier_commitments": result["candidate_frontier_commitments"],
        "matched_instances_path": result["matched_instances_path"],
        "matched_bindings_path": result["matched_bindings_path"],
        "stage_times_s": {
            "setup": round(setup1 - setup0, 6),
            **result["stage_times_s"],
        },
        "metrics": {
            "setup_s": round(setup1 - setup0, 6),
            "query_latency_s": result["query_latency_s"],
            "query_wall_time_s": round(done - setup0, 6),
            "summary_only": True,
        },
        "ablation_semantics_note": (
            "No Anchor disables anchor-signature recall and starts from all precomputed flashloan-induced episode closures. "
            "The exact matcher still binds the query semantic anchor to the closure anchor to preserve anchored-episode semantics."
            if args.ablation == "no_anchor" else ""
        ),
    }
    (outdir / "query_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


def parse_query_report(outdir: Path) -> Dict[str, Any]:
    report_path = outdir / "query_result.json"
    report = read_json(report_path) if report_path.exists() else {}
    metrics = report.get("metrics", {}) if isinstance(report.get("metrics", {}), dict) else {}
    stage_times = report.get("stage_times_s", {}) if isinstance(report.get("stage_times_s", {}), dict) else {}
    return {
        "report_path": str(report_path),
        "query_name_reported": report.get("query_name", ""),
        "ablation_variant_reported": report.get("ablation_variant", ""),
        "num_matches": report.get("num_matches", ""),
        "num_result_units": report.get("num_result_units", report.get("num_result_episodes", "")),
        "num_residual_episodes": report.get("num_residual_episodes", ""),
        "reported_query_time_s": metrics.get("query_latency_s", ""),
        "aux_total_or_wall_time_s": metrics.get("query_wall_time_s", ""),
        "num_cand0": report.get("num_cand0", ""),
        "num_cand1": report.get("num_cand1", ""),
        "num_cand2": report.get("num_cand2", ""),
        "anchor_enabled": report.get("anchor_enabled", ""),
        "base_synopsis_enabled": report.get("base_synopsis_enabled", ""),
        "refined_synopsis_enabled": report.get("refined_synopsis_enabled", ""),
        "terminal_anti_join_dirs_built": report.get("terminal_anti_join_dirs_built", ""),
        "num_candidate_event_bindings_examined": report.get("num_candidate_event_bindings_examined", ""),
        "num_terminal_aggregate_calls": report.get("num_terminal_aggregate_calls", ""),
        "num_terminal_candidates_aggregated": report.get("num_terminal_candidates_aggregated", ""),
        "stage_setup_s": stage_times.get("setup", ""),
        "stage_candidate_generation_s": stage_times.get("candidate_generation", ""),
        "stage_base_filter_s": stage_times.get("base_filter", ""),
        "stage_refined_filter_s": stage_times.get("refined_filter", ""),
        "stage_exact_match_s": stage_times.get("exact_match", ""),
    }


def aggregate(rows: List[Dict[str, Any]], queries: List[str], variants: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for qname in queries:
        for variant in variants:
            group = [r for r in rows if r.get("query_name") == qname and r.get("ablation_variant") == variant]
            ok = [r for r in group if r.get("status") == "ok"]
            row: Dict[str, Any] = {
                "window_sec": 300,
                "mode": "episode",
                "query_name": qname,
                "ablation_variant": variant,
                "ablation_label": VARIANT_LABELS[variant],
                "runs": len(group),
                "successful_runs": len(ok),
                "num_matches_consistent": len({str(r.get("num_matches")) for r in ok}) == 1 if ok else False,
                "num_matches": next(iter({r.get("num_matches") for r in ok}), "") if ok else "",
                "num_result_units": next(iter({r.get("num_result_units") for r in ok}), "") if ok else "",
            }
            for field in [
                "reported_query_time_s",
                "peak_rss_mb",
                "num_cand0",
                "num_cand1",
                "num_cand2",
                "num_residual_episodes",
                "num_candidate_event_bindings_examined",
                "num_terminal_aggregate_calls",
                "num_terminal_candidates_aggregated",
                "stage_setup_s",
                "stage_candidate_generation_s",
                "stage_base_filter_s",
                "stage_refined_filter_s",
                "stage_exact_match_s",
            ]:
                vals = [x for x in (to_float(r.get(field)) for r in ok) if x is not None]
                row.update(metric_stats(vals, field))
            out.append(row)
    return out


def alignment(rows: List[Dict[str, Any]], queries: List[str], variants: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for qname in queries:
        group = [r for r in rows if r.get("query_name") == qname and r.get("status") == "ok"]
        matches_by_variant = {
            variant: sorted({str(r.get("num_matches")) for r in group if r.get("ablation_variant") == variant})
            for variant in variants
        }
        out.append({
            "window_sec": 300,
            "mode": "episode",
            "query_name": qname,
            "variants_completed": ",".join(variant for variant in variants if matches_by_variant.get(variant)),
            "num_matches_by_variant": json.dumps(matches_by_variant, ensure_ascii=False),
            "matches_aligned": len({tuple(v) for v in matches_by_variant.values() if v}) == 1,
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


def print_final_summary(stats: List[Dict[str, Any]], queries: List[str], variants: List[str], runs: int) -> None:
    by_key = {(r["query_name"], r["ablation_variant"]): r for r in stats}
    print(f"\nFinal HVCEQ episode-300 generic ablation mean results over {max(1, int(runs))} run(s)")
    print("window_sec,query,variant,ok/runs,mean_query_s,median_query_s,mean_peak_rss_mb,num_matches,cand0,cand1,cand2,examined")
    for qname in queries:
        for variant in variants:
            stat = by_key.get((qname, variant), {})
            print(",".join([
                "300",
                qname,
                VARIANT_LABELS[variant],
                f"{int(stat.get('successful_runs') or 0)}/{max(1, int(runs))}",
                _fmt_float(stat.get("reported_query_time_s_mean")),
                _fmt_float(stat.get("reported_query_time_s_median")),
                _fmt_float(stat.get("peak_rss_mb_mean"), digits=3),
                _fmt_int(stat.get("num_matches")),
                _fmt_int(stat.get("num_cand0_mean")),
                _fmt_int(stat.get("num_cand1_mean")),
                _fmt_int(stat.get("num_cand2_mean")),
                _fmt_int(stat.get("num_candidate_event_bindings_examined_mean")),
            ]))


def parent_main(args: argparse.Namespace) -> int:
    queries = parse_list(args.queries, EPISODE_QUERIES, "query")
    variants = parse_list(args.variants, VARIANTS, "variant")
    stores_dir = Path(args.stores_dir).resolve() if args.stores_dir else latest_stores_dir()
    paths = store_paths(stores_dir)
    run_dir = next_numbered_dir(WORK_ROOT, "hvceq_episode300_generic_ablation")
    rows: List[Dict[str, Any]] = []

    for run_id in range(1, max(1, int(args.runs)) + 1):
        for qname in queries:
            for variant in variants:
                outdir = run_dir / f"run_{run_id:02d}" / variant / qname
                qpath = effective_episode_query_json(qname, outdir)
                cmd = [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--worker",
                    "--episode-sidecar-db",
                    str(paths["hvceq_episode_db"]),
                    "--query-json",
                    str(qpath),
                    "--outdir",
                    str(outdir),
                    "--ablation",
                    variant,
                    "--terminal-direct-scan-limit",
                    str(args.terminal_direct_scan_limit),
                ]
                if args.verbose:
                    print(f"[episode300-ablation][run {run_id}/{args.runs}][{VARIANT_LABELS[variant]}][{qname}] start", flush=True)
                meta = run_monitored(cmd, outdir, timeout_sec=args.timeout_sec)
                row: Dict[str, Any] = {
                    "cost_scope": "hvceq_episode300_generic_ablation_query_cost",
                    "window_sec": 300,
                    "mode": "episode",
                    "system": "HVCEQ",
                    "query_name": qname,
                    "ablation_variant": variant,
                    "ablation_label": VARIANT_LABELS[variant],
                    "run_id": run_id,
                    "stores_dir": str(stores_dir),
                    "stores_reused": True,
                    "full_result_materialized": False,
                    "sample_limit": 0,
                    **meta,
                    "output_dir": str(outdir),
                }
                if meta["status"] == "ok":
                    row.update(parse_query_report(outdir))
                row.update(memory_from_monitor(meta["monitor_report_path"]))
                rows.append(row)
                if args.verbose:
                    print(
                        f"[episode300-ablation][run {run_id}/{args.runs}][{VARIANT_LABELS[variant]}][{qname}] "
                        f"done status={row['status']} matches={row.get('num_matches','')} "
                        f"time={row.get('reported_query_time_s','')} rss={row.get('peak_rss_mb','')}",
                        flush=True,
                    )

    stats = aggregate(rows, queries, variants)
    align_rows = alignment(rows, queries, variants)
    write_csv(run_dir / "hvceq_episode300_generic_ablation_all_runs.csv", rows)
    write_csv(run_dir / "hvceq_episode300_generic_ablation_stats_by_query.csv", stats)
    write_csv(run_dir / "hvceq_episode300_generic_ablation_alignment_summary.csv", align_rows)
    write_json(run_dir / "hvceq_episode300_generic_ablation_report.json", {
        "cost_scope": "hvceq_episode300_generic_ablation",
        "window_sec": 300,
        "stores_dir": str(stores_dir),
        "run_dir": str(run_dir),
        "queries": queries,
        "variants": variants,
        "variant_definitions": {
            "full": "Anchor recall + base synopsis + refined synopsis + generic local matcher.",
            "no_anchor": "Disable anchor-signature recall; start from all precomputed flashloan-induced episode closures. Exact matching still preserves anchored-episode semantics.",
            "no_refined": "Anchor recall + base synopsis; skip refined synopsis.",
            "no_synopsis": "Anchor recall only; skip both base and refined synopsis.",
        },
        "notes": [
            "This experiment runs only HVCEQ anchored-episode queries with forward_window_sec=300.",
            "The worker imports the final generic JSON-compiled episode runner and does not use query-name specialized plans.",
            "Matched-detail materialization is disabled; this is count-only matching-cost ablation.",
        ],
    })
    print_final_summary(stats, queries, variants, args.runs)
    print(f"\nWrote results to: {run_dir}")
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="HVCEQ generic anchored-episode ablation at window=300s.")
    ap.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--stores-dir", default="", help="Prepared stores_XXX directory. Defaults to latest.")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--timeout-sec", type=int, default=7200)
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--queries", default=",".join(EPISODE_QUERIES), help="Comma-separated episode query names.")
    ap.add_argument("--variants", default=",".join(VARIANTS), help="Comma-separated ablation variants.")
    ap.add_argument(
        "--terminal-direct-scan-limit",
        type=int,
        default=2048,
        help="Passed to the final HVCEQ episode runtime for lazy terminal anti-join counting directories.",
    )
    ap.add_argument("--episode-sidecar-db", default="")
    ap.add_argument("--query-json", default="")
    ap.add_argument("--outdir", default="")
    ap.add_argument("--ablation", choices=VARIANTS, default="full")
    return ap.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.worker:
        missing = [name for name in ["episode_sidecar_db", "query_json", "outdir"] if not getattr(args, name)]
        if missing:
            raise ValueError(f"Worker mode missing arguments: {missing}")
        return worker_main(args)
    return parent_main(args)


if __name__ == "__main__":
    raise SystemExit(main())
