#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

from sharded_store_common import atomic_write_json, run_monitored

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
FINAL5 = ["C1", "C2", "L3_RETURN", "L6_NEG_RETURN_SUPPORT", "H5_23"]


def query_json_path(query_dir: Path, qid: str) -> Path:
    mapping = {
        "C1": "C1.json",
        "C2": "C2.json",
        "L3": "L3_RETURN.json",
        "L3_RETURN": "L3_RETURN.json",
        "L6": "L6_NEG_RETURN_SUPPORT.json",
        "L6_NEG_RETURN_SUPPORT": "L6_NEG_RETURN_SUPPORT.json",
        "H5": "H5_23.json",
        "H5_23": "H5_23.json",
    }
    path = query_dir / mapping.get(qid, f"{qid}.json")
    if not path.is_file():
        raise FileNotFoundError(f"query JSON not found: {path}")
    return path


def required_atomic_types(query_dir: Path, qids: list[str]) -> list[str]:
    required: set[str] = set()
    for qid in qids:
        query = json.loads(query_json_path(query_dir, qid).read_text(encoding="utf-8"))
        for branch in query["full_binding_plan"]["branches"]:
            for pattern in branch["event_patterns"]:
                raw = pattern["event_type"]
                event_types = raw if isinstance(raw, list) else [raw]
                required.update(str(value).strip().lower() for value in event_types)
    supported = {"nfttrade", "nfttransfer"}
    unknown = required - supported
    if unknown:
        raise ValueError(f"unsupported MBFT NFT atomic types: {sorted(unknown)}")
    return [kind for kind in ("nfttrade", "nfttransfer") if kind in required]


def write_csv(path: Path, rows: list[dict]) -> None:
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


def main() -> int:
    ap = argparse.ArgumentParser(description="MBFT-NFT sharded persistent full-binding query.")
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--exe", required=True, type=Path)
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--queries", default=",".join(FINAL5))
    ap.add_argument("--query-dir", default=str(ROOT / "queries" / "nft" / "final5"), type=Path)
    ap.add_argument("--root-limit", type=int, default=0)
    ap.add_argument("--timeout-per-shard-sec", type=float, default=0.0)
    ap.add_argument("--max-shard-rss-mb", type=float, default=0.0)
    ap.add_argument("--matcher-timeout-sec", type=float, default=0.0)
    ap.add_argument("--max-matcher-rss-mb", type=float, default=0.0)
    ap.add_argument("--resume-atomic", action="store_true")
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--phase-marker", default="", type=Path)
    args = ap.parse_args()

    wall0 = time.perf_counter()
    outdir = args.outdir.resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    atomic_dir = outdir / "atomic"
    qids = [q.strip().upper() for q in args.queries.split(",") if q.strip()]
    atomic_types = required_atomic_types(args.query_dir.resolve(), qids)
    phase_marker = (
        args.phase_marker.resolve()
        if str(args.phase_marker).strip() not in {"", "."}
        else None
    )
    if phase_marker is not None:
        phase_marker.unlink(missing_ok=True)

    atomic_cmd = [
        sys.executable, str(HERE / "query_mbft_nft_sharded_stores.py"),
        "--manifest", str(args.manifest.resolve()),
        "--exe", str(args.exe.resolve()),
        "--outdir", str(atomic_dir),
        "--types", ",".join(atomic_types),
        "--timeout-per-shard-sec", str(args.timeout_per_shard_sec),
        "--max-rss-mb", str(args.max_shard_rss_mb),
        "--warmup-runs", str(max(0, args.warmup_runs)),
    ]
    if phase_marker is not None:
        atomic_cmd += ["--phase-marker", str(phase_marker)]
    if args.resume_atomic:
        atomic_cmd.append("--resume")
    atomic_proc, stdout, stderr = run_monitored(atomic_cmd, cwd=HERE)
    (outdir / "atomic.stdout.txt").write_text(stdout, encoding="utf-8")
    (outdir / "atomic.stderr.txt").write_text(stderr, encoding="utf-8")
    if int(atomic_proc.get("return_code", 1)) != 0:
        raise RuntimeError(f"MBFT atomic query failed: {stderr[-2000:]}")
    atomic = json.loads((atomic_dir / "atomic_selection_summary.json").read_text(encoding="utf-8"))

    candidate_args: list[str] = []
    for kind in atomic_types:
        candidate_args.extend(atomic["candidate_paths"][kind])
    match_dir = outdir / "server_match"
    matcher_cmd = [
        sys.executable, str(HERE / "nft_semantic_matcher.py"),
        "--events-jsonl", *candidate_args,
        "--query-dir", str(args.query_dir.resolve()),
        "--queries", ",".join(qids),
        "--outdir", str(match_dir),
        "--warmup-runs", str(max(0, args.warmup_runs)),
    ]
    if phase_marker is not None:
        matcher_cmd += ["--phase-marker", str(phase_marker)]
    if args.root_limit:
        matcher_cmd.extend(["--root-limit", str(args.root_limit)])
    matcher_proc, mstdout, mstderr = run_monitored(
        matcher_cmd,
        cwd=HERE,
        timeout_s=args.matcher_timeout_sec,
        max_rss_mb=args.max_matcher_rss_mb,
    )
    (outdir / "matcher.stdout.txt").write_text(mstdout, encoding="utf-8")
    (outdir / "matcher.stderr.txt").write_text(mstderr, encoding="utf-8")
    if int(matcher_proc.get("return_code", 1)) != 0:
        raise RuntimeError(f"MBFT semantic matcher failed: {mstderr[-2000:]}")
    matcher = json.loads((match_dir / "matcher_report.json").read_text(encoding="utf-8"))

    atomic_query_s = float(atomic.get("atomic_query_s", 0.0))
    store_load_s = float(atomic.get("store_load_s_sum", 0.0))
    candidate_write_s = float(atomic.get("candidate_write_s_sum", 0.0))
    candidate_load_s = float(matcher.get("candidate_load_s", 0.0))
    matcher_prepare_s = float(matcher.get("prepare_s", 0.0))
    rows: list[dict] = []
    execution_reports: list[dict] = []
    for result in matcher["queries"]:
        match_s = float(result.get("match_s", 0.0))
        result_canonicalize_s = float(result.get("result_canonicalize_s", 0.0))
        csv_write_s = float(result.get("csv_write_s", 0.0))
        query_latency_s = (
            atomic_query_s
            + candidate_write_s
            + candidate_load_s
            + matcher_prepare_s
            + match_s
            + result_canonicalize_s
        )
        row = {
            "system": "MBFT-Adapt",
            "query": result["query"],
            "result_semantics": "full_event_binding_v1",
            "result_count": int(result["result_count"]),
            "duplicate_count": int(result["duplicate_count"]),
            "result_digest_sha256": result["result_digest_sha256"],
            "instances_csv": result["instances_csv"],
            "store_load_s": store_load_s,
            "atomic_retrieval_s": atomic_query_s,
            "candidate_jsonl_write_s": candidate_write_s,
            "candidate_jsonl_load_s": candidate_load_s,
            "adapter_prepare_s": matcher_prepare_s,
            "adapter_match_s": match_s,
            "result_canonicalize_s": result_canonicalize_s,
            "csv_write_s": csv_write_s,
            "query_latency_s": query_latency_s,
            "candidate_events": int(matcher["event_count"]),
            "candidate_root_closures": int(matcher["root_count"]),
            "identical_duplicate_event_rows": int(
                matcher.get("identical_duplicate_event_rows", 0)
            ),
            "shard_count": int(atomic["shard_count"]),
            "atomic_peak_shard_rss_mb": float(atomic["peak_shard_process_rss_mb"]),
            "matcher_peak_tree_rss_mb": float(matcher_proc["peak_tree_rss_mb"]),
            "offline_index_build_excluded": True,
            "result_file_write_excluded_from_query_latency": True,
            "imports_funnelq_verifier": False,
        }
        rows.append(row)
        execution_reports.append({
            "system": "MBFT-Adapt",
            "engine": "persistent MBFT MergeBloom no-proof atomic retrieval + independent adapter matcher",
            **{k: row[k] for k in ("query", "result_semantics", "result_count", "duplicate_count", "result_digest_sha256", "instances_csv")},
            "timing": {
                "store_load_s": store_load_s,
                "atomic_retrieval_s": atomic_query_s,
                "candidate_jsonl_write_s": candidate_write_s,
                "candidate_jsonl_load_s": candidate_load_s,
            "adapter_prepare_s": matcher_prepare_s,
                "adapter_match_s": match_s,
                "result_canonicalize_s": result_canonicalize_s,
                "csv_write_s": csv_write_s,
                "query_latency_s": query_latency_s,
                "wall_s": time.perf_counter() - wall0,
                "runtime_load_excluded_from_query_latency": True,
                "candidate_transport_and_adapter_prepare_included": True,
                "same_runtime_component_warmup": True,
                "offline_index_build_excluded": True,
                "result_file_write_excluded_from_query_latency": True,
            },
            "candidate_events": row["candidate_events"],
            "shard_count": row["shard_count"],
            "imports_funnelq_verifier": False,
            "proof_generation_included": False,
            "verification_included": False,
        })
    write_csv(outdir / "mbft_nft_sharded_query_summary.csv", rows)
    report = {
        "mode": "MBFT_NFT_sharded_persistent_full_binding_v2",
        "build_manifest": str(args.manifest.resolve()),
        "atomic_summary": str(atomic_dir / "atomic_selection_summary.json"),
        "atomic_process_wall_s": atomic_proc["wall_s"],
        "matcher_process_wall_s": matcher_proc["wall_s"],
        "rows": rows,
        "timing_contract": {
            "query_latency_s": (
                "MBFT atomic tree query + candidate JSONL write/load + "
                "adapter preparation + exact semantic match + full-result "
                "canonicalization; persistent store load and final CSV write excluded"
            ),
            "offline_index_build_excluded": True,
            "result_csv_write_excluded": True,
            "warmup_runs": max(0, args.warmup_runs),
        },
        "implementation_boundary": (
            "MBFT performs no-proof indexed atomic event selection. The independent compact adapter "
            "matcher materializes every full event binding and imports no FunnelQ verifier. "
            "Proof generation and client verification are both excluded in this query-only protocol."
        ),
    }
    atomic_write_json(outdir / "mbft_nft_sharded_query_report.json", report)

    if len(execution_reports) == 1:
        atomic_write_json(outdir / "execution_report.json", execution_reports[0])
    else:
        atomic_write_json(outdir / "execution_report.json", {"system": "MBFT-Adapt", "queries": execution_reports})
    print(json.dumps(report, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
