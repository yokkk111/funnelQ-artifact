#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path

import psutil

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
FINAL5 = ["C1", "C2", "L3_RETURN", "L6_NEG_RETURN_SUPPORT", "H5_23"]


def run(cmd: list[str], cwd: Path) -> tuple[dict, str, str]:
    t0 = time.perf_counter()
    p = subprocess.Popen(cmd, cwd=str(cwd), text=True, encoding="utf-8", errors="replace", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    peak = 0
    while p.poll() is None:
        try:
            proc = psutil.Process(p.pid)
            rss = proc.memory_info().rss
            for child in proc.children(recursive=True):
                try: rss += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied): pass
            peak = max(peak, rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        time.sleep(0.05)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}\n{stdout[-4000:]}\n{stderr[-4000:]}")
    return {"wall_s": time.perf_counter() - t0, "peak_tree_rss_mb": peak / (1024 * 1024)}, stdout, stderr


def ensure_exe(raw: str) -> Path:
    if raw:
        path = Path(raw).resolve()
        if not path.is_file(): raise FileNotFoundError(path)
        return path
    build = HERE / "build"; build.mkdir(exist_ok=True)
    run(["cmake", "-S", str(HERE / "mbft"), "-B", str(build), "-DCMAKE_BUILD_TYPE=Release"], HERE)
    run(["cmake", "--build", str(build), "--config", "Release", "--target", "mbft_nft_adapt", "-j", "2"], HERE)
    for path in (build / "mbft_nft_adapt", build / "Release" / "mbft_nft_adapt.exe", build / "mbft_nft_adapt.exe"):
        if path.is_file(): return path
    raise FileNotFoundError("mbft_nft_adapt executable")


def write_csv(path: Path, rows: list[dict]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields: fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)


def compare_sets(qid: str, server_csv: Path, client_csv: Path, out: Path) -> dict:
    cmd = [
        sys.executable, str(ROOT / "tools" / "compare_nft_full_instances.py"),
        "--query", qid, "--out", str(out), f"server={server_csv}", f"client={client_csv}",
    ]
    proc = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True)
    if not out.exists():
        raise RuntimeError(proc.stdout + proc.stderr)
    report = json.loads(out.read_text(encoding="utf-8"))
    if proc.returncode != 0:
        raise RuntimeError(f"MBFT full-binding replay mismatch for {qid}: {json.dumps(report, indent=2)}")
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description="MBFT NFT full-binding query and original Hint-VO verification.")
    ap.add_argument("--events", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--exe", default="")
    ap.add_argument("--queries", default=",".join(FINAL5))
    ap.add_argument("--combine", type=int, default=0)
    ap.add_argument("--fp-rate", type=float, default=0.001)
    ap.add_argument("--root-limit", type=int, default=0)
    args = ap.parse_args()
    out = Path(args.outdir).resolve(); out.mkdir(parents=True, exist_ok=True)
    exe = ensure_exe(args.exe)
    atomic = out / "atomic"
    atomic_proc, so, se = run([
        str(exe), "run-all", "--events", str(Path(args.events).resolve()), "--outdir", str(atomic),
        "--combine", str(args.combine), "--fp-rate", str(args.fp_rate),
    ], HERE)
    (out / "atomic.stdout.txt").write_text(so, encoding="utf-8")
    (out / "atomic.stderr.txt").write_text(se, encoding="utf-8")
    summary = json.loads((atomic / "atomic_selection_summary.json").read_text(encoding="utf-8"))
    qids = [q.strip().upper() for q in args.queries.split(",") if q.strip()]

    def run_match(candidate_root: str, target: Path, label: str):
        cmd = [
            sys.executable, str(HERE / "nft_semantic_matcher.py"), "--events-jsonl",
            str(atomic / candidate_root / "nfttrade.jsonl"), str(atomic / candidate_root / "nfttransfer.jsonl"),
            "--query-dir", str(HERE / "queries"), "--queries", ",".join(qids), "--outdir", str(target),
        ]
        if args.root_limit: cmd += ["--root-limit", str(args.root_limit)]
        proc, stdout, stderr = run(cmd, HERE)
        (out / f"{label}.stdout.txt").write_text(stdout, encoding="utf-8")
        (out / f"{label}.stderr.txt").write_text(stderr, encoding="utf-8")
        return proc, json.loads((target / "matcher_report.json").read_text(encoding="utf-8"))

    server_dir = out / "server_claims"
    server_proc, server_report = run_match("server_candidates", server_dir, "server_match")
    client_dir = out / "client_recompute"
    client_proc, client_report = run_match("verified_candidates", client_dir, "client_match")

    atomic_query_s = sum(float(v.get("query_time_us", 0)) / 1e6 for v in summary["query_only"].values())
    candidate_write_s = sum(float(v.get("candidate_write_time_us", 0)) / 1e6 for v in summary["query_only"].values())
    vo_gen_s = sum((float(v.get("vo_generation_time_us", 0)) + float(v.get("proof_write_time_us", 0))) / 1e6 for v in summary["proofs"].values())
    mbft_verify_s = sum((float(v.get("proof_load_time_us", 0)) + float(v.get("verify_time_us", 0)) + float(v.get("candidate_write_time_us", 0))) / 1e6 for v in summary["verification"].values())
    proof_bytes = sum(int(v.get("proof_bytes", 0)) for v in summary["proofs"].values())
    atomic_valid = all(bool(v.get("valid")) for v in summary["verification"].values())
    server_by = {r["query"]: r for r in server_report["queries"]}
    client_by = {r["query"]: r for r in client_report["queries"]}
    rows = []
    for qid in qids:
        s = server_by[qid]; c = client_by[qid]
        cmp = compare_sets(qid, Path(s["instances_csv"]), Path(c["instances_csv"]), out / "comparisons" / f"{qid}.json")
        server_semantic_s = float(s["match_s"]) + float(s.get("result_canonicalize_s", 0.0))
        client_semantic_s = float(c["match_s"]) + float(c.get("result_canonicalize_s", 0.0))
        server_candidate_load_s = float(server_report.get("candidate_load_s", 0.0))
        client_candidate_load_s = float(client_report.get("candidate_load_s", 0.0))
        query_s = atomic_query_s + candidate_write_s + server_candidate_load_s + float(server_report.get("prepare_s", 0.0)) + server_semantic_s
        verification_s = vo_gen_s + mbft_verify_s + client_candidate_load_s + float(client_report.get("prepare_s", 0.0)) + client_semantic_s
        rows.append({
            "system": "MBFT-NFT-Adapt", "query": qid, "result_semantics": "full_event_binding_v1",
            "result_count": int(s["result_count"]), "result_digest_sha256": s["result_digest_sha256"],
            "atomic_retrieval_s": atomic_query_s, "server_candidate_jsonl_write_s": candidate_write_s,
            "server_candidate_jsonl_load_s": server_candidate_load_s,
            "server_prepare_s": float(server_report.get("prepare_s", 0.0)),
            "server_full_binding_match_s": server_semantic_s, "query_s": query_s,
            "vo_generation_s": vo_gen_s, "client_mbft_verify_s": mbft_verify_s,
            "client_candidate_jsonl_load_s": client_candidate_load_s,
            "client_prepare_s": float(client_report.get("prepare_s", 0.0)),
            "client_full_binding_recompute_s": client_semantic_s, "verification_overhead_s": verification_s,
            "query_plus_verification_s": query_s + verification_s, "vo_size_bytes": proof_bytes,
            "vo_size_bytes_amortized_batch": proof_bytes / len(qids), "candidate_events": server_report["event_count"],
            "candidate_root_closures": server_report["root_count"], "candidate_closure_completeness": atomic_valid,
            "full_binding_result_correctness": bool(cmp["all_pass"]), "all_pass": atomic_valid and bool(cmp["all_pass"]),
            "atomic_process_peak_tree_rss_mb": atomic_proc["peak_tree_rss_mb"],
            "server_match_peak_tree_rss_mb": server_proc["peak_tree_rss_mb"],
            "client_recompute_peak_tree_rss_mb": client_proc["peak_tree_rss_mb"],
        })
    write_csv(out / "mbft_nft_query_verify_summary.csv", rows)
    report = {
        "mode": "MBFT_NFT_query_plus_original_hint_vo_full_binding_v2",
        "result_semantics": "full_event_binding_v1", "proof_bytes": proof_bytes, "rows": rows,
        "all_pass": bool(rows and all(r["all_pass"] for r in rows)),
        "security_statement": (
            "MergeBloom Hint VO authenticates the complete atomic NFT candidate set. An independent client adapter "
            "then reconstructs and compares the exact canonical full-event-binding result set."
        ),
        "implementation_boundary": "MBFT atomic authentication is unchanged; the NFT semantic matcher imports no FunnelQ verifier.",
    }
    (out / "mbft_nft_query_verify_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["all_pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
