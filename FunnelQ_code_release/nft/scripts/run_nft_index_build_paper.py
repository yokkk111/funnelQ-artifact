#!/usr/bin/env python3
"""Build final NFT index/store artifacts for paper reporting.

This runner intentionally avoids the old deleted prepare helper and records a
single auditable build of:

  * HVCEQ final compact store used by the NFT final-five runner;
  * Enhanced SQL generic relational store;
  * HGMatch generic hypergraph-style store.

For HVCEQ, the build includes the final compaction/VACUUM step, and the
reported final size is the post-compaction SQLite file size.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psutil


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVENT_ATOMS = ROOT / "data" / "nft_workload" / "derived" / "event_atoms_nft.csv"
DEFAULT_OUTDIR = ROOT / "runs" / "nft_index_build_paper_20260708"


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: list[str] = []
    for row in rows:
        for k in row:
            if k not in keys:
                keys.append(k)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def tree_rss_mb(proc: psutil.Process) -> float:
    total = 0
    procs = [proc]
    try:
        procs.extend(proc.children(recursive=True))
    except psutil.Error:
        pass
    for p in procs:
        try:
            total += p.memory_info().rss
        except psutil.Error:
            pass
    return total / 1024.0 / 1024.0


def terminate_tree(proc: psutil.Process) -> None:
    children = []
    try:
        children = proc.children(recursive=True)
    except psutil.Error:
        pass
    for p in children:
        try:
            p.terminate()
        except psutil.Error:
            pass
    try:
        proc.terminate()
    except psutil.Error:
        pass
    gone, alive = psutil.wait_procs(children + [proc], timeout=5)
    for p in alive:
        try:
            p.kill()
        except psutil.Error:
            pass


def run_monitored(
    name: str,
    cmd: list[str],
    outdir: Path,
    *,
    max_rss_mb: float,
    timeout_sec: float | None,
    cwd: Path,
) -> dict[str, Any]:
    outdir.mkdir(parents=True, exist_ok=True)
    stdout_path = outdir / "stdout.txt"
    stderr_path = outdir / "stderr.txt"
    samples_path = outdir / "monitor_samples.csv"
    t0 = time.perf_counter()
    peak = 0.0
    status = "ok"
    killed_for_memory = False
    killed_for_timeout = False
    with stdout_path.open("w", encoding="utf-8", errors="replace") as stdout, stderr_path.open("w", encoding="utf-8", errors="replace") as stderr:
        p = subprocess.Popen(cmd, cwd=str(cwd), stdout=stdout, stderr=stderr)
        ps = psutil.Process(p.pid)
        with samples_path.open("w", newline="", encoding="utf-8") as sf:
            sw = csv.DictWriter(sf, fieldnames=["t_s", "rss_mb"])
            sw.writeheader()
            while True:
                now = time.perf_counter()
                ret = p.poll()
                rss = 0.0
                if ret is None:
                    try:
                        rss = tree_rss_mb(ps)
                    except psutil.Error:
                        rss = 0.0
                    peak = max(peak, rss)
                    sw.writerow({"t_s": round(now - t0, 3), "rss_mb": round(rss, 3)})
                    sf.flush()
                    if max_rss_mb and rss > max_rss_mb:
                        status = "memory_killed"
                        killed_for_memory = True
                        terminate_tree(ps)
                        break
                    if timeout_sec and now - t0 > timeout_sec:
                        status = "timeout"
                        killed_for_timeout = True
                        terminate_tree(ps)
                        break
                    time.sleep(0.5)
                else:
                    status = "ok" if ret == 0 else "error"
                    break
    wall = time.perf_counter() - t0
    return_code = p.poll()
    report = {
        "name": name,
        "cmd": cmd,
        "status": status,
        "return_code": return_code,
        "wall_s": wall,
        "peak_rss_mb": peak,
        "killed_for_memory": killed_for_memory,
        "killed_for_timeout": killed_for_timeout,
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "samples": str(samples_path),
    }
    write_json(outdir / "monitor_report.json", report)
    if status != "ok":
        raise RuntimeError(f"{name} failed with status={status}; see {outdir}")
    return report


def db_size(path: Path) -> dict[str, Any]:
    size = path.stat().st_size if path.exists() else 0
    return {"size_bytes": size, "size_mb": round(size / 1024.0 / 1024.0, 3)}


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-atoms-csv", default=str(DEFAULT_EVENT_ATOMS))
    ap.add_argument("--outdir", default=str(DEFAULT_OUTDIR))
    ap.add_argument("--max-rss-mb", type=float, default=18000.0)
    ap.add_argument("--timeout-sec", type=float, default=7200.0)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    event_atoms = Path(args.event_atoms_csv).resolve()
    outdir = Path(args.outdir).resolve()
    if not event_atoms.exists():
        raise FileNotFoundError(event_atoms)
    if outdir.exists() and any(outdir.iterdir()):
        if not args.force:
            raise FileExistsError(f"Output directory exists and is not empty: {outdir}; pass --force to remove it.")
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    hv_db = outdir / "hvceq" / "nft_hvceq_episode_sidecar.db"
    sql_db = outdir / "sql" / "nft_sql_store.db"
    hg_db = outdir / "hgmatch" / "nft_hgmatch_store.db"
    py = sys.executable

    rows: list[dict[str, Any]] = []

    hv_steps = [
        (
            "funnelq_compact_store_v4",
            [
                py,
                str(ROOT / "scripts" / "build_nft_funnelq_compact_store.py"),
                "--event-atoms-csv", str(event_atoms),
                "--outdir", str(hv_db.parent),
                "--sqlite-out", str(hv_db),
                "--report-json", str(hv_db.parent / "compact_build_report.json"),
                "--max-rss-mb", str(args.max_rss_mb),
                "--force",
            ],
        ),
        (
            "funnelq_compact_store_v4_validate",
            [
                py,
                str(ROOT / "scripts" / "validate_nft_funnelq_compact_store.py"),
                "--db", str(hv_db),
                "--out-json", str(hv_db.parent / "compact_validation.json"),
            ],
        ),
    ]

    for name, cmd in hv_steps:
        print(f"[paper-build] {name} start", flush=True)
        rep = run_monitored(name, cmd, outdir / f"build_{name}", max_rss_mb=args.max_rss_mb, timeout_sec=args.timeout_sec, cwd=ROOT)
        row = {"system": "HVCEQ", "component": name, **rep, **db_size(hv_db)}
        rows.append(row)
        write_csv(outdir / "nft_index_build_steps.csv", rows)
        print(f"[paper-build] {name} done wall_s={rep['wall_s']:.3f} size_mb={row['size_mb']}", flush=True)

    for system, name, db, cmd in [
        (
            "SQL",
            "sql_store",
            sql_db,
            [
                py,
                str(ROOT / "systems" / "nft" / "sql" / "build_nft_sql_store.py"),
                "--event-atoms-csv",
                str(event_atoms),
                "--sqlite-out",
                str(sql_db),
                "--report-json",
                str(sql_db.parent / "build_report.json"),
            ],
        ),
        (
            "HGMatch",
            "hgmatch_store",
            hg_db,
            [
                py,
                str(ROOT / "systems" / "nft" / "hgmatch" / "build_nft_hgmatch_store.py"),
                "--event-atoms-csv",
                str(event_atoms),
                "--sqlite-out",
                str(hg_db),
                "--report-json",
                str(hg_db.parent / "build_report.json"),
            ],
        ),
    ]:
        print(f"[paper-build] {name} start", flush=True)
        rep = run_monitored(name, cmd, outdir / f"build_{name}", max_rss_mb=args.max_rss_mb, timeout_sec=args.timeout_sec, cwd=ROOT)
        row = {"system": system, "component": name, **rep, **db_size(db)}
        rows.append(row)
        write_csv(outdir / "nft_index_build_steps.csv", rows)
        print(f"[paper-build] {name} done wall_s={rep['wall_s']:.3f} size_mb={row['size_mb']}", flush=True)

    hv_rows = [r for r in rows if r["system"] == "HVCEQ"]
    summary = [
        {
            "system": "HVCEQ",
            "build_time_s": round(sum(float(r["wall_s"]) for r in hv_rows), 6),
            "final_size_mb": db_size(hv_db)["size_mb"],
            "peak_rss_mb": round(max(float(r["peak_rss_mb"]) for r in hv_rows), 3),
            "db": str(hv_db),
            "notes": "DeFi-style compact v4: one integer event row plus one sliding-window summary per root; refined postings are built at query time.",
        },
        {
            "system": "SQL",
            "build_time_s": round(float(next(r for r in rows if r["system"] == "SQL")["wall_s"]), 6),
            "final_size_mb": db_size(sql_db)["size_mb"],
            "peak_rss_mb": round(float(next(r for r in rows if r["system"] == "SQL")["peak_rss_mb"]), 3),
            "db": str(sql_db),
            "notes": "Generic relational store with role-transition table and SQL indexes.",
        },
        {
            "system": "HGMatch",
            "build_time_s": round(float(next(r for r in rows if r["system"] == "HGMatch")["wall_s"]), 6),
            "final_size_mb": db_size(hg_db)["size_mb"],
            "peak_rss_mb": round(float(next(r for r in rows if r["system"] == "HGMatch")["peak_rss_mb"]), 3),
            "db": str(hg_db),
            "notes": "Generic hypergraph-style store and postings/indexes.",
        },
    ]
    # Preserve builder-reported timings and row counts for audit.
    audit = {
        "event_atoms_csv": str(event_atoms),
        "outdir": str(outdir),
        "summary": summary,
        "steps": rows,
        "funnelq_compact_build_report": load_json(hv_db.parent / "compact_build_report.json"),
        "sql_build_report": load_json(sql_db.parent / "build_report.json"),
        "hgmatch_build_report": load_json(hg_db.parent / "build_report.json"),
    }
    write_csv(outdir / "nft_index_build_summary.csv", summary)
    write_json(outdir / "nft_index_build_report.json", audit)
    print(json.dumps({"summary_csv": str(outdir / "nft_index_build_summary.csv"), "summary": summary}, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
