#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List

from fair_cost_runner_lib import (
    COMBINED_EVENT_ATOMS,
    COMBINED_RAW,
    DATASET_NAME,
    ROOT,
    SAME_TX_QUERIES,
    EPISODE_QUERIES,
    RAW_FILES,
    WORK_ROOT,
    ensure_dir,
    next_numbered_dir,
    run_monitored,
    store_paths,
    write_csv,
    write_json,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Prepare dataset stores for fair-cost A/B experiments.")
    ap.add_argument("--reuse-existing-combined-stores", action="store_true", help="Copy existing combined stores instead of rebuilding.")
    ap.add_argument("--timeout-sec", type=int, default=None)
    ap.add_argument("--max-rss-mb", type=float, default=None)
    return ap.parse_args()


def file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def copy_existing_stores(stores_dir: Path) -> List[Dict[str, Any]]:
    src_map = {
        "hvceq_same_tx": ROOT / "data/week2_build_work/combined_same_tx_runtime_from_atoms/same_tx_runtime.db",
        "hvceq_episode": ROOT / "data/week2_build_work/combined_anchored_episode_runtime_sidecar/anchored_episode_runtime_sidecar.db",
        "sql_same_tx": ROOT / "data/week2_build_work/combined_sql_same_tx/same_tx_relational_flat.db",
        "sql_episode": ROOT / "data/week2_build_work/combined_sql_anchored_episode/anchored_episode_relational_flat.db",
        "hgmatch_same_tx": ROOT / "data/week2_build_work/combined_hgmatch_same_tx_store/hgmatch_same_tx_rust_fair.db",
        "hgmatch_episode": ROOT / "data/week2_build_work/combined_hgmatch_episode_store/hgmatch_episode_rust_fair.db",
    }
    dst_map = store_paths(stores_dir)
    rows = []
    for key, src in src_map.items():
        dst_key = f"{key}_db"
        dst = dst_map[dst_key]
        ensure_dir(dst.parent)
        shutil.copy2(src, dst)
        rows.append({
            "component": key,
            "action": "copy_existing_combined_store",
            "source": str(src),
            "output": str(dst),
            "status": "ok",
            "storage_size_bytes": file_size(dst),
        })
    return rows


def build_commands(stores_dir: Path) -> List[Dict[str, Any]]:
    paths = store_paths(stores_dir)
    raw = RAW_FILES
    same_tx_builder = (
        ROOT / "systems/hvceq/same_tx/build_same_tx_runtime_from_atoms_streaming.py"
        if DATASET_NAME != "twoweek"
        else ROOT / "systems/hvceq/same_tx/build_same_tx_runtime_from_atoms.py"
    )
    hgmatch_same_tx_builder = (
        ROOT / "scripts/build_hgmatch_same_tx_streaming.py"
        if DATASET_NAME != "twoweek"
        else ROOT / "systems/hgmatch/same_tx/bin/build_hgmatch_same_tx_store_rust_fair.exe"
    )
    return [
        {
            "component": "hvceq_same_tx",
            "cmd": [
                sys.executable,
                str(same_tx_builder),
                "--builder-script",
                str(ROOT / "systems/hvceq/same_tx/builder.py"),
                "--event-atoms-csv",
                str(COMBINED_EVENT_ATOMS),
                "--outdir",
                str(paths["hvceq_same_tx_db"].parent),
                "--sqlite-out",
                str(paths["hvceq_same_tx_db"]),
            ],
            "output_db": paths["hvceq_same_tx_db"],
        },
        {
            "component": "hvceq_episode",
            "cmd": [
                sys.executable,
                str(ROOT / "systems/hvceq/episode/build_episode_sidecar.py"),
                "--event-atoms-csv",
                str(COMBINED_EVENT_ATOMS),
                "--outdir",
                str(paths["hvceq_episode_db"].parent),
                "--sqlite-out",
                str(paths["hvceq_episode_db"]),
                "--windows-sec",
                "60,300,600",
            ],
            "output_db": paths["hvceq_episode_db"],
        },
        {
            "component": "sql_same_tx",
            "cmd": [
                sys.executable,
                str(ROOT / "systems/sql/same_tx/build_relational_same_tx_flat_store.py"),
                "--flashloans",
                str(raw["flashloans"]),
                "--borrow",
                str(raw["borrow"]),
                "--dex",
                str(raw["dex"]),
                "--sqlite-out",
                str(paths["sql_same_tx_db"]),
                "--report-json",
                str(paths["sql_same_tx_db"].parent / "build_report.json"),
            ],
            "output_db": paths["sql_same_tx_db"],
        },
        {
            "component": "sql_episode",
            "cmd": [
                sys.executable,
                str(ROOT / "systems/sql/episode/build_relational_anchored_episode_flat_store.py"),
                "--flashloans",
                str(raw["flashloans"]),
                "--borrow",
                str(raw["borrow"]),
                "--dex",
                str(raw["dex"]),
                "--sqlite-out",
                str(paths["sql_episode_db"]),
                "--report-json",
                str(paths["sql_episode_db"].parent / "build_report.json"),
            ],
            "output_db": paths["sql_episode_db"],
        },
        {
            "component": "hgmatch_same_tx",
            "cmd": [
                *([sys.executable] if hgmatch_same_tx_builder.suffix == ".py" else []),
                str(hgmatch_same_tx_builder),
                "--flashloans",
                str(raw["flashloans"]),
                "--borrow",
                str(raw["borrow"]),
                "--dex",
                str(raw["dex"]),
                "--outdir",
                str(paths["hgmatch_same_tx_db"].parent),
                "--sqlite-out",
                str(paths["hgmatch_same_tx_db"]),
            ],
            "output_db": paths["hgmatch_same_tx_db"],
        },
        {
            "component": "hgmatch_episode",
            "cmd": [
                str(ROOT / "systems/hgmatch/episode/bin/build_hgmatch_same_tx_store_rust_fair.exe"),
                "--flashloans",
                str(raw["flashloans"]),
                "--borrow",
                str(raw["borrow"]),
                "--dex",
                str(raw["dex"]),
                "--outdir",
                str(paths["hgmatch_episode_db"].parent),
                "--sqlite-out",
                str(paths["hgmatch_episode_db"]),
            ],
            "output_db": paths["hgmatch_episode_db"],
        },
    ]


def prebuild_sql_indexes(stores_dir: Path, timeout_sec: int | None, max_rss_mb: float | None) -> List[Dict[str, Any]]:
    paths = store_paths(stores_dir)
    jobs = [
        ("sql_same_tx_generic_indexes", "same_tx", paths["sql_same_tx_db"], SAME_TX_QUERIES),
        ("sql_episode_generic_indexes", "episode", paths["sql_episode_db"], EPISODE_QUERIES),
    ]
    rows: List[Dict[str, Any]] = []
    for component, mode, db, queries in jobs:
        outdir = stores_dir / component
        cmd = [
            sys.executable,
            str(ROOT / "scripts/prebuild_sql_query_indexes.py"),
            "--db",
            str(db),
            "--mode",
            mode,
            "--queries",
            ",".join(queries),
            "--report-json",
            str(outdir / "prebuild_indexes_report.json"),
        ]
        print(f"[prepare][{component}] start", flush=True)
        before_bytes = file_size(db)
        meta = run_monitored(cmd, outdir, timeout_sec=timeout_sec, max_rss_mb=max_rss_mb)
        after_bytes = file_size(db)
        report_path = outdir / "prebuild_indexes_report.json"
        timing = {}
        if report_path.exists():
            timing = json.loads(report_path.read_text(encoding="utf-8")).get("timing_s", {})
        rows.append({
            "component": component,
            "action": "prebuild_generic_indexes_once",
            "mode": mode,
            "db": str(db),
            "status": meta["status"],
            "return_code": meta["return_code"],
            "build_time_s": timing.get("total_s", ""),
            "storage_size_bytes": after_bytes,
            "storage_before_indexes_bytes": before_bytes,
            "storage_after_indexes_bytes": after_bytes,
            "storage_generic_index_delta_bytes": max(0, after_bytes - before_bytes),
            **meta,
        })
        if meta["status"] != "ok":
            raise RuntimeError(f"{component} failed; see {outdir}")
    return rows


def main() -> int:
    args = parse_args()
    stores_dir = next_numbered_dir(WORK_ROOT, "stores")
    ensure_dir(stores_dir)
    rows: List[Dict[str, Any]] = []

    if args.reuse_existing_combined_stores:
        rows.extend(copy_existing_stores(stores_dir))
    else:
        for job in build_commands(stores_dir):
            outdir = stores_dir / f"build_{job['component']}"
            print(f"[prepare][{job['component']}] start", flush=True)
            meta = run_monitored(job["cmd"], outdir, timeout_sec=args.timeout_sec, max_rss_mb=args.max_rss_mb)
            output_db = Path(job["output_db"])
            rows.append({
                "component": job["component"],
                "action": "build_store",
                "status": meta["status"],
                "return_code": meta["return_code"],
                "output": str(output_db),
                "storage_size_bytes": file_size(output_db),
                **meta,
            })
            if meta["status"] != "ok":
                write_csv(stores_dir / "prepare_build_summary.csv", rows)
                raise RuntimeError(f"{job['component']} failed; see {outdir}")

    rows.extend(prebuild_sql_indexes(stores_dir, args.timeout_sec, args.max_rss_mb))
    write_csv(stores_dir / "prepare_build_summary.csv", rows)
    write_json(stores_dir / "stores_manifest.json", {
        "stores_dir": str(stores_dir),
        "combined_event_atoms": str(COMBINED_EVENT_ATOMS),
        "combined_raw_dir": str(COMBINED_RAW),
        "paths": {k: str(v) for k, v in store_paths(stores_dir).items()},
        "build_rows": rows,
    })
    (WORK_ROOT / "LATEST_STORES_DIR.txt").write_text(str(stores_dir), encoding="utf-8")
    print(f"[prepare] stores ready: {stores_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

