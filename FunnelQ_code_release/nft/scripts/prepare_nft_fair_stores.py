#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

from nft_fair_runner_lib import NFT_EVENT_ATOMS, WORK_ROOT, ensure_dir, next_numbered_dir, nft_store_paths, run_monitored, write_csv, write_json

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Prepare NFT stores for fair-cost experiments.")
    ap.add_argument("--event-atoms-csv", default=str(NFT_EVENT_ATOMS))
    ap.add_argument(
        "--outdir",
        default="",
        help="Optional output store directory. Defaults to the next runs/nft_stores_### directory.",
    )
    ap.add_argument("--timeout-sec", type=int, default=None)
    ap.add_argument("--max-rss-mb", type=float, default=18000.0)
    ap.add_argument("--compact", action="store_true", help="Build the HVCEQ NFT store in compact generic sidecar mode.")
    ap.add_argument("--omit-closure-payload", action="store_true", help="Do not persist nft_closure_payload in the HVCEQ store.")
    ap.add_argument("--build-closure-local", action="store_true", help="Enable v20 QuerySpec metadata and query-local closure edge arrays for paper-aligned HVCEQ local exact.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    event_atoms = Path(args.event_atoms_csv)
    if not event_atoms.exists():
        raise FileNotFoundError(f"Missing NFT event atoms CSV: {event_atoms}")
    stores_dir = Path(args.outdir).resolve() if str(args.outdir).strip() else next_numbered_dir(WORK_ROOT, "nft_stores")
    if stores_dir.exists() and any(stores_dir.iterdir()):
        raise FileExistsError(f"Output directory already exists and is not empty: {stores_dir}")
    ensure_dir(stores_dir)
    paths = nft_store_paths(stores_dir)
    hvceq_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "build_nft_funnelq_compact_store.py"),
        "--event-atoms-csv", str(event_atoms),
        "--outdir", str(paths["hvceq_db"].parent),
        "--sqlite-out", str(paths["hvceq_db"]),
        "--report-json", str(paths["hvceq_db"].parent / "compact_build_report.json"),
        "--max-rss-mb", str(float(args.max_rss_mb)),
        "--force",
    ]
    jobs = [
        {"component": "nft_funnelq_compact_v4", "cmd": hvceq_cmd, "output": paths["hvceq_db"]},
        {"component": "nft_funnelq_compact_v4_validate", "cmd": [sys.executable, str(ROOT / "scripts/validate_nft_funnelq_compact_store.py"), "--db", str(paths["hvceq_db"]), "--out-json", str(paths["hvceq_db"].parent / "compact_validation.json")], "output": paths["hvceq_db"]},
        {"component": "nft_sql", "cmd": [sys.executable, str(ROOT / "systems/nft/sql/build_nft_sql_store.py"), "--event-atoms-csv", str(event_atoms), "--sqlite-out", str(paths["sql_db"]), "--report-json", str(paths["sql_db"].parent / "build_report.json")], "output": paths["sql_db"]},
        {"component": "nft_hgmatch", "cmd": [sys.executable, str(ROOT / "systems/nft/hgmatch/build_nft_hgmatch_store.py"), "--event-atoms-csv", str(event_atoms), "--sqlite-out", str(paths["hgmatch_db"]), "--report-json", str(paths["hgmatch_db"].parent / "build_report.json")], "output": paths["hgmatch_db"]},
    ]
    rows: List[Dict[str, Any]] = []
    for job in jobs:
        outdir = stores_dir / f"build_{job['component']}"
        print(f"[prepare-nft][{job['component']}] start", flush=True)
        meta = run_monitored(job["cmd"], outdir, timeout_sec=args.timeout_sec, max_rss_mb=float(args.max_rss_mb))
        output = Path(job["output"])
        rows.append({"component": job["component"], "status": meta["status"], "return_code": meta["return_code"], "output": str(output), "storage_size_bytes": output.stat().st_size if output.exists() else 0, **meta})
        print(f"[prepare-nft][{job['component']}] done status={meta['status']}", flush=True)
        if meta["status"] != "ok":
            write_csv(stores_dir / "nft_prepare_build_summary.csv", rows)
            raise RuntimeError(f"{job['component']} failed; see {outdir}")
    write_csv(stores_dir / "nft_prepare_build_summary.csv", rows)
    write_json(stores_dir / "nft_stores_manifest.json", {"stores_dir": str(stores_dir), "event_atoms_csv": str(event_atoms), "paths": {k: str(v) for k, v in paths.items()}, "build_rows": rows})
    (WORK_ROOT / "LATEST_NFT_STORES_DIR.txt").write_text(str(stores_dir), encoding="utf-8")
    print(f"[prepare-nft] stores ready: {stores_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
