from __future__ import annotations

import csv
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts/run_final_queryset_4scheme_once.py"
OUT = ROOT / "data/month3_build_work/full_4scheme_rust_refined_10repeats_formal"
REPEATS = range(1, 11)
EXPECTED_ROWS_PER_REPEAT = 80


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def aggregate() -> list[dict[str, object]]:
    combined: list[dict[str, object]] = []
    for repeat in REPEATS:
        result_path = OUT / f"repeat_{repeat:02d}" / "results.csv"
        for row in read_rows(result_path):
            combined.append({"repeat": repeat, **row})
    write_csv(OUT / "all_10repeats_results.csv", combined)
    return combined


def snapshot(active_repeat: int | None, return_codes: dict[int, int]) -> None:
    repeats: list[dict[str, object]] = []
    for repeat in REPEATS:
        rows = read_rows(OUT / f"repeat_{repeat:02d}" / "results.csv")
        repeats.append(
            {
                "repeat": repeat,
                "rows": len(rows),
                "ok": sum(row.get("status") == "ok" for row in rows),
                "non_ok": sum(row.get("status") != "ok" for row in rows),
                "return_code": return_codes.get(repeat),
                "complete": len(rows) == EXPECTED_ROWS_PER_REPEAT,
            }
        )
    payload = {
        "updated_at": datetime.now().astimezone().isoformat(),
        "active_repeat": active_repeat,
        "ordering": "complete all 80 cells in repeat N before repeat N+1",
        "repeats": repeats,
        "total_rows_recorded": sum(int(item["rows"]) for item in repeats),
        "expected_total_rows": EXPECTED_ROWS_PER_REPEAT * len(REPEATS),
        "measurement_policy": "one warmup then one measured query phase",
        "timeout_sec_per_cell": 3600,
        "job_commit_hard_limit_mb": 18432,
        "system_available_floor_mb": 2048,
    }
    (OUT / "controller_status.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    return_codes: dict[int, int] = {}
    snapshot(None, return_codes)

    for repeat in REPEATS:
        repeat_dir = OUT / f"repeat_{repeat:02d}"
        existing = read_rows(repeat_dir / "results.csv")
        if len(existing) == EXPECTED_ROWS_PER_REPEAT:
            print(
                f"[repeat {repeat}/10] already has {len(existing)} rows; skip",
                flush=True,
            )
            snapshot(None, return_codes)
            aggregate()
            continue

        repeat_dir.mkdir(parents=True, exist_ok=True)
        snapshot(repeat, return_codes)
        print(
            f"[repeat {repeat}/10] start; existing_rows={len(existing)}",
            flush=True,
        )
        started = time.perf_counter()
        completed = subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--warm-query-once",
                "--out-dir",
                str(repeat_dir),
            ],
            cwd=ROOT,
            check=False,
        )
        return_codes[repeat] = completed.returncode
        elapsed = time.perf_counter() - started
        rows = read_rows(repeat_dir / "results.csv")
        aggregate()
        snapshot(None, return_codes)
        print(
            f"[repeat {repeat}/10] end; return_code={completed.returncode} "
            f"rows={len(rows)}/{EXPECTED_ROWS_PER_REPEAT} elapsed_s={elapsed:.1f}",
            flush=True,
        )

        # A match-count mismatch is a correctness failure, not an ordinary
        # timeout/memory skip. Preserve the evidence and stop the formal run.
        if completed.returncode == 2:
            print("correctness mismatch detected; stop formal run", flush=True)
            return 2

    rows = aggregate()
    snapshot(None, return_codes)
    complete = len(rows) == EXPECTED_ROWS_PER_REPEAT * len(REPEATS)
    print(
        f"formal experiment finished; rows={len(rows)}/"
        f"{EXPECTED_ROWS_PER_REPEAT * len(REPEATS)}",
        flush=True,
    )
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
