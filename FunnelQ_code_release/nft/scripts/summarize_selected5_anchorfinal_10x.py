#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def stats(values: list[float]) -> dict[str, float]:
    return {
        "mean": statistics.fmean(values),
        "stddev": statistics.stdev(values) if len(values) > 1 else 0.0,
        "min": min(values),
        "max": max(values),
    }


def summarize(
    rows: list[dict[str, str]], group_key: str, success_predicate
) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[(row["query"], row[group_key])].append(row)
    output: list[dict[str, object]] = []
    for (query, method), group in sorted(groups.items()):
        successful = [row for row in group if success_predicate(row)]
        latency = [float(row["query_latency_s"]) for row in successful]
        memory = [float(row["peak_job_commit_mb"]) for row in successful]
        latency_stats = stats(latency) if latency else {}
        memory_stats = stats(memory) if memory else {}
        digests = sorted({row.get("result_digest_sha256", "") for row in successful})
        result_counts = sorted({row.get("result_count", "") for row in successful})
        record: dict[str, object] = {
            "query": query,
            group_key: method,
            "successful_runs": len(successful),
            "total_runs": len(group),
            "result_count": result_counts[0] if len(result_counts) == 1 else "|".join(result_counts),
            "digest_consistent": len(digests) == 1,
        }
        for prefix, values in (("query_latency_s", latency_stats), ("peak_job_commit_mb", memory_stats)):
            for name, value in values.items():
                record[f"{prefix}_{name}"] = value
        if group_key == "variant" and successful:
            for key in (
                "closure_universe",
                "cand0_anchor_hits",
                "cand1_base_hits",
                "cand2_refined_hits",
            ):
                record[key] = successful[0].get(key, "")
        output.append(record)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend-dir", type=Path, required=True)
    parser.add_argument("--ablation-dir", type=Path, required=True)
    parser.add_argument("--outdir", type=Path, required=True)
    args = parser.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    backend_rows = read_rows(args.backend_dir / "comparison_all_runs.csv")
    ablation_rows = read_rows(args.ablation_dir / "ablation_all_runs.csv")
    backend_summary = summarize(
        backend_rows,
        "backend",
        lambda row: row.get("monitor_status") == "ok"
        and row.get("process_return_code") == "0"
        and row.get("duplicate_count") == "0",
    )
    ablation_summary = summarize(
        ablation_rows,
        "variant",
        lambda row: row.get("status") == "success",
    )
    write_rows(args.outdir / "four_backend_10x_summary.csv", backend_summary)
    write_rows(args.outdir / "funnelq_ablation_10x_summary.csv", ablation_summary)

    backend_by_query: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in backend_summary:
        backend_by_query[str(row["query"])].append(row)
    fastest = {
        query: min(
            rows,
            key=lambda row: float(row.get("query_latency_s_mean", float("inf"))),
        ).get("backend")
        for query, rows in backend_by_query.items()
    }
    completed = (
        len(backend_rows) == 5 * 4 * 10
        and len(ablation_rows) == 5 * 6 * 10
        and all(int(row["successful_runs"]) == 10 for row in backend_summary)
        and all(int(row["successful_runs"]) == 10 for row in ablation_summary)
        and all(bool(row["digest_consistent"]) for row in backend_summary + ablation_summary)
    )
    report = {
        "completed": completed,
        "backend_rows": len(backend_rows),
        "ablation_rows": len(ablation_rows),
        "fastest_backend_by_query": fastest,
        "funnelq_fastest_all_queries": all(value == "hvceq" for value in fastest.values()),
        "backend_summary_csv": str(args.outdir / "four_backend_10x_summary.csv"),
        "ablation_summary_csv": str(args.outdir / "funnelq_ablation_10x_summary.csv"),
    }
    (args.outdir / "final_10x_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return 0 if completed else 1


if __name__ == "__main__":
    raise SystemExit(main())
