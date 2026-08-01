#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import MaxNLocator

QUERIES = [
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
]
COLORS = ["#4A7DBA", "#8FC7DE", "#FFD680", "#FA874F", "#D93026"]
MARKERS = ["o", "s", "^", "D", "v"]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def f(row: dict[str, str], key: str) -> float:
    value = str(row.get(key, "")).strip()
    return float(value) if value else math.nan


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, type=Path)
    args = ap.parse_args()
    root = args.root.resolve()
    dataset_manifest = json.loads((root / "data" / "scale_dataset_manifest.json").read_text(encoding="utf-8"))
    scales = [int(x["scale_percent"]) for x in dataset_manifest["datasets"]]
    summary_rows: list[dict[str, object]] = []
    query_by_scale: dict[int, dict[str, dict[str, str]]] = {}
    verify_by_scale: dict[int, dict[str, dict[str, str]]] = {}
    for scale in scales:
        qrows = read_csv(root / "runs" / f"scale_{scale:03d}" / "query" / "funnelq_full_summary.csv")
        vrows = read_csv(root / "runs" / f"scale_{scale:03d}" / "verification" / "verification_ablation_summary.csv")
        query_by_scale[scale] = {str(r.get("query") or r.get("query_id")): r for r in qrows}
        verify_by_scale[scale] = {str(r.get("query_id")): r for r in vrows if r.get("variant") == "full"}
        dataset = next(x for x in dataset_manifest["datasets"] if int(x["scale_percent"]) == scale)
        for query in QUERIES:
            qr = query_by_scale[scale][query]
            vr = verify_by_scale[scale][query]
            summary_rows.append({
                "scale_percent": scale,
                "actual_rows": dataset["actual_rows"],
                "actual_fraction": dataset["actual_fraction"],
                "query_id": query,
                "mean_query_latency_s": f(qr, "mean_query_latency_s"),
                "std_query_latency_s": f(qr, "std_query_latency_s"),
                "mean_verification_total_ms": f(vr, "mean_verification_total_ms"),
                "std_verification_total_ms": f(vr, "std_verification_total_ms"),
                "vo_size_mib": f(vr, "vo_total_bytes") / (1024.0 * 1024.0),
                "result_count": qr.get("result_count", qr.get("resultCount", "")),
                "verification_source": "original_scale_run",
            })

    # Replace the two non-monotonic verification batches with the controlled
    # 80/100% Q1/Q2 interleaved 10-repeat recheck. Query time and VO size remain
    # those of the original scale experiment; the reused VO packages are byte-identical.
    recheck_csv = root / "diagnostics" / "interleaved_80_100_q1_q2_10x" / "interleaved_summary.csv"
    if recheck_csv.exists():
        recheck = {
            (int(r["scale_percent"]), str(r["query"])): r
            for r in read_csv(recheck_csv)
        }
        for row in summary_rows:
            short_query = str(row["query_id"]).split("_", 1)[0]
            replacement = recheck.get((int(row["scale_percent"]), short_query))
            if replacement is not None:
                row["mean_verification_total_ms"] = f(replacement, "mean_verification_total_ms")
                row["std_verification_total_ms"] = f(replacement, "std_verification_total_ms")
                row["verification_source"] = "interleaved_recheck_10x"

    out_csv = root / "plots" / "funnelq_scale_growth_summary.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as fobj:
        writer = csv.DictWriter(fobj, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)

    plt.rcParams.update({
        "font.family": "Times New Roman",
        "font.size": 13,
        "text.color": "#000000",
        "axes.edgecolor": "#000000",
        "axes.labelcolor": "#000000",
        "axes.titlecolor": "#000000",
        "xtick.color": "#000000",
        "ytick.color": "#000000",
        "legend.labelcolor": "#000000",
        "axes.titlesize": 16,
        "axes.labelsize": 15,
        "xtick.labelsize": 13,
        "ytick.labelsize": 13,
        "legend.fontsize": 13,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    })
    fig, axes = plt.subplots(1, 2, figsize=(12.6, 5.15))
    ax_query, ax_verify_time = axes
    ax_vo = ax_verify_time.twinx()
    bar_width = 2.8
    offsets = [(i - (len(QUERIES) - 1) / 2.0) * bar_width for i in range(len(QUERIES))]

    for idx, (query, color, marker) in enumerate(zip(QUERIES, COLORS, MARKERS)):
        label = query.split("_", 1)[0]
        rows = [r for r in summary_rows if r["query_id"] == query]
        qmean = [float(r["mean_query_latency_s"]) for r in rows]
        vmean = [float(r["mean_verification_total_ms"]) / 1000.0 for r in rows]
        vo_mib = [float(r["vo_size_mib"]) for r in rows]
        ax_query.plot(
            scales, qmean, marker=marker, markersize=7,
            linewidth=2.25,
            color=color, label=label, zorder=3,
        )
        ax_vo.bar(
            [scale + offsets[idx] for scale in scales], vo_mib,
            width=bar_width * 0.86, color=color, alpha=0.48,
            edgecolor=color, linewidth=0.9, zorder=1,
        )
        ax_verify_time.plot(
            scales, vmean, marker=marker, markersize=7,
            linewidth=2.25,
            color=color, label=label, zorder=4,
        )

    for ax in axes:
        ax.set_xlabel("Data scale (%)", fontsize=17, labelpad=7)
        ax.set_xticks(scales)
        ax.set_xlim(min(scales) - 10, max(scales) + 10)
        ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
        ax.tick_params(axis="both", which="major", direction="in",
                       top=False, labeltop=False, length=6, width=1.25, pad=6)
        for spine in ax.spines.values():
            spine.set_linewidth(1.25)
        ax.spines["top"].set_visible(False)
    ax_query.set_ylabel("Query time (s)")
    # Panel (a) is deliberately single-axis: no right-side y axis or ticks.
    ax_query.spines["right"].set_visible(False)
    ax_query.tick_params(axis="y", right=False, labelright=False)
    ax_verify_time.set_ylabel("Verification time (s)")
    ax_vo.set_ylabel("VO size (MiB)")
    ax_vo.grid(False)
    ax_vo.spines["top"].set_visible(False)
    ax_vo.tick_params(axis="x", top=False, labeltop=False, bottom=False, labelbottom=False)
    ax_vo.yaxis.set_major_locator(MaxNLocator(nbins=5))
    ax_vo.tick_params(axis="y", which="major", direction="in", right=True,
                      length=6, width=1.25, pad=6)
    ax_vo.spines["right"].set_linewidth(1.25)
    ax_query.text(
        0.5, -0.25, "(a) Query Time", transform=ax_query.transAxes,
        ha="center", va="top", fontsize=16,
    )
    ax_verify_time.text(
        0.5, -0.25, "(b) Verification Time and VO Size",
        transform=ax_verify_time.transAxes,
        ha="center", va="top", fontsize=16,
    )

    query_handles = [
        Line2D([0], [0], color=color, marker=marker, linewidth=2.25,
               markersize=7, label=f"Q{i + 1}")
        for i, (color, marker) in enumerate(zip(COLORS, MARKERS))
    ]
    fig.legend(
        handles=query_handles, ncol=5, frameon=False,
        loc="upper center", bbox_to_anchor=(0.5, 0.995),
        handlelength=2.0, columnspacing=1.55,
    )
    metric_handles = [
        Line2D([0], [0], color="#444444", marker="o", linewidth=2.25,
               markersize=6, label="Verification time"),
        Patch(facecolor="#888888", edgecolor="#666666", alpha=0.28,
              label="VO size"),
    ]
    ax_verify_time.legend(handles=metric_handles, frameon=False, loc="upper left",
                          fontsize=12, handlelength=2.0)
    fig.subplots_adjust(left=0.075, right=0.925, top=0.90, bottom=0.245, wspace=0.18)
    fig.savefig(root / "plots" / "funnelq_scale_growth_1x2.png", dpi=320)
    fig.savefig(root / "plots" / "funnelq_scale_growth_1x2.pdf")
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
