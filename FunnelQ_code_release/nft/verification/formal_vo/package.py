from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

from .coverage import VARIANTS, build_coverage, verify_coverage
from .memory import peak_rss_mb
from .protocol import epoch_root, manifest_root
from .residual import build_residual_payload
from .transcript import compile_transcript_file


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_package(
    index_dir: Path,
    main_db: Path,
    tx_db: Path,
    query_spec: Path,
    out_dir: Path,
    *,
    variant: str = "full",
    candidate_hint: Path | None = None,
    force: bool = False,
) -> dict[str, Any]:
    if variant not in VARIANTS:
        raise ValueError(f"unsupported verification ablation variant: {variant}")
    started = time.perf_counter()
    index_dir, out_dir = Path(index_dir), Path(out_dir)
    if out_dir.exists():
        if not force:
            raise FileExistsError(f"{out_dir} exists; pass force=True")
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    manifest = json.loads((index_dir / "epoch_manifest.json").read_text(encoding="utf-8"))
    if manifest_root(manifest) != manifest["manifestRoot"] or epoch_root(
        manifest["manifestRoot"]
    ) != manifest["epochRoot"]:
        raise ValueError("authenticated index manifest is invalid")
    shutil.copy2(index_dir / "epoch_manifest.json", out_dir / "epoch_manifest.json")
    _query, transcript = compile_transcript_file(query_spec)
    if int(transcript["refined"]["maxCompletionDeltaSeconds"]) > int(
        manifest["systemParameters"]["closureHorizonSeconds"]
    ):
        raise ValueError(
            "query window exceeds the authenticated closure horizon; replay would be incomplete"
        )
    (out_dir / "transcript.json").write_text(
        json.dumps(transcript, indent=2, sort_keys=True), encoding="utf-8"
    )
    coverage_dir = out_dir / "coverage"
    coverage = build_coverage(
        index_dir / "authenticated_index.sqlite",
        transcript,
        coverage_dir,
        variant=variant,
        candidate_hint=candidate_hint,
    )
    # Self-check the generated coverage before using its replay count.  For the
    # large ablation domains, the residual builder streams candidates directly
    # from the authenticated index rather than retaining this list.
    _verified_coverage, verified_candidates = verify_coverage(
        coverage_dir,
        transcript,
        manifest["roots"]["summaryRoot"],
        manifest["roots"]["anchorDirectoryRoot"],
        manifest["roots"]["closureDomainRoot"],
        int(manifest["counts"]["closureCount"]),
        collect_candidates=False,
    )
    if int(_verified_coverage["verifiedReplayCount"]) != int(coverage["replayCount"]):
        raise AssertionError("generated coverage replay count does not self-verify")
    if verified_candidates:
        raise AssertionError("coverage self-check unexpectedly retained candidates")
    residual_metrics = build_residual_payload(
        index_dir / "authenticated_index.sqlite",
        main_db,
        tx_db,
        transcript,
        out_dir / "residual",
        manifest["roots"]["eventStreamRoot"],
        manifest["roots"]["txBindingRoot"],
        variant=variant,
    )
    if int(residual_metrics["replayCount"]) != int(coverage["replayCount"]):
        raise AssertionError("coverage and residual replay domains differ")

    relative_files = [
        Path("epoch_manifest.json"),
        Path("transcript.json"),
        Path("coverage") / "coverage.json",
        Path("coverage") / "negative_base.jsonl.gz",
        Path("coverage") / "negative_refined.jsonl.gz",
        Path("coverage") / "replay_authenticated.jsonl.gz",
        Path("coverage") / "closure_domain_authenticated.jsonl.gz",
        Path("residual") / "manifest.json",
        Path("residual") / "ranges.jsonl.gz",
    ]
    inventory = [
        {
            "path": path.as_posix(),
            "bytes": (out_dir / path).stat().st_size,
            "sha256": sha256_file(out_dir / path),
        }
        for path in relative_files
    ]
    package_index = {
        "packageVersion": "formal_vo_package_v3",
        "variant": variant,
        "queryId": transcript["queryId"],
        "epochRoot": manifest["epochRoot"],
        "transcriptRoot": transcript["transcriptRoot"],
        "requiredFiles": inventory,
        "instancesCsvIncluded": False,
        "candidateHintIncluded": False,
    }
    package_index_path = out_dir / "package_index.json"
    package_index_path.write_text(
        json.dumps(package_index, indent=2, sort_keys=True), encoding="utf-8"
    )
    sizes = {item["path"]: int(item["bytes"]) for item in inventory}
    sizes["package_index.json"] = package_index_path.stat().st_size
    replay_count = int(coverage["replayCount"])
    cand0_count = (
        int(manifest["counts"]["closureCount"])
        if variant == "no_pruning"
        else int(manifest["counts"]["anchorCount"])
    )
    cand1_count = (
        cand0_count
        if variant == "no_pruning"
        else int(manifest["counts"]["anchorCount"])
        - int(coverage["baseNegativeAnchorCount"])
    )
    metrics = {
        "status": "BUILT",
        "variant": variant,
        "queryId": transcript["queryId"],
        "epochRoot": manifest["epochRoot"],
        "voGenerationMs": (time.perf_counter() - started) * 1000.0,
        "cand0Count": cand0_count,
        "cand1Count": cand1_count,
        "replayCount": replay_count,
        "cand2Count": replay_count if variant in {"full", "no_anchor"} else None,
        "tauqBytes": sizes["transcript.json"],
        "vocovBytes": (
            sizes["coverage/coverage.json"]
            + sizes["coverage/replay_authenticated.jsonl.gz"]
            + sizes["coverage/closure_domain_authenticated.jsonl.gz"]
            + sizes["epoch_manifest.json"]
            + sizes["package_index.json"]
        ),
        "vonegBaseBytes": sizes["coverage/negative_base.jsonl.gz"],
        "vonegRefinedBytes": sizes["coverage/negative_refined.jsonl.gz"],
        "vorepBytes": sizes["residual/manifest.json"]
        + sizes["residual/ranges.jsonl.gz"],
        "voTotalBytes": sum(sizes.values()),
        "sharedEpochBytes": sizes["epoch_manifest.json"],
        "querySpecificVoBytes": sum(sizes.values()) - sizes["epoch_manifest.json"],
        "uniqueOpenedEventCount": residual_metrics["uniqueOpenedEventCount"],
        "residualRangeCount": residual_metrics["rangeCount"],
        "candidateHintUsedAsProof": False,
        "voGeneratorPeakRssMiB": peak_rss_mb(),
    }
    if metrics["voTotalBytes"] != sum(
        metrics[key]
        for key in (
            "tauqBytes",
            "vocovBytes",
            "vonegBaseBytes",
            "vonegRefinedBytes",
            "vorepBytes",
        )
    ):
        raise AssertionError("component byte attribution is not exact")
    (out_dir / "build_metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    return metrics
