from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


def _binary(root: Path) -> Path:
    suffix = ".exe" if sys.platform == "win32" else ""
    packaged = root / "bin" / f"formal_vo_verifier_core{suffix}"
    if packaged.is_file():
        return packaged
    return (
        root
        / "rust_verifier_core"
        / "target"
        / "release"
        / f"formal_vo_verifier_core{suffix}"
    )


def ensure_binary(*, require: bool) -> Path | None:
    root = Path(__file__).resolve().parents[1]
    suffix = ".exe" if sys.platform == "win32" else ""
    packaged = root / "bin" / f"formal_vo_verifier_core{suffix}"
    if packaged.is_file():
        return packaged
    cargo = shutil.which("cargo")
    if cargo:
        subprocess.run(
            [cargo, "build", "--release"],
            cwd=root / "rust_verifier_core",
            check=True,
        )
    binary = _binary(root)
    if binary.is_file():
        return binary
    if require:
        raise FileNotFoundError(
            "Rust verifier core is unavailable; install Cargo or run build_rust_cores.ps1"
        )
    return None


def verify_package_with_rust(
    *,
    package_dir: Path,
    query_spec: Path,
    instances_csv: Path,
    work_dir: Path,
    require: bool,
    progress_every: int = 100_000,
) -> dict[str, Any] | None:
    """Run the independent full Rust verifier over the existing v3 package.

    The Rust core verifies the coverage certificates, reconstructs the replay
    domain, checks the shared residual Merkle openings, derives every closure
    window directly over the authenticated shared ranges, enumerates exact
    matches, and compares the canonical result set.  It never materializes the
    old per-closure ``authenticated_replay.jsonl`` expansion.
    """

    binary = ensure_binary(require=require)
    if binary is None:
        return None
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    out_json = work_dir / "rust_verifier_result.json"
    work_db = work_dir / "rust_result_compare.sqlite"
    candidate_db = work_db.with_suffix(".candidates.sqlite")
    command = [
        str(binary),
        "--package",
        str(package_dir),
        "--query",
        str(query_spec),
        "--instances",
        str(instances_csv),
        "--work-db",
        str(work_db),
        "--out-json",
        str(out_json),
        "--progress-every",
        str(max(1, int(progress_every))),
    ]
    try:
        subprocess.run(command, check=True)
        result = json.loads(out_json.read_text(encoding="utf-8"))
        if result.get("status") != "VERIFIED":
            raise RuntimeError("Rust verifier core did not return VERIFIED")
        if result.get("directSharedRangeReplay") is not True:
            raise RuntimeError("Rust verifier did not use direct shared-range replay")
        if int(result.get("temporaryReplayBytes", -1)) != 0:
            raise RuntimeError("Rust verifier unexpectedly materialized replay payloads")
        return result
    finally:
        out_json.unlink(missing_ok=True)
        work_db.unlink(missing_ok=True)
        candidate_db.unlink(missing_ok=True)
