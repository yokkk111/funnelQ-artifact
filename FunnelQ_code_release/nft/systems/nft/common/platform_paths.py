#!/usr/bin/env python3
"""Cross-platform executable resolution for NFT experiments."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def executable_name(base: str) -> str:
    return base + (".exe" if os.name == "nt" else "")


def cargo_binary(crate_dir: Path, base: str, *, build: bool = True) -> Path:
    crate_dir = Path(crate_dir).resolve()
    candidate = crate_dir / "target" / "release" / executable_name(base)
    if candidate.is_file():
        return candidate
    # Cross-platform fallback for packages copied from another OS.
    alternatives = [
        crate_dir / "target" / "release" / base,
        crate_dir / "target" / "release" / f"{base}.exe",
    ]
    for path in alternatives:
        if path.is_file() and (os.name == "nt" or os.access(path, os.X_OK)):
            return path
    if not build:
        raise FileNotFoundError(candidate)
    cargo = shutil.which("cargo")
    if not cargo:
        raise RuntimeError(f"cargo is required to build {base}; no binary found under {crate_dir / 'target' / 'release'}")
    subprocess.run([cargo, "build", "--release"], cwd=str(crate_dir), check=True)
    if not candidate.is_file():
        for path in alternatives:
            if path.is_file():
                return path
        raise FileNotFoundError(candidate)
    return candidate


def cmake_binary(build_dir: Path, base: str) -> Path:
    build_dir = Path(build_dir).resolve()
    candidates = [
        build_dir / executable_name(base),
        build_dir / "Release" / executable_name(base),
        build_dir / base,
        build_dir / "Release" / base,
        build_dir / f"{base}.exe",
        build_dir / "Release" / f"{base}.exe",
    ]
    for path in candidates:
        if path.is_file():
            return path
    raise FileNotFoundError("no executable found: " + ", ".join(str(x) for x in candidates))
