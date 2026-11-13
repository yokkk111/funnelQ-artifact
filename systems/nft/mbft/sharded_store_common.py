#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any, Sequence

import psutil


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def sha256_file(path: Path, block_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            block = stream.read(block_size)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def _kill_tree(pid: int) -> None:
    try:
        parent = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    children = parent.children(recursive=True)
    for proc in children:
        try:
            proc.terminate()
        except psutil.NoSuchProcess:
            pass
    try:
        parent.terminate()
    except psutil.NoSuchProcess:
        pass
    _, alive = psutil.wait_procs(children + [parent], timeout=3.0)
    for proc in alive:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass


def run_monitored(
    cmd: Sequence[str],
    *,
    cwd: Path,
    timeout_s: float = 0.0,
    max_rss_mb: float = 0.0,
    sample_interval_s: float = 0.10,
) -> tuple[dict[str, Any], str, str]:
    started = time.perf_counter()
    proc = subprocess.Popen(
        list(cmd),
        cwd=str(cwd),
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    peak_rss = 0
    kill_reason = ""
    while proc.poll() is None:
        rss = 0
        try:
            root = psutil.Process(proc.pid)
            rss += root.memory_info().rss
            for child in root.children(recursive=True):
                try:
                    rss += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        peak_rss = max(peak_rss, rss)
        elapsed = time.perf_counter() - started
        if max_rss_mb > 0 and rss > max_rss_mb * 1024 * 1024:
            kill_reason = f"rss_mb {rss / 1024 / 1024:.2f} exceeded {max_rss_mb:.2f}"
            _kill_tree(proc.pid)
            break
        if timeout_s > 0 and elapsed > timeout_s:
            kill_reason = f"timeout after {elapsed:.2f}s"
            _kill_tree(proc.pid)
            break
        time.sleep(sample_interval_s)
    stdout, stderr = proc.communicate()
    wall_s = time.perf_counter() - started
    result = {
        "return_code": proc.returncode,
        "wall_s": wall_s,
        "peak_tree_rss_mb": peak_rss / (1024.0 * 1024.0),
        "kill_reason": kill_reason,
        "command": list(cmd),
    }
    if kill_reason or proc.returncode != 0:
        tail = ((stdout or "") + "\n" + (stderr or ""))[-8000:]
        raise RuntimeError(
            f"command failed: {' '.join(cmd)}\n"
            f"return_code={proc.returncode} kill_reason={kill_reason!r}\n{tail}"
        )
    return result, stdout, stderr
