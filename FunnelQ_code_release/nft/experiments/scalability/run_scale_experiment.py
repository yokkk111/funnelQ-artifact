#!/usr/bin/env python3
"""Run the isolated FunnelQ 20/40/60/80/100% query+verification experiment."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
CODE_ROOT = Path(__file__).resolve().parents[2]
FORMAL_TOOL = CODE_ROOT / "verification"
SCALES = (20, 40, 60, 80, 100)
QUERIES = (
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
)


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def load_json(path: Path, default: Any = None) -> Any:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else default


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(16 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rpc(rpc_url: str, method: str, params: list[Any]) -> Any:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(rpc_url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.loads(response.read().decode())
    if "error" in result:
        raise RuntimeError(f"RPC {method} failed: {result['error']}")
    return result["result"]


class Experiment:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.checkpoint_path = ROOT / "manifests" / "experiment_checkpoint.json"
        self.checkpoint = load_json(self.checkpoint_path, {
            "format": "FunnelQ-NFT-scale-growth-v1",
            "status": "running",
            "source": str(args.source.resolve()),
            "source_sha256": args.source_sha256,
            "scales": {},
            "events": [],
        })
        self.python = sys.executable

    def save(self) -> None:
        atomic_json(self.checkpoint_path, self.checkpoint)

    def event(self, message: str, **extra: Any) -> None:
        row = {"time": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "message": message, **extra}
        self.checkpoint.setdefault("events", []).append(row)
        self.checkpoint["events"] = self.checkpoint["events"][-200:]
        self.save()
        print(json.dumps(row, ensure_ascii=False), flush=True)

    def stage_done(self, scale: int | str, stage: str) -> bool:
        return self.checkpoint.get("scales", {}).get(str(scale), {}).get("stages", {}).get(stage, {}).get("status") == "success"

    def mark(self, scale: int | str, stage: str, status: str, **details: Any) -> None:
        scale_rec = self.checkpoint.setdefault("scales", {}).setdefault(str(scale), {"stages": {}})
        scale_rec.setdefault("stages", {})[stage] = {"status": status, **details}
        scale_rec["current_stage"] = stage
        self.checkpoint["current_scale"] = scale
        self.checkpoint["current_stage"] = stage
        self.save()

    def run_logged(
        self,
        scale: int | str,
        stage: str,
        command: list[str],
        *,
        timeout_s: float = 0,
        cwd: Path = ROOT,
    ) -> dict[str, Any]:
        if self.args.resume and self.stage_done(scale, stage):
            self.event("resume-skip", scale=scale, stage=stage)
            return self.checkpoint["scales"][str(scale)]["stages"][stage]
        log_dir = ROOT / "logs" / f"scale_{scale}" 
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = log_dir / f"{stage}.stdout.log"
        stderr_path = log_dir / f"{stage}.stderr.log"
        self.mark(scale, stage, "running", command=command, stdout=str(stdout_path), stderr=str(stderr_path))
        started = time.perf_counter()
        peak_rss_mb = 0.0
        killed_timeout = False
        with stdout_path.open("ab") as out, stderr_path.open("ab") as err:
            out.write(("\nCOMMAND " + json.dumps(command) + "\n").encode())
            out.flush()
            process = subprocess.Popen(command, cwd=cwd, stdout=out, stderr=err)
            try:
                import psutil
                root_process = psutil.Process(process.pid)
            except Exception:
                root_process = None
            while process.poll() is None:
                elapsed = time.perf_counter() - started
                if timeout_s > 0 and elapsed > timeout_s:
                    killed_timeout = True
                    if root_process is not None:
                        for child in root_process.children(recursive=True):
                            try: child.kill()
                            except Exception: pass
                        try: root_process.kill()
                        except Exception: pass
                    else:
                        process.kill()
                    break
                if root_process is not None:
                    try:
                        family = [root_process, *root_process.children(recursive=True)]
                        rss = sum(p.memory_info().rss for p in family if p.is_running()) / 2**20
                        peak_rss_mb = max(peak_rss_mb, rss)
                        if rss > self.args.max_rss_mb:
                            for child in root_process.children(recursive=True):
                                try: child.kill()
                                except Exception: pass
                            root_process.kill()
                            raise MemoryError(f"stage exceeded {self.args.max_rss_mb} MiB")
                    except MemoryError:
                        raise
                    except Exception:
                        pass
                time.sleep(0.1)
            code = process.wait()
        result = {
            "return_code": code,
            "wall_s": time.perf_counter() - started,
            "peak_tree_rss_mb": peak_rss_mb,
            "killed_for_timeout": killed_timeout,
        }
        if code != 0 or killed_timeout:
            self.mark(scale, stage, "failed", **result)
            raise RuntimeError(f"stage failed: scale={scale} stage={stage}; see {stderr_path}")
        self.mark(scale, stage, "success", **result)
        self.event("stage-success", scale=scale, stage=stage, wall_s=result["wall_s"])
        return result

    def prepare_data(self) -> dict[str, Any]:
        manifest = ROOT / "data" / "scale_dataset_manifest.json"
        self.run_logged(
            "global", "make_scale_prefixes",
            [
                self.python, str(ROOT / "make_scale_prefixes.py"),
                "--source", str(self.args.source),
                "--out-root", str(ROOT / "data"),
                "--source-rows", str(self.args.source_rows),
                "--scales", ",".join(map(str, self.args.scales)),
                "--expected-source-sha256", self.args.source_sha256,
                "--resume",
            ], timeout_s=7200,
        )
        data = load_json(manifest)
        if data.get("status") != "success" or data.get("source_sha256") != self.args.source_sha256:
            raise RuntimeError("scale dataset manifest failed validation")
        return data

    def build_store(self, scale: int, csv_path: Path) -> tuple[Path, Path]:
        store_dir = ROOT / "stores" / f"scale_{scale:03d}" / "funnelq"
        main_db = store_dir / "nft_hvceq_episode_sidecar.db"
        tx_db = store_dir / "nft_tx_sidecar.db"
        self.run_logged(scale, "build_main", [
            self.python, str(CODE_ROOT / "scripts" / "build_nft_funnelq_compact_store.py"),
            "--event-atoms-csv", str(csv_path), "--sqlite-out", str(main_db),
            "--outdir", str(store_dir), "--report-json", str(store_dir / "compact_build_report.json"),
            "--max-rss-mb", str(self.args.max_rss_mb), "--force",
        ], timeout_s=7200)
        self.run_logged(scale, "build_tx", [
            self.python, str(CODE_ROOT / "scripts" / "build_nft_tx_sidecar.py"),
            "--event-atoms-csv", str(csv_path), "--sqlite-out", str(tx_db),
            "--report-json", str(store_dir / "nft_tx_sidecar.report.json"), "--force",
        ], timeout_s=7200)
        return main_db, tx_db

    def run_queries(self, scale: int, main_db: Path, tx_db: Path) -> Path:
        out = ROOT / "runs" / f"scale_{scale:03d}" / "query"
        exe = CODE_ROOT / "systems" / "nft" / "hvceq" / "local_executor_rust" / "target" / "release" / "nft_hvceq_local_executor.exe"
        self.run_logged(scale, "query_10x", [
            self.python, str(CODE_ROOT / "scripts" / "run_nft_hvceq_final5_full_memory.py"),
            "--stores-dir", str(main_db.parents[1]), "--outdir", str(out),
            "--runs", str(self.args.runs), "--queries", ",".join(QUERIES),
            "--query-dir", str(CODE_ROOT / "queries" / "nft" / "selected5"),
            "--hvceq-db", str(main_db), "--tx-sidecar-db", str(tx_db), "--hvceq-exe", str(exe),
            "--timeout-sec", str(self.args.query_timeout_s),
            "--process-timeout-sec", str(self.args.query_process_timeout_s),
            "--job-memory-limit-mb", str(self.args.max_rss_mb),
            "--sample-interval-ms", "10", "--allow-missing-memory",
            "--warmup-runs", "1", "--threads", str(self.args.threads),
        ], timeout_s=self.args.query_process_timeout_s * len(QUERIES) * self.args.runs + 3600)
        manifest = load_json(out / "funnelq_full_report.json")
        if not manifest.get("all_run_result_sets_consistent"):
            raise RuntimeError(f"query result-set consistency failed at scale {scale}")
        return out

    def create_claims(self, scale: int, query_out: Path) -> Path:
        claims = ROOT / "claims" / f"scale_{scale:03d}"
        if self.args.resume and self.stage_done(scale, "create_claims"):
            return claims
        rows: list[dict[str, str]]
        with (query_out / "funnelq_full_all_runs.csv").open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        for query in QUERIES:
            selected = next((
                r for r in rows
                if r.get("query") == query
                and r.get("monitor_status", r.get("status")) in {"ok", "success"}
                and str(r.get("process_return_code", r.get("return_code", ""))) == "0"
            ), None)
            if selected is None:
                raise RuntimeError(f"no successful query output for {query} at scale {scale}")
            source = Path(selected["instances_csv"])
            target_dir = claims / query
            target_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target_dir / "instances.csv")
        self.mark(scale, "create_claims", "success", claims_root=str(claims))
        return claims

    def build_formal(self, scale: int, main_db: Path, tx_db: Path) -> tuple[Path, Path]:
        formal = ROOT / "stores" / f"scale_{scale:03d}" / "formal"
        descriptors = formal / "descriptors.db"
        index_dir = formal / "epoch"
        tool = FORMAL_TOOL / "run_formal_vo.py"
        self.run_logged(scale, "build_descriptors", [
            self.python, str(tool), "build-descriptors", "--db", str(main_db),
            "--out-db", str(descriptors), "--force",
        ], timeout_s=7200, cwd=FORMAL_TOOL)
        self.run_logged(scale, "build_authenticated_index", [
            self.python, str(tool), "build-index", "--db", str(main_db), "--tx-db", str(tx_db),
            "--descriptor-db", str(descriptors), "--out-dir", str(index_dir), "--force",
        ], timeout_s=14400, cwd=FORMAL_TOOL)
        packages = ROOT / "vo_packages" / f"scale_{scale:03d}"
        for query in QUERIES:
            self.run_logged(scale, f"build_vo_{query}", [
                self.python, str(tool), "build-vo", "--index-dir", str(index_dir),
                "--db", str(main_db), "--tx-db", str(tx_db),
                "--query", str(FORMAL_TOOL / "queries" / "selected5" / f"{query}.json"),
                "--variant", "full", "--out-dir", str(packages / "full" / query), "--force",
            ], timeout_s=14400, cwd=FORMAL_TOOL)
        return index_dir, packages

    def commit_root(self, scale: int, index_dir: Path) -> dict[str, Any]:
        out = ROOT / "runs" / f"scale_{scale:03d}" / "chain_commit.json"
        if self.args.resume and self.stage_done(scale, "chain_commit") and out.is_file():
            return load_json(out)
        tool = FORMAL_TOOL / "run_formal_vo.py"
        self.run_logged(scale, "chain_commit", [
            self.python, str(tool), "commit-chain-root",
            "--rpc-url", self.args.rpc_url, "--registry-address", self.args.registry_address,
            "--manifest", str(index_dir / "epoch_manifest.json"), "--epoch-id", "auto",
            "--expected-chain-id", str(self.args.expected_chain_id), "--out-json", str(out),
        ], timeout_s=300, cwd=FORMAL_TOOL)
        evidence = load_json(out)
        if evidence.get("status") != "COMMITTED" or int(evidence["receipt"]["status"]) != 1:
            raise RuntimeError(f"chain commit failed at scale {scale}")
        if evidence["readback"]["epochRoot"] != evidence["epochRoot"]:
            raise RuntimeError(f"chain root readback mismatch at scale {scale}")
        return evidence

    def verify(self, scale: int, main_db: Path, tx_db: Path, claims: Path, packages: Path, chain: dict[str, Any]) -> None:
        out = ROOT / "runs" / f"scale_{scale:03d}" / "verification"
        block_number = int(chain["receipt"]["blockNumber"])
        self.run_logged(scale, "verification_10x", [
            self.python, str(FORMAL_TOOL / "run_verification_ablation.py"),
            "--db", str(main_db), "--tx-db", str(tx_db), "--claims-root", str(claims),
            "--out-root", str(out), "--runs", str(self.args.runs), "--query-ids", ",".join(QUERIES),
            "--variants", "full", "--engine", "rust", "--verify-timeout-sec", str(self.args.verify_timeout_s),
            "--build-timeout-sec", "7200", "--max-rss-mb", str(self.args.max_rss_mb), "--resume",
            "--reuse-packages-from", str(packages), "--rpc-url", self.args.rpc_url,
            "--registry-address", self.args.registry_address, "--epoch-id", chain["epochId"],
            "--chain-read-signature", "getEpochRoot(bytes32)", "--chain-block-tag", hex(block_number),
            "--expected-chain-id", str(self.args.expected_chain_id),
        ], timeout_s=self.args.verify_timeout_s * len(QUERIES) * self.args.runs + 3600, cwd=FORMAL_TOOL)
        report = load_json(out / "verification_ablation_report.json")
        required = ("completed", "chain_bound", "chain_context_consistent", "all_results_equal_to_claims")
        if not all(report.get(key) is True for key in required):
            raise RuntimeError(f"verification final gate failed at scale {scale}: {report}")

    def run(self) -> None:
        self.checkpoint["status"] = "running"
        self.checkpoint["completed"] = False
        self.checkpoint.pop("error", None)
        self.save()
        if sha256_file(self.args.source) != self.args.source_sha256:
            raise RuntimeError("canonical source SHA-256 mismatch")
        chain_id = int(rpc(self.args.rpc_url, "eth_chainId", []), 16)
        if chain_id != self.args.expected_chain_id:
            raise RuntimeError(f"chain id mismatch: {chain_id}")
        if not rpc(self.args.rpc_url, "eth_accounts", []):
            raise RuntimeError("RPC exposes no unlocked account for scale-root commitments")
        code = rpc(self.args.rpc_url, "eth_getCode", [self.args.registry_address, "latest"])
        if not isinstance(code, str) or len(code) <= 2:
            raise RuntimeError("registry contract has no code")
        self.event("preflight-success", chain_id=chain_id, registry_code_bytes=(len(code) - 2) // 2)
        dataset_manifest = self.prepare_data()
        by_scale = {int(x["scale_percent"]): x for x in dataset_manifest["datasets"]}
        for scale in self.args.scales:
            self.event("scale-start", scale=scale)
            csv_path = Path(by_scale[scale]["csv"])
            main_db, tx_db = self.build_store(scale, csv_path)
            query_out = self.run_queries(scale, main_db, tx_db)
            claims = self.create_claims(scale, query_out)
            index_dir, packages = self.build_formal(scale, main_db, tx_db)
            chain = self.commit_root(scale, index_dir)
            self.verify(scale, main_db, tx_db, claims, packages, chain)
            self.checkpoint["scales"][str(scale)]["status"] = "success"
            self.save()
            self.event("scale-success", scale=scale)
        self.run_logged("global", "plot", [self.python, str(ROOT / "plot_scale_growth.py"), "--root", str(ROOT)], timeout_s=600)
        self.checkpoint["status"] = "success"
        self.checkpoint["completed"] = True
        self.save()
        self.event("experiment-success")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, type=Path)
    ap.add_argument("--source-rows", type=int, default=5_886_895)
    ap.add_argument("--source-sha256", default="97cacd4b3cf2b78255a0ef00bfa359ad33626bc4e8eb0a9e3822e67d8227c150")
    ap.add_argument("--scales", default="20,40,60,80,100")
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--rpc-url", required=True)
    ap.add_argument("--registry-address", required=True)
    ap.add_argument("--expected-chain-id", type=int, default=31337)
    ap.add_argument("--query-timeout-s", type=float, default=7200)
    ap.add_argument("--query-process-timeout-s", type=float, default=15000)
    ap.add_argument("--verify-timeout-s", type=float, default=7200)
    ap.add_argument("--max-rss-mb", type=float, default=18000)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    args.source = args.source.resolve()
    args.scales = tuple(sorted({int(x) for x in args.scales.split(",") if x.strip()}))
    if not args.scales or any(x not in SCALES for x in args.scales):
        ap.error("--scales must be a subset of 20,40,60,80,100")
    if args.runs != 10:
        ap.error("formal growth experiment requires exactly 10 repetitions")
    return args


if __name__ == "__main__":
    experiment = Experiment(parse_args())
    try:
        experiment.run()
    except Exception as exc:
        experiment.checkpoint["status"] = "failed"
        experiment.checkpoint["error"] = repr(exc)
        experiment.save()
        experiment.event("experiment-failed", error=repr(exc))
        raise
