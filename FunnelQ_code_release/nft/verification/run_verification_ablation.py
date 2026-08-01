#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from formal_vo.chain import commit_epoch_root, read_epoch_root, resolve_epoch_id

QUERY_IDS = [
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
]
VARIANTS = ["full", "no_anchor", "no_refined", "no_pruning"]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def package_inventory_ready(package: Path) -> tuple[bool, str | None]:
    """Cheap fail-fast inventory check before a long formal run.

    The verifier still performs the authoritative SHA-256 validation.  This
    preflight catches interrupted package builds whose metrics file survived
    while a large required artifact did not.
    """
    metrics_path = package / "build_metrics.json"
    index_path = package / "package_index.json"
    if not metrics_path.is_file():
        return False, f"missing {metrics_path}"
    if not index_path.is_file():
        return False, f"missing {index_path}"
    try:
        package_index = json.loads(index_path.read_text(encoding="utf-8"))
        required_files = package_index["requiredFiles"]
        if not isinstance(required_files, list) or not required_files:
            return False, "package_index requiredFiles is empty or invalid"
        for item in required_files:
            path = package / str(item["path"])
            if not path.is_file():
                return False, f"missing required package file: {item['path']}"
            if path.stat().st_size != int(item["bytes"]):
                return False, f"package file size mismatch: {item['path']}"
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return False, f"invalid package inventory: {exc}"
    return True, None


def kill_tree(process: subprocess.Popen[Any]) -> None:
    try:
        import psutil
        root = psutil.Process(process.pid)
        children = root.children(recursive=True)
        for child in children:
            try: child.kill()
            except psutil.Error: pass
        try: root.kill()
        except psutil.Error: pass
    except (ImportError, Exception):
        process.kill()


def run_monitored(
    command: list[str],
    *,
    stdout_path: Path,
    stderr_path: Path,
    timeout_s: float,
    max_rss_mb: float,
) -> dict[str, Any]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    peak = 0.0
    killed_timeout = False
    killed_memory = False
    with stdout_path.open("wb") as out, stderr_path.open("wb") as err:
        process = subprocess.Popen(command, stdout=out, stderr=err)
        monitor = None
        try:
            import psutil
            monitor = psutil.Process(process.pid)
        except ImportError:
            pass
        while process.poll() is None:
            elapsed = time.perf_counter() - started
            if timeout_s > 0 and elapsed > timeout_s:
                killed_timeout = True
                kill_tree(process)
                break
            if monitor is not None:
                try:
                    processes = [monitor, *monitor.children(recursive=True)]
                    rss = sum(p.memory_info().rss for p in processes if p.is_running()) / 2**20
                    peak = max(peak, rss)
                    if max_rss_mb > 0 and rss > max_rss_mb:
                        killed_memory = True
                        kill_tree(process)
                        break
                except Exception:
                    pass
            time.sleep(0.05)
        try:
            code = process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            kill_tree(process)
            code = process.wait()
    return {
        "return_code": int(code),
        "wall_s": time.perf_counter() - started,
        "peak_job_rss_mb": peak or None,
        "killed_for_timeout": killed_timeout,
        "killed_for_memory": killed_memory,
    }


def mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def std(values: list[float]) -> float | None:
    return statistics.stdev(values) if len(values) > 1 else (0.0 if values else None)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def plot(
    summary: list[dict[str, Any]],
    out_dir: Path,
    query_ids: list[str],
    variants: list[str],
) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("[warning] matplotlib is not installed; CSV results are complete but figures were skipped")
        return
    labels = {q: q.split("_", 1)[0] for q in query_ids}
    names = {
        "full": "Full",
        "no_anchor": "w/o Anchor Directory",
        "no_refined": "w/o Refined Certificates",
        "no_pruning": "Full Closure Replay",
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    for metric, ylabel, filename, logy in [
        ("mean_verification_total_ms", "Verification time (ms)", "verification_ablation_latency", False),
        ("vo_total_bytes", "VO size (bytes)", "verification_ablation_vo_size", True),
        ("replay_count", "Authenticated replay closures", "verification_ablation_replay_count", True),
    ]:
        x = list(range(len(query_ids)))
        width = 0.2
        fig, ax = plt.subplots(figsize=(10, 5))
        for j, variant in enumerate(variants):
            vals=[]
            for q in query_ids:
                row=next((r for r in summary if r["query_id"]==q and r["variant"]==variant),None)
                vals.append(float(row.get(metric) or math.nan) if row else math.nan)
            ax.bar([i+(j-1.5)*width for i in x], vals, width, label=names[variant])
        ax.set_xticks(x, [labels[q] for q in query_ids])
        ax.set_ylabel(ylabel)
        if logy: ax.set_yscale("log")
        ax.legend()
        fig.tight_layout()
        fig.savefig(out_dir / f"{filename}.png", dpi=240)
        fig.savefig(out_dir / f"{filename}.pdf")
        plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run aligned FunnelQ verification ablations")
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--tx-db", required=True, type=Path)
    parser.add_argument("--index-dir", type=Path)
    parser.add_argument("--claims-root", required=True, type=Path)
    parser.add_argument("--out-root", required=True, type=Path)
    parser.add_argument("--query-dir", type=Path, default=Path(__file__).parent / "queries" / "selected5")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--query-ids", default=",".join(QUERY_IDS))
    parser.add_argument("--variants", default=",".join(VARIANTS))
    parser.add_argument("--engine", choices=("auto", "rust", "python"), default="auto")
    parser.add_argument("--expected-epoch-root", default="")
    parser.add_argument("--rpc-url", default="")
    parser.add_argument("--registry-address", default="")
    parser.add_argument("--epoch-id", default="auto")
    parser.add_argument("--chain-read-signature", default="getEpochRoot(bytes32)")
    parser.add_argument("--chain-write-signature", default="commitEpochRoot(bytes32,bytes32)")
    parser.add_argument("--chain-block-tag", default="latest")
    parser.add_argument("--expected-chain-id", type=int)
    parser.add_argument("--rpc-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--commit-chain-root", action="store_true")
    parser.add_argument("--from-address")
    parser.add_argument("--gas", type=int)
    parser.add_argument("--transaction-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--transaction-poll-interval-seconds", type=float, default=0.25)
    parser.add_argument(
        "--allow-local-root-dry-run",
        action="store_true",
        help="Explicitly allow a non-chain-bound diagnostic run; never marks the formal experiment complete",
    )
    parser.add_argument("--verify-timeout-sec", type=float, default=600.0)
    parser.add_argument("--build-timeout-sec", type=float, default=3600.0)
    parser.add_argument("--max-rss-mb", type=float, default=18000.0)
    parser.add_argument("--force-packages", action="store_true")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume successful query/variant/run rows from the durable checkpoint",
    )
    parser.add_argument(
        "--reuse-packages-from",
        type=Path,
        help="Reuse an existing packages directory without rebuilding VO packages",
    )
    args = parser.parse_args()
    query_ids = [q.strip() for q in args.query_ids.split(",") if q.strip()]
    bad_queries = set(query_ids) - set(QUERY_IDS)
    if not query_ids: parser.error("--query-ids must not be empty")
    if bad_queries: parser.error(f"unsupported query ids: {sorted(bad_queries)}")
    variants = [v.strip() for v in args.variants.split(",") if v.strip()]
    bad = set(variants) - set(VARIANTS)
    if bad: parser.error(f"unsupported variants: {sorted(bad)}")
    if args.runs < 1: parser.error("--runs must be positive")
    if args.reuse_packages_from is None and args.index_dir is None:
        parser.error("--index-dir is required unless --reuse-packages-from is used")
    if args.reuse_packages_from is not None and args.force_packages:
        parser.error("--reuse-packages-from cannot be combined with --force-packages")
    has_rpc = bool(args.rpc_url.strip())
    has_registry = bool(args.registry_address.strip())
    if has_rpc != has_registry:
        parser.error("--rpc-url and --registry-address must be supplied together")
    live_chain = has_rpc and has_registry
    if live_chain and args.expected_epoch_root.strip():
        parser.error("use live-chain arguments or --expected-epoch-root, not both")
    if not live_chain and not args.allow_local_root_dry_run:
        parser.error(
            "formal verification ablation requires --rpc-url and --registry-address; "
            "use --allow-local-root-dry-run only for a diagnostic dry run"
        )
    if args.commit_chain_root and not live_chain:
        parser.error("--commit-chain-root requires live-chain arguments")

    tool_root = Path(__file__).resolve().parent
    cli = tool_root / "run_formal_vo.py"
    args.out_root.mkdir(parents=True, exist_ok=True)
    packages = (
        # Preserve a caller-supplied short Windows path/junction.  resolve()
        # expands junctions back to the long target and can cross MAX_PATH for
        # Q4's closure-domain artifact.
        args.reuse_packages_from.absolute()
        if args.reuse_packages_from is not None
        else args.out_root / "packages"
    )
    reports = args.out_root / "reports"
    logs = args.out_root / "logs"
    checkpoint_json = args.out_root / "verification_ablation_checkpoint.json"
    checkpoint_csv = args.out_root / "verification_ablation_checkpoint.csv"
    if args.reuse_packages_from is not None:
        manifest_source = packages / variants[0] / query_ids[0] / "epoch_manifest.json"
    else:
        assert args.index_dir is not None
        manifest_source = args.index_dir / "epoch_manifest.json"
    manifest = json.loads(manifest_source.read_text(encoding="utf-8"))
    epoch = args.expected_epoch_root.strip() or str(manifest["epochRoot"])
    expected_root = epoch.lower().removeprefix("0x")

    chain_commit_evidence: dict[str, Any] | None = None
    chain_preflight_evidence: dict[str, Any] | None = None
    fixed_chain_block_tag: str | None = None
    resolved_epoch_id: str | None = None
    if live_chain:
        resolved_epoch_id = resolve_epoch_id(args.epoch_id, manifest)
        existing_commit_path = args.out_root / "chain_commit.json"
        if args.resume and existing_commit_path.is_file():
            chain_commit_evidence = json.loads(existing_commit_path.read_text(encoding="utf-8"))
        elif args.commit_chain_root:
            chain_commit_evidence = commit_epoch_root(
                rpc_url=args.rpc_url,
                contract_address=args.registry_address,
                epoch_id=resolved_epoch_id,
                epoch_root=epoch,
                read_signature=args.chain_read_signature,
                write_signature=args.chain_write_signature,
                from_address=args.from_address,
                expected_chain_id=args.expected_chain_id,
                gas=args.gas,
                timeout_seconds=args.transaction_timeout_seconds,
                poll_interval_seconds=args.transaction_poll_interval_seconds,
            )
            (args.out_root / "chain_commit.json").write_text(
                json.dumps(chain_commit_evidence, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        preflight_path = args.out_root / "chain_preflight.json"
        preflight_block_tag = args.chain_block_tag
        if args.resume and preflight_path.is_file():
            previous_preflight = json.loads(preflight_path.read_text(encoding="utf-8"))
            if str(previous_preflight.get("contractAddress", "")).lower() != args.registry_address.lower():
                raise ValueError("resume registry address differs from the saved chain preflight")
            if str(previous_preflight.get("epochId", "")).lower().removeprefix("0x") != str(resolved_epoch_id).lower().removeprefix("0x"):
                raise ValueError("resume epoch id differs from the saved chain preflight")
            preflight_block_tag = hex(int(previous_preflight["blockNumber"]))
        chain_preflight_evidence = read_epoch_root(
            rpc_url=args.rpc_url,
            contract_address=args.registry_address,
            epoch_id=resolved_epoch_id,
            read_signature=args.chain_read_signature,
            block_tag=preflight_block_tag,
            expected_chain_id=args.expected_chain_id,
            timeout_seconds=args.rpc_timeout_seconds,
        )
        if str(chain_preflight_evidence["epochRoot"]).lower().removeprefix("0x") != expected_root:
            raise ValueError("preflight on-chain EpochRoot does not match the VO manifest")
        fixed_chain_block_tag = hex(int(chain_preflight_evidence["blockNumber"]))
        chain_preflight_evidence["fixedVerificationBlockTag"] = fixed_chain_block_tag
        preflight_path.write_text(
            json.dumps(chain_preflight_evidence, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    build_status: dict[tuple[str,str], dict[str,Any]] = {}
    for q in query_ids:
        query = args.query_dir / f"{q}.json"
        for variant in variants:
            package = packages / variant / q
            metrics_path = package / "build_metrics.json"
            package_ready, package_error = package_inventory_ready(package)
            if package_ready and not args.force_packages:
                build_status[(q,variant)] = {
                    "status":"success",
                    "metrics":json.loads(metrics_path.read_text()),
                    "reused": args.reuse_packages_from is not None,
                }
                continue
            if args.reuse_packages_from is not None:
                build_status[(q,variant)] = {
                    "status":"failed",
                    "reason":f"reused package failed inventory preflight: {package_error}",
                }
                continue
            assert args.index_dir is not None
            command = [sys.executable, str(cli), "build-vo", "--index-dir", str(args.index_dir), "--db", str(args.db), "--tx-db", str(args.tx_db), "--query", str(query), "--variant", variant, "--out-dir", str(package), "--force"]
            status = run_monitored(command, stdout_path=logs/f"build_{q}_{variant}.stdout.log", stderr_path=logs/f"build_{q}_{variant}.stderr.log", timeout_s=args.build_timeout_sec, max_rss_mb=args.max_rss_mb)
            package_ready, package_error = package_inventory_ready(package)
            if status["return_code"] == 0 and package_ready:
                build_status[(q,variant)] = {"status":"success", "metrics":json.loads(metrics_path.read_text()), **status}
            else:
                build_status[(q,variant)] = {"status":"failed", "reason":package_error, **status}

    if args.reuse_packages_from is not None:
        invalid_packages = [
            f"{q}/{variant}: {status.get('reason')}"
            for (q, variant), status in build_status.items()
            if status["status"] != "success"
        ]
        if invalid_packages:
            raise ValueError(
                "reused package inventory preflight failed:\n- "
                + "\n- ".join(invalid_packages)
            )

    all_rows: list[dict[str,Any]]=[]
    if args.resume and checkpoint_json.is_file():
        checkpoint_rows = json.loads(checkpoint_json.read_text(encoding="utf-8"))
        if not isinstance(checkpoint_rows, list):
            raise ValueError("verification checkpoint must contain a JSON list")
        all_rows = [
            dict(row)
            for row in checkpoint_rows
            if isinstance(row, dict) and row.get("status") == "success"
        ]
    resumed_success_keys = {
        (str(row.get("query_id")), str(row.get("variant")), int(row.get("run", 0)))
        for row in all_rows
    }
    resumed_success_count = len(resumed_success_keys)

    def checkpoint() -> None:
        checkpoint_json.write_text(
            json.dumps(all_rows, indent=2, sort_keys=True), encoding="utf-8"
        )
        write_csv(checkpoint_csv, all_rows)

    for run in range(1,args.runs+1):
        for qi,q in enumerate(query_ids):
            rotated=variants[(run-1+qi)%len(variants):]+variants[:(run-1+qi)%len(variants)]
            for order,variant in enumerate(rotated):
                base={"query_id":q,"variant":variant,"run":run,"variant_order":order+1}
                if (q, variant, run) in resumed_success_keys:
                    continue
                build=build_status[(q,variant)]
                if build["status"]!="success":
                    all_rows.append({**base,"status":"package_build_failed",**{k:v for k,v in build.items() if k!="metrics"}})
                    checkpoint()
                    continue
                package=packages/variant/q
                claim=args.claims_root/q/"instances.csv"
                out_json=reports/f"run_{run:02d}"/variant/f"{q}.json"
                work=reports/f"run_{run:02d}"/variant/f"{q}.work"
                command=[sys.executable,str(cli),"verify","--package",str(package),"--query",str(args.query_dir/f"{q}.json"),"--instances",str(claim)]
                if live_chain:
                    command += [
                        "--rpc-url", args.rpc_url,
                        "--registry-address", args.registry_address,
                        "--epoch-id", str(resolved_epoch_id),
                        "--chain-read-signature", args.chain_read_signature,
                        "--chain-block-tag", str(fixed_chain_block_tag),
                        "--rpc-timeout-seconds", str(args.rpc_timeout_seconds),
                    ]
                    if args.expected_chain_id is not None:
                        command += ["--expected-chain-id", str(args.expected_chain_id)]
                else:
                    command += ["--expected-epoch-root", epoch]
                command += ["--engine",args.engine,"--out-json",str(out_json),"--work-dir",str(work)]
                proc=run_monitored(command,stdout_path=logs/f"verify_r{run:02d}_{q}_{variant}.stdout.log",stderr_path=logs/f"verify_r{run:02d}_{q}_{variant}.stderr.log",timeout_s=args.verify_timeout_sec,max_rss_mb=args.max_rss_mb)
                row={**base,**proc}
                if proc["return_code"]==0 and out_json.is_file():
                    verify=json.loads(out_json.read_text())
                    metrics=build["metrics"]
                    row.update({"claim_sha256":sha256_file(claim),**verify,
                        "vo_generation_ms":metrics.get("voGenerationMs"),"vo_total_bytes":metrics.get("voTotalBytes"),"vocov_bytes":metrics.get("vocovBytes"),"voneg_base_bytes":metrics.get("vonegBaseBytes"),"voneg_refined_bytes":metrics.get("vonegRefinedBytes"),"vorep_bytes":metrics.get("vorepBytes"),"unique_opened_event_count":metrics.get("uniqueOpenedEventCount"),"residual_range_count":metrics.get("residualRangeCount")})
                    evidence = dict(verify.get("chainReadEvidence") or {})
                    row.update({
                        "chain_bound": verify.get("chainReadMode") == "ethereum_json_rpc_eth_call",
                        "chain_id": evidence.get("chainId"),
                        "chain_contract_address": evidence.get("contractAddress"),
                        "chain_epoch_id": evidence.get("epochId"),
                        "chain_block_number": evidence.get("blockNumber"),
                        "chain_block_hash": evidence.get("blockHash"),
                        "chain_contract_code_bytes": evidence.get("contractCodeBytes"),
                        "chain_epoch_root": evidence.get("epochRoot"),
                        "chain_context_ms": evidence.get("chainContextMs"),
                        "chain_eth_call_ms": evidence.get("ethCallMs"),
                        "chain_evidence_json": json.dumps(evidence, sort_keys=True),
                        "verification_without_chain_ms": float(verify["verification_total_ms"])
                        - float(verify.get("chain_read_ms") or 0.0),
                    })
                    row["verification_status"] = row.get("status")
                    if live_chain and not row["chain_bound"]:
                        row["status"] = "chain_binding_failed"
                    else:
                        row["status"] = "success"
                    row.pop("comparison",None);row.pop("chainReadEvidence",None)
                else:
                    row["status"]="timeout" if proc["killed_for_timeout"] else ("memory_limit" if proc["killed_for_memory"] else "failed")
                all_rows.append(row)
                checkpoint()

    all_csv=args.out_root/"verification_ablation_all_runs.csv";write_csv(all_csv,all_rows)
    groups:dict[tuple[str,str],list[dict[str,Any]]]=defaultdict(list)
    for row in all_rows: groups[(row["query_id"],row["variant"])].append(row)
    metric_names=["verification_total_ms","verification_without_chain_ms","chain_read_ms","wall_ms","vocov_ms","voneg_base_ms","voneg_refined_ms","vorep_ms","exact_match_ms","result_compare_ms","verifier_peak_rss_mb","peak_job_rss_mb"]
    summary=[]
    for q in query_ids:
        for variant in variants:
            rows=groups[(q,variant)];ok=[r for r in rows if r.get("status")=="success"]
            item={"query_id":q,"variant":variant,"requested_runs":args.runs,"successful_runs":len(ok),"failed_runs":len(rows)-len(ok)}
            for name in metric_names:
                vals=[float(r[name]) for r in ok if r.get(name) not in (None,"")]
                item[f"mean_{name}"]=mean(vals);item[f"std_{name}"]=std(vals)
            if ok:
                for name in [
                    "cand0Count", "cand1Count", "replayCount", "cand2Count",
                    "resultCount", "vo_total_bytes", "vocov_bytes",
                    "voneg_base_bytes", "voneg_refined_bytes", "vorep_bytes",
                    "unique_opened_event_count", "residual_range_count",
                    "directSharedRangeReplay", "temporaryReplayBytes",
                    "authenticatedCandidateDbBytes",
                ]:
                    values = {str(r.get(name)) for r in ok}
                    item[{
                        "cand0Count": "cand0_count",
                        "cand1Count": "cand1_count",
                        "replayCount": "replay_count",
                        "cand2Count": "cand2_count",
                        "resultCount": "result_count",
                        "directSharedRangeReplay": "direct_shared_range_replay",
                        "temporaryReplayBytes": "temporary_replay_bytes",
                        "authenticatedCandidateDbBytes": "authenticated_candidate_db_bytes",
                    }.get(name, name)] = (
                        ok[0].get(name) if len(values) == 1 else "INCONSISTENT"
                    )
                item["verifier_engine"]=";".join(sorted({str(r.get("verifierEngine")) for r in ok}))
                item["result_consistent"]=(len({r.get("resultCount") for r in ok})==1)
            summary.append(item)
    summary_csv=args.out_root/"verification_ablation_summary.csv";write_csv(summary_csv,summary)
    execution_completed=all(r["successful_runs"]==args.runs for r in summary)
    successful_rows=[r for r in all_rows if r.get("status")=="success"]
    all_chain_bound=bool(successful_rows) and all(r.get("chain_bound") is True for r in successful_rows)
    expected_chain_tuple = None
    chain_context_consistent = False
    if all_chain_bound:
        chain_tuples = {
            (
                int(r["chain_id"]),
                str(r["chain_contract_address"]).lower(),
                str(r["chain_epoch_id"]).lower(),
                int(r["chain_block_number"]),
                str(r["chain_block_hash"]).lower(),
                str(r["chain_epoch_root"]).lower().removeprefix("0x"),
            )
            for r in successful_rows
        }
        chain_context_consistent = len(chain_tuples) == 1
        if chain_context_consistent:
            expected_chain_tuple = next(iter(chain_tuples))
            chain_context_consistent = expected_chain_tuple[-1] == expected_root
    all_results_equal_to_claims = bool(successful_rows) and all(
        r.get("verification_status") == "VERIFIED" for r in successful_rows
    )
    completed=execution_completed and all_chain_bound and chain_context_consistent and all_results_equal_to_claims
    report={
        "completed":completed,
        "execution_completed":execution_completed,
        "chain_bound":all_chain_bound,
        "chain_context_consistent":chain_context_consistent,
        "all_results_equal_to_claims":all_results_equal_to_claims,
        "chain_mode_required":not args.allow_local_root_dry_run,
        "chain_read_mode":"ethereum_json_rpc_eth_call" if all_chain_bound else "not_live_chain_bound",
        "chain_preflight_json":str(args.out_root / "chain_preflight.json") if chain_preflight_evidence else None,
        "chain_commit_json":str(args.out_root / "chain_commit.json") if chain_commit_evidence else None,
        "fixed_chain_block_tag":fixed_chain_block_tag,
        "chain_context":({
            "chain_id":expected_chain_tuple[0],
            "contract_address":expected_chain_tuple[1],
            "epoch_id":expected_chain_tuple[2],
            "block_number":expected_chain_tuple[3],
            "block_hash":expected_chain_tuple[4],
            "epoch_root":expected_chain_tuple[5],
        } if expected_chain_tuple else None),
        "runs":args.runs,
        "resume_enabled":args.resume,
        "resumed_successful_rows":resumed_success_count,
        "checkpoint_json":str(checkpoint_json),
        "checkpoint_csv":str(checkpoint_csv),
        "query_ids":query_ids,
        "variants":variants,
        "package_root":str(packages),
        "packages_reused":args.reuse_packages_from is not None,
        "total_requested":len(query_ids)*len(variants)*args.runs,
        "successful":sum(1 for r in all_rows if r.get("status")=="success"),
        "all_runs_csv":str(all_csv),
        "summary_csv":str(summary_csv),
    }
    (args.out_root/"verification_ablation_report.json").write_text(json.dumps(report,indent=2,sort_keys=True),encoding="utf-8")
    try:
        plot(summary,args.out_root/"figures",query_ids,variants)
    except Exception as exc:
        print(f"[warning] plotting failed but CSV results are complete: {exc}")
    print(json.dumps(report,sort_keys=True))
    return 0 if completed else 2

if __name__=="__main__": raise SystemExit(main())
