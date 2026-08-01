from __future__ import annotations

import argparse
import json
import subprocess
import sys
import shutil
import sqlite3
import time
from pathlib import Path

from .chain import commit_epoch_root, read_epoch_root, resolve_epoch_id
from .descriptor import build_descriptors_python
from .index import build_index
from .coverage import VARIANTS
from .package import build_package
from .verifier import verify_package
from .protocol import epoch_root, file_sha256, manifest_root

QUERY_IDS = [
    "Q1_RAPID_SAME_NFT_ROUNDTRIP",
    "Q2_CROSS_NFT_RECIPROCAL",
    "Q3_SAME_NFT_THREE_ACTOR_CYCLE",
    "Q4_THREE_NFT_THREE_ACTOR_CYCLE",
    "Q5_TWO_NFT_RETURN_CYCLE",
]


def _descriptor_binary(tool_root: Path) -> Path:
    suffix = ".exe" if sys.platform == "win32" else ""
    packaged = tool_root / "bin" / f"formal_vo_descriptor_builder{suffix}"
    if packaged.is_file():
        return packaged
    return tool_root / "rust_descriptor_builder" / "target" / "release" / (
        f"formal_vo_descriptor_builder{suffix}"
    )




def _run_monitored(command: list[str], *, cwd: Path | None = None) -> float | None:
    """Run the descriptor executable and sample its process-tree RSS."""
    process = subprocess.Popen(command, cwd=cwd)
    peak_mib = 0.0
    monitor = None
    try:
        import psutil

        monitor = psutil.Process(process.pid)
    except ImportError:
        monitor = None
    while process.poll() is None:
        if monitor is not None:
            try:
                processes = [monitor, *monitor.children(recursive=True)]
                rss = sum(item.memory_info().rss for item in processes if item.is_running())
                peak_mib = max(peak_mib, rss / (1024.0 * 1024.0))
            except (psutil.Error, OSError):
                pass
        time.sleep(0.05)
    if monitor is not None:
        try:
            rss = monitor.memory_info().rss
            peak_mib = max(peak_mib, rss / (1024.0 * 1024.0))
        except (psutil.Error, OSError):
            pass
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command)
    return peak_mib or None


def _load_validated_manifest(path: Path) -> dict:
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    if manifest_root(manifest) != manifest.get("manifestRoot"):
        raise ValueError("ManifestRoot mismatch")
    if epoch_root(manifest["manifestRoot"]) != manifest.get("epochRoot"):
        raise ValueError("EpochRoot derivation mismatch")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Compact-v4 formal event-binding VO")
    sub = parser.add_subparsers(dest="command", required=True)
    descriptor = sub.add_parser("build-descriptors")
    descriptor.add_argument("--db", required=True, type=Path)
    descriptor.add_argument("--out-db", required=True, type=Path)
    descriptor.add_argument("--force", action="store_true")

    index = sub.add_parser("build-index")
    index.add_argument("--db", required=True, type=Path)
    index.add_argument("--tx-db", required=True, type=Path)
    index.add_argument("--descriptor-db", required=True, type=Path)
    index.add_argument("--out-dir", required=True, type=Path)
    index.add_argument("--force", action="store_true")

    package = sub.add_parser("build-vo")
    package.add_argument("--index-dir", required=True, type=Path)
    package.add_argument("--db", required=True, type=Path)
    package.add_argument("--tx-db", required=True, type=Path)
    package.add_argument("--query", required=True, type=Path)
    package.add_argument("--candidate-hint", type=Path)
    package.add_argument("--variant", choices=VARIANTS, default="full")
    package.add_argument("--out-dir", required=True, type=Path)
    package.add_argument("--force", action="store_true")

    verify = sub.add_parser("verify")
    verify.add_argument("--package", required=True, type=Path)
    verify.add_argument("--query", required=True, type=Path)
    verify.add_argument("--instances", required=True, type=Path)
    verify.add_argument("--expected-epoch-root")
    verify.add_argument("--rpc-url")
    verify.add_argument("--registry-address")
    verify.add_argument("--epoch-id", default="auto")
    verify.add_argument("--chain-read-signature", default="getEpochRoot(bytes32)")
    verify.add_argument("--chain-block-tag", default="latest")
    verify.add_argument("--expected-chain-id", type=int)
    verify.add_argument("--rpc-timeout-seconds", type=float, default=15.0)
    verify.add_argument("--out-json", required=True, type=Path)
    verify.add_argument("--work-dir", type=Path)
    verify.add_argument("--engine", choices=("auto", "rust", "python"), default="auto")

    read_chain = sub.add_parser("read-chain-root")
    read_chain.add_argument("--rpc-url", required=True)
    read_chain.add_argument("--registry-address", required=True)
    read_chain.add_argument("--manifest", type=Path)
    read_chain.add_argument("--epoch-id", default="auto")
    read_chain.add_argument("--read-signature", default="getEpochRoot(bytes32)")
    read_chain.add_argument("--block-tag", default="latest")
    read_chain.add_argument("--expected-chain-id", type=int)
    read_chain.add_argument("--rpc-timeout-seconds", type=float, default=15.0)
    read_chain.add_argument("--out-json", type=Path)

    commit_chain = sub.add_parser("commit-chain-root")
    commit_chain.add_argument("--rpc-url", required=True)
    commit_chain.add_argument("--registry-address", required=True)
    commit_chain.add_argument("--manifest", required=True, type=Path)
    commit_chain.add_argument("--epoch-id", default="auto")
    commit_chain.add_argument("--read-signature", default="getEpochRoot(bytes32)")
    commit_chain.add_argument(
        "--write-signature", default="commitEpochRoot(bytes32,bytes32)"
    )
    commit_chain.add_argument("--from-address")
    commit_chain.add_argument("--expected-chain-id", type=int)
    commit_chain.add_argument("--gas", type=int)
    commit_chain.add_argument("--transaction-timeout-seconds", type=float, default=60.0)
    commit_chain.add_argument("--poll-interval-seconds", type=float, default=0.25)
    commit_chain.add_argument("--out-json", type=Path)
    args = parser.parse_args()
    tool_root = Path(__file__).resolve().parents[1]
    if args.command == "build-descriptors":
        # Always rebuild from the bundled Rust source when Cargo is available.
        # Reusing an arbitrary pre-existing target/release binary would let a
        # stale implementation self-identify as the current descriptor version.
        if shutil.which("cargo"):
            subprocess.run(
                ["cargo", "build", "--release", "--locked"],
                cwd=tool_root / "rust_descriptor_builder",
                check=True,
            )
            binary = _descriptor_binary(tool_root)
            if not binary.is_file():
                raise FileNotFoundError("Cargo completed but the descriptor binary is absent")
        else:
            result = build_descriptors_python(
                args.db, args.out_db, force=args.force
            )
            print(json.dumps(result, sort_keys=True))
            return 0
        started = time.perf_counter()
        command = [
            str(binary),
            "--source-db",
            str(args.db),
            "--out-db",
            str(args.out_db),
        ]
        if args.force:
            command.append("--force")
        descriptor_peak_rss = _run_monitored(command)
        # Bind the descriptor artifact to the exact compact-v4 source file.
        # The index builder refuses descriptor DBs that lack this metadata.
        conn = sqlite3.connect(args.out_db)
        conn.execute(
            "INSERT OR REPLACE INTO descriptor_meta VALUES('source_db_sha256',?)",
            (file_sha256(args.db),),
        )
        conn.execute(
            "INSERT OR REPLACE INTO descriptor_meta VALUES('source_db_bytes',?)",
            (str(args.db.stat().st_size),),
        )
        conn.execute(
            "INSERT OR REPLACE INTO descriptor_meta VALUES('builder_version','generic_descriptor_builder_v3')"
        )
        conn.commit()
        anchor_count = int(
            conn.execute("SELECT COUNT(*) FROM formal_anchor_descriptor").fetchone()[0]
        )
        closure_count = int(
            conn.execute("SELECT COUNT(*) FROM formal_closure_descriptor").fetchone()[0]
        )
        conn.close()
        result = {
            "status": "BUILT",
            "builder": "rust_release",
            "builderVersion": "generic_descriptor_builder_v3",
            "descriptorBuildSeconds": time.perf_counter() - started,
            "descriptorBytes": args.out_db.stat().st_size,
            "anchorCount": anchor_count,
            "closureCount": closure_count,
            "sourceDbSha256": file_sha256(args.db),
            "descriptorPeakRssMiB": descriptor_peak_rss,
        }
        args.out_db.with_suffix(".metrics.json").write_text(
            json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.command == "build-index":
        result = build_index(
            args.db,
            args.tx_db,
            args.descriptor_db,
            args.out_dir,
            force=args.force,
        )
    elif args.command == "build-vo":
        result = build_package(
            args.index_dir,
            args.db,
            args.tx_db,
            args.query,
            args.out_dir,
            variant=args.variant,
            candidate_hint=args.candidate_hint,
            force=args.force,
        )
    elif args.command == "read-chain-root":
        manifest = _load_validated_manifest(args.manifest) if args.manifest else None
        epoch_id = resolve_epoch_id(args.epoch_id, manifest)
        result = read_epoch_root(
            rpc_url=args.rpc_url,
            contract_address=args.registry_address,
            epoch_id=epoch_id,
            read_signature=args.read_signature,
            block_tag=args.block_tag,
            expected_chain_id=args.expected_chain_id,
            timeout_seconds=args.rpc_timeout_seconds,
        )
        if args.out_json:
            args.out_json.parent.mkdir(parents=True, exist_ok=True)
            args.out_json.write_text(
                json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
            )
    elif args.command == "commit-chain-root":
        manifest = _load_validated_manifest(args.manifest)
        epoch_id = resolve_epoch_id(args.epoch_id, manifest)
        result = commit_epoch_root(
            rpc_url=args.rpc_url,
            contract_address=args.registry_address,
            epoch_id=epoch_id,
            epoch_root=manifest["epochRoot"],
            read_signature=args.read_signature,
            write_signature=args.write_signature,
            from_address=args.from_address,
            expected_chain_id=args.expected_chain_id,
            gas=args.gas,
            timeout_seconds=args.transaction_timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        if args.out_json:
            args.out_json.parent.mkdir(parents=True, exist_ok=True)
            args.out_json.write_text(
                json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
            )
    else:
        rpc_fields = (args.rpc_url, args.registry_address)
        if any(rpc_fields) and not all(rpc_fields):
            parser.error("--rpc-url and --registry-address must be supplied together")
        if all(rpc_fields) and args.expected_epoch_root:
            parser.error(
                "use either the live chain reader or --expected-epoch-root, not both"
            )
        chain_config = None
        if all(rpc_fields):
            chain_config = {
                "rpc_url": args.rpc_url,
                "contract_address": args.registry_address,
                "epoch_id": args.epoch_id,
                "read_signature": args.chain_read_signature,
                "block_tag": args.chain_block_tag,
                "expected_chain_id": args.expected_chain_id,
                "timeout_seconds": args.rpc_timeout_seconds,
            }
        elif not args.expected_epoch_root:
            parser.error(
                "verify requires either --expected-epoch-root or live chain arguments"
            )
        result = verify_package(
            args.package,
            args.query,
            args.instances,
            args.expected_epoch_root,
            work_dir=args.work_dir,
            chain_config=chain_config,
            engine=args.engine,
        )
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(
            json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
        )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
