#!/usr/bin/env python3
"""Negative tests for the formal sharded MBFT verification boundary."""
from __future__ import annotations

import argparse
import csv
import json
import struct
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def tamper_opened_payload(source: Path, target: Path) -> None:
    data = bytearray(source.read_bytes())
    offset = 7

    def u32() -> int:
        nonlocal offset
        value = struct.unpack_from("<I", data, offset)[0]
        offset += 4
        return value

    def u64() -> int:
        nonlocal offset
        value = struct.unpack_from("<Q", data, offset)[0]
        offset += 8
        return value

    def skip_string() -> tuple[int, int]:
        nonlocal offset
        length = u32()
        start = offset
        offset += length
        return start, length

    offset += 8
    for _ in range(u32()):
        for _ in range(u32()):
            skip_string()
    changed = False
    for _ in range(u32()):
        offset += 12
        for _ in range(u64()):
            flag = data[offset]
            offset += 1 + 4 + 4
            payload_length = u32()
            offset += payload_length + 4 + 4 + 8
            start, length = skip_string()
            if flag == 1 and length and not changed:
                marker = b'"event_id":"'
                relative = bytes(data[start : start + length]).find(marker)
                if relative >= 0:
                    position = start + relative + len(marker)
                    data[position] = ord("X") if data[position] != ord("X") else ord("Y")
                    changed = True
    if not changed:
        raise RuntimeError("proof has no opened payload to tamper")
    target.write_bytes(data)


def expect_verify_rejection(
    exe: Path,
    proof: Path,
    commitments: Path,
    event_type: str,
    outdir: Path,
    label: str,
) -> None:
    proc = subprocess.run(
        [
            str(exe),
            "verify",
            "--proof",
            str(proof),
            "--commitments",
            str(commitments),
            "--out",
            str(outdir / f"{label}.jsonl"),
            "--metrics",
            str(outdir / f"{label}.metrics.json"),
            "--event-type",
            event_type,
        ],
        cwd=str(exe.parent),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode == 0:
        raise AssertionError(f"{label}: verifier accepted a tampered artifact")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--exe", required=True, type=Path)
    parser.add_argument("--outdir", required=True, type=Path)
    args = parser.parse_args()
    artifact = args.artifact_dir.resolve()
    exe = args.exe.resolve()
    outdir = args.outdir.resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    shard = sorted((artifact / "server" / "shards").glob("shard_*"))[0]
    proof = shard / "proofs" / "nfttrade.mbfhint.proof"
    commitments = shard / "commitments.json"

    tampered_proof = outdir / "tampered_payload.proof"
    tamper_opened_payload(proof, tampered_proof)
    expect_verify_rejection(exe, tampered_proof, commitments, "nfttrade", outdir, "payload_tamper")

    bad_commitments = json.loads(commitments.read_text(encoding="utf-8"))
    bad_commitments["roots"][0]["root_hash"] = (int(bad_commitments["roots"][0]["root_hash"]) + 1) % (2**32)
    bad_commitment_path = outdir / "tampered_commitments.json"
    bad_commitment_path.write_text(json.dumps(bad_commitments, indent=2), encoding="utf-8")
    expect_verify_rejection(exe, proof, bad_commitment_path, "nfttrade", outdir, "commitment_tamper")

    # The proof embeds type=nfttrade. It must not be accepted as a proof for a
    # caller-selected different predicate.
    expect_verify_rejection(exe, proof, commitments, "nfttransfer", outdir, "query_substitution")

    qid = "Q1_RAPID_SAME_NFT_ROUNDTRIP"
    claimed = artifact / "server" / "claims" / qid / "instances.csv"
    omitted = outdir / "omitted_claim.csv"
    with claimed.open("r", newline="", encoding="utf-8") as source:
        rows = list(csv.reader(source))
    if len(rows) < 2:
        raise RuntimeError("smoke claim has no result row to omit")
    with omitted.open("w", newline="", encoding="utf-8") as target:
        csv.writer(target).writerows(rows[:-1])
    comparison = outdir / "omitted_claim_comparison.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "compare_nft_full_instances.py"),
            "--query",
            qid,
            "--out",
            str(comparison),
            f"server={omitted}",
            "client="
            + str(
                artifact
                / "client"
                / qid
                / "recompute"
                / qid
                / "instances.csv"
            ),
        ],
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode == 0:
        raise AssertionError("exact comparison accepted an omitted result instance")

    report = {
        "allPass": True,
        "rejected": [
            "opened_payload_tamper",
            "root_commitment_tamper",
            "atomic_query_substitution",
            "omitted_full_binding_instance",
        ],
    }
    (outdir / "tamper_test_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
