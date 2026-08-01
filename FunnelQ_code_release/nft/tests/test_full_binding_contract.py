from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import bisect
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVENTS = ROOT / "validation" / "synthetic_nft_events.csv"
QUERIES = ["C1", "C2", "L3_RETURN", "L6_NEG_RETURN_SUPPORT", "H5_23"]


class FullBindingContractTest(unittest.TestCase):
    def run_cmd(self, *args: str, cwd: Path = ROOT) -> None:
        proc = subprocess.run([str(x) for x in args], cwd=str(cwd), text=True, capture_output=True)
        if proc.returncode != 0:
            self.fail(f"command failed: {' '.join(map(str,args))}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")

    def compare(self, query: str, out: Path, **inputs: Path) -> None:
        self.run_cmd(
            sys.executable, ROOT / "tools" / "compare_nft_full_instances.py",
            "--query", query, "--out", out,
            *[f"{name}={path}" for name, path in inputs.items()],
        )

    def test_oracle_sql_and_independent_mbft_matcher(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nft_full_binding_") as td:
            work = Path(td)
            oracle = work / "oracle"
            self.run_cmd(sys.executable, ROOT / "tools" / "nft_bruteforce_oracle.py", "--events-csv", EVENTS, "--outdir", oracle)

            sql = work / "sql"
            sql.mkdir()
            db = sql / "nft_sql_store.db"
            self.run_cmd(
                sys.executable, ROOT / "systems" / "nft" / "sql" / "build_nft_sql_store.py",
                "--event-atoms-csv", EVENTS, "--sqlite-out", db, "--report-json", sql / "build.json",
            )
            for q in QUERIES:
                self.run_cmd(
                    sys.executable, ROOT / "systems" / "nft" / "sql" / "query_nft_sql_full.py",
                    "--db", db, "--query-json", ROOT / "queries" / "nft" / "final5" / f"{q}.json",
                    "--outdir", sql / q,
                )

            events_jsonl = work / "events.jsonl"
            convert = (
                "from pathlib import Path; import json; "
                "from systems.nft.common.nft_semantics import load_event_atoms; "
                f"df=load_event_atoms(Path(r'{EVENTS}')); p=Path(r'{events_jsonl}'); "
                "p.write_text(''.join(json.dumps(r,default=str)+'\\n' for r in df.to_dict('records')),encoding='utf-8')"
            )
            self.run_cmd(sys.executable, "-c", convert)
            mbft = work / "mbft"
            self.run_cmd(
                sys.executable, ROOT / "systems" / "nft" / "mbft" / "nft_semantic_matcher.py",
                "--events-jsonl", events_jsonl, "--query-dir", ROOT / "queries" / "nft" / "final5",
                "--queries", ",".join(QUERIES), "--outdir", mbft,
            )
            matcher_source = (ROOT / "systems" / "nft" / "mbft" / "nft_semantic_matcher.py").read_text(encoding="utf-8")
            self.assertNotIn("NftLocalVerifier", matcher_source)
            self.assertNotIn("NFT_verification", matcher_source)

            expected_counts = {"C1": 1, "C2": 1, "L3_RETURN": 1, "L6_NEG_RETURN_SUPPORT": 1, "H5_23": 2}
            for q in QUERIES:
                self.compare(
                    q, work / f"check_{q}.json",
                    oracle=oracle / q / "instances.csv", sql=sql / q / "instances.csv", mbft=mbft / q / "instances.csv",
                )
                report = json.loads((work / f"check_{q}.json").read_text(encoding="utf-8"))
                self.assertTrue(report["all_pass"])
                self.assertEqual(report["backends"]["oracle"]["result_count"], expected_counts[q])

    def test_compact_funnelq_event_source_against_oracle(self) -> None:
        """The compact store must retain every canonical full binding.

        This test does not need Cargo. It bypasses only the Rust candidate
        pruning stage by opening all valid roots, then exercises the exact same
        compact event source and neutral full-binding matcher used by the formal
        FunnelQ entry.
        """
        from systems.nft.common.canonical_results import canonicalize_rows, summarize_canonical_csv
        from systems.nft.common.full_binding_spec import load_full_binding_plan
        from systems.nft.common.generic_exact import iter_plan_matches
        from systems.nft.hvceq.query_nft_hvceq_full import (
            _load_closure_descriptors,
            _load_collection_ranges,
        )

        with tempfile.TemporaryDirectory(prefix="nft_compact_funnelq_") as td:
            work = Path(td)
            oracle = work / "oracle"
            self.run_cmd(
                sys.executable,
                ROOT / "tools" / "nft_bruteforce_oracle.py",
                "--events-csv", EVENTS,
                "--outdir", oracle,
            )
            fq = work / "fq"
            fq.mkdir()
            fqdb = fq / "nft_hvceq_episode_sidecar.db"
            self.run_cmd(
                sys.executable,
                ROOT / "scripts" / "build_nft_funnelq_compact_store.py",
                "--event-atoms-csv", EVENTS,
                "--sqlite-out", fqdb,
                "--outdir", fq,
                "--max-rss-mb", "4096",
                "--skip-vacuum",
            )

            with closing(sqlite3.connect(fqdb)) as conn:
                conn.row_factory = sqlite3.Row
                candidates = [
                    {
                        "closure_id": int(r[0]),
                        "anchor_order": int(r[1]),
                        "end_ts": int(r[2]),
                        "collection_id": int(r[3]),
                    }
                    for r in conn.execute(
                        "SELECT closure_id,anchor_order_seq,end_ts,collection_id "
                        "FROM nft_closure_summary ORDER BY closure_id"
                    )
                ]
                descriptors = _load_closure_descriptors(conn, candidates)
                events_by_collection, _ = _load_collection_ranges(conn, descriptors)

            order_arrays = {
                collection_id: [int(e["order_seq"]) for e in events]
                for collection_id, events in events_by_collection.items()
            }
            for q in QUERIES:
                _, plan = load_full_binding_plan(ROOT / "queries" / "nft" / "final5" / f"{q}.json")
                rows = []
                for cand in candidates:
                    desc = descriptors[int(cand["closure_id"])]
                    collection_id = int(desc["collection_id"])
                    events = events_by_collection[collection_id]
                    orders = order_arrays[collection_id]
                    lo = bisect.bisect_left(orders, int(desc["anchor_order"]))
                    closure_events = [
                        e for e in events[lo:]
                        if int(e["block_ts"]) <= int(desc["end_ts"])
                    ]
                    root = next(
                        (e for e in closure_events if int(e["order_seq"]) == int(desc["anchor_order"])),
                        None,
                    )
                    self.assertIsNotNone(root)
                    rows.extend(iter_plan_matches(plan, closure_events, fixed_root=root))
                _, summary = canonicalize_rows(rows)
                oracle_summary = summarize_canonical_csv(oracle / q / "instances.csv")
                self.assertEqual(summary["result_count"], oracle_summary["result_count"])
                self.assertEqual(summary["result_digest_sha256"], oracle_summary["result_digest_sha256"])

            with closing(sqlite3.connect(fqdb)) as conn:
                tables = {
                    r[0]
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            for forbidden in {
                "nft_events",
                "nft_pathlet_index",
                "nft_atom_postings",
                "nft_atom_pair_postings",
                "nft_role_postings",
                "nft_closure_records",
            }:
                self.assertNotIn(forbidden, tables)

    @unittest.skipUnless(shutil.which("cargo"), "cargo is unavailable; Rust backends are validated on the experiment host")
    def test_rust_hgmatch_and_funnelq_against_oracle(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nft_full_binding_rust_") as td:
            work = Path(td)
            oracle = work / "oracle"
            self.run_cmd(sys.executable, ROOT / "tools" / "nft_bruteforce_oracle.py", "--events-csv", EVENTS, "--outdir", oracle)

            hg = work / "hg"
            hg.mkdir()
            hgdb = hg / "nft_hgmatch_store.db"
            self.run_cmd(
                sys.executable, ROOT / "systems" / "nft" / "hgmatch" / "build_nft_hgmatch_store.py",
                "--event-atoms-csv", EVENTS, "--sqlite-out", hgdb, "--report-json", hg / "build.json",
            )

            fq = work / "fq"
            fq.mkdir()
            fqdb = fq / "nft_hvceq_episode_sidecar.db"
            self.run_cmd(
                sys.executable, ROOT / "scripts" / "build_nft_funnelq_compact_store.py",
                "--event-atoms-csv", EVENTS,
                "--outdir", fq,
                "--sqlite-out", fqdb,
                "--report-json", fq / "build.json",
                "--max-delta", "86400",
                "--max-rss-mb", "4096",
                "--skip-vacuum",
            )

            for q in QUERIES:
                qjson = ROOT / "queries" / "nft" / "final5" / f"{q}.json"
                self.run_cmd(
                    sys.executable, ROOT / "systems" / "nft" / "hgmatch" / "query_nft_hgmatch.py",
                    "--db", hgdb, "--query-json", qjson, "--outdir", hg / q, "--threads", "2",
                )
                self.run_cmd(
                    sys.executable, ROOT / "systems" / "nft" / "hvceq" / "query_nft_hvceq_full.py",
                    "--db", fqdb, "--query-json", qjson, "--outdir", fq / q, "--ablation", "no_pruning",
                )
                self.compare(
                    q, work / f"rust_check_{q}.json",
                    oracle=oracle / q / "instances.csv", hgmatch=hg / q / "instances.csv", funnelq=fq / q / "instances.csv",
                )


if __name__ == "__main__":
    unittest.main()

