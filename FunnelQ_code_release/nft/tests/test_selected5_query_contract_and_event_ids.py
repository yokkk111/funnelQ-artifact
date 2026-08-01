from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.build_nft_funnelq_compact_store import create_schema
from systems.nft.common.full_binding_spec import load_full_binding_plan
from systems.nft.hvceq.query_nft_hvceq_full import _match_selected_cycle
from systems.nft.hvceq.selected5_query_contract import (
    validate_selected5_query_contract,
)
from systems.nft.mbft.nft_semantic_matcher import load_jsonl


ROOT = Path(__file__).resolve().parents[1]
QUERY_DIR = ROOT / "queries" / "nft" / "selected5"
Q1 = QUERY_DIR / "Q1_RAPID_SAME_NFT_ROUNDTRIP.json"
Q3 = QUERY_DIR / "Q3_SAME_NFT_THREE_ACTOR_CYCLE.json"


class SelectedFiveQueryContractTest(unittest.TestCase):
    def test_reference_query_is_accepted(self) -> None:
        document = json.loads(Q1.read_text(encoding="utf-8"))
        result = validate_selected5_query_contract(document)
        self.assertEqual(result["status"], "MATCHED")
        self.assertEqual(result["query_id"], "Q1_RAPID_SAME_NFT_ROUNDTRIP")

    def test_changed_query_semantics_are_rejected(self) -> None:
        document = json.loads(Q1.read_text(encoding="utf-8"))
        changed = copy.deepcopy(document)
        changed["full_binding_plan"]["delta_seconds"] += 1
        with self.assertRaisesRegex(ValueError, "does not match"):
            validate_selected5_query_contract(changed)

    def test_specialized_exact_match_rejects_reused_event_id(self) -> None:
        _, plan = load_full_binding_plan(Q1)
        root = {
            "event_id": "duplicate-id",
            "event_type": "NFTTrade",
            "move_from": "A",
            "move_to": "B",
            "nft": "X",
            "collection_contract": "C",
            "block_ts": 100,
            "order_seq": 10,
            "tx_hash": "tx-root",
        }
        back = {
            "event_id": "duplicate-id",
            "event_type": "NFTTrade",
            "move_from": "B",
            "move_to": "A",
            "nft": "X",
            "collection_contract": "C",
            "block_ts": 101,
            "order_seq": 11,
            "tx_hash": "tx-back",
        }
        self.assertEqual(_match_selected_cycle(plan, [root, back], root), [])

    def test_three_actor_exact_match_requires_three_distinct_addresses(self) -> None:
        _, plan = load_full_binding_plan(Q3)
        root = {
            "event_id": "root",
            "event_type": "NFTTrade",
            "move_from": "A",
            "move_to": "B",
            "nft": "X",
            "collection_contract": "COLL",
            "block_ts": 100,
            "order_seq": 10,
            "tx_hash": "tx-root",
        }
        middle = {
            "event_id": "middle",
            "event_type": "NFTTrade",
            "move_from": "B",
            "move_to": "A",
            "nft": "X",
            "collection_contract": "COLL",
            "block_ts": 101,
            "order_seq": 11,
            "tx_hash": "tx-middle",
        }
        back = {
            "event_id": "back",
            "event_type": "NFTTrade",
            "move_from": "A",
            "move_to": "A",
            "nft": "X",
            "collection_contract": "COLL",
            "block_ts": 102,
            "order_seq": 12,
            "tx_hash": "tx-back",
        }
        self.assertEqual(
            _match_selected_cycle(plan, [root, middle, back], root),
            [],
        )


class EventIdUniquenessTest(unittest.TestCase):
    def test_compact_schema_rejects_duplicate_event_id(self) -> None:
        connection = sqlite3.connect(":memory:")
        try:
            create_schema(connection)
            row = (1, "same-id", 1, 100, 1, 2, 3, 4)
            connection.execute(
                """
                INSERT INTO nft_role_transition_edges(
                    order_seq,event_id,event_type_id,block_ts,
                    src_addr_id,dst_addr_id,nft_id,collection_id
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                row,
            )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO nft_role_transition_edges(
                        order_seq,event_id,event_type_id,block_ts,
                        src_addr_id,dst_addr_id,nft_id,collection_id
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (2, "same-id", 1, 101, 2, 1, 3, 4),
                )
        finally:
            connection.close()

    def test_mbft_rejects_conflicting_duplicate_event_id(self) -> None:
        first = {
            "event_id": "same-id",
            "event_type": "NFTTrade",
            "move_from": "A",
            "move_to": "B",
            "nft": "X",
            "collection_contract": "C",
            "block_ts": 100,
            "order_seq": 1,
            "tx_hash": "tx-1",
        }
        second = dict(first, move_to="Z", order_seq=2)
        with tempfile.TemporaryDirectory(prefix="mbft_duplicate_id_") as td:
            path = Path(td) / "events.jsonl"
            path.write_text(
                json.dumps(first) + "\n" + json.dumps(second) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "conflicting MBFT"):
                load_jsonl([path])

    def test_mbft_deduplicates_identical_duplicate_event_id(self) -> None:
        event = {
            "event_id": "same-id",
            "event_type": "NFTTrade",
            "move_from": "A",
            "move_to": "B",
            "nft": "X",
            "collection_contract": "C",
            "block_ts": 100,
            "order_seq": 1,
            "tx_hash": "tx-1",
        }
        with tempfile.TemporaryDirectory(prefix="mbft_identical_id_") as td:
            path = Path(td) / "events.jsonl"
            path.write_text(
                json.dumps(event) + "\n" + json.dumps(event) + "\n",
                encoding="utf-8",
            )
            events, duplicate_count = load_jsonl([path])
            self.assertEqual(len(events), 1)
            self.assertEqual(duplicate_count, 1)


if __name__ == "__main__":
    unittest.main()
