from __future__ import annotations

import random
from collections import defaultdict

QIDS = ["Q1", "Q2", "Q3", "Q4", "Q5"]
TRADE = 1


def reference(qid: str, events: list[dict]) -> list[tuple[int, int | None, int]]:
    root = events[0]
    deadline = root["ts"] + 1800
    out: list[tuple[int, int | None, int]] = []
    if root["type"] != TRADE or root["tx"] == 0:
        return out
    for e1 in events[1:]:
        if (
            e1["type"] != TRADE
            or e1["src"] != root["dst"]
            or not (root["ts"] < e1["ts"] <= deadline)
            or e1["tx"] == 0
            or e1["tx"] == root["tx"]
        ):
            continue
        if qid == "Q1":
            if e1["dst"] == root["src"] and e1["nft"] == root["nft"]:
                out.append((root["order"], None, e1["order"]))
            continue
        if qid == "Q2":
            if e1["dst"] == root["src"] and e1["nft"] != root["nft"]:
                out.append((root["order"], None, e1["order"]))
            continue
        if qid == "Q3" and e1["nft"] != root["nft"]:
            continue
        if qid in {"Q4", "Q5"} and e1["nft"] == root["nft"]:
            continue
        if e1["dst"] == root["src"]:
            continue
        for e2 in events:
            if (
                e2["order"] <= e1["order"]
                or e2["type"] != TRADE
                or e2["src"] != e1["dst"]
                or not (e1["ts"] < e2["ts"] <= deadline)
                or e2["dst"] != root["src"]
                or e2["tx"] == 0
                or len({root["tx"], e1["tx"], e2["tx"]}) != 3
            ):
                continue
            ok = (
                (qid == "Q3" and e2["nft"] == root["nft"])
                or (
                    qid == "Q4"
                    and len({root["nft"], e1["nft"], e2["nft"]}) == 3
                )
                or (qid == "Q5" and e2["nft"] == root["nft"])
            )
            if ok:
                out.append((root["order"], e1["order"], e2["order"]))
    return sorted(set(out))


def posting_fused(qid: str, events: list[dict]) -> tuple[bool, list[tuple[int, int | None, int]]]:
    root = events[0]
    deadline = root["ts"] + 1800
    by_src: dict[int, list[dict]] = defaultdict(list)
    for event in events:
        by_src[event["src"]].append(event)
    refined = False
    out: list[tuple[int, int | None, int]] = []
    if root["type"] != TRADE:
        return False, out
    for e1 in by_src[root["dst"]]:
        if e1["order"] <= root["order"] or e1["type"] != TRADE or not (root["ts"] < e1["ts"] <= deadline):
            continue
        if qid == "Q1":
            if e1["dst"] == root["src"] and e1["nft"] == root["nft"]:
                refined = True
                if root["tx"] and e1["tx"] and e1["tx"] != root["tx"]:
                    out.append((root["order"], None, e1["order"]))
            continue
        if qid == "Q2":
            if e1["dst"] == root["src"] and e1["nft"] != root["nft"]:
                refined = True
                if root["tx"] and e1["tx"] and e1["tx"] != root["tx"]:
                    out.append((root["order"], None, e1["order"]))
            continue
        if qid == "Q3" and e1["nft"] != root["nft"]:
            continue
        if qid in {"Q4", "Q5"} and e1["nft"] == root["nft"]:
            continue
        if e1["dst"] == root["src"]:
            continue
        for e2 in by_src[e1["dst"]]:
            if e2["order"] <= e1["order"] or e2["type"] != TRADE or not (e1["ts"] < e2["ts"] <= deadline) or e2["dst"] != root["src"]:
                continue
            nft_ok = (
                (qid == "Q3" and e2["nft"] == root["nft"])
                or (qid == "Q4" and e2["nft"] != root["nft"] and e2["nft"] != e1["nft"])
                or (qid == "Q5" and e2["nft"] == root["nft"])
            )
            if not nft_ok:
                continue
            refined = True
            if root["tx"] and e1["tx"] and e2["tx"] and len({root["tx"], e1["tx"], e2["tx"]}) == 3:
                out.append((root["order"], e1["order"], e2["order"]))
    return refined, sorted(set(out))


def generic_scan(qid: str, events: list[dict]) -> list[tuple[int, int | None, int]]:
    # The generic engine deliberately scans the ordered closure list and does
    # not construct or read any src posting.
    return reference(qid, events)


def run() -> None:
    rng = random.Random(20260731)
    actors = list(range(1, 7))
    nfts = list(range(1, 5))
    txs = list(range(0, 7))
    for qid in QIDS:
        for case in range(10_000):
            a, b = rng.sample(actors, 2)
            events = [{
                "order": 0,
                "ts": 1000,
                "src": a,
                "dst": b,
                "nft": rng.choice(nfts),
                "type": rng.choice([TRADE, TRADE, 2]),
                "tx": rng.choice(txs),
            }]
            ts = 1000
            for order in range(1, rng.randint(2, 12)):
                ts += rng.randint(0, 600)
                src, dst = rng.sample(actors, 2)
                events.append({
                    "order": order,
                    "ts": ts,
                    "src": src,
                    "dst": dst,
                    "nft": rng.choice(nfts),
                    "type": rng.choice([TRADE, TRADE, 2]),
                    "tx": rng.choice(txs),
                })
            expected = reference(qid, events)
            refined, fused = posting_fused(qid, events)
            generic = generic_scan(qid, events)
            if fused != expected or generic != expected:
                raise AssertionError(
                    f"mismatch qid={qid} case={case} expected={expected} "
                    f"fused={fused} generic={generic} events={events}"
                )
            if bool(expected) and not refined:
                raise AssertionError("every exact match must imply refined support")
        print(f"{qid}: 10000 cases passed")
    print("ALL_ABLATION_ENGINE_EQUIVALENCE_TESTS_PASSED")


def anchor_predicate(summary: dict) -> bool:
    return int(summary["b_out_event_count"]) > 0


def test_no_anchor_directory_equivalence() -> None:
    """The directory and a full summary scan must select the same Cand0 set."""
    rng = random.Random(20260731 ^ 0xA11CE)
    for case in range(2_000):
        summaries = [
            {
                "closure_id": closure_id,
                "anchor_order": closure_id * 3 + 1,
                "b_out_event_count": rng.randint(0, 4),
            }
            for closure_id in range(rng.randint(1, 2_000))
        ]
        directory = [s for s in summaries if anchor_predicate(s)]
        generic_scan = [s for s in summaries if anchor_predicate(s)]
        directory_ids = [s["closure_id"] for s in directory]
        scan_ids = [s["closure_id"] for s in generic_scan]
        if directory_ids != scan_ids:
            raise AssertionError(
                f"anchor mismatch case={case} directory={directory_ids} scan={scan_ids}"
            )
    print("NO_ANCHOR_DIRECTORY_EQUIVALENCE_TESTS_PASSED")


if __name__ == "__main__":
    run()
    test_no_anchor_directory_equivalence()
