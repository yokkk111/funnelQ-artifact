#!/usr/bin/env python
from __future__ import annotations

import argparse
import bisect
import json
import sqlite3
import time
from array import array
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


QUERY_TO_PLAN = {
    "flashloan_swap_borrow_episode_v1": "swap_borrow",
    "flashloan_swap_borrow_followup_episode_v1": "swap_borrow_followup",
    "flashloan_2swaps_distinct_followup_episode_v1": "two_swaps_distinct_followup",
    "flashloan_3swaps_chain_reuse_episode_v1": "three_swaps_chain_reuse",
    "flashloan_3swaps_distinct_parallel_episode_v1": "three_swaps_distinct_parallel",
}


class EncodedRuntime:
    def __init__(
        self,
        conn: sqlite3.Connection,
        window_sec: int,
        enable_terminal_complete_dirs: bool = True,
        enable_terminal_anti_join_dirs: bool = False,
        terminal_direct_scan_limit: int = 2048,
        load_tx_type_positions: bool = True,
        enable_role_postings: bool = True,
        event_types: Sequence[str] | None = None,
    ):
        self.conn = conn
        self.window_sec = int(window_sec)
        self.enable_terminal_complete_dirs = bool(enable_terminal_complete_dirs)
        self.enable_terminal_anti_join_dirs = bool(enable_terminal_anti_join_dirs)
        self.terminal_direct_scan_limit = int(terminal_direct_scan_limit)
        self.enable_role_postings = bool(enable_role_postings)
        self.loaded_event_types = set(event_types or self._event_type_ids().keys())
        self.swap_terminal_anti_join_dirs_built = False
        self.event_type_ids = self._event_type_ids()
        self.max_tx = int(conn.execute("SELECT MAX(global_tx_ord) FROM episode_tx_timeline").fetchone()[0] or 0)
        self.window_end = self._load_window_endpoints()
        self.tx_max_event_ord = self._load_tx_max_event_ord()
        self.type_prefix = self._load_type_prefix()
        self.tx_type_positions = (
            self._load_tx_type_positions() if load_tx_type_positions else {}
        )
        self._load_anchors()
        self._load_swaps()
        self._load_borrows()

    def _has_column(self, table: str, column: str) -> bool:
        return any(
            str(row[1]) == column
            for row in self.conn.execute(f"PRAGMA table_info({table})")
        )

    def _event_type_ids(self) -> Dict[str, int]:
        return {name: int(eid) for eid, name in self.conn.execute("SELECT event_type_id, event_type FROM event_type_dict")}

    def _load_window_endpoints(self) -> array:
        out = array("q", range(self.max_tx + 1))
        for start, end in self.conn.execute(
            "SELECT start_tx_ord, end_tx_ord FROM episode_window_endpoints WHERE window_sec=? ORDER BY start_tx_ord",
            (self.window_sec,),
        ):
            s = int(start)
            if 0 <= s <= self.max_tx:
                out[s] = int(end)
        return out

    def _load_tx_max_event_ord(self) -> array:
        out = array("q", [0] * (self.max_tx + 1))
        for tx, mx in self.conn.execute(
            "SELECT global_tx_ord, MAX(global_event_ord) FROM episode_event_timeline GROUP BY global_tx_ord"
        ):
            out[int(tx)] = int(mx or 0)
        last = 0
        for i in range(len(out)):
            if out[i] == 0 and i > 0:
                out[i] = last
            else:
                last = out[i]
        return out

    def _load_type_prefix(self) -> Dict[str, array]:
        prefixes = {
            name: array("i", [0] * (self.max_tx + 2))
            for name in self.event_type_ids
            if name in self.loaded_event_types
        }
        id_to_name = {v: k for k, v in self.event_type_ids.items()}
        per_tx: Dict[int, List[Tuple[str, int]]] = defaultdict(list)
        for tx, etid, cnt in self.conn.execute("SELECT global_tx_ord, event_type_id, count FROM episode_tx_type_counts"):
            name = id_to_name.get(int(etid))
            if name:
                per_tx[int(tx)].append((name, int(cnt)))
        running = {name: 0 for name in self.event_type_ids}
        for tx in range(self.max_tx + 1):
            for name, cnt in per_tx.get(tx, []):
                running[name] += cnt
            for name, arr in prefixes.items():
                arr[tx + 1] = running[name]
        return prefixes

    def _load_tx_type_positions(self) -> Dict[int, Dict[str, array]]:
        id_to_name = {v: k for k, v in self.event_type_ids.items()}
        tmp: Dict[int, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))
        for tx, etid, seq in self.conn.execute(
            "SELECT global_tx_ord, event_type_id, seq_index FROM episode_event_timeline ORDER BY global_tx_ord, event_type_id, seq_index"
        ):
            name = id_to_name.get(int(etid))
            if name:
                tmp[int(tx)][name].append(int(seq or 0))
        return {tx: {name: array("i", vals) for name, vals in by_type.items()} for tx, by_type in tmp.items()}

    def _load_anchors(self):
        self.a_event_ord = array("q")
        self.a_tx_ord = array("q")
        self.a_seq = array("i")
        self.a_protocol = array("i")
        self.a_recipient = array("i")
        self.a_asset = array("i")
        self.a_pool = array("i")
        self.a_amount = array("d")
        if "flashloan" not in self.loaded_event_types:
            return
        amount_expr = "amount_num" if self._has_column("support_flashloan", "amount_num") else "0.0"
        for row in self.conn.execute(
            f"SELECT global_event_ord, global_tx_ord, seq_index, protocol_id, recipient_id, asset_id, pool_id, {amount_expr} FROM support_flashloan ORDER BY global_event_ord"
        ):
            self.a_event_ord.append(int(row[0] or 0))
            self.a_tx_ord.append(int(row[1] or 0))
            self.a_seq.append(int(row[2] or 0))
            self.a_protocol.append(int(row[3] or 0))
            self.a_recipient.append(int(row[4] or 0))
            self.a_asset.append(int(row[5] or 0))
            self.a_pool.append(int(row[6] or 0))
            self.a_amount.append(float(row[7] or 0.0))

    def _load_swaps(self):
        self.s_event_ord = array("q")
        self.s_tx_ord = array("q")
        self.s_seq = array("i")
        self.s_protocol = array("i")
        self.s_taker = array("i")
        self.s_sold = array("i")
        self.s_bought = array("i")
        self.s_pool = array("i")
        self.s_amount_in = array("d")
        self.s_amount_out = array("d")
        self.swap_ts = {}
        self.swap_ts_complete = {}
        self.swap_terminal_bought = {}
        self.swap_terminal_pool = {}
        self.swap_terminal_bought_pool = {}
        if "swap" not in self.loaded_event_types:
            return
        amount_in_expr = "amount_in_num" if self._has_column("support_swap", "amount_in_num") else "0.0"
        amount_out_expr = "amount_out_num" if self._has_column("support_swap", "amount_out_num") else "0.0"
        tmp: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        # A query-independent directory for canonical swaps that contain all
        # four schema roles.  It is used only by the count-only terminal
        # aggregate in the generic matcher; normal matching keeps the full
        # posting directory and validates roles event by event.
        tmp_complete: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        # Anti-join counting directories are intentionally not built here.
        # They can add noticeable RSS for simple episode queries that never
        # need terminal neq aggregation.  When a count-only query actually
        # reaches a large terminal anti-join domain, they are built lazily by
        # _ensure_swap_terminal_anti_join_dirs().
        for row in self.conn.execute(
            """
            SELECT global_event_ord, global_tx_ord, seq_index, protocol_id, taker_id, token_sold_id, token_bought_id, pool_id,
                   {amount_in_expr}, {amount_out_expr}
            FROM support_swap
            ORDER BY global_event_ord
            """.format(amount_in_expr=amount_in_expr, amount_out_expr=amount_out_expr)
        ):
            idx = len(self.s_event_ord)
            ev, tx, seq, protocol, taker, sold, bought, pool = (
                int(x or 0) for x in row[:8]
            )
            self.s_event_ord.append(ev)
            self.s_tx_ord.append(tx)
            self.s_seq.append(seq)
            self.s_protocol.append(protocol)
            self.s_taker.append(taker)
            self.s_sold.append(sold)
            self.s_bought.append(bought)
            self.s_pool.append(pool)
            self.s_amount_in.append(float(row[8] or 0.0))
            self.s_amount_out.append(float(row[9] or 0.0))
            if self.enable_role_postings:
                tmp[(taker, sold)].append(idx)
            if taker and sold and bought and pool:
                if self.enable_terminal_complete_dirs:
                    tmp_complete[(taker, sold)].append(idx)
        self.swap_ts: Dict[Tuple[int, int], Tuple[array, array]] = {}
        for key, idxs in tmp.items():
            self.swap_ts[key] = (array("q", (self.s_event_ord[i] for i in idxs)), array("i", idxs))
        self.swap_ts_complete: Dict[Tuple[int, int], Tuple[array, array]] = {}
        for key, idxs in tmp_complete.items():
            self.swap_ts_complete[key] = (
                array("q", (self.s_event_ord[i] for i in idxs)),
                array("i", idxs),
            )
        self.swap_terminal_bought: Dict[Tuple[int, int, int], array] = {}
        self.swap_terminal_pool: Dict[Tuple[int, int, int], array] = {}
        self.swap_terminal_bought_pool: Dict[Tuple[int, int, int, int], array] = {}

    def _load_borrows(self):
        self.b_event_ord = array("q")
        self.b_tx_ord = array("q")
        self.b_seq = array("i")
        self.b_protocol = array("i")
        self.b_borrower = array("i")
        self.b_asset = array("i")
        self.b_pool = array("i")
        self.b_amount = array("d")
        self.borrow_ba = {}
        self.borrow_ba_complete = {}
        if "borrow" not in self.loaded_event_types:
            return
        amount_expr = "amount_num" if self._has_column("support_borrow", "amount_num") else "0.0"
        tmp: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        tmp_complete: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        for row in self.conn.execute(
            """
            SELECT global_event_ord, global_tx_ord, seq_index, protocol_id, borrower_id, asset_id, pool_id,
                   {amount_expr}
            FROM support_borrow
            ORDER BY global_event_ord
            """.format(amount_expr=amount_expr)
        ):
            idx = len(self.b_event_ord)
            ev, tx, seq, protocol, borrower, asset, pool = (
                int(x or 0) for x in row[:7]
            )
            self.b_event_ord.append(ev)
            self.b_tx_ord.append(tx)
            self.b_seq.append(seq)
            self.b_protocol.append(protocol)
            self.b_borrower.append(borrower)
            self.b_asset.append(asset)
            self.b_pool.append(pool)
            self.b_amount.append(float(row[7] or 0.0))
            if self.enable_role_postings:
                tmp[(borrower, asset)].append(idx)
            if borrower and asset and pool:
                if self.enable_terminal_complete_dirs:
                    tmp_complete[(borrower, asset)].append(idx)
        self.borrow_ba: Dict[Tuple[int, int], Tuple[array, array]] = {}
        for key, idxs in tmp.items():
            self.borrow_ba[key] = (array("q", (self.b_event_ord[i] for i in idxs)), array("i", idxs))
        self.borrow_ba_complete: Dict[Tuple[int, int], Tuple[array, array]] = {}
        for key, idxs in tmp_complete.items():
            self.borrow_ba_complete[key] = (
                array("q", (self.b_event_ord[i] for i in idxs)),
                array("i", idxs),
            )

    def _ensure_swap_terminal_anti_join_dirs(self) -> bool:
        """Build fixed swap anti-join directories only when a query needs them.

        This keeps simple Episode queries from paying the RSS cost of
        `(taker, sold, bought)`, `(taker, sold, pool)`, and
        `(taker, sold, bought, pool)` directories.  The directories are still
        schema-level and query-name independent; the query JSON only decides
        whether the exact terminal-domain optimization is worth activating.
        """
        if self.swap_terminal_anti_join_dirs_built:
            return True
        if not self.enable_terminal_anti_join_dirs:
            return False
        tmp_bought: Dict[Tuple[int, int, int], List[int]] = defaultdict(list)
        tmp_pool: Dict[Tuple[int, int, int], List[int]] = defaultdict(list)
        tmp_bought_pool: Dict[Tuple[int, int, int, int], List[int]] = defaultdict(list)
        for i, ev in enumerate(self.s_event_ord):
            taker = int(self.s_taker[i])
            sold = int(self.s_sold[i])
            bought = int(self.s_bought[i])
            pool = int(self.s_pool[i])
            if taker and sold and bought and pool:
                ev_ord = int(ev)
                tmp_bought[(taker, sold, bought)].append(ev_ord)
                tmp_pool[(taker, sold, pool)].append(ev_ord)
                tmp_bought_pool[(taker, sold, bought, pool)].append(ev_ord)
        self.swap_terminal_bought = {key: array("q", vals) for key, vals in tmp_bought.items()}
        self.swap_terminal_pool = {key: array("q", vals) for key, vals in tmp_pool.items()}
        self.swap_terminal_bought_pool = {key: array("q", vals) for key, vals in tmp_bought_pool.items()}
        self.swap_terminal_anti_join_dirs_built = True
        return True

    def _count_complete_swaps_by_scan(
        self,
        taker: int,
        sold: int,
        lo_exclusive: int,
        hi_inclusive: int,
        exclude_bought: Iterable[int] = (),
        exclude_pool: Iterable[int] = (),
    ) -> int:
        eb = {int(v) for v in exclude_bought if int(v) != 0}
        ep = {int(v) for v in exclude_pool if int(v) != 0}
        total = 0
        for si in self.complete_swaps(int(taker), int(sold), int(lo_exclusive) + 1, int(hi_inclusive)):
            if eb and int(self.s_bought[si]) in eb:
                continue
            if ep and int(self.s_pool[si]) in ep:
                continue
            total += 1
        return total

    def range_count(self, etype: str, lo_tx: int, hi_tx: int) -> int:
        arr = self.type_prefix[etype]
        lo = max(0, int(lo_tx))
        hi = max(lo, int(hi_tx))
        return int(arr[min(len(arr) - 1, hi + 1)] - arr[min(len(arr) - 1, lo)])

    @staticmethod
    def _slice(bucket: Tuple[array, array] | None, lo_ord: int, hi_ord: int) -> array:
        if bucket is None:
            return array("i")
        ords, idxs = bucket
        l = bisect.bisect_left(ords, int(lo_ord))
        r = bisect.bisect_right(ords, int(hi_ord))
        return idxs[l:r]

    def swaps(self, taker: int, sold: int, lo_ord: int, hi_ord: int) -> array:
        return self._slice(self.swap_ts.get((int(taker), int(sold))), lo_ord, hi_ord)

    def borrows(self, borrower: int, asset: int, lo_ord: int, hi_ord: int) -> array:
        return self._slice(self.borrow_ba.get((int(borrower), int(asset))), lo_ord, hi_ord)

    def complete_swaps(self, taker: int, sold: int, lo_ord: int, hi_ord: int) -> array:
        return self._slice(self.swap_ts_complete.get((int(taker), int(sold))), lo_ord, hi_ord)

    def complete_borrows(self, borrower: int, asset: int, lo_ord: int, hi_ord: int) -> array:
        return self._slice(self.borrow_ba_complete.get((int(borrower), int(asset))), lo_ord, hi_ord)

    @staticmethod
    def _count_after_before(ords: array | None, lo_exclusive: int, hi_inclusive: int) -> int:
        if ords is None:
            return 0
        left = bisect.bisect_right(ords, int(lo_exclusive))
        right = bisect.bisect_right(ords, int(hi_inclusive))
        return max(0, right - left)

    def count_complete_swaps_with_neq(
        self,
        taker: int,
        sold: int,
        lo_exclusive: int,
        hi_inclusive: int,
        exclude_bought: Iterable[int] = (),
        exclude_pool: Iterable[int] = (),
    ) -> int:
        """Exact count in a canonical swap terminal domain with anti-joins.

        With no terminal neq predicates, this is just a range count over the
        complete `(taker, sold)` directory.  With neq predicates, the method
        first uses a direct per-domain scan for small terminal domains; only
        large anti-join domains trigger construction of the global fixed
        anti-join directories.  This reduces peak RSS for simple queries while
        preserving the fast inclusion-exclusion path for large three-swap
        Episode queries.
        """
        if not self.enable_terminal_complete_dirs:
            return 0
        t = int(taker)
        s = int(sold)
        lo = int(lo_exclusive)
        hi = int(hi_inclusive)
        base = self.swap_ts_complete.get((t, s))
        total = self._count_after_before(base[0] if base else None, lo, hi)
        bought_values = {int(v) for v in exclude_bought if int(v) != 0}
        pool_values = {int(v) for v in exclude_pool if int(v) != 0}
        if not bought_values and not pool_values:
            return total
        if total <= max(0, int(self.terminal_direct_scan_limit)):
            return self._count_complete_swaps_by_scan(t, s, lo, hi, bought_values, pool_values)
        if not self._ensure_swap_terminal_anti_join_dirs():
            return self._count_complete_swaps_by_scan(t, s, lo, hi, bought_values, pool_values)
        removed_bought = sum(
            self._count_after_before(self.swap_terminal_bought.get((t, s, b)), lo, hi)
            for b in bought_values
        )
        removed_pool = sum(
            self._count_after_before(self.swap_terminal_pool.get((t, s, p)), lo, hi)
            for p in pool_values
        )
        restored_overlap = sum(
            self._count_after_before(self.swap_terminal_bought_pool.get((t, s, b, p)), lo, hi)
            for b in bought_values
            for p in pool_values
        )
        return int(total - removed_bought - removed_pool + restored_overlap)


    def count_swaps(
        self,
        taker: int,
        sold: int,
        lo_ord: int,
        hi_ord: int,
        exclude_bought: Iterable[int] = (),
        exclude_pool: Iterable[int] = (),
    ) -> int:
        eb = set(int(x) for x in exclude_bought)
        ep = set(int(x) for x in exclude_pool)
        total = 0
        for si in self.swaps(taker, sold, lo_ord, hi_ord):
            if eb and self.s_bought[si] in eb:
                continue
            if ep and self.s_pool[si] in ep:
                continue
            total += 1
        return total


def parse_args():
    ap = argparse.ArgumentParser(description="Encoded-array SQLite sidecar HVCEQ episode specialized runner.")
    ap.add_argument("--episode-sidecar-db", required=True)
    ap.add_argument("--query-json", required=True)
    ap.add_argument("--outdir", required=True)
    return ap.parse_args()


def load_query(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def need_counts(plan: str) -> Dict[str, int]:
    if plan == "swap_borrow":
        return {"flashloan": 1, "swap": 1, "borrow": 1}
    if plan == "swap_borrow_followup":
        return {"flashloan": 1, "swap": 2, "borrow": 1}
    return {"flashloan": 1, "swap": 3}


def base_ok(rt: EncodedRuntime, ai: int, end_tx: int, plan: str) -> bool:
    lo_tx = rt.a_tx_ord[ai]
    anchor_seq = rt.a_seq[ai]
    for etype, needed in need_counts(plan).items():
        total = rt.range_count(etype, lo_tx, end_tx)
        pos = rt.tx_type_positions.get(lo_tx, {}).get(etype)
        before_anchor = bisect.bisect_left(pos, anchor_seq) if pos is not None else 0
        if total - before_anchor < needed:
            return False
    return True


def exact_swap_borrow(rt: EncodedRuntime, ai: int, hi_ord: int, first: bool) -> int:
    total = 0
    u = rt.a_recipient[ai]
    x = rt.a_asset[ai]
    lo = rt.a_event_ord[ai] + 1
    for e2 in rt.swaps(u, x, lo, hi_ord):
        c = len(rt.borrows(u, rt.s_bought[e2], rt.s_event_ord[e2] + 1, hi_ord))
        total += c
        if first and total:
            return total
    return total


def exact_swap_borrow_followup(rt: EncodedRuntime, ai: int, hi_ord: int, first: bool) -> int:
    total = 0
    u = rt.a_recipient[ai]
    x = rt.a_asset[ai]
    lo = rt.a_event_ord[ai] + 1
    for e2 in rt.swaps(u, x, lo, hi_ord):
        y = rt.s_bought[e2]
        p1 = rt.s_pool[e2]
        for b in rt.borrows(u, y, rt.s_event_ord[e2] + 1, hi_ord):
            total += rt.count_swaps(u, y, rt.b_event_ord[b] + 1, hi_ord, exclude_pool=[p1])
            if first and total:
                return total
    return total


def exact_two_swaps_distinct_followup(rt: EncodedRuntime, ai: int, hi_ord: int, first: bool) -> int:
    total = 0
    u = rt.a_recipient[ai]
    x = rt.a_asset[ai]
    rows = rt.swaps(u, x, rt.a_event_ord[ai] + 1, hi_ord)
    for pos_i, e2 in enumerate(rows):
        y1 = rt.s_bought[e2]
        p1 = rt.s_pool[e2]
        for e3 in rows[pos_i + 1 :]:
            if rt.s_bought[e3] == y1 or rt.s_pool[e3] == p1:
                continue
            total += rt.count_swaps(u, y1, rt.s_event_ord[e3] + 1, hi_ord)
            if first and total:
                return total
    return total


def exact_three_swaps_chain_reuse(rt: EncodedRuntime, ai: int, hi_ord: int, first: bool) -> int:
    total = 0
    u = rt.a_recipient[ai]
    x = rt.a_asset[ai]
    for e2 in rt.swaps(u, x, rt.a_event_ord[ai] + 1, hi_ord):
        y1 = rt.s_bought[e2]
        if not y1 or y1 == x:
            continue
        p1 = rt.s_pool[e2]
        for e3 in rt.swaps(u, y1, rt.s_event_ord[e2] + 1, hi_ord):
            y2 = rt.s_bought[e3]
            if not y2 or y2 == x or rt.s_pool[e3] == p1:
                continue
            total += rt.count_swaps(
                u,
                y2,
                rt.s_event_ord[e3] + 1,
                hi_ord,
                exclude_bought=[x, y1],
                exclude_pool=[p1, rt.s_pool[e3]],
            )
            if first and total:
                return total
    return total


def exact_three_swaps_distinct_parallel(rt: EncodedRuntime, ai: int, hi_ord: int, first: bool) -> int:
    total = 0
    u = rt.a_recipient[ai]
    x = rt.a_asset[ai]
    rows = rt.swaps(u, x, rt.a_event_ord[ai] + 1, hi_ord)
    n = len(rows)
    for i in range(n):
        e2 = rows[i]
        y1 = rt.s_bought[e2]
        p1 = rt.s_pool[e2]
        if not y1:
            continue
        for j in range(i + 1, n):
            e3 = rows[j]
            y2 = rt.s_bought[e3]
            p2 = rt.s_pool[e3]
            if not y2 or y2 == y1 or p2 == p1:
                continue
            for k in range(j + 1, n):
                e4 = rows[k]
                if rt.s_bought[e4] in {0, y1, y2}:
                    continue
                if rt.s_pool[e4] in {p1, p2}:
                    continue
                total += 1
                if first:
                    return total
    return total


def exact(rt: EncodedRuntime, plan: str, ai: int, hi_ord: int, first: bool) -> int:
    if plan == "swap_borrow":
        return exact_swap_borrow(rt, ai, hi_ord, first)
    if plan == "swap_borrow_followup":
        return exact_swap_borrow_followup(rt, ai, hi_ord, first)
    if plan == "two_swaps_distinct_followup":
        return exact_two_swaps_distinct_followup(rt, ai, hi_ord, first)
    if plan == "three_swaps_chain_reuse":
        return exact_three_swaps_chain_reuse(rt, ai, hi_ord, first)
    if plan == "three_swaps_distinct_parallel":
        return exact_three_swaps_distinct_parallel(rt, ai, hi_ord, first)
    raise ValueError(plan)


def main() -> int:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    query = load_query(Path(args.query_json))
    plan = QUERY_TO_PLAN[query["query_name"]]
    window_sec = int(query["scope_params"]["forward_window_sec"])

    t0 = time.perf_counter()
    conn = sqlite3.connect(args.episode_sidecar_db)
    rt = EncodedRuntime(conn, window_sec)
    conn.close()
    t1 = time.perf_counter()

    cand0 = [(i, rt.window_end[rt.a_tx_ord[i]]) for i in range(len(rt.a_event_ord))]
    t2 = time.perf_counter()

    cand1 = [(i, end_tx) for i, end_tx in cand0 if base_ok(rt, i, end_tx, plan)]
    t3 = time.perf_counter()

    cand2: List[Tuple[int, int]] = []
    for i, end_tx in cand1:
        hi_ord = rt.tx_max_event_ord[end_tx]
        if exact(rt, plan, i, hi_ord, first=True) > 0:
            cand2.append((i, hi_ord))
    t4 = time.perf_counter()

    total = 0
    for i, hi_ord in cand2:
        total += exact(rt, plan, i, hi_ord, first=False)
    t5 = time.perf_counter()

    report = {
        "query_name": query["query_name"],
        "scope": "anchored_episode",
        "forward_window_sec": window_sec,
        "num_cand0": len(cand0),
        "num_cand1": len(cand1),
        "num_cand2": len(cand2),
        "num_matches": int(total),
        "num_result_episodes": int(len(cand2)),
        "num_false_positive_episodes": int(len(cand1) - len(cand2)),
        "num_residual_episodes": int(len(cand1) - len(cand2)),
        "stage_times_s": {
            "setup": round(t1 - t0, 6),
            "candidate_generation": round(t2 - t1, 6),
            "base_filter": round(t3 - t2, 6),
            "refined_filter": round(t4 - t3, 6),
            "exact_match": round(t5 - t4, 6),
        },
        "metrics": {
            "setup_s": round(t1 - t0, 6),
            "query_latency_s": round(t5 - t1, 6),
            "query_wall_time_s": round(t5 - t0, 6),
            "candidate_seed_mode": "encoded_sidecar_flashloan_array",
            "refined_filter_mode": "encoded_sidecar_specialized_existence",
            "specialized_exact_plan": plan,
            "exact_backend": "encoded_sidecar_python_arrays",
            "summary_only": True,
            "runtime_pack_enabled": False,
            "runtime_pack_used": False,
            "runtime_representation": "integer_encoded_arrays_and_postings_no_pickle_no_record_dicts",
            "num_flashloan_records": len(rt.a_event_ord),
            "num_swap_records": len(rt.s_event_ord),
            "num_borrow_records": len(rt.b_event_ord),
            "num_swap_postings": len(rt.swap_ts),
            "num_borrow_postings": len(rt.borrow_ba),
        },
    }
    (outdir / "query_result.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
