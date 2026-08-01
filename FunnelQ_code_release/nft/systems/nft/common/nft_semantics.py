#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import combinations, product
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pandas as pd

QUERY_NAMES = [
    "nft_q1_buy_resell_24h",
    "nft_q2_round_trip_24h",
    "nft_q3_multi_hop_loop_24h",
    "nft_q4_trade_transfer_resell_24h",
    "nft_q5_repeated_pair_roundtrip_24h",
]

SECOND_TIER_QUERY_NAMES = [
    "nft_s1_trade_pair_swap_24h",
    "nft_s2_trade_pingpong_24h",
    "nft_s5_trade_swap_then_resell_received_24h",
    "nft_b1_same_nft_trade_pingpong_24h",
    "nft_s12_same_seller_two_asset_resale_24h",
]

QUERY_FILES = [f"{q}.json" for q in QUERY_NAMES]

EVENT_COLUMNS = [
    "event_id",
    "event_type",
    "block_time",
    "block_number",
    "tx_hash",
    "local_order",
    "nft",
    "seller",
    "buyer",
    "from_address",
    "to_address",
]

RUNTIME_COLUMNS = [
    "event_id",
    "event_type",
    "block_time",
    "block_number",
    "tx_hash",
    "local_order",
    "nft",
    "collection_contract",
    "seller",
    "buyer",
    "from_address",
    "to_address",
    "move_from",
    "move_to",
    "block_ts",
    "order_seq",
    "nft_pos",
]

ROLE_FIELDS = {"nft", "collection_contract", "seller", "buyer", "from_address", "to_address", "move_from", "move_to"}
META_PATTERN_KEYS = {"id", "event_type", "roles", "roles_any", "description"}
NULL_STRINGS = {"", "nan", "none", "null", "nat", "<nil>"}
VAR_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in NULL_STRINGS else text


def norm_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in NULL_STRINGS else text


def is_variable_token(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    s = value.strip()
    if not s or s.lower() in NULL_STRINGS:
        return False
    if s.startswith("$"):
        return True
    # NFT query variables are written as A/B/C/x. Literal blockchain values are
    # addresses, ids, or composite NFT identifiers and therefore fail this test.
    return VAR_RE.match(s) is not None


def var_name(value: str) -> str:
    return value[1:] if value.startswith("$") else value


def configure_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-32000")


def load_query(path: Path) -> Dict[str, Any]:
    query = json.loads(Path(path).read_text(encoding="utf-8"))
    if "query_name" not in query:
        raise ValueError(f"Missing query_name in {path}")
    if "event_patterns" not in query or not query["event_patterns"]:
        raise ValueError(f"Missing event_patterns in {path}")
    query["delta_seconds"] = int(query.get("delta_seconds", 86400))
    query.setdefault("constraints", [])
    return query


def load_event_atoms(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, low_memory=False).fillna("")
    missing = [c for c in EVENT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"NFT event atoms missing columns: {missing}")
    df = df[EVENT_COLUMNS].copy()
    for col in EVENT_COLUMNS:
        df[col] = df[col].map(clean_text)
    parsed = pd.to_datetime(df["block_time"], errors="coerce", utc=True)
    if parsed.isna().any():
        raise ValueError(f"Cannot parse block_time for {int(parsed.isna().sum())} rows")
    # pandas 2.x may preserve datetime64[us] on Windows.  Dividing its raw
    # integer by 1e9 silently turns seconds into 1000-second buckets.  Cast to
    # an explicit second resolution so this is independent of pandas/platform.
    df["block_ts"] = parsed.dt.as_unit("s").astype("int64")
    df["block_number_sort"] = pd.to_numeric(df["block_number"], errors="coerce").fillna(-1).astype("int64")
    df["local_order_sort"] = pd.to_numeric(df["local_order"], errors="coerce").fillna(-1).astype("int64")
    df["move_from"] = ""
    df["move_to"] = ""
    # Canonical NFT identifiers are contract:token_id.  Collection is derived
    # from that committed canonical identifier, never from optional source tags.
    df["collection_contract"] = df["nft"].map(lambda x: str(x).split(":", 1)[0] if ":" in str(x) else "")
    trade = df["event_type"].eq("NFTTrade")
    transfer = df["event_type"].eq("NFTTransfer")
    df.loc[trade, "move_from"] = df.loc[trade, "seller"]
    df.loc[trade, "move_to"] = df.loc[trade, "buyer"]
    df.loc[transfer, "move_from"] = df.loc[transfer, "from_address"]
    df.loc[transfer, "move_to"] = df.loc[transfer, "to_address"]
    df = df.sort_values(
        ["block_ts", "block_number_sort", "tx_hash", "local_order_sort", "event_id"],
        kind="mergesort",
    ).reset_index(drop=True)
    df["order_seq"] = range(len(df))
    df = df.sort_values(["nft", "block_ts", "order_seq"], kind="mergesort").reset_index(drop=True)
    df["nft_pos"] = df.groupby("nft", sort=False).cumcount().astype("int64")
    df = df.sort_values("order_seq", kind="mergesort").reset_index(drop=True)
    return df.drop(columns=["block_number_sort", "local_order_sort"])


@dataclass(frozen=True)
class PatternAlt:
    event_types: Tuple[str, ...]
    roles: Tuple[Tuple[str, Any], ...]


@dataclass(frozen=True)
class QueryPattern:
    qid: str
    alternatives: Tuple[PatternAlt, ...]


@dataclass
class CompiledQuery:
    query_name: str
    delta_seconds: int
    patterns: List[QueryPattern]
    constraints: List[Dict[str, Any]]
    variables: List[str]
    pattern_ids: List[str]


def _as_event_types(raw: Any) -> Tuple[str, ...]:
    if isinstance(raw, list):
        return tuple(str(x) for x in raw)
    if raw is None:
        return tuple()
    return (str(raw),)


def _roles_from_pattern(pattern: Dict[str, Any]) -> Dict[str, Any]:
    roles: Dict[str, Any] = {}
    roles.update(pattern.get("roles", {}) or {})
    for k, v in pattern.items():
        if k in META_PATTERN_KEYS:
            continue
        if k in ROLE_FIELDS:
            roles[k] = v
    return roles


def compile_query(query: Dict[str, Any]) -> CompiledQuery:
    patterns: List[QueryPattern] = []
    variables: set[str] = set()
    for p in query.get("event_patterns", []):
        qid = str(p["id"])
        alts: List[PatternAlt] = []
        if p.get("roles_any"):
            for raw_alt in p.get("roles_any") or []:
                alt = dict(raw_alt)
                event_types = _as_event_types(alt.pop("event_type", p.get("event_type")))
                roles = {str(k): v for k, v in alt.items() if str(k) in ROLE_FIELDS}
                alts.append(PatternAlt(event_types=event_types, roles=tuple(sorted(roles.items()))))
        else:
            event_types = _as_event_types(p.get("event_type"))
            roles = _roles_from_pattern(p)
            alts.append(PatternAlt(event_types=event_types, roles=tuple(sorted(roles.items()))))
        if not alts:
            raise ValueError(f"Pattern {qid} has no alternatives")
        for alt in alts:
            if not alt.event_types:
                raise ValueError(f"Pattern {qid} alternative has no event_type")
            for _field, value in alt.roles:
                if is_variable_token(value):
                    variables.add(var_name(str(value)))
        patterns.append(QueryPattern(qid=qid, alternatives=tuple(alts)))
    for c in query.get("constraints", []) or []:
        for key in ("left", "right"):
            v = c.get(key)
            if isinstance(v, str) and "." not in v and is_variable_token(v):
                variables.add(var_name(v))
        for v in c.get("values", []) or []:
            if isinstance(v, str) and "." not in v and is_variable_token(v):
                variables.add(var_name(v))
    return CompiledQuery(
        query_name=str(query["query_name"]),
        delta_seconds=int(query.get("delta_seconds", 86400)),
        patterns=patterns,
        constraints=list(query.get("constraints", []) or []),
        variables=sorted(variables, key=lambda x: (x != "x", x.lower(), x)),
        pattern_ids=[p.qid for p in patterns],
    )


def event_field(row: Dict[str, Any], field: str) -> str:
    if field == "event_id":
        return norm_str(row.get("event_id"))
    if field == "event_type":
        return norm_str(row.get("event_type"))
    if field == "block_ts":
        return str(int(row.get("block_ts", 0) or 0))
    if field == "order_seq":
        return str(int(row.get("order_seq", 0) or 0))
    return norm_str(row.get(field, ""))


def event_matches_alt(row: Dict[str, Any], alt: PatternAlt, bindings: Dict[str, str]) -> Optional[Dict[str, str]]:
    if norm_str(row.get("event_type")) not in set(alt.event_types):
        return None
    new_bindings = dict(bindings)
    for field, want_raw in alt.roles:
        actual = event_field(row, field)
        if is_variable_token(want_raw):
            if not actual:
                return None
            name = var_name(str(want_raw))
            if name in new_bindings and new_bindings[name] != actual:
                return None
            new_bindings[name] = actual
        else:
            want = norm_str(want_raw)
            if not want or actual != want:
                return None
    return new_bindings


def event_matches_pattern(row: Dict[str, Any], pattern: QueryPattern, bindings: Dict[str, str]) -> Optional[Dict[str, str]]:
    for alt in pattern.alternatives:
        out = event_matches_alt(row, alt, bindings)
        if out is not None:
            return out
    return None


def _resolve_ref(ref: Any, assignment: Dict[str, Dict[str, Any]], bindings: Dict[str, str]) -> Optional[str]:
    if not isinstance(ref, str):
        return norm_str(ref)
    s = ref.strip()
    if "." in s:
        eid, field = s.split(".", 1)
        row = assignment.get(eid)
        if row is None:
            return None
        return event_field(row, field)
    if is_variable_token(s):
        return bindings.get(var_name(s))
    return norm_str(s)


def _assigned_event_ids(c: Dict[str, Any]) -> Iterable[str]:
    typ = c.get("type")
    if typ in {"after", "within"}:
        yield str(c.get("earlier", ""))
        yield str(c.get("later", ""))
    if typ == "order":
        for x in c.get("sequence", []) or []:
            yield str(x)
    if typ == "distinct_event_ids":
        for x in c.get("events", []) or []:
            yield str(x)


def constraints_ok(compiled: CompiledQuery, assignment: Dict[str, Dict[str, Any]], bindings: Dict[str, str], final: bool = False) -> bool:
    # Pattern instances are injective unless a future query explicitly relaxes it.
    seen = set()
    for row in assignment.values():
        eid = event_field(row, "event_id")
        if eid in seen:
            return False
        seen.add(eid)
    for c in compiled.constraints:
        typ = str(c.get("type", ""))
        if typ == "same":
            l = _resolve_ref(c.get("left"), assignment, bindings)
            r = _resolve_ref(c.get("right"), assignment, bindings)
            if l is not None and r is not None and l != r:
                return False
            if final and (l is None or r is None):
                return False
        elif typ in {"after", "within"}:
            earlier = str(c.get("earlier", ""))
            later = str(c.get("later", ""))
            if earlier in assignment and later in assignment:
                e = assignment[earlier]
                l = assignment[later]
                if int(l["order_seq"]) <= int(e["order_seq"]):
                    return False
                if typ == "within":
                    delta = int(c.get("delta_seconds", compiled.delta_seconds))
                    if int(l["block_ts"]) > int(e["block_ts"]) + delta:
                        return False
            elif final:
                return False
        elif typ == "order":
            seq = [str(x) for x in c.get("sequence", []) or []]
            for a, b in zip(seq, seq[1:]):
                if a in assignment and b in assignment:
                    if int(assignment[b]["order_seq"]) <= int(assignment[a]["order_seq"]):
                        return False
                elif final:
                    return False
        elif typ == "not_equal":
            l = _resolve_ref(c.get("left"), assignment, bindings)
            r = _resolve_ref(c.get("right"), assignment, bindings)
            if l is not None and r is not None and l == r:
                return False
            if final and (l is None or r is None):
                return False
        elif typ == "pairwise_distinct":
            vals = [_resolve_ref(v, assignment, bindings) for v in c.get("values", []) or []]
            ready = [v for v in vals if v is not None]
            if len(set(ready)) != len(ready):
                return False
            if final and len(ready) != len(vals):
                return False
        elif typ == "distinct_event_ids":
            ids = [str(x) for x in c.get("events", []) or []]
            ready = [event_field(assignment[x], "event_id") for x in ids if x in assignment]
            if len(set(ready)) != len(ready):
                return False
            if final and len(ready) != len(ids):
                return False
        else:
            raise ValueError(f"Unsupported NFT constraint type: {typ}")
    return True


def materialized_row(compiled: CompiledQuery, assignment: Dict[str, Dict[str, Any]], bindings: Dict[str, str], match_id: int) -> Dict[str, Any]:
    if "x" in bindings:
        nft = bindings["x"]
    else:
        nft = ""
        for pid in compiled.pattern_ids:
            nft = event_field(assignment[pid], "nft")
            if nft:
                break
    pattern_key = "\x1f".join(bindings.get(v, "") for v in compiled.variables)
    row: Dict[str, Any] = {"match_id": match_id, "nft": nft, "pattern_key": pattern_key}
    for v in compiled.variables:
        row[f"var_{v}"] = bindings.get(v, "")
    for pid in compiled.pattern_ids:
        ev = assignment[pid]
        row[f"{pid}_event_id"] = event_field(ev, "event_id")
        row[f"{pid}_event_type"] = event_field(ev, "event_type")
        row[f"{pid}_tx_hash"] = event_field(ev, "tx_hash")
        row[f"{pid}_block_time"] = event_field(ev, "block_time")
        row[f"{pid}_order_seq"] = int(ev.get("order_seq", 0) or 0)
    return row


def output_columns(compiled: CompiledQuery) -> List[str]:
    cols = ["match_id", "nft", "pattern_key"]
    cols.extend([f"var_{v}" for v in compiled.variables])
    for pid in compiled.pattern_ids:
        cols.extend([f"{pid}_event_id", f"{pid}_event_type", f"{pid}_tx_hash", f"{pid}_block_time", f"{pid}_order_seq"])
    return cols


def iter_exact_matches(compiled: CompiledQuery, events: Sequence[Dict[str, Any]], fixed_first_event: Optional[Dict[str, Any]] = None) -> Iterator[Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]]:
    patterns = compiled.patterns
    order = list(range(len(patterns)))
    initial_assignment: Dict[str, Dict[str, Any]] = {}
    initial_bindings: Dict[str, str] = {}
    if fixed_first_event is not None:
        first = patterns[0]
        b = event_matches_pattern(fixed_first_event, first, initial_bindings)
        if b is None:
            return
        initial_assignment[first.qid] = fixed_first_event
        initial_bindings = b
        if not constraints_ok(compiled, initial_assignment, initial_bindings, final=False):
            return
        order = list(range(1, len(patterns)))

    # Precompute type-compatible event lists for each query event. Variable and
    # constraint checks are still enforced during DFS.
    type_domains: Dict[int, List[Dict[str, Any]]] = {}
    for i, p in enumerate(patterns):
        allowed = set()
        for alt in p.alternatives:
            allowed.update(alt.event_types)
        type_domains[i] = [e for e in events if event_field(e, "event_type") in allowed]

    def dfs(pos: int, assignment: Dict[str, Dict[str, Any]], bindings: Dict[str, str]) -> Iterator[Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]]:
        if pos >= len(order):
            if constraints_ok(compiled, assignment, bindings, final=True):
                yield dict(assignment), dict(bindings)
            return
        idx = order[pos]
        pattern = patterns[idx]
        used_event_ids = {event_field(e, "event_id") for e in assignment.values()}
        for ev in type_domains[idx]:
            eid = event_field(ev, "event_id")
            if eid in used_event_ids:
                continue
            new_bindings = event_matches_pattern(ev, pattern, bindings)
            if new_bindings is None:
                continue
            assignment[pattern.qid] = ev
            if constraints_ok(compiled, assignment, new_bindings, final=False):
                yield from dfs(pos + 1, assignment, new_bindings)
            assignment.pop(pattern.qid, None)

    yield from dfs(0, initial_assignment, initial_bindings)


def load_events_from_sqlite(conn: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = []
    for r in conn.execute(f"SELECT {', '.join(RUNTIME_COLUMNS)} FROM {table} WHERE nft <> '' ORDER BY nft, order_seq"):
        d = {k: r[k] for k in r.keys()}
        d["block_ts"] = int(d.get("block_ts", 0) or 0)
        d["order_seq"] = int(d.get("order_seq", 0) or 0)
        rows.append(d)
    return rows


def group_events_by_nft(events: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in events:
        nft = event_field(e, "nft")
        if nft:
            groups[nft].append(e)
    for g in groups.values():
        g.sort(key=lambda x: int(x.get("order_seq", 0) or 0))
    return dict(groups)


def max_query_delta(compiled: CompiledQuery) -> int:
    delta = compiled.delta_seconds
    for c in compiled.constraints:
        if c.get("type") == "within":
            delta = max(delta, int(c.get("delta_seconds", delta)))
    return delta


def anchor_candidates(compiled: CompiledQuery, events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    first = compiled.patterns[0]
    out = []
    for ev in events:
        b = event_matches_pattern(ev, first, {})
        if b is not None:
            out.append(ev)
    return out


def closure_events_for_anchor(anchor: Dict[str, Any], grouped: Dict[str, List[Dict[str, Any]]], delta: int) -> List[Dict[str, Any]]:
    nft = event_field(anchor, "nft")
    if not nft:
        return []
    start_order = int(anchor["order_seq"])
    end_ts = int(anchor["block_ts"]) + int(delta)
    return [e for e in grouped.get(nft, []) if int(e["order_seq"]) >= start_order and int(e["block_ts"]) <= end_ts]


def closure_events_for_anchor_scope(anchor: Dict[str, Any], events: Sequence[Dict[str, Any]], grouped: Dict[str, List[Dict[str, Any]]], delta: int, same_nft_scope: bool) -> List[Dict[str, Any]]:
    if same_nft_scope:
        return closure_events_for_anchor(anchor, grouped, delta)
    start_order = int(anchor["order_seq"])
    end_ts = int(anchor["block_ts"]) + int(delta)
    return [e for e in events if int(e["order_seq"]) >= start_order and int(e["block_ts"]) <= end_ts]


def base_filter_pass(compiled: CompiledQuery, closure_events: Sequence[Dict[str, Any]]) -> bool:
    counts = Counter(event_field(e, "event_type") for e in closure_events)
    # Coarse Hall-style necessary condition over event labels.
    for p in compiled.patterns:
        allowed = set()
        for alt in p.alternatives:
            allowed.update(alt.event_types)
        if sum(counts[t] for t in allowed) <= 0:
            return False
    single_required = Counter()
    for p in compiled.patterns:
        allowed = set()
        for alt in p.alternatives:
            allowed.update(alt.event_types)
        if len(allowed) == 1:
            single_required[next(iter(allowed))] += 1
    for typ, need in single_required.items():
        if counts[typ] < need:
            return False
    return True


def refined_filter_pass(compiled: CompiledQuery, closure_events: Sequence[Dict[str, Any]]) -> bool:
    # Generic role-support filter: every query event must have a non-empty local
    # domain, and every repeated variable must occur in at least one common value
    # across its participating query roles. This is a necessary condition only;
    # exact enumeration still follows.
    domains: Dict[str, List[Dict[str, Any]]] = {}
    for p in compiled.patterns:
        domains[p.qid] = [e for e in closure_events if event_matches_pattern(e, p, {}) is not None]
        if not domains[p.qid]:
            return False
    var_values: Dict[str, Optional[set[str]]] = {v: None for v in compiled.variables}
    for p in compiled.patterns:
        # A query event may have alternatives, e.g., trade-back OR transfer-back.
        # For one query-event occurrence of a variable, support is the union over
        # alternatives, not the intersection of alternatives.
        per_pattern_values: Dict[str, set[str]] = defaultdict(set)
        for alt in p.alternatives:
            for field, raw in alt.roles:
                if not is_variable_token(raw):
                    continue
                name = var_name(str(raw))
                for e in domains[p.qid]:
                    if event_field(e, "event_type") not in set(alt.event_types):
                        continue
                    val = event_field(e, field)
                    if val:
                        per_pattern_values[name].add(val)
        for name, vals in per_pattern_values.items():
            if var_values[name] is None:
                var_values[name] = set(vals)
            else:
                var_values[name] = var_values[name] & set(vals)
    for vals in var_values.values():
        if vals is not None and not vals:
            return False
    return True



def _base_required_counts_for_compiled(compiled: CompiledQuery) -> Dict[str, int]:
    """Return required per-event-type lower bounds for the Base synopsis.

    This is the same necessary condition as the posting-list Base synopsis, but
    v12 stores the NFT same-asset Base layer as a compact count table instead of
    one row per threshold feature.  The function returns an empty dictionary for
    alternative-type query events because those need a more general fallback.
    """
    required: Counter[str] = Counter()
    for p in compiled.patterns:
        allowed = set()
        for alt in p.alternatives:
            allowed.update(alt.event_types)
        if len(allowed) != 1:
            return {}
        required[next(iter(allowed))] += 1
    return {str(k): int(v) for k, v in required.items()}


def _create_base_cand1_from_counts(conn: sqlite3.Connection, compiled: CompiledQuery) -> int | None:
    """Create Cand1 from the compact v12 Base count synopsis.

    The table ``nft_base_counts`` stores, for each same-NFT anchor closure, the
    number of trade and transfer events available within the window.  This is a
    generic Base synopsis over event-label support, not a query-specific result
    table.  It replaces the much larger threshold-posting table used in v9/v10.
    """
    required = _base_required_counts_for_compiled(compiled)
    if not required:
        return None
    conds: List[str] = []
    for typ, need in sorted(required.items()):
        if typ == "NFTTrade":
            conds.append(f"b.nft_trade_count >= {int(need)}")
        elif typ == "NFTTransfer":
            conds.append(f"b.nft_transfer_count >= {int(need)}")
        else:
            return None
    where = " AND ".join(conds) if conds else "1=1"
    conn.execute("DROP TABLE IF EXISTS temp_hvceq_base_fast")
    conn.execute("DROP TABLE IF EXISTS temp_hvceq_cand1")
    conn.execute(
        "CREATE TEMP TABLE temp_hvceq_cand1 AS "
        "SELECT c.closure_id AS closure_id "
        "FROM temp_hvceq_cand0 c JOIN nft_base_counts b ON b.closure_id = c.closure_id "
        f"WHERE {where}"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_temp_hvceq_cand1 ON temp_hvceq_cand1(closure_id)")
    return int(conn.execute("SELECT COUNT(*) FROM temp_hvceq_cand1").fetchone()[0])


def _anchor_component_sets(compiled: CompiledQuery) -> List[List[str]]:
    """Return generic anchor-directory signatures for the first query event.

    Variable role predicates only require non-empty fields and are checked by
    the exact first-pattern predicate when Cand0 is materialized. Using the
    event-type signature as the primary anchor keeps recall complete while
    avoiding unnecessary posting-list intersections such as
    type=NFTTrade|seller=* ∩ type=NFTTrade|buyer=* ∩ type=NFTTrade|nft=*.
    Literal NFT predicates remain in the anchor signature because the posting
    directory explicitly stores type=<T>|nft=<value> components.
    """
    out: List[List[str]] = []
    first = compiled.patterns[0]
    for alt in first.alternatives:
        for et in alt.event_types:
            comps = [f"type={et}"]
            for field, raw in alt.roles:
                if field == "nft" and not is_variable_token(raw):
                    lit = norm_str(raw)
                    if lit:
                        comps.append(f"type={et}|nft={lit}")
            out.append(list(dict.fromkeys(comps)))
    return out



def _simple_event_type_anchor_types(compiled: CompiledQuery) -> List[str]:
    """Return anchor event types when the anchor signature is purely type-based.

    A pure type anchor is still an Anchor-directory lookup.  For the final
    second-tier NFT evidence queries, the first event is ``NFTTrade`` and all
    role predicates are variables.  In that case the compact event-type anchor
    directory can directly recall the candidate first events.  The final exact
    materialization still enforces all role bindings and constraints.
    """
    types: List[str] = []
    for alt in compiled.patterns[0].alternatives:
        if any(field == "nft" and not is_variable_token(raw) for field, raw in alt.roles):
            return []
        types.extend(alt.event_types)
    return list(dict.fromkeys(types))


def compile_hvceq_execution_plan(conn: sqlite3.Connection, compiled: CompiledQuery) -> Dict[str, Any]:
    """Compile a deterministic EpisodeDelta plan from the committed stats table.

    EpisodeDelta semantics bind the first query pattern to the closure anchor,
    so its anchor is fixed by the query.  The remaining required events are
    ordered by the LexAnchor key for safe refined-filter evaluation.  This only
    changes the order of necessary-condition checks, never Cand2 semantics.
    """
    stats = {
        str(event_type): int(hit_count)
        for event_type, hit_count in conn.execute(
            "SELECT d.event_type, s.closure_hit_count "
            "FROM nft_anchor_stats s JOIN nft_event_type_dict d ON d.event_type_id = s.event_type_id"
        )
    }

    def key(pattern: QueryPattern) -> Tuple[int, int, str]:
        event_types = sorted({event_type for alt in pattern.alternatives for event_type in alt.event_types})
        freq = min((stats.get(event_type, 2**63 - 1) for event_type in event_types), default=2**63 - 1)
        bindings = sum(1 for alt in pattern.alternatives for _field, raw in alt.roles if not is_variable_token(raw))
        return (freq, -bindings, pattern.qid)

    refined = sorted(compiled.patterns[1:], key=key)
    anchor = compiled.patterns[0]
    return {
        "closure_anchor_pattern": anchor.qid,
        "anchor_key": list(key(anchor)),
        "refined_pattern_order": [pattern.qid for pattern in refined],
        "refined_keys": {pattern.qid: list(key(pattern)) for pattern in refined},
    }


def _create_type_anchor_cand0_table(conn: sqlite3.Connection, compiled: CompiledQuery, use_collection_anchor: bool = True) -> int | None:
    """Create Cand0 from the v12 event-type anchor directory."""
    simple_types = _simple_event_type_anchor_types(compiled)
    if not simple_types:
        raise ValueError("v12 runtime requires a type-only first-event anchor")
    conn.execute("DROP TABLE IF EXISTS temp_hvceq_anchor_raw")
    conn.execute("DROP TABLE IF EXISTS temp_hvceq_cand0")
    type_ids: List[int] = []
    for typ in simple_types:
        row = conn.execute(
            "SELECT event_type_id FROM nft_event_type_dict WHERE event_type = ?",
            (typ,),
        ).fetchone()
        if row is not None:
            type_ids.append(int(row[0]))
    collection_literals = sorted({str(raw) for alt in compiled.patterns[0].alternatives for field, raw in alt.roles if field == "collection_contract" and not is_variable_token(raw)})
    if use_collection_anchor and collection_literals and len(collection_literals) == 1:
        conn.execute(
            "CREATE TEMP TABLE temp_hvceq_cand0 AS "
            "SELECT closure_id FROM nft_anchor_event_collection_candidates "
            f"WHERE event_type_id IN ({','.join(str(int(x)) for x in sorted(set(type_ids)))}) AND collection_contract = {sql_literal(collection_literals[0])}"
        )
    elif not type_ids:
        conn.execute("CREATE TEMP TABLE temp_hvceq_cand0 AS SELECT closure_id FROM nft_anchor_event_type_candidates WHERE 0")
    else:
        ids_sql = ",".join(str(int(x)) for x in sorted(set(type_ids)))
        conn.execute(
            "CREATE TEMP TABLE temp_hvceq_cand0 AS "
            "SELECT closure_id FROM nft_anchor_event_type_candidates "
            f"WHERE event_type_id IN ({ids_sql})"
        )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_temp_hvceq_cand0 ON temp_hvceq_cand0(closure_id)")
    return int(conn.execute("SELECT COUNT(*) FROM temp_hvceq_cand0").fetchone()[0])


def _anchor_variable_fields(compiled: CompiledQuery) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    first = compiled.patterns[0]
    for alt in first.alternatives:
        for field, raw in alt.roles:
            if is_variable_token(raw):
                name = var_name(str(raw))
                fields.setdefault(name, field)
    return fields


def _first_pattern_variable_fields(compiled: CompiledQuery) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    first = compiled.patterns[0]
    for alt in first.alternatives:
        for field, raw in alt.roles:
            if is_variable_token(raw):
                fields.setdefault(var_name(str(raw)), field)
    return fields


def _is_same_nft_scope(compiled: CompiledQuery) -> bool:
    """Return True when every NFT role in the query is the same variable.

    This lets the HVCEQ base synopsis safely use the precomputed same-NFT
    closure statistics. Cross-asset wallet queries intentionally return False
    because using same-NFT closure counts would create false negatives.
    """
    first_nft_var = None
    for p in compiled.patterns:
        for alt in p.alternatives:
            nft_roles = [(field, raw) for field, raw in alt.roles if field == "nft"]
            if not nft_roles:
                return False
            for _field, raw in nft_roles:
                if not is_variable_token(raw):
                    return False
                name = var_name(str(raw))
                if first_nft_var is None:
                    first_nft_var = name
                elif name != first_nft_var:
                    return False
    return first_nft_var is not None


_REFINED_INTERFACE_FIELDS = ("seller", "buyer")
_REFINED_INTERFACE_EVENT_TYPES = ("NFTTrade", "NFTTransfer")


def _refined_interface_bit(event_type: str, source_field: str, target_field: str) -> int:
    return (
        _REFINED_INTERFACE_EVENT_TYPES.index(event_type) * len(_REFINED_INTERFACE_FIELDS) ** 2
        + _REFINED_INTERFACE_FIELDS.index(source_field) * len(_REFINED_INTERFACE_FIELDS)
        + _REFINED_INTERFACE_FIELDS.index(target_field)
    )


def _pattern_is_after_anchor(compiled: CompiledQuery, pattern: QueryPattern) -> bool:
    anchor_id = compiled.patterns[0].qid
    for c in compiled.constraints:
        if c.get("type") != "order":
            continue
        sequence = [str(x) for x in c.get("sequence", [])]
        if anchor_id in sequence and pattern.qid in sequence:
            return sequence.index(anchor_id) < sequence.index(pattern.qid)
    return False


def _refined_mask_filter_for_pattern(compiled: CompiledQuery, pattern: QueryPattern, anchor_var_fields: Dict[str, str]) -> str:
    """Compile a necessary refined test against the build-time interface masks.

    We only use a bit when the query explicitly orders the pattern after the
    anchor.  Each bit proves existence of *some* compatible later hyperedge;
    exact materialization still proves that all bindings cohere.
    """
    if not _pattern_is_after_anchor(compiled, pattern):
        return "1=1"
    branches: List[str] = []
    for alt in pattern.alternatives:
        bits: List[int] = []
        if len(alt.event_types) != 1 or alt.event_types[0] not in _REFINED_INTERFACE_EVENT_TYPES:
            return "1=1"
        event_type = alt.event_types[0]
        for target_field, raw in alt.roles:
            if target_field not in _REFINED_INTERFACE_FIELDS or not is_variable_token(raw):
                continue
            name = var_name(str(raw))
            source_field = anchor_var_fields.get(name)
            if source_field in _REFINED_INTERFACE_FIELDS:
                bits.append(_refined_interface_bit(event_type, source_field, target_field))
        if not bits:
            branches.append("1=1")
            continue
        terms = []
        for bit in sorted(set(bits)):
            column = "m.mask_lo" if bit < 64 else "m.mask_hi"
            value = 1 << (bit if bit < 64 else bit - 64)
            terms.append(f"({column} & {value}) <> 0")
        branches.append("(" + " AND ".join(terms) + ")")
    return "(" + " OR ".join(branches) + ")" if branches else "1=1"


def _prepare_hvceq_candidate_tables(conn: sqlite3.Connection, compiled: CompiledQuery, refined_patterns: Sequence[QueryPattern], use_base: bool = True, use_refined: bool = True, use_collection_anchor: bool = True) -> Dict[str, Any]:
    t0 = time.perf_counter()

    cand0 = _create_type_anchor_cand0_table(conn, compiled, use_collection_anchor=use_collection_anchor)
    anchor_raw = cand0
    t1 = time.perf_counter()

    # Base synopsis is safe only for same-NFT episode queries. For cross-asset
    # wallet-style evidence queries, same-NFT closure counts would wrongly reject
    # anchors whose later evidence is on a different NFT.  In that case Cand1 is
    # logically identical to Cand0, so we reuse Cand0 instead of copying it.
    if use_base and _is_same_nft_scope(compiled):
        cand1 = _create_base_cand1_from_counts(conn, compiled)
        if cand1 is None:
            conn.execute("DROP TABLE IF EXISTS temp_hvceq_cand1")
            conn.execute("CREATE TEMP TABLE temp_hvceq_cand1 AS SELECT closure_id FROM temp_hvceq_cand0")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_temp_hvceq_cand1 ON temp_hvceq_cand1(closure_id)")
            cand1 = cand0
        refined_source_table = "temp_hvceq_cand1"
    else:
        conn.execute("DROP TABLE IF EXISTS temp_hvceq_cand1")
        cand1 = cand0
        refined_source_table = "temp_hvceq_cand0"
    t2 = time.perf_counter()

    # The persisted refined mask is a same-NFT role-interface synopsis.  It is
    # one-sided safe only when the query's NFT variables all refer to the same
    # NFT.  Cross-asset queries must bypass it; otherwise valid matches on a
    # different NFT can be removed before exact matching.
    refined_safe = bool(use_refined and _is_same_nft_scope(compiled))

    conn.execute("DROP TABLE IF EXISTS temp_hvceq_cand2")
    if not refined_safe:
        conn.execute(f"CREATE TEMP TABLE temp_hvceq_cand2 AS SELECT closure_id FROM {refined_source_table}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_temp_hvceq_cand2 ON temp_hvceq_cand2(closure_id)")
        cand2 = int(conn.execute("SELECT COUNT(*) FROM temp_hvceq_cand2").fetchone()[0])
        t3 = time.perf_counter()
        return {
            "anchor_raw": anchor_raw, "cand0": cand0, "cand1": cand1, "cand2": cand2,
            "anchor_recall_s": t1 - t0, "base_synopsis_s": t2 - t1, "refined_synopsis_s": t3 - t2,
            "refined_safe": refined_safe,
        }
    anchor_var_fields = _first_pattern_variable_fields(compiled)
    exists_terms = [
        _refined_mask_filter_for_pattern(compiled, p, anchor_var_fields)
        for p in refined_patterns
    ]
    where = " AND ".join(exists_terms) if exists_terms else "1=1"
    conn.execute(
        "CREATE TEMP TABLE temp_hvceq_cand2 AS "
        "SELECT DISTINCT c.closure_id AS closure_id "
        f"FROM {refined_source_table} c JOIN nft_refined_interface_masks m ON m.closure_id = c.closure_id "
        f"WHERE {where}"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_temp_hvceq_cand2 ON temp_hvceq_cand2(closure_id)")
    cand2 = int(conn.execute("SELECT COUNT(*) FROM temp_hvceq_cand2").fetchone()[0])
    t3 = time.perf_counter()
    return {
        "anchor_raw": anchor_raw,
        "cand0": cand0,
        "cand1": cand1,
        "cand2": cand2,
        "anchor_recall_s": t1 - t0,
        "base_synopsis_s": t2 - t1,
        "refined_synopsis_s": t3 - t2,
        "refined_safe": refined_safe,
    }


def _materialize_sql_rows(conn: sqlite3.Connection, compiled: CompiledQuery, sql: str, out_csv: Path) -> Dict[str, Any]:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    cols = output_columns(compiled)
    num_matches = 0
    distinct_nfts: set[str] = set()
    distinct_patterns: set[str] = set()
    csv_write_s = 0.0
    t0 = time.perf_counter()
    cur = conn.execute(sql)
    desc = [d[0] for d in cur.description]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        while True:
            batch = cur.fetchmany(20000)
            if not batch:
                break
            for raw in batch:
                row = dict(zip(desc, raw))
                num_matches += 1
                row["match_id"] = num_matches
                tw = time.perf_counter()
                writer.writerow({c: row.get(c, "") for c in cols})
                csv_write_s += time.perf_counter() - tw
                distinct_nfts.add(str(row.get("nft", "")))
                distinct_patterns.add(str(row.get("pattern_key", "")))
    elapsed = time.perf_counter() - t0
    return {
        "num_matches": num_matches,
        "num_distinct_nfts": len({x for x in distinct_nfts if x}),
        "num_patterns": len({x for x in distinct_patterns if x}),
        "matched_instances_csv": str(out_csv),
        "materialize_s": elapsed,
        "instance_enumeration_s": elapsed - csv_write_s,
        "csv_write_s": csv_write_s,
    }

def _create_cand2_anchor_order_table(conn: sqlite3.Connection) -> None:
    """Expose Cand2 closure anchors as order_seq for the optional SQL backend."""
    conn.execute("DROP TABLE IF EXISTS temp_hvceq_cand2_anchor")
    conn.execute(
        "CREATE TEMP TABLE temp_hvceq_cand2_anchor AS "
        "SELECT r.anchor_order_seq AS order_seq "
        "FROM temp_hvceq_cand2 c JOIN nft_closure_records r ON r.closure_id = c.closure_id"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_temp_hvceq_cand2_anchor ON temp_hvceq_cand2_anchor(order_seq)")


def materialize_hvceq_instances(conn: sqlite3.Connection, query: Dict[str, Any], out_csv: Path, ablation: str = "full", exact_backend: str = "local") -> Dict[str, Any]:
    compiled = compile_query(query)
    exact_backend = (exact_backend or "local").strip().lower()
    if exact_backend not in {"local", "sql"}:
        raise ValueError(f"Unknown HVCEQ exact backend: {exact_backend}")
    t0 = time.perf_counter()
    plan = compile_hvceq_execution_plan(conn, compiled)
    pattern_by_id = {pattern.qid: pattern for pattern in compiled.patterns}
    refined_patterns = [pattern_by_id[qid] for qid in plan["refined_pattern_order"]]
    if ablation not in {"full", "naive_first", "no_anchor", "no_base", "no_refined", "no_synopsis"}:
        raise ValueError(f"Unknown HVCEQ ablation variant: {ablation}")
    stages = _prepare_hvceq_candidate_tables(
        conn, compiled, refined_patterns,
        use_base=ablation not in {"no_base", "no_synopsis"},
        use_refined=ablation not in {"no_refined", "no_synopsis"},
        use_collection_anchor=(ablation != "no_anchor"),
    )
    t1 = time.perf_counter()

    if exact_backend == "local":
        from systems.nft.hvceq.anchor_seeded_local import materialize_anchor_seeded_local
        mat = materialize_anchor_seeded_local(conn, compiled, out_csv)
        query_s = float(mat.get("total_s", 0.0))
        instance_s = float(mat.get("instance_enumeration_s", 0.0))
        csv_s = float(mat.get("csv_write_s", 0.0))
        load_s = float(mat.get("local_hypergraph_load_s", 0.0))
        backend_label = "closure_anchor_seeded_local_exact_matching"
        examined = int(mat.get("examined_bindings", mat.get("num_matches", 0)))
    else:
        _create_cand2_anchor_order_table(conn)
        sql = compile_materialization_sql(query, "nft_events", first_event_filter_table="temp_hvceq_cand2_anchor", first_event_filter_field="order_seq")
        mat = _materialize_sql_rows(conn, compiled, sql, out_csv)
        query_s = float(mat["materialize_s"])
        instance_s = float(mat["instance_enumeration_s"])
        csv_s = float(mat["csv_write_s"])
        load_s = 0.0
        backend_label = "cand2_closure_restricted_sqlite_instance_materialization"
        examined = int(mat["num_matches"])

    total_s = time.perf_counter() - t0
    return {
        "query_name": compiled.query_name,
        "delta_seconds": compiled.delta_seconds,
        "num_matches": int(mat["num_matches"]),
        "num_distinct_nfts": int(mat["num_distinct_nfts"]),
        "num_patterns": int(mat["num_patterns"]),
        "matched_instances_csv": mat["matched_instances_csv"],
        "anchor_raw": int(stages.get("anchor_raw", 0)),
        "cand0": int(stages.get("cand0", 0)),
        "cand1": int(stages.get("cand1", 0)),
        "cand2": int(stages.get("cand2", 0)),
        "examined_bindings": examined,
        "closures_examined": int(mat.get("closures_examined", 0)),
        "closure_payload_rows": int(mat.get("closure_payload_rows", 0)),
        "closure_payload_scope": mat.get("closure_payload_scope", ""),
        "load_s": load_s,
        "candidate_s": t1 - t0,
        "anchor_recall_s": float(stages.get("anchor_recall_s", 0.0)),
        "base_synopsis_s": float(stages.get("base_synopsis_s", 0.0)),
        "refined_synopsis_s": float(stages.get("refined_synopsis_s", 0.0)),
        "query_s": query_s,
        "instance_enumeration_s": instance_s,
        "csv_write_s": csv_s,
        "total_s": total_s,
        "exact_backend": backend_label,
        "execution_plan": {**plan, "candidate_unit": "closure_id", "refined_safe": bool(stages.get("refined_safe", False))},
        "ablation_variant": ablation,
    }

def materialize_generic_python_instances(conn: sqlite3.Connection, query: Dict[str, Any], table: str, out_csv: Path) -> Dict[str, Any]:
    compiled = compile_query(query)
    t0 = time.perf_counter()
    events = load_events_from_sqlite(conn, table)
    grouped = group_events_by_nft(events)
    anchors = anchor_candidates(compiled, events)
    delta = max_query_delta(compiled)
    same_nft_scope = _is_same_nft_scope(compiled)
    load_s = time.perf_counter() - t0
    cols = output_columns(compiled)
    num_matches = 0
    distinct_nfts: set[str] = set()
    distinct_patterns: set[str] = set()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    t1 = time.perf_counter()
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for anchor in anchors:
            closure = closure_events_for_anchor_scope(anchor, events, grouped, delta, same_nft_scope)
            for assignment, bindings in iter_exact_matches(compiled, closure, fixed_first_event=anchor):
                num_matches += 1
                row = materialized_row(compiled, assignment, bindings, num_matches)
                distinct_nfts.add(str(row.get("nft", "")))
                distinct_patterns.add(str(row.get("pattern_key", "")))
                writer.writerow(row)
    query_s = time.perf_counter() - t1
    return {
        "query_name": compiled.query_name,
        "delta_seconds": compiled.delta_seconds,
        "num_matches": num_matches,
        "num_distinct_nfts": len({x for x in distinct_nfts if x}),
        "num_patterns": len({x for x in distinct_patterns if x}),
        "matched_instances_csv": str(out_csv),
        "cand0": len(anchors),
        "load_s": load_s,
        "query_s": query_s,
        "total_s": time.perf_counter() - t0,
    }


def sql_field(alias: str, field: str) -> str:
    if field not in set(RUNTIME_COLUMNS):
        raise ValueError(f"Unsupported SQL field: {field}")
    return f'{alias}."{field}"'


def sql_literal(value: Any) -> str:
    s = norm_str(value).replace("'", "''")
    return f"'{s}'"


def _sql_concat(parts: List[str]) -> str:
    if not parts:
        return "''"
    expr = parts[0]
    for p in parts[1:]:
        expr = f"({expr} || char(31) || {p})"
    return expr


def compile_materialization_sql(query: Dict[str, Any], table: str, first_event_filter_table: str = "", first_event_filter_field: str = "event_id") -> str:
    compiled = compile_query(query)
    branches = list(product(*[p.alternatives for p in compiled.patterns]))
    select_cols = output_columns(compiled)
    branch_sqls: List[str] = []
    for branch in branches:
        aliases = {p.qid: f"e{i+1}" for i, p in enumerate(compiled.patterns)}
        wheres: List[str] = []
        var_exprs: Dict[str, List[str]] = defaultdict(list)
        # Per-alias label and role constraints.
        for p, alt in zip(compiled.patterns, branch):
            a = aliases[p.qid]
            types = ",".join(sql_literal(t) for t in alt.event_types)
            wheres.append(f"{sql_field(a, 'event_type')} IN ({types})")
            wheres.append(f"{sql_field(a, 'nft')} <> ''")
            for field, raw in alt.roles:
                expr = sql_field(a, field)
                if is_variable_token(raw):
                    name = var_name(str(raw))
                    wheres.append(f"{expr} <> ''")
                    var_exprs[name].append(expr)
                else:
                    wheres.append(f"{expr} = {sql_literal(raw)}")
        for name, exprs in var_exprs.items():
            for a, b in zip(exprs, exprs[1:]):
                wheres.append(f"{a} = {b}")
        # Pattern injectivity.
        for p1, p2 in combinations(compiled.patterns, 2):
            wheres.append(f"{sql_field(aliases[p1.qid], 'event_id')} <> {sql_field(aliases[p2.qid], 'event_id')}")
        if first_event_filter_table:
            first_alias_for_filter = aliases[compiled.patterns[0].qid]
            field = first_event_filter_field or "event_id"
            wheres.append(f"{sql_field(first_alias_for_filter, field)} IN (SELECT {field} FROM {first_event_filter_table})")

        def resolve(ref: Any) -> str:
            if not isinstance(ref, str):
                return sql_literal(ref)
            s = ref.strip()
            if "." in s:
                eid, field = s.split(".", 1)
                return sql_field(aliases[eid], field)
            if is_variable_token(s):
                exprs = var_exprs.get(var_name(s), [])
                if not exprs:
                    raise ValueError(f"Variable {s} has no SQL occurrence")
                return exprs[0]
            return sql_literal(s)

        for c in compiled.constraints:
            typ = str(c.get("type", ""))
            if typ == "same":
                wheres.append(f"{resolve(c.get('left'))} = {resolve(c.get('right'))}")
            elif typ == "after":
                wheres.append(f"{sql_field(aliases[str(c.get('later'))], 'order_seq')} > {sql_field(aliases[str(c.get('earlier'))], 'order_seq')}")
            elif typ == "within":
                later = aliases[str(c.get("later"))]
                earlier = aliases[str(c.get("earlier"))]
                delta = int(c.get("delta_seconds", compiled.delta_seconds))
                wheres.append(f"{sql_field(later, 'order_seq')} > {sql_field(earlier, 'order_seq')}")
                wheres.append(f"{sql_field(later, 'block_ts')} <= {sql_field(earlier, 'block_ts')} + {delta}")
            elif typ == "order":
                seq = [str(x) for x in c.get("sequence", []) or []]
                for a, b in zip(seq, seq[1:]):
                    wheres.append(f"{sql_field(aliases[b], 'order_seq')} > {sql_field(aliases[a], 'order_seq')}")
            elif typ == "not_equal":
                wheres.append(f"{resolve(c.get('left'))} <> {resolve(c.get('right'))}")
            elif typ == "pairwise_distinct":
                vals = [resolve(v) for v in c.get("values", []) or []]
                for a, b in combinations(vals, 2):
                    wheres.append(f"{a} <> {b}")
            elif typ == "distinct_event_ids":
                ids = [str(x) for x in c.get("events", []) or []]
                for a, b in combinations(ids, 2):
                    wheres.append(f"{sql_field(aliases[a], 'event_id')} <> {sql_field(aliases[b], 'event_id')}")
            else:
                raise ValueError(f"Unsupported NFT constraint type: {typ}")
        first_alias = aliases[compiled.patterns[0].qid]
        if "x" in var_exprs:
            nft_expr = var_exprs["x"][0]
        else:
            nft_expr = sql_field(first_alias, "nft")
        pattern_exprs = [var_exprs[v][0] if var_exprs.get(v) else "''" for v in compiled.variables]
        pattern_key_expr = _sql_concat(pattern_exprs)
        select_exprs: List[str] = [
            "0 AS match_id",
            f"{nft_expr} AS nft",
            f"{pattern_key_expr} AS pattern_key",
        ]
        for v in compiled.variables:
            expr = var_exprs[v][0] if var_exprs.get(v) else "''"
            select_exprs.append(f"{expr} AS var_{v}")
        for p in compiled.patterns:
            a = aliases[p.qid]
            select_exprs.extend([
                f"{sql_field(a, 'event_id')} AS {p.qid}_event_id",
                f"{sql_field(a, 'event_type')} AS {p.qid}_event_type",
                f"{sql_field(a, 'tx_hash')} AS {p.qid}_tx_hash",
                f"{sql_field(a, 'block_time')} AS {p.qid}_block_time",
                f"{sql_field(a, 'order_seq')} AS {p.qid}_order_seq",
            ])
        from_clause = ", ".join([f'{table} {aliases[p.qid]}' for p in compiled.patterns])
        branch_sqls.append("SELECT " + ", ".join(select_exprs) + " FROM " + from_clause + " WHERE " + " AND ".join(wheres))
    return "\nUNION ALL\n".join(branch_sqls)


def materialize_sql_instances(conn: sqlite3.Connection, query: Dict[str, Any], table: str, out_csv: Path) -> Dict[str, Any]:
    compiled = compile_query(query)
    sql = compile_materialization_sql(query, table)
    mat = _materialize_sql_rows(conn, compiled, sql, out_csv)
    return {
        "query_name": compiled.query_name,
        "delta_seconds": compiled.delta_seconds,
        "num_matches": int(mat["num_matches"]),
        "num_distinct_nfts": int(mat["num_distinct_nfts"]),
        "num_patterns": int(mat["num_patterns"]),
        "matched_instances_csv": mat["matched_instances_csv"],
        "materialize_s": float(mat["materialize_s"]),
        "total_s": float(mat["materialize_s"]),
        "compiled_sql": sql,
    }
