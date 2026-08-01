from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping

from .exact_match import compact, instance_key
from .transcript import CANONICAL_COLUMNS


def normalize_row(raw: Mapping[str, Any], closure_map: Mapping[int, int] | None = None) -> dict[str, Any]:
    if list(raw.keys()) != CANONICAL_COLUMNS and set(raw) != set(CANONICAL_COLUMNS):
        raise ValueError("result row does not have the exact eight-column contract")
    events = json.loads(str(raw["event_bindings_json"]))
    orders = {key: int(value) for key, value in json.loads(str(raw["event_orders_json"])).items()}
    variables = {key: str(value) for key, value in json.loads(str(raw["variable_bindings_json"])).items()}
    if not isinstance(events, dict) or set(events) != set(orders) or "root" not in orders:
        raise ValueError("event bindings and orders are inconsistent")
    anchor_order = int(raw["anchor_order"])
    closure_id = int(raw["closure_id"])
    if anchor_order != int(orders["root"]):
        raise ValueError("anchor_order is not the root event order")
    if closure_map is not None and closure_map.get(anchor_order) != closure_id:
        raise ValueError("closure_id is not the authenticated mapping for anchor_order")
    expected_key = instance_key(
        str(raw["query_id"]), str(raw["branch_id"]), events, variables
    )
    if str(raw["instance_key"]).lower() != expected_key:
        raise ValueError("instance_key does not match semantic bindings")
    return {
        "query_id": str(raw["query_id"]),
        "branch_id": str(raw["branch_id"]),
        "closure_id": closure_id,
        "anchor_order": anchor_order,
        "instance_key": expected_key,
        "event_bindings_json": compact({str(k): str(v) for k, v in events.items()}),
        "event_orders_json": compact(orders),
        "variable_bindings_json": compact(variables),
    }


class ResultStore:
    """Disk-backed exact set comparison; no full result dictionaries in RAM."""

    def __init__(self, path: Path):
        self.path = Path(path)
        if self.path.exists():
            self.path.unlink()
        self.conn = sqlite3.connect(self.path)
        self.conn.executescript(
            """
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            CREATE TABLE generated(instance_key TEXT PRIMARY KEY, row_json TEXT NOT NULL);
            CREATE TABLE claimed(instance_key TEXT PRIMARY KEY, row_json TEXT NOT NULL);
            """
        )

    def _insert(self, table: str, row: Mapping[str, Any]) -> None:
        payload = compact({column: row[column] for column in CANONICAL_COLUMNS})
        try:
            self.conn.execute(
                f"INSERT INTO {table}(instance_key,row_json) VALUES (?,?)",
                (str(row["instance_key"]), payload),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"duplicate {table} instance_key: {row['instance_key']}") from exc

    def add_generated(self, rows: Iterable[Mapping[str, Any]]) -> int:
        count = 0
        for row in rows:
            self._insert("generated", row)
            count += 1
        return count

    def load_claimed(
        self,
        path: Path,
        *,
        query_id: str,
        closure_map: Mapping[int, int],
    ) -> int:
        count = 0
        with Path(path).open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if list(reader.fieldnames or []) != CANONICAL_COLUMNS:
                raise ValueError("instances.csv has the wrong eight-column header")
            for raw in reader:
                row = normalize_row(raw, closure_map)
                if row["query_id"] != query_id:
                    raise ValueError("cross-query row in instances.csv")
                self._insert("claimed", row)
                count += 1
        self.conn.commit()
        return count

    def compare(self) -> dict[str, Any]:
        missing = self.conn.execute(
            "SELECT COUNT(*) FROM generated g LEFT JOIN claimed c USING(instance_key) "
            "WHERE c.instance_key IS NULL"
        ).fetchone()[0]
        extra = self.conn.execute(
            "SELECT COUNT(*) FROM claimed c LEFT JOIN generated g USING(instance_key) "
            "WHERE g.instance_key IS NULL"
        ).fetchone()[0]
        changed = self.conn.execute(
            "SELECT COUNT(*) FROM generated g JOIN claimed c USING(instance_key) "
            "WHERE g.row_json<>c.row_json"
        ).fetchone()[0]
        generated = self.conn.execute("SELECT COUNT(*) FROM generated").fetchone()[0]
        claimed = self.conn.execute("SELECT COUNT(*) FROM claimed").fetchone()[0]
        return {
            "generated": int(generated),
            "claimed": int(claimed),
            "missing": int(missing),
            "extra": int(extra),
            "changed": int(changed),
            "equal": not (missing or extra or changed),
        }

    def close(self) -> None:
        self.conn.close()
