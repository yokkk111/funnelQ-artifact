from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterator, Mapping

from .index import ANCHOR_COLUMNS


class CandidateStore:
    """Disk-backed authenticated replay descriptors.

    Verification ablations may authenticate millions of closures.  Keeping each
    descriptor as a Python dictionary would dominate verifier memory.  This
    store batches canonical integer descriptors into SQLite and exposes stable
    collection/order and anchor-order iterators for later proof and replay
    phases.
    """

    _BATCH_SIZE = 8192

    def __init__(self, path: Path):
        self.path = Path(path)
        if self.path.exists():
            self.path.unlink()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        descriptor_columns = ",\n              ".join(
            f'"{column}" INTEGER NOT NULL' for column in ANCHOR_COLUMNS
        )
        self.conn.executescript(
            f"""
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=FILE;
            PRAGMA cache_size=-65536;
            CREATE TABLE candidates(
              {descriptor_columns},
              PRIMARY KEY(anchor_order_seq),
              UNIQUE(closure_id)
            ) WITHOUT ROWID;
            CREATE INDEX idx_candidates_collection_order
              ON candidates(collection_id,anchor_order_seq);
            """
        )
        quoted = ",".join(f'"{column}"' for column in ANCHOR_COLUMNS)
        placeholders = ",".join("?" for _ in ANCHOR_COLUMNS)
        self._insert_sql = f"INSERT INTO candidates({quoted}) VALUES({placeholders})"
        self._buffer: list[tuple[int, ...]] = []
        self._count = 0

    def _flush(self) -> None:
        if not self._buffer:
            return
        try:
            self.conn.executemany(self._insert_sql, self._buffer)
        except sqlite3.IntegrityError as exc:
            raise ValueError(
                "authenticated replay set contains duplicate closure/anchor"
            ) from exc
        self._buffer.clear()

    def add(self, descriptor: Mapping[str, Any]) -> None:
        if set(descriptor) != set(ANCHOR_COLUMNS):
            raise ValueError("authenticated replay descriptor fields are not canonical")
        self._buffer.append(tuple(int(descriptor[column]) for column in ANCHOR_COLUMNS))
        self._count += 1
        if len(self._buffer) >= self._BATCH_SIZE:
            self._flush()

    def commit(self) -> None:
        self._flush()
        self.conn.commit()

    def __len__(self) -> int:
        return self._count

    @staticmethod
    def _decode(row: tuple[Any, ...]) -> dict[str, int]:
        return {
            column: int(row[offset]) for offset, column in enumerate(ANCHOR_COLUMNS)
        }

    def iter_collection_order(self) -> Iterator[dict[str, int]]:
        quoted = ",".join(f'"{column}"' for column in ANCHOR_COLUMNS)
        cursor = self.conn.execute(
            f"SELECT {quoted} FROM candidates "
            "ORDER BY collection_id,anchor_order_seq"
        )
        for row in cursor:
            yield self._decode(row)

    def __iter__(self) -> Iterator[dict[str, int]]:
        return self.iter_collection_order()

    def iter_anchor_order(self) -> Iterator[dict[str, int]]:
        quoted = ",".join(f'"{column}"' for column in ANCHOR_COLUMNS)
        cursor = self.conn.execute(
            f"SELECT {quoted} FROM candidates ORDER BY anchor_order_seq"
        )
        for row in cursor:
            yield self._decode(row)

    def __getitem__(self, anchor_order: int) -> int:
        value = self.get(anchor_order, None)
        if value is None:
            raise KeyError(int(anchor_order))
        return int(value)

    def get(self, anchor_order: int, default: Any = None) -> Any:
        row = self.conn.execute(
            "SELECT closure_id FROM candidates WHERE anchor_order_seq=?",
            (int(anchor_order),),
        ).fetchone()
        return default if row is None else int(row[0])

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "CandidateStore":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
