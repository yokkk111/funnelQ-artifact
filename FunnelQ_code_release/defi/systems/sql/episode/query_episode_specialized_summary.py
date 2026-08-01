#!/usr/bin/env python3
"""Compatibility wrapper for the generic JSON-compiled episode SQL runner.

The original file contained query-name-specific SQL templates.  It is kept only
so old scripts do not break; execution is delegated to the generic compiler in
``query_episode_summary_reuse_indexes.py``.
"""
from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from systems.sql.episode.query_episode_summary_reuse_indexes import main


if __name__ == "__main__":
    raise SystemExit(main())
