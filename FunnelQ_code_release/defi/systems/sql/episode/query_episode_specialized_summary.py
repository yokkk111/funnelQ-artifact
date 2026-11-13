#!/usr/bin/env python3


from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from systems.sql.episode.query_episode_summary_reuse_indexes import main


if __name__ == "__main__":
    raise SystemExit(main())
