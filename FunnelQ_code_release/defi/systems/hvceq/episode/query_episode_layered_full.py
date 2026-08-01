#!/usr/bin/env python3
"""Compatibility entrypoint for the generic JSON-compiled HVCEQ Episode runner."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from systems.hvceq.episode.query_episode_generic import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
