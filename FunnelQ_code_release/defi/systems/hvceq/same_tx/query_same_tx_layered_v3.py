#!/usr/bin/env python3
"""Compatibility entrypoint for the generic JSON-compiled HVCEQ SameTx runner.

The previous implementation dispatched by query_name to five specialized
physical plans.  The fair-cost generic-engine setting requires the query
semantics to come from the query JSON, so this wrapper preserves the old CLI
path while delegating to query_same_tx_generic.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from systems.hvceq.same_tx.query_same_tx_generic import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
