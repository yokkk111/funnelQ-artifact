# Changes from the supplied MBFT source

## Authentication mechanism retained

No replacement Bloom tree is introduced. Query expansion emits `SELECT_HINT` nodes containing `MergeBloomFilter::getHint()`. Verification calls the supplied `MergeBloomFilter::reconstruct()` and recomputes the supplied Merkle-BF hash formula.

## Source changes

- `transaction.*`: canonical payload support and shared immutable event material for scalable copies.
- `blockchain.*`: corrected block item accounting, deterministic stable sorting, total ADS size, and memory-safe construction.
- `merkle.cpp`: preserves a leaf index for single-event blocks and releases build-only `vHash` vectors after parent creation.
- `adapt.*`: DeFi CSV mapping, complete atomic selection, MergeBloom Hint VO serialization, commitments, independent verification, and metrics.
- `adapt_main.cpp`: server/client-separated command-line workflow.
- `mbft_adapt/python`: neutral complex-event matcher and workload runner.

## Query-proof node meanings

- `SELECT_PRUNED`: original node hash, full node MergeBloom filter, bounds, and aggregate value; verifier checks that the atomic predicate cannot match the subtree.
- `SELECT_OPEN_LEAF`: canonical event payload; verifier recreates its fixed keyword set, leaf MergeBloom filter, value, and Murmur leaf hash.
- `SELECT_HINT`: original parent MergeBloom hint plus bound/value metadata; verifier reconstructs the parent from the two child filters.

## Fairness boundary

MBFT proves complete atomic-event retrieval. It does not receive FunnelQ's semantic candidate summaries. The client separately evaluates role equality, asset-flow continuity, order, distinct-event, distinct-pool, and other cross-event constraints.

## v3 query-only addition

- Added `selectAllChainNoProof`, which traverses the original MBFT MergeBloom
  trees and returns all exact matching atomic payloads without constructing VO
  nodes.
- Added `query-only-select` CLI mode. It builds the original index, executes
  atomic Boolean selections, and emits server-side candidates and timing
  metrics; no proof or verifier is invoked.
- Added `run_query_only_workload.py` for the five count-only SameTx queries and
  the five Episode queries under 60/300/600-second windows.
- The server-side complex matcher uses the same query JSON semantics as the
  supplied FunnelQ/SQL/HGMatch workload. This matcher is explicitly outside the
  original MBFT query language.
