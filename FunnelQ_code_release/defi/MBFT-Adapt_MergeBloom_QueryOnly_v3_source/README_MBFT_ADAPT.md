# MBFT-Adapt: MergeBloom Hint VO baseline for FunnelQ

This repository starts from the supplied MBFT source and keeps its core authenticated-data-structure path:

- `MergeBloomFilter::merge()` and `genHint()` during construction;
- `MergeBloomFilter::reconstruct()` during verification;
- the original Merkle-BF node hash composition;
- optional MBFT combined roots through `--combine`.

The adaptation changes the query from **first matching aggregate value** to **complete authenticated atomic-event selection**. A client verifies the complete Flashloan, Swap, and Borrow event sets, then executes the five cross-event DeFi patterns locally using the neutral matcher.

## What is preserved

The proof of an internal expanded node carries the original MergeBloom hint. The verifier reconstructs the parent filter from the two child filters and the hint, then recomputes the original 32-bit MurmurHash node root. A pruned subtree carries the same MBFT summary fields as the original VO.

## What is added

- Canonical FunnelQ event CSV loader.
- Fixed 16-keyword event mapping, required because the original hint codec assumes equal-size child MergeBloom filters.
- Canonical event payload opening at report leaves.
- `select-all` proof generation for all atomic matches rather than MAX early termination.
- Independent proof-file verifier that does not access the server tree or full block.
- Event-ID deduplication for the original tree's duplicate-last-child padding.
- Neutral SameTx/Episode matcher and the five supplied query configurations.
- Logical ADS size, proof-size, candidate-payload, hint, false-positive, query, and verification metrics.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Binaries:

```text
build/mbft/mbft        original MBFT experimental executable
build/mbft/mbft_adapt  adapted complete-selection executable
```

## Generate and verify atomic candidates

Non-combined MBFT, which was smaller for the supplied full-range type queries:

```bash
build/mbft/mbft_adapt run \
  --events /path/to/event_atoms.csv \
  --outdir /path/to/mbft_results \
  --combine 0 \
  --fp-rate 0.001 \
  --seed 0 \
  --tweak 0
```

The original combined-root variant remains available:

```bash
build/mbft/mbft_adapt run \
  --events /path/to/event_atoms.csv \
  --outdir /path/to/mbft_combined_results \
  --combine 8
```

The command first creates all proof files, destroys the in-memory server chain, and only then reloads the commitments and proof files for client verification. Outputs include:

```text
commitments.json
flashloan.mbfhint.proof
swap.mbfhint.proof
borrow.mbfhint.proof
flashloan.verified.jsonl
swap.verified.jsonl
borrow.verified.jsonl
*_server_metrics.json
*_client_metrics.json
atomic_selection_summary.json
```

A proof can also be verified independently:

```bash
build/mbft/mbft_adapt verify \
  --proof results/swap.mbfhint.proof \
  --commitments results/commitments.json \
  --out results/swap.reverified.jsonl \
  --metrics results/swap_reverify_metrics.json \
  --event-type swap
```

## Run the five complex queries

```bash
python mbft_adapt/python/run_workload.py \
  --scope same_tx \
  --query-dir mbft_adapt/configs/same_tx \
  --flashloan results/flashloan.verified.jsonl \
  --swap results/swap.verified.jsonl \
  --borrow results/borrow.verified.jsonl \
  --outdir results/same_tx
```

Use `--scope episode` and `mbft_adapt/configs/episode` for Episode queries.

## Full-data validation

On the supplied 437,124-event dataset, the authenticated selections returned exactly:

| Type | Unique verified events |
|---|---:|
| Flashloan | 84,869 |
| Swap | 323,263 |
| Borrow | 28,992 |

The neutral SameTx matcher reproduced all supplied ground-truth counts:

| Query | Result count |
|---|---:|
| FL-SB | 16 |
| FL-SB-FU | 2 |
| FL-2SW-FU | 5,331 |
| FL-3SW-CR | 10,898 |
| FL-3SW-DP | 12,269 |

Detailed measurements are in `mbft_adapt/validation/full_validation_summary.json`.

## Important implementation notes

1. The default is `--combine 0`. On this workload, combined roots reduce the number of trusted roots but enlarge report-all proofs because larger trees introduce more padding, false-positive openings, and hint reconstruction.
2. The original MBFT code duplicates the final child whenever a tree level has odd cardinality. This is harmless for MAX but duplicates leaves in a report query. MBFT-Adapt verifies those paths faithfully and deduplicates the final atomic set by `event_id`.
3. The original source retains all per-keyword hash positions in every constructed node. MBFT-Adapt releases a child's build-only `vHash` after its parent has been created. This does not modify the Bloom data, hint, or node hash; it only prevents retained O(n log n) construction memory.
4. The faithful executable retains the original 32-bit MurmurHash. It should be described as implementation-faithful rather than cryptographically collision resistant.
5. The client matcher uses only verified canonical event payloads. It does not read FunnelQ anchors, synopses, support buckets, or precomputed match counters.

## Query-only DeFi baseline (no VO)

This package also provides a query-only mode for the DeFi count workload. It
keeps the original MBFT MergeBloom index construction and Boolean/range tree
pruning, but deliberately skips HINT VO construction and client verification.
The selected atomic events are consumed by a server-side complex-event matcher.

```bash
./build/mbft/mbft_adapt query-only-select \
  --events /path/to/event_atoms.csv \
  --outdir /path/to/atomic \
  --combine 0 --fp-rate 0.001 \
  --types flashloan,swap,borrow
```

To run SameTx plus Episode 60/300/600 once:

```bash
python mbft_adapt/python/run_query_only_workload.py \
  --mbft-adapt ./build/mbft/mbft_adapt \
  --events /path/to/event_atoms.csv \
  --same-tx-query-dir /path/to/queries/same_tx \
  --episode-query-dir /path/to/queries/episode/hvceq_sql \
  --outdir /path/to/results \
  --windows 60,300,600 --timeout-sec 600
```

`query-only-select` does not call `selectAllChain`, `genHint`, proof
serialization, `reconstruct`, or `verifyAtomicSelection`. Its traversal uses the
same MBFT roots, MergeBloom summaries, `batchContain` predicate, range-overlap
check, leaf layout, and original MurmurHash configuration as the supplied
source. The complex-event matcher is an adaptation layer rather than an
original MBFT feature.
