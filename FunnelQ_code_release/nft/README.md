# NFT workload

This directory contains the final three-month NFT workload used in the paper.

## Final queries

`queries/nft/selected5` contains Q1–Q5. All systems consume the same full-event-binding QuerySpecs and must materialize the same canonical result set.

## Build

Build the FunnelQ and HGMatch Rust executors:

```powershell
cargo build --release --manifest-path systems\nft\hvceq\local_executor_rust\Cargo.toml
cargo build --release --manifest-path systems\nft\hgmatch\rust_engine\Cargo.toml
```

Build MBFT-Adapt with CMake from `systems/nft/mbft/mbft`.

The Python store builders are:

- FunnelQ: `scripts/build_nft_funnelq_compact_store.py` and `scripts/build_nft_tx_sidecar.py`
- SQL: `systems/nft/sql/build_nft_sql_store.py`
- HGMatch: `systems/nft/hgmatch/build_nft_hgmatch_store.py`
- MBFT-Adapt: `systems/nft/mbft/build_mbft_nft_sharded_stores.py`

## Experiments

- Four-system query comparison: `scripts/run_nft_final5_hvceq_sql_compare.py`
- FunnelQ-only query and three memory metrics: `scripts/run_nft_hvceq_final5_full_memory.py`
- FunnelQ query ablation: `scripts/run_nft_selected5_3m_hvceq_ablation.py`
- Verification and verification ablation: `verification/`
- 20/40/60/80/100% scalability: `experiments/scalability/`

All dataset, store, output, RPC, and contract-address paths must be supplied by the caller.
