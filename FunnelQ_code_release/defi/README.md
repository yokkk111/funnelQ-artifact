# DeFi workload

This directory contains the final DeFi SameTx and Episode implementations for FunnelQ, SQL, HGMatch, and MBFT-Adapt.

## Final workload

- `queries/replacement_trial` and `queries/new_patterns` contain S1–S5 and E1–E5.
- `queries/same_tx` and `queries/episode` retain the QuerySpecs required by the sensitivity and ablation drivers included in this artifact.

## Build

```powershell
cargo build --release --manifest-path systems\hvceq\rust_kernel\Cargo.toml
cargo build --release --manifest-path systems\hgmatch\same_tx\source\Cargo.toml
cargo build --release --manifest-path systems\hgmatch\episode\source\Cargo.toml
cmake -S MBFT-Adapt_MergeBloom_QueryOnly_v3_source -B build\mbft
cmake --build build\mbft --config Release
```

## Configuration and experiments

Set `HVCEQ_DATASET_CONFIG` to `configs/month3_paths.json` or another configuration with the same schema. Dataset files are not included.

- Store construction: `scripts/prepare_twoweek_fair_stores.py`
- One complete four-system pass: `scripts/run_final_queryset_4scheme_once.py`
- Ten passes: `scripts/run_final_queryset_4scheme_10repeats.py`
- FunnelQ ablation and window sensitivity: the remaining `run_hvceq_*` and `run_episode_*` drivers in `scripts/`

The four-system driver retains the experimental system identifier `hvceq`; this is FunnelQ in the paper.
