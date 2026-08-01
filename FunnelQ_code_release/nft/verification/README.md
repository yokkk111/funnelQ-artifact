# Chain-bound NFT verification

This is the final selected-five formal verifier with Rust descriptor construction and Rust full verification support.

## Components

- `formal_vo/`: transcript compilation, authenticated descriptors, Merkle coverage, negative pruning, residual replay, result comparison, and JSON-RPC chain binding.
- `rust_descriptor_builder/`: authenticated descriptor builder.
- `rust_verifier_core/`: Rust verifier core.
- `chain/`: epoch-root registry contract and ABI.
- `run_verification_ablation.py`: formal Full/No Anchor/No Refined/No Pruning experiment.

## Build

```powershell
.\build_rust_cores.ps1
```

## Run

Use `run_verification_ablation.ps1` or the Python entry point. The main FunnelQ database, transaction sidecar, server claims, RPC endpoint, deployed registry address, epoch identifier, and fixed block tag must be supplied explicitly. Local-root dry-run mode is diagnostic only; the paper experiment uses `ethereum_json_rpc_eth_call` and a fixed block/root context.

Datasets, claims, indexes, VO packages, chain state, and verification results are not included.
