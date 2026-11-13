# FunnelQ

Source code for **FunnelQ: Hypergraph-Based Verifiable Complex Event Querying over Blockchain Data**.

FunnelQ evaluates event patterns within transaction or bounded episode scopes. Its verifier authenticates a complete necessary event domain and checks canonical answers using binding validation and exact counting. Verification assumes correct, query-independent construction and sealing of the indexes.

## Requirements

- Python 3.11 or newer
- Rust and Cargo; this release was checked with Rust 1.94.1
- A C++17 compiler and CMake for MBFT
- PostgreSQL and MongoDB for their respective query baselines
- Linux for the process-isolated benchmark drivers

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python tools/test.py
```

## Build

```sh
python tools/build.py nft-funnelq
python tools/build.py defi-funnelq
python tools/build.py scc
```

The same command supports `nft-hgmatch`, `defi-hgmatch-sametx`, `defi-hgmatch-episode`, `nft-mbft`, `defi-vchain` and `nft-vchain`. Cargo lockfiles are included. Use `--offline` when the dependencies are already cached. vChain+ also requires the native development dependencies used by RocksDB and bindgen, including Clang/libclang and a C++ compiler.

## Code map

| Component | Directory |
|---|---|
| NFT QuerySpecs | `queries/nft/selected5` |
| NFT event model, exact matching and store builders | `systems/nft`, `scripts` |
| NFT resident FunnelQ, HGMatch and MBFT kernels | `native/nft` |
| DeFi event model, queries, builders and FunnelQ kernel | `FunnelQ_code_release/defi` |
| DeFi resident HGMatch kernels | `native/defi/hgmatch` |
| Six-method NFT query driver | `experiments/nft_query/run.py` |
| DeFi resident query driver and database query compiler | `experiments/defi_query` |
| Role co-occurrence classes and authentication | `research/defi_authentication` |
| Multiscale SCC domains, authentication and exact counting | `FunnelQ_Columnar_Count_Experiment/src/semantic_core.rs` |
| Shared/independent/single-scale/full-domain comparison | `FunnelQ_Columnar_Count_Experiment/src/semantic_ablation.rs` |
| DeFi MBFT and vChain+ verification adapters | `experiments/defi_native_verification/revision_v4` |
| Verification response encoding and client integration | `experiments/verification_codec` |
| Epoch commitment checks | `experiments/chain_verification` |
| YO route-risk queries | `experiments/yo_query` |
| YO domain authentication and client checks | `experiments/yo_verification` |

Historical module names such as `hvceq` are retained because scripts and serialized schemas reference them. They denote FunnelQ in this artifact. Baseline adaptations are included with the applicable third-party notices.

## Running experiments

See [REPRODUCING.md](REPRODUCING.md) for entry points and input requirements, [DATA.md](DATA.md) for data formats, and [VALIDATION.md](VALIDATION.md) for the checks performed on this source release.

Datasets, generated stores, proofs, private keys, runtime environments and measured result files are not bundled. The tests use synthetic inputs; running them does not reproduce the full paper benchmarks. The supplied workloads and adapters are the evaluated implementations, not a claim of support for arbitrary event-query languages.

## Third-party code

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Included third-party licenses and copyright notices remain in force. No additional license grant is made by this source snapshot.
