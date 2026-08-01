# FunnelQ paper artifact — source snapshot

This directory contains the final source code selected from the NFT and DeFi experiment projects. It includes FunnelQ and the three comparison systems used in the paper: SQL, HGMatch, and MBFT-Adapt.

The historical implementation name `hvceq` is retained in module paths and command-line output for compatibility. In this artifact, `hvceq` denotes FunnelQ.

## Contents

- `nft/`: three-month NFT workload, the final Q1–Q5 QuerySpecs, four-system query experiment, FunnelQ ablation, scalability experiment, and chain-bound verification.
- `defi/`: DeFi SameTx/Episode workloads, the final S1–S5 and E1–E5 QuerySpecs, four-system experiment, and the corresponding system implementations.
- `common/`: the Windows process-tree monitor used to record Job Commit, working set, and private bytes.

## Systems

- FunnelQ: Python orchestration plus Rust query kernels.
- SQL: SQLite-based relational baselines.
- HGMatch: Rust hypergraph matching engines and their store builders.
- MBFT-Adapt: C++ MergeBloom/MBFT code plus Python adapters and external complex-event matchers.

## Data policy

No dataset, generated database, serialized index, VO package, experiment result, log, figure, compiled executable, dynamic library, cache, or Cargo/CMake build directory is included. Supply the required data paths through the command line or the workload configuration files.

See `DATASETS.md`, `nft/README.md`, and `defi/README.md` before running experiments.

## Software

- Python 3.11 or newer
- Rust stable toolchain and Cargo
- CMake plus a C++17 compiler
- Python packages in `requirements.txt`

## Integrity

`SHA256SUMS.txt` records the final packaged files after path sanitization. `SOURCE_SELECTION.md` records the two source projects and the version-selection policy.
