# Source selection

The artifact was assembled on 2026-08-02 from the final NFT and DeFi project
trees supplied for this release.

Selected final implementations:

- NFT FunnelQ: selected-five full-event-binding query engine and Rust local executor.
- NFT verification: `NFT_verification_compact_v4_rust_verifier_ablation_v4_20260731`.
- DeFi FunnelQ: the Rust-refined Full query snapshot dated 2026-07-28, supplemented with the final builders and experiment drivers.
- SQL and HGMatch: the generic full-binding implementations invoked by the final experiment drivers.
- MBFT-Adapt: the sharded NFT implementation and the DeFi query-only/offline-build implementation invoked by the final four-system experiments.

Excluded material includes superseded verification directories, legacy compressed-result scripts, `.bak` files, intermediate fixes, generated stores, datasets, experiment outputs, plots, binaries, and build caches.

Several copied launchers originally contained machine-specific absolute paths. Only the release copies were changed: data paths are now required parameters, package-relative code paths are used, and the NFT comparison driver defaults to the final Q1–Q5 workload.
