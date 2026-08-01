# Datasets and generated artifacts

Datasets are intentionally not included.

The NFT code expects the normalized three-month NFT event-atom CSV used by the paper. Store builders generate the FunnelQ, SQL, HGMatch, and MBFT-Adapt indexes from that input. The final query workload is in `nft/queries/nft/selected5`.

The DeFi code expects normalized flash-loan, borrowing, and DEX event files. Relative path templates are provided in `defi/configs`. The final query workload is under `defi/queries/new_patterns` and `defi/queries/replacement_trial`.

Generated directories such as `data/`, `stores/`, `runs/`, `logs/`, `target/`, `build/`, `packages/`, and `vo_packages/` are ignored by Git and must be produced locally.
