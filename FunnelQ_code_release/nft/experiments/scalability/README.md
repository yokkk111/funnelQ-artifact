# FunnelQ NFT scale-growth experiment

This directory is an isolated snapshot experiment. It never writes to the source
three-month dataset or to previous query/verification run directories.

Data scales are chronological row-count prefixes at 20/40/60/80/100 percent.
Each boundary is extended through the Ethereum block containing the target row.
Every scale rebuilds the compact FunnelQ store, transaction sidecar, claims,
authenticated index, full VO packages, and a distinct chain-bound epoch root.

Each of the five selected queries is measured ten times after one warm-up. Formal
verification is also measured ten times and reads the committed root at one fixed
block using `ethereum_json_rpc_eth_call`.

Use `start_scale_experiment.ps1 -Resume` to continue from successful stages.
