# Data and result contracts

The source release does not include blockchain datasets or generated indexes.

## NFT

The selected workload is defined by the five JSON files in `queries/nft/selected5`. Each query uses a 1,800-second episode and returns canonical full event bindings. The schema, role names, ordering and event-identity checks are implemented in `systems/nft/common/full_binding_spec.py` and `systems/nft/hvceq/selected5_query_contract.py`.

Use the builders in `scripts` to construct the compact event store, transaction sidecar and anchor directory from canonical event CSV input. Inspect each builder's `--help` for required columns and arguments. `systems/nft/sql`, `systems/nft/postgresql` and `systems/nft/mongodb` provide the database-specific builders. The original event order and identifiers must remain unchanged across methods.

## DeFi

`FunnelQ_code_release/defi` contains the SameTx and Episode QuerySpecs and their original store builders. The runtime and verification adapters use canonical event IDs, transaction IDs, event times, actor/asset/pool roles and amount-sign information. `experiments/defi_native_verification/revision_v4/legacy_evaluate.py` defines the numeric record schema; `row_codec.py` defines the authenticated-row handoff format.

The co-occurrence builder can read its source database from `FUNNELQ_DEFI_DB`. It defaults to `data/defi/anchored_episode_runtime_sidecar.db` relative to the repository root.

## YO

The route-risk specification and canonical input loader are in `experiments/yo_query/business.py`. Answers are sorted, unique transaction hashes. They are not NFT full-binding rows. The synthetic tests exercise the same semantic and authentication code without the original incident dataset.

## Trust inputs

Verification requires a separately supplied trusted seal or chain pin, a query and the server response. Generated proofs, dictionaries, directories and native stores must come from the same sealed epoch. A server-supplied root must not be substituted for the client's trusted commitment.
