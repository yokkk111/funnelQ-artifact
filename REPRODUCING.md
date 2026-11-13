# Running the artifact

Run commands from the repository root. Benchmark outputs must use new directories. Keep Python assertions enabled; do not use `python -O` for verification.

## Tests

```sh
pip install -r requirements-test.txt
python tools/test.py
python tools/test.py --rust
```

The Python suites cover QuerySpec validation, event identity, exact-matcher equivalence, authenticated-row decoding, response framing, epoch binding and omitted or altered records. The SCC Rust tests cover applicability, temporal boundaries, reachability, full-prefix authentication and exact result semantics. Some integration tests require Linux or external experiment inputs and are skipped when unavailable.

## NFT query execution

Build the compact store and transaction sidecar from the same canonical CSV:

```sh
python scripts/build_nft_funnelq_compact_store.py --event-atoms-csv data/nft/events.csv --sqlite-out data/nft/funnelq.db
python scripts/build_nft_tx_sidecar.py --event-atoms-csv data/nft/events.csv --sqlite-out data/nft/transactions.db
python scripts/build_nft_anchor_directory.py --db data/nft/funnelq.db
python tools/build.py nft-funnelq
```

`experiments/nft_query/run.py` accepts `--method`, `--job`, `--out` and `--repeats`. A FunnelQ job has this structure; replace the input and reference paths with your files:

```json
{
  "db": "data/nft/funnelq.db",
  "tx_db": "data/nft/transactions.db",
  "executable": "native/nft/funnelq/target/release/nft_hvceq_local_executor",
  "cases": [
    {
      "case": "Q1",
      "query_file": "queries/nft/selected5/Q1_RAPID_SAME_NFT_ROUNDTRIP.json",
      "reference": "data/nft/reference/Q1.csv"
    }
  ]
}
```

```sh
python experiments/nft_query/run.py --method funnelq --job nft-job.json --out results/nft-run --repeats 10
```

Reference files are used by the benchmark's correctness gate, not by the query adapter. Add the other four QuerySpecs to run the complete NFT workload. SQLite, PostgreSQL, MongoDB, HGMatch and MBFT use the same driver with their respective store or service settings. Database-server memory accounting requires the driver's external process-memory input.

## DeFi query execution

```sh
python tools/build.py defi-funnelq
python experiments/defi_query/resident_query.py --help
```

The resident driver accepts store paths, a case-list JSON and explicit baseline executables. Use `FunnelQ_code_release/defi` as its `--source`. Each case supplies `case`, `query_file` and `expected_count`. The `--scope` selects `same_tx` or `episode`; `--timing-only --repeats 10` requires stores and configurations that have already passed the correctness gate. PostgreSQL and MongoDB predicates are compiled in `compiler.py`.

## Domain authentication

- DeFi: `research/defi_authentication/run.py` and `column_codec.py` implement sealing, class selection and complete-domain checks. Source conversion is in the adjacent `defi_interfaces` and `defi_scc` directories. Set `FUNNELQ_DEFI_DB` to the canonical event database.
- NFT: build `scc` with `tools/build.py`. Its `semantic-core`, `semantic-client` and `scc-ablation-*` commands provide the SCC-domain and layout experiments. `scc-ablation-seal ROOT NEW_OUTPUT VARIANT` accepts `shared`, `independent`, `single_max` and `complete`.
- DeFi baseline native adapters: build `defi-mbft-verify` or `defi-vchain`.
- NFT response integration: `experiments/verification_codec/client.py` authenticates the native response and applies the query semantics.
- Chain binding: `experiments/chain_verification/binding.py` checks the pinned chain, registry, epoch and manifest. The compiled test-registry artifact is included; deployed addresses and private keys are not.

The SCC benchmark loader expects a 37-byte little-endian record layout: `u64 order`, `u64 time`, five `u32` values (`src`, `dst`, `nft`, `collection`, `tx`) and `u8 event_type`. NFTTrade has type 1. The original benchmark bundle places this file at `FunnelQ_CSR/artifacts/nft_csr_profit_screen_stage6_block_b16_t1800/canonical_events_collection_order_v3.bin` beneath `ROOT`. The full-data loader checks the evaluated NFTTrade cardinality. The scale driver applies its explicit subset-loader adjustment.

Authentication clients require the appropriate query, sealed metadata, response and native executable paths. They do not reconstruct trusted state from an untrusted response. Native baseline stores and trusted dictionaries must be built before measuring verification.

## YO route-risk case

`experiments/yo_query/prepare.py` builds query stores. `benchmark.py --help` lists the six-method benchmark arguments. `native/yo` contains the prepared HGMatch, MBFT and vChain+ entry points. The FunnelQ query adapter is in `benchmark.py`; the complete-domain authentication implementation is in `experiments/yo_verification/protocol.py`.

```sh
python tools/build.py yo-hgmatch
python tools/build.py yo-mbft
python tools/build.py yo-mbft-verify
python tools/build.py yo-vchain
```

## Full benchmark setup

The timing, ablation and scale drivers retain the recorded Linux experiment layouts, including `/suite` and paths to generated artifacts. They are not one-command downloads of the original experiment environment. Configure these paths or mount the corresponding data/store directories before using them. `FUNNELQ_SUITE` overrides the ablation suite root; canonical stores, baseline services, prior full-answer references and chain state are external inputs.

Do not interpret synthetic tests, compiler checks or a subset run as a reproduction of the three-month benchmark. This release contains the source and workload definitions; it does not include generated data, previous measurements or compiled runtime caches.
