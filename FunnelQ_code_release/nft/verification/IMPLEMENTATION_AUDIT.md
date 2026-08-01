# Protocol-v3 accelerated verifier implementation audit

## Correctness properties retained

- verifier-derived replay deadline and immediate-successor boundary witness;
- canonical Merkle range frontiers and position-bound inclusion proofs;
- strict QuerySpec/transcript, manifest, package, coverage and residual schemas;
- transaction leaves aligned with every opened event range;
- source/descriptor fingerprint and ordered mapping checks;
- exact full-binding set comparison; Cand2 is never treated as the result set;
- complete `closureDomainRoot` and committed `is_anchor` for aligned ablations;
- all four variants verify the same soundness and completeness statement.

## v5 performance change

The package and proof statement are unchanged. Only the client verifier execution
path changes.

Previous v4 path:

```text
Python VOcov/VOneg/VOrep checks
  -> per-closure authenticated_replay.jsonl expansion
  -> Rust exact core
```

v5 path:

```text
Rust VOcov/VOneg checks
  -> compact authenticated descriptor SQLite
Rust VOrep checks over canonical shared ranges
  -> integer window offsets into authenticated events
  -> Rust exact replay and canonical set comparison
```

The full Rust core consumes existing gzip package files directly and reports
`temporaryReplayBytes=0`. It never imports the server query executor or accepts
server candidate state.

## Scalability measures

- coverage and residual files are decompressed sequentially;
- million-scale replay descriptors are inserted in one SQLite transaction;
- candidate lookup is indexed by `(collection_id, anchor_order_seq)`;
- one shared range is parsed and verified at a time;
- closure windows are represented by offsets, not copied event lists;
- generated and claimed canonical result sets are disk-backed;
- progress is emitted at bounded record intervals;
- the ablation runner rotates variant order and records timeout, memory and engine.

## Compatibility

Descriptor/index/package generation code is unchanged from the preceding v4
protocol-v3 release. Existing 20 VO packages can be supplied through
`-ReusePackagesFrom`; no descriptor, index, EpochRoot or VO rebuild is required.

## Test evidence in this archive

- 14 automated tests pass;
- all 5 queries × 4 variants verify the same demo result set on the Python
  reference path;
- omission/tamper and query/Epoch/result substitution tests reject;
- the Rust wrapper contract test confirms package-direct invocation, absence of
  `--replay-jsonl`, `directSharedRangeReplay=true`, and cleanup of temporary DBs;
- Python compilation and ZIP integrity checks pass.

## Evidence not claimed

Cargo/Rust is unavailable in this execution environment, so the new Rust crate
could not be compiled here. The target Windows machine must run
`build_rust_cores.ps1` and a one-repetition `-Engine rust` smoke run. The code does
not claim that every million-scale variant will finish under the existing 600 s
experimental timeout; actual timeout results remain valid experimental outcomes.
