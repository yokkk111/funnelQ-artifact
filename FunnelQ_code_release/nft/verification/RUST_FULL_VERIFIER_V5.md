# Rust full verifier v5

## Problem fixed

The v4 path was:

```text
Python proof verification
  -> expand each closure window into authenticated_replay.jsonl
  -> Rust exact replay
```

For Q1 No-Refined, the intermediate JSONL reached roughly 56.7 GiB even though
the VO package was about 410 MiB.

## New path

```text
Rust coverage verifier
  -> authenticated descriptor SQLite
Rust residual verifier
  -> verify canonical shared ranges and Merkle frontiers
  -> query descriptors by collection/order range
  -> derive complete closure window offsets
  -> exact replay over the same authenticated event array
Rust result comparator
  -> canonical, duplicate-free set equality
```

No event window is copied into a per-closure transport file.

## Trust separation

`rust_verifier_core` is a client verifier crate. It does not import, call or link
the server query executor. Server Cand0/Cand1/Cand2 state is not an input.

## Local confirmation checklist

After compiling, a successful report must contain:

```text
status: VERIFIED
verifierEngine: rust-full-proof+shared-range+exact-core
directSharedRangeReplay: true
temporaryReplayBytes: 0
comparison.equal: true
```

All four variants for the same query must return identical `resultCount` and
canonical result equality.
