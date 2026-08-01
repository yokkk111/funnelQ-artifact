# Verification ablation contract

The verification ablation is not implemented by skipping checks. Every variant
must establish the same result soundness, completeness, QuerySpec binding and
EpochRoot binding.

## Full

```text
Anchor frontier
  -> Base-negative certificates
  -> Refined-negative certificates
  -> authenticated shared-range replay of Cand2
```

## w/o Anchor Directory

The complete `closureDomainRoot` is opened. The verifier checks every closure
leaf, derives the `is_anchor=1` subset, reconstructs `summaryRoot` and
`anchorDirectoryRoot`, and then applies Base and Refined. Full and No-Anchor must
have identical replay sets.

## w/o Refined Certificates

The verifier checks the complete Anchor partition and Base-negative certificates.
Every Base-positive Anchor enters the authenticated shared ranges and exact
replay. No Refined-negative certificate is accepted.

## Full Closure Replay

The verifier authenticates the complete closure domain and replays every closure.
No Anchor, Base or Refined exclusion is accepted. The final canonical result set
must still equal Full.

## Required invariants

For every query:

```text
Results(Full)
 = Results(w/o Anchor Directory)
 = Results(w/o Refined Certificates)
 = Results(Full Closure Replay)
```

and:

```text
Replay(Full) = Replay(w/o Anchor Directory)
Replay(Full) <= Replay(w/o Refined Certificates) <= Replay(Full Closure Replay)
Replay(Full Closure Replay) = closureCount
```

## Direct shared-range contract

The optimized verifier changes only the client implementation, not the proof
statement or VO package. It verifies the existing canonical merged ranges and
uses offsets into those authenticated arrays. It must report:

```text
directSharedRangeReplay = true
temporaryReplayBytes = 0
```

A verifier that expands the VO into a per-closure replay payload is not accepted
as the accelerated Rust engine.

The experiment reports actual success, failure, timeout and memory termination.
A timed-out variant is not assigned an estimated latency.
