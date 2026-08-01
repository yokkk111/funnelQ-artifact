# EpochRoot registry binding

The verifier reads the root with an actual Ethereum JSON-RPC `eth_call`. The
included reference contract exposes:

- `commitEpochRoot(bytes32,bytes32)`
- `getEpochRoot(bytes32)`

The local node must expose JSON-RPC. For the bundled commit command, the owner
account must be unlocked by the local development node, or `--from-address`
must name an unlocked account. The verifier itself needs no account or private
key.

`epochId=auto` is deterministically derived from the protocol version, schema,
data range and source database fingerprints in `epoch_manifest.json`. It is
independent of Q1–Q5 and remains the same for all five VO packages.
