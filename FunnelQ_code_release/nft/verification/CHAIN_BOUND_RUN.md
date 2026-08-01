# Live-chain EpochRoot run

This release reads the trusted root inside the verifier with Ethereum JSON-RPC
`eth_call`; it no longer requires a separately copied root string.

## Values needed from the local chain

- RPC URL, for example `http://127.0.0.1:8545`
- deployed registry address
- numeric chain ID
- an unlocked owner account for the one-time commit
- contract method names if an existing registry does not use the bundled
  `commitEpochRoot(bytes32,bytes32)` / `getEpochRoot(bytes32)` interface

`EpochId=auto` is recommended. It is derived from the authenticated data range
and source fingerprints, so Q1–Q5 use the same query-independent epoch key.

## Commit once

```powershell
& "$ToolRoot\run_real_selected5.ps1" `
  -Stage chain `
  -OutRoot $OutRoot `
  -RpcUrl "http://127.0.0.1:8545" `
  -RegistryAddress "0x..." `
  -ExpectedChainId 31337 `
  -FromAddress "0x..."
```

The command first reads the current slot. It sends no transaction when the
same root is already committed and rejects a conflicting nonzero root.

## Verify Q1-Q5 with a live root read

Use a separate report directory to preserve the previous dry-run reports:

```powershell
$ChainReports = "$OutRoot\reports_chain_rpc_run1"

& "$ToolRoot\run_real_selected5.ps1" `
  -Stage verify `
  -MainDb $MainDb `
  -TxDb $TxDb `
  -ProjectRoot $ProjectRoot `
  -OutRoot $OutRoot `
  -ReportsRoot $ChainReports `
  -RpcUrl "http://127.0.0.1:8545" `
  -RegistryAddress "0x..." `
  -ExpectedChainId 31337
```

Each query independently pins a block number, checks the chain ID and deployed
bytecode, calls the registry, binds the returned bytes32 root to the VO, and
records the block number/hash and RPC timing in its report.

No descriptor, authenticated index, VO package, Cand2 file, query result or
source database is rebuilt or modified by these two stages.
