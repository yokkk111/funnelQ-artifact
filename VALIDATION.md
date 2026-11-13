# Release validation

Checks performed on the cleaned source package on 2026-11-13:

| Check | Result |
|---|---|
| Selected NFT query contracts and event identity | 7 passed |
| NFT resident protocol checks | 5 passed |
| DeFi query compiler, accounting and resident protocol | 18 passed, 2 platform-dependent tests skipped |
| DeFi exact matcher and authenticated-row codec | 8 passed |
| DeFi verification contract | 3 passed, 1 external-input test skipped |
| Unified response framing and trust-envelope checks | 9 passed |
| Chain commitment binding checks | 14 passed |
| YO semantics and domain-authentication checks | 14 passed |
| SCC Rust test suite | 44 passed, 1 ignored |

`cargo check --offline --locked` passed for the SCC executable, NFT FunnelQ and HGMatch resident executables, the DeFi FunnelQ kernel, the DeFi SameTx/Episode HGMatch resident executables and the YO HGMatch executable.

Python comment/docstring cleanup was checked by comparing parsed syntax trees with comments, docstrings and no-op placeholders excluded. Cargo manifest and lockfile cleanup preserved their parsed TOML values. Module-location changes and prepared native entry points were checked separately.

The source archive excludes caches, generated stores, data, execution logs, binaries and private credentials. Third-party license files and copyright-bearing headers are preserved.

The complete three-month benchmark, database services, C++ baseline builds, vChain+ native dependency builds and live chain deployment were not rerun for this packaging task. Compiler checks retain existing unused-code warnings. Passing the listed checks does not assert that every optional historical benchmark configuration has been executed on this machine.
