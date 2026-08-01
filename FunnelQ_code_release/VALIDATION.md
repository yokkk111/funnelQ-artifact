# Packaging validation

Validation performed on 2026-08-02:

- Parsed all 100 Python source files with Python's AST parser.
- Parsed all 48 JSON files.
- Passed the seven selected-five query-contract and event-ID unit tests.
- Ran `cargo check --locked` successfully for seven Rust projects: NFT FunnelQ, NFT HGMatch, both formal-verification crates, DeFi FunnelQ, and both DeFi HGMatch crates.
- Confirmed that five main CLI entry points return `--help` successfully: NFT four-system runner, NFT FunnelQ runner, NFT verification ablation, NFT MBFT-Adapt runner, and DeFi four-system runner.
- Confirmed that no dataset/database, result CSV, binary, compiled library, archive, figure, Python cache, Cargo target directory, CMake build directory, or machine-specific absolute path remains in the package.

C++ compilation was not executed because CMake is not installed in the packaging environment. Both MBFT-Adapt source trees include their CMake files and all source/header paths referenced by those files.
