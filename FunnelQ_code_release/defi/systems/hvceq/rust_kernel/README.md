# HVCEQ integer-domain kernel

This crate is the count-only execution kernel used by the generic SameTx and
Episode runners. Python keeps query parsing, candidate synopsis layers,
numeric-value parsing, and reporting. Rust receives borrowed NumPy integer
buffers through a C ABI and evaluates:

- query-variable equality;
- strict event order;
- event injectivity;
- field-level inequality;
- exact match and result-unit counts.

Build on Windows:

```powershell
cargo build --release
```

The Python bridge loads
`target/release/hvceq_integer_kernel.dll`. Set
`HVCEQ_DISABLE_RUST_KERNEL=1` to run the preserved Python fallback for
regression comparisons.
