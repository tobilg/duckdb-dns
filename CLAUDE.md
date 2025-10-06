# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension called "dns" that provides DNS lookup functionality. It's built using Rust and the DuckDB C Extension API, using the experimental DuckDB Rust extension template that requires no DuckDB build or C/C++ code.

### Extension Functions

The extension provides two scalar functions:

1. **`reverse_dns_lookup(ip_address)`**: Takes an IPv4 address string and returns the resolved hostname as a VARCHAR, or NULL on error
2. **`dns_lookup(hostname)`**: Takes a hostname string and returns the first resolved IPv4 address as a VARCHAR, or NULL on error
3. **`dns_lookup_all(hostname)`**: Takes a hostname string and returns a VARCHAR array of all resolved IPv4 addresses, or NULL on error

Both functions return NULL on errors rather than throwing exceptions.

## Build System

The project uses a Make-based build system that wraps cargo and DuckDB extension tooling:

### Initial Setup
```bash
make configure
```
This sets up a Python venv with DuckDB and the test runner, and determines the compilation platform.

### Building
- **Debug build**: `make debug`
- **Release build**: `make release`

Debug builds output to `target/debug/` and are then transformed into loadable extensions in `build/debug/extension/dns/`.
Release builds use LTO and strip symbols (see Cargo.toml profile).

### Testing
- **Debug tests**: `make test_debug` or `make test`
- **Release tests**: `make test_release`

Tests are written in SQLLogicTest format in `test/sql/dns.test`.

### Testing with Different DuckDB Versions
```bash
make clean_all
DUCKDB_TEST_VERSION=v1.3.2 make configure
make debug
make test_debug
```

### Cleaning
- **Clean build artifacts**: `make clean`
- **Clean everything including venv**: `make clean_all`

## Architecture

### Extension Entry Point
The extension uses the `#[duckdb_entrypoint_c_api()]` macro (from duckdb-loadable-macros) to define the entry point in `src/lib.rs`. The extension registers two scalar functions on load.

### Scalar Functions
Scalar functions are implemented using the `ScalarFunction` and `ScalarFunctionSet` types from the duckdb-rs crate:

1. **Function Definition**: Each function is defined using `ScalarFunction::new()` with:
   - Input parameter type (VARCHAR for both functions)
   - Return type (VARCHAR for both functions)
   - C function pointer to the implementation

2. **Vectorized Execution**: The C function implementations process entire input vectors at once:
   - `reverse_dns_lookup_scalar()`: Processes IP address strings in batches
   - `dns_lookup_scalar()`: Processes hostname strings in batches
   - Both functions handle NULL inputs and set NULL outputs on errors

3. **String Handling**: Custom helper functions convert between DuckDB's `duckdb_string_t` and Rust `String` types

### DNS Resolution Implementation

The extension uses the `trust-dns-resolver` crate for DNS queries:

- **Forward DNS**: `dns_lookup_impl()` resolves hostnames to the first IPv4 address using `Resolver::lookup_ip()`
- **Reverse DNS**: `reverse_dns_lookup_impl()` resolves IPv4 addresses to hostnames using `Resolver::reverse_lookup()`

Key implementation details:
- IPv4 validation is performed in `validate_ipv4()` using standard library's `Ipv4Addr::from_str()`
- Only IPv4 addresses are returned (IPv6 is filtered out)
- `dns_lookup()` returns only the first IPv4 address found (not all addresses)
- DNS queries use `Resolver::new()` with default configuration
- The resolver is synchronous (not async/tokio-based)
- Errors return NULL rather than propagating exceptions
- The vectorized C functions process entire input batches for performance

### Dependencies
- `duckdb` (v1.4.0) with "vtab-loadable" and "vscalar" features
- `duckdb-loadable-macros` (v0.1.10) for entry point macros
- `libduckdb-sys` (v1.4.0) with "loadable-extension" feature
- `tokio` (v1.42) with "rt" and "net" features (for DNS resolution)
- `trust-dns-resolver` (v0.23) for DNS lookups

### Configuration
- **DuckDB target version**: v1.4.0 (defined in Makefile)
- **Uses unstable C API**: Yes (`USE_UNSTABLE_C_API=1` in Makefile)
- **Extension name**: "dns" (Makefile and Cargo.toml)
- **Rust edition**: 2021
- **Library type**: cdylib (native dynamic library)

## Running the Extension Locally

```bash
duckdb -unsigned
```

Then in DuckDB:
```sql
LOAD './build/debug/extension/dns/dns.duckdb_extension';
SELECT dns_lookup('google.com');
SELECT reverse_dns_lookup('8.8.8.8');
```

## CI/CD

The project uses DuckDB's extension-ci-tools (v1.4.0) via GitHub Actions. The main distribution pipeline builds binaries for multiple platforms (excluding wasm_mvp, wasm_eh, wasm_threads, and linux_amd64_musl).

The workflow is defined in `.github/workflows/MainDistributionPipeline.yml`.

## Known Issues

- Extensions may fail to load on Windows with Python 3.11 (use Python 3.12)
- Only IPv4 addresses are supported; IPv6 addresses are filtered out in DNS lookups
