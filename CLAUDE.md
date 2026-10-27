# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension called "dns" that provides DNS lookup functionality. It's built using Rust, `duckdb-neo`, and DuckDB's experimental v2 C API, using the experimental DuckDB Rust extension template that requires no DuckDB build or C/C++ code.

### Extension Functions

The extension provides three lookup scalar functions:

1. **`reverse_dns_lookup(ip_address)`**: Takes a mandatory `ip_address` IPv4 or IPv6 address string and returns the resolved hostname as a VARCHAR, or NULL on error

2. **`dns_lookup(hostname, [record_type])`**: Takes a mandatory `hostname` string, and an optional `record_type` string. Supported record types: A, AAAA, CNAME, MX, NS, PTR, SOA, SRV, TXT, CAA (parsed in `parse_record_type()`; see https://docs.rs/hickory-proto/0.26.1/hickory_proto/rr/record_type/enum.RecordType.html#variants for all hickory record types). Returns the first resolved IPv4 address as a VARCHAR if `record_type` is not specified, or the first record of the specified `record_type` as a VARCHAR, or NULL on error.

3. **`dns_lookup_all(hostname, [record_type])`**: Takes a mandatory `hostname` string, and an optional `record_type` string (same types as above). Returns all resolved IPv4 addresses as a VARCHAR[] if `record_type` is not specified, or a VARCHAR array of all resolved records of the specified `record_type`, or NULL on error.

The lookup functions return NULL on errors rather than throwing exceptions.

The extension provides three configuration scalar functions, which return a success or error message as a VARCHAR (called with `SELECT`):

4. **`set_dns_config(preset)`**: Replaces the resolver with one of the presets `default` (system configuration, falling back to Google), `google`, `cloudflare` or `quad9`. This clears the DNS cache.

5. **`set_dns_concurrency_limit(limit)`**: Sets the maximum number of concurrent DNS requests (default 50, must be greater than 0).

6. **`set_dns_cache_size(size)`**: Sets the resolver's cache size in entries (default 4096, must be greater than 0). This rebuilds the resolver, which clears the cache.

The extension provides one table function:

7. **`corey(hostname)`**: Takes a mandatory `hostname` string and queries for TXT records. Returns all TXT records found as a table with a single column `txt_record` of type VARCHAR, or a binding error if the TXT lookup fails.

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

Tests are written in SQLLogicTest format in `test/sql/`: `api.test` exercises v2 registration, vector batches, NULLs and configuration without external DNS; `corey.test` covers the TXT table function; `dns.test`, `cache_size.test`, `cache_performance.test` and `performance.test` cover DNS behavior. All files except `api.test` need network access.

### Testing with the Matching DuckDB Version
```bash
make clean_all
DUCKDB_TEST_VERSION=2.0.0 make configure
make debug
make test_debug
```

The extension uses DuckDB's experimental v2 C API through `duckdb-neo` and requires the matching DuckDB release (currently `v2.0.0`). DuckDB 1.x is no longer supported. Keep the crates, `api_version` in `src/lib.rs`, `TARGET_DUCKDB_VERSION`, the CI DuckDB version and `DUCKDB_TEST_VERSION` aligned. The test venv in `configure/` is created only once: after upgrading DuckDB, run `make clean_all` before `make configure` to install the matching runtime.

### Cleaning
- **Clean build artifacts**: `make clean`
- **Clean everything including venv**: `make clean_all`

## Architecture

All code lives in `src/lib.rs`.

### Extension Entry Point
The extension uses `#[duckdb_entrypoint_c_api_v2(extension_name = "dns", api_version = "v2.0.0")]` from `duckdb-loadable-macros`. Its entrypoint receives an `Extension` and registers six scalar functions with `ScalarFunctionBuilder` and `corey` with `TableFunctionBuilder`. The exported symbol is `dns_init_c_api_v2`.

### Scalar Functions
Each scalar function implements `duckdb_neo::scalar::ScalarCallbacks` (`ReverseDnsLookup`, `DnsLookup`, `DnsLookupAll`, `SetDnsConfig`, `SetConcurrencyLimit`, `SetDnsCacheSize`):

1. **Signatures**: The entrypoint builds signatures with `SignatureBuilder`. `dns_lookup` and `dns_lookup_all` retain both overloads, with and without `record_type`. Explicit NULL arguments produce NULL.
2. **Vectorized Execution**: `exec()` processes input vectors with typed readers that handle flat, constant and dictionary storage. Lookup futures run concurrently via `join_all` on the global Tokio runtime.
3. **Output**: Typed `String` and `List<String>` writers manage values and validity without raw FFI pointers. Output vectors are sized to the input row count.
4. **Optimizer properties**: Functions are volatile because DNS can change and configuration calls have side effects; these calls must execute when a statement runs.

### Table Function
`Corey` implements `TableFunctionCallbacks`. `bind()` declares the `txt_record` column and resolves TXT records with its own resolver. `init_local_state()` creates a scan cursor, and `exec()` writes batches of records. The scan uses one worker and a cursor per execution to emit each record once. TXT lookup errors become DuckDB input errors.

### Global Resolver State
`GLOBAL_DNS_STATE` (a `once_cell::sync::Lazy<DnsResolverState>`) is shared by all functions so the DNS cache is reused across queries. It holds:
- One multi-threaded Tokio runtime, created once and used with `block_on()` for every batch
- The hickory `Resolver`, wrapped in `ArcSwap` so configuration changes swap it atomically without locks
- The current `ResolverConfig` and `ResolverOpts`, used when rebuilding the resolver
- A `tokio::sync::Semaphore` (also in `ArcSwap`) that limits concurrent DNS requests
- The configured cache size

### DNS Resolution Implementation

The extension uses the `hickory-resolver` crate (formerly `trust-dns-resolver`) for DNS queries:

- **Forward DNS**: `dns_lookup_async()` resolves hostnames to the first IPv4 address using `Resolver::lookup_ip()`
- **Reverse DNS**: `reverse_dns_lookup_async()` resolves IPv4 and IPv6 addresses to hostnames using `Resolver::reverse_lookup()`, which builds the `in-addr.arpa` / `ip6.arpa` PTR name

Key implementation details:
- IP validation for reverse lookups is performed in `validate_ip()` using standard library's `IpAddr::from_str()`; IPv4-mapped IPv6 addresses are canonicalized to IPv4
- Forward lookups without a record type return only IPv4 addresses (IPv6 is filtered out)
- `dns_lookup()` returns only the first IPv4 address found (not all addresses)
- Typed lookups use `Resolver::lookup()` with the `RecordType` from `parse_record_type()`
- Resolvers are built with `Resolver::builder_with_config()` and `TokioRuntimeProvider`. The default configuration is read from the system (`read_system_conf()`), falling back to Google DNS
- Hickory's built-in cache honours record TTLs; its size is set through `ResolverOpts::cache_size`
- Errors return NULL rather than propagating exceptions

### Dependencies
- `duckdb-neo` (`~0.20000.0`) with `build-extension`, default features disabled; it pulls in `libduckdb-sys` 1.20000.0 with v2 bindings
- `duckdb-loadable-macros` (`~1.20000.0`) with `neo` for the v2 entrypoint macro
- The tilde requirements keep Rust patch updates within the same encoded DuckDB release
- `tokio` (v1.52) with "rt", "net", "macros", "rt-multi-thread", and "sync" features (for async DNS resolution)
- `hickory-resolver` (v0.26) for DNS lookups (successor to `trust-dns-resolver`)
- `hickory-proto` (v0.26) for DNS protocol types such as `RecordType`
- `futures` (v0.3) for `join_all`
- `once_cell` (v1.21) for the lazily initialized global state
- `arc-swap` (v1.9) for lock-free swapping of the resolver, semaphore and settings

### Configuration
- **DuckDB target version**: v2.0.0 (`TARGET_DUCKDB_VERSION` and `DUCKDB_TEST_VERSION` in Makefile)
- **Uses unstable C API**: Yes (`USE_UNSTABLE_C_API=1`); v2 entrypoint with `C_STRUCT_UNSTABLE` metadata
- **Extension name**: "dns" (Makefile and Cargo.toml)
- **Rust edition**: 2021 (toolchain 1.88 or newer for `duckdb-neo`)
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

The project uses DuckDB's extension-ci-tools (`v2.0-cyanoptera` branch, DuckDB v2.0.0) via GitHub Actions. The main distribution pipeline builds binaries for multiple platforms (excluding wasm_mvp, wasm_eh, wasm_threads, and linux_amd64_musl).

The workflow is defined in `.github/workflows/MainDistributionPipeline.yml`.

## Known Issues

- Extensions may fail to load on Windows with Python 3.11 (use Python 3.12)
- Forward lookups without a record type return only IPv4 addresses; IPv6 addresses are filtered out (use record type `AAAA`)

## Pull Requests

Never merge PRs or push to `main` directly. Open a PR and request review from the other maintainer; see [AGENTS.md](AGENTS.md).
