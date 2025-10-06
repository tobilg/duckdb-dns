# DNS Extension for DuckDB

A DuckDB extension for performing DNS lookups and reverse DNS lookups, written in pure Rust using the DuckDB C Extension API.

## Features

- **Forward DNS Lookup**: Resolve hostnames to their first IPv4 address or all addresses
- **Reverse DNS Lookup**: Resolve IPv4 addresses to hostnames
- **Scalar Functions**: Functions return single values or arrays and can be used in expressions
- **Pure Rust Implementation**: No DuckDB build or C++ code required
- **Efficient DNS Resolution**: Uses trust-dns-resolver for DNS queries
- **NULL-safe**: Returns NULL on errors instead of throwing exceptions
- **CI/CD Pipeline**: Pre-configured build pipeline for multiple platforms

## Functions

### `dns_lookup(hostname)`

Performs a forward DNS lookup to resolve a hostname to its first IPv4 address.

**Parameters:**
- `hostname` (VARCHAR): The hostname to resolve

**Returns:** VARCHAR - The first resolved IPv4 address, or NULL on error

**Example:**
```sql
SELECT dns_lookup('google.com');
-- Returns: 142.250.181.206 (or similar)
```

### `dns_lookup_all(hostname)`

Performs a forward DNS lookup to resolve a hostname to all its IPv4 addresses.

**Parameters:**
- `hostname` (VARCHAR): The hostname to resolve

**Returns:** VARCHAR[] - An array of all resolved IPv4 addresses, or NULL on error

**Example:**
```sql
SELECT dns_lookup_all('cloudflare.com');
-- Returns: [104.16.132.229, 104.16.133.229] (or similar)

-- Unnest to get individual IPs
SELECT unnest(dns_lookup_all('google.com')) as ip;
```

### `reverse_dns_lookup(ip_address)`

Performs a reverse DNS lookup to resolve an IPv4 address to a hostname.

**Parameters:**
- `ip_address` (VARCHAR): The IPv4 address to resolve (must be valid IPv4 format)

**Returns:** VARCHAR - The resolved hostname, or NULL on error

**Example:**
```sql
SELECT reverse_dns_lookup('8.8.8.8');
-- Returns: dns.google
```

## Installation

### Building from Source

#### Prerequisites

- Rust toolchain
- Python 3
- Python 3 venv
- Make
- Git

Installation varies by platform:
- **Linux**: Usually pre-installed or available through package manager
- **macOS**: Install via [Homebrew](https://formulae.brew.sh/)
- **Windows**: Install via [Chocolatey](https://community.chocolatey.org/)

#### Cloning

Clone the repository with submodules:

```shell
git clone --recurse-submodules https://github.com/tobilg/duckdb-dns.git
cd duckdb-dns
```

#### Building

1. Configure the build environment:
```shell
make configure
```

2. Build the extension:
```shell
make debug    # For debug build
make release  # For release build
```

The extension will be output to:
- Debug: `build/debug/extension/dns/dns.duckdb_extension`
- Release: `build/release/extension/dns/dns.duckdb_extension`

### Running the Extension

Start DuckDB with the `-unsigned` flag to load local extensions:

```shell
duckdb -unsigned
```

Load the extension:

```sql
LOAD './build/debug/extension/dns/dns.duckdb_extension';
```

## Usage Examples

### Basic DNS Lookup

```sql
-- Look up IP for a domain
SELECT dns_lookup('github.com') as ip;

-- Use in WHERE clause
SELECT * FROM users WHERE ip_address = dns_lookup('example.com');
```

### Basic Reverse DNS Lookup

```sql
-- Look up hostname for an IP
SELECT reverse_dns_lookup('1.1.1.1') as hostname;

-- Check if hostname matches
SELECT reverse_dns_lookup('8.8.8.8') = 'dns.google' as is_google_dns;
```

### Advanced Queries

```sql
-- Look up multiple domains
SELECT
    'google.com' as domain,
    dns_lookup('google.com') as ip
UNION ALL
SELECT
    'cloudflare.com' as domain,
    dns_lookup('cloudflare.com') as ip;

-- Get all IPs for multiple domains
SELECT
    domain,
    dns_lookup_all(domain) as all_ips,
    len(dns_lookup_all(domain)) as ip_count
FROM (VALUES ('google.com'), ('cloudflare.com'), ('github.com')) AS domains(domain);

-- Unnest all IPs from multiple domains
SELECT
    domain,
    unnest(dns_lookup_all(domain)) as ip
FROM (VALUES ('google.com'), ('cloudflare.com')) AS domains(domain);

-- Look up multiple IPs with table
SELECT
    ip,
    reverse_dns_lookup(ip) as hostname
FROM (VALUES
    ('8.8.8.8'),
    ('1.1.1.1'),
    ('208.67.222.222')
) AS ips(ip);

-- Filter NULL results (failed lookups)
SELECT
    ip,
    reverse_dns_lookup(ip) as hostname
FROM (VALUES ('8.8.8.8'), ('999.999.999.999')) AS ips(ip)
WHERE reverse_dns_lookup(ip) IS NOT NULL;

-- Use in computed columns
SELECT
    server_name,
    ip_address,
    reverse_dns_lookup(ip_address) as hostname
FROM servers
WHERE dns_lookup(server_name) = ip_address;

-- Check if specific IP is in DNS results
SELECT
    domain,
    list_contains(dns_lookup_all(domain), '142.250.181.206') as has_google_ip
FROM (VALUES ('google.com')) AS domains(domain);
```

## Testing

The extension uses SQLLogicTest format for testing with the DuckDB Python client.

Run tests with:

```shell
make test_debug    # Test debug build
make test_release  # Test release build
```

### Testing with Different DuckDB Versions

```shell
make clean_all
DUCKDB_TEST_VERSION=v1.3.2 make configure
make debug
make test_debug
```

## Development

### Project Structure

- `src/lib.rs`: Main extension code with DNS lookup implementations
- `test/sql/dns.test`: SQLLogicTest test suite
- `Makefile`: Build configuration
- `Cargo.toml`: Rust dependencies and package configuration

### Architecture

The extension implements three scalar functions using the `VScalar` trait:

1. **ReverseDnsLookup**: Reverse DNS lookup (IP → hostname)
2. **DnsLookup**: Forward DNS lookup (hostname → first IP)
3. **DnsLookupAll**: Forward DNS lookup (hostname → all IPs as array)

All functions:
- Implement the `VScalar` trait for vectorized processing
- Validate input formats (IPv4 for reverse lookup)
- Use trust-dns-resolver for DNS queries
- Return VARCHAR or VARCHAR[] results, or NULL on error
- Process inputs in batches for efficiency
- Handle NULL inputs gracefully

### Configuration

- **Extension Name**: `dns`
- **DuckDB Version**: v1.4.0 (defined in Makefile)
- **Rust Edition**: 2021
- **Uses Unstable C API**: Yes (`USE_UNSTABLE_C_API=1` in Makefile)
- **Library Type**: cdylib (native dynamic library)

## Known Issues

- Extensions may fail to load on Windows with Python 3.11 (use Python 3.12)
- Only IPv4 addresses are supported (IPv6 is filtered out)
- `dns_lookup()` returns only the first IPv4 address found (not all addresses)

## CI/CD

The project uses DuckDB's extension-ci-tools (v1.4.0) for automated builds across multiple platforms via GitHub Actions.

## License

This project follows the DuckDB extension template structure.

## Contributing

Contributions are welcome! Please ensure all tests pass before submitting pull requests.
