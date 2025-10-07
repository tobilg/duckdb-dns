# DNS Extension for DuckDB

A DuckDB extension for performing DNS lookups and reverse DNS lookups, written in pure Rust using the DuckDB C Extension API.

## Features

- **Forward DNS Lookup**: Resolve hostnames to IPv4 addresses or other DNS record types
- **Reverse DNS Lookup**: Resolve IPv4 addresses to hostnames
- **Multiple Record Types**: Query A, AAAA, CNAME, MX, NS, PTR, SOA, SRV, TXT, CAA records
- **Configurable DNS Resolver**: Switch between DNS providers (Google, Cloudflare, Quad9) with instant configuration changes
- **Pure Rust Implementation**: No DuckDB build or C++ code required
- **Efficient DNS Resolution**: Uses hickory-resolver with built-in LRU cache and TTL support
- **NULL-safe**: Returns NULL on errors instead of throwing exceptions

## Installation

### From community extensions repository

Load the extension:

```sql
INSTALL dns FROM community;
LOAD dns;
```

## Functions

### `dns_lookup(hostname, [record_type])`

Performs a forward DNS lookup to resolve a hostname to its first IPv4 address, or to the first record of a specified DNS record type.

**Parameters:**
- `hostname` (VARCHAR): The hostname to resolve
- `record_type` (VARCHAR, optional): The DNS record type to query. Supported types: `A`, `AAAA`, `CNAME`, `MX`, `NS`, `PTR`, `SOA`, `SRV`, `TXT`, `CAA`

**Returns:** VARCHAR - The first resolved record (IPv4 address if no record_type specified, or first record of specified type), or NULL on error

**Examples:**
```sql
-- Get first IPv4 address (default behavior)
SELECT dns_lookup('google.com');
-- Returns: 142.250.181.206 (or similar)

-- Get TXT record
SELECT dns_lookup('_dmarc.google.com', 'TXT');
-- Returns: v=DMARC1; p=reject; rua=mailto:mailauth-reports@google.com

-- Get MX record
SELECT dns_lookup('google.com', 'MX');
-- Returns: 10 smtp.google.com.

-- Get CNAME record
SELECT dns_lookup('www.github.com', 'CNAME');
-- Returns: github.com.
```

### `dns_lookup_all(hostname, [record_type])`

Performs a forward DNS lookup to resolve a hostname to all its IPv4 addresses, or to all records of a specified DNS record type.

**Parameters:**
- `hostname` (VARCHAR): The hostname to resolve
- `record_type` (VARCHAR, optional): The DNS record type to query. Supported types: `A`, `AAAA`, `CNAME`, `MX`, `NS`, `PTR`, `SOA`, `SRV`, `TXT`, `CAA`

**Returns:** VARCHAR[] - An array of all resolved records (all IPv4 addresses if no record_type specified, or all records of specified type), or NULL on error

**Examples:**
```sql
-- Get all IPv4 addresses (default behavior)
SELECT dns_lookup_all('cloudflare.com');
-- Returns: [104.16.132.229, 104.16.133.229] (or similar)

-- Get all MX records
SELECT dns_lookup_all('google.com', 'MX');
-- Returns: [10 smtp.google.com.]

-- Get all TXT records
SELECT dns_lookup_all('google.com', 'TXT');
-- Returns: [v=spf1 include:_spf.google.com ~all, google-site-verification=..., ...]

-- Unnest to get individual records
SELECT unnest(dns_lookup_all('google.com', 'TXT')) as txt_record;
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

### `set_dns_config(preset)`

Updates the DNS resolver configuration for all subsequent DNS queries.

**Parameters:**
- `preset` (VARCHAR): The DNS resolver preset to use. Supported presets:
  - `'default'`: System default DNS servers
  - `'google'`: Google Public DNS (8.8.8.8, 8.8.4.4)
  - `'cloudflare'`: Cloudflare DNS (1.1.1.1, 1.0.0.1)
  - `'quad9'`: Quad9 DNS (9.9.9.9, 149.112.112.112)

**Returns:** VARCHAR - A success or error message

**Examples:**
```sql
-- Switch to Google Public DNS
SELECT set_dns_config('google');
-- Returns: DNS configuration updated to 'google'

-- Switch to Cloudflare DNS
SELECT set_dns_config('cloudflare');
-- Returns: DNS configuration updated to 'cloudflare'

-- All subsequent queries use the new configuration
SELECT dns_lookup('example.com');

-- Reset to system default
SELECT set_dns_config('default');
-- Returns: DNS configuration updated to 'default'

-- Invalid preset returns error
SELECT set_dns_config('invalid');
-- Returns: Unknown preset 'invalid'. Supported: default, google, cloudflare, quad9
```

### `corey(hostname)` - Table Function

Queries all TXT records for a hostname and returns them as a table with one row per TXT record. This is useful for advanced filtering, aggregation, and analysis of TXT records.

Finally [Route 53 can be a real database](https://www.lastweekinaws.com/blog/route-53-amazons-premier-database/)!

**Parameters:**
- `hostname` (VARCHAR): The hostname to query for TXT records

**Returns:** A table with a single column:
- `txt_record` (VARCHAR): Each TXT record as a separate row

**Examples:**
```sql
-- Get all TXT records for a domain
SELECT * FROM corey('google.com');

-- Filter TXT records
SELECT * FROM corey('google.com')
WHERE txt_record LIKE '%spf%';

-- Count TXT records
SELECT COUNT(*) as record_count
FROM corey('google.com');

-- Query multiple domains using UNION ALL
SELECT 'google.com' as domain, * FROM corey('google.com')
UNION ALL
SELECT 'github.com' as domain, * FROM corey('github.com')
UNION ALL
SELECT 'cloudflare.com' as domain, * FROM corey('cloudflare.com');

-- Find domains with DMARC records
SELECT '_dmarc.google.com' as domain, * FROM corey('_dmarc.google.com')
WHERE txt_record LIKE 'v=DMARC%'
UNION ALL
SELECT '_dmarc.github.com' as domain, * FROM corey('_dmarc.github.com')
WHERE txt_record LIKE 'v=DMARC%';

-- Alternative: Use dns_lookup_all for dynamic queries with columns
SELECT
    domain,
    dns_lookup_all(domain, 'TXT') as txt_records
FROM (VALUES ('google.com'), ('github.com')) AS domains(domain)
WHERE dns_lookup_all(domain, 'TXT') IS NOT NULL;
```

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

-- Look up MX record
SELECT dns_lookup('gmail.com', 'MX') as mx_server;

-- Look up TXT records
SELECT dns_lookup('_dmarc.google.com', 'TXT') as dmarc_policy;

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

### Working with Multiple Record Types

```sql
-- Get all TXT records for a domain
SELECT dns_lookup_all('google.com', 'TXT') as txt_records;

-- Get all MX records sorted by preference
SELECT unnest(dns_lookup_all('gmail.com', 'MX')) as mx_record;

-- Check for SPF records
SELECT
    domain,
    list_contains(dns_lookup_all(domain, 'TXT'), 'v=spf1') as has_spf
FROM (VALUES ('google.com'), ('github.com')) AS domains(domain);
```

### Using the Corey Table Function

```sql
-- Find SPF records for a specific domain
SELECT * FROM corey('google.com')
WHERE txt_record LIKE 'v=spf%';

-- Query multiple domains with UNION ALL
SELECT 'google.com' as domain, * FROM corey('google.com')
WHERE txt_record LIKE 'v=spf%'
UNION ALL
SELECT 'github.com' as domain, * FROM corey('github.com')
WHERE txt_record LIKE 'v=spf%';

-- Count verification records for a domain
SELECT
    COUNT(*) FILTER (WHERE txt_record LIKE '%verification%') as verification_count
FROM corey('google.com');

-- For dynamic queries with column references, use dns_lookup_all instead
SELECT
    domain,
    dns_lookup_all(domain, 'TXT') as txt_records
FROM (VALUES ('google.com'), ('github.com')) AS domains(domain);
```

### Configuring DNS Resolver

```sql
-- Switch to Google Public DNS for faster resolution
SELECT set_dns_config('google');

-- Use Cloudflare DNS for privacy-focused queries
SELECT set_dns_config('cloudflare');

-- All subsequent queries use the new resolver
SELECT dns_lookup('example.com');
SELECT dns_lookup_all('example.com');
SELECT reverse_dns_lookup('1.1.1.1');
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

The extension implements four scalar functions and one table function:

#### Scalar Functions (using `VScalar` trait):

1. **ReverseDnsLookup**: Reverse DNS lookup (IP → hostname)
2. **DnsLookup**: Forward DNS lookup (hostname → first record)
   - Without record_type: Returns first IPv4 address
   - With record_type: Returns first record of specified type
3. **DnsLookupAll**: Forward DNS lookup (hostname → all records as array)
   - Without record_type: Returns all IPv4 addresses
   - With record_type: Returns all records of specified type
4. **SetDnsConfig**: Update DNS resolver configuration
   - Supports presets: default, google, cloudflare, quad9
   - Uses lock-free atomic operations (arc-swap) for instant changes

#### Table Function (using `VTab` trait):

5. **Corey**: TXT record query (hostname → table of TXT records)
   - Returns a table with one row per TXT record
   - Useful for filtering and aggregation

All functions:
- Implement vectorized processing for efficiency
- Validate input formats (IPv4 for reverse lookup)
- Use hickory-resolver with global cached instance for consistent resolution
- Support multiple DNS record types: A, AAAA, CNAME, MX, NS, PTR, SOA, SRV, TXT, CAA
- Return VARCHAR, VARCHAR[], or table results, or NULL on error
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
- IPv6 addresses are filtered out in default IP lookups (only IPv4 returned)
- `dns_lookup()` without record_type returns only the first IPv4 address found (use `dns_lookup_all()` for all addresses)

## CI/CD

The project uses DuckDB's extension-ci-tools (v1.4.0) for automated builds across multiple platforms via GitHub Actions.

## License

This project follows the DuckDB extension template structure.

## Contributing

Contributions are welcome! Please ensure all tests pass before submitting pull requests.
