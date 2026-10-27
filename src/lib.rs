use arc_swap::ArcSwap;
use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::{
    bind_arguments::BindArgument,
    connection::Context,
    data_chunk::{DataChunkRef, VectorCollection},
    enums::{FunctionProperty, FunctionPropertyStability},
    extension::Extension,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    table_function::{
        BindFunctionHandle, ExecColumnInfo, InitColumnData, TableFunctionBuilder,
        TableFunctionCallbacks,
    },
    types::List,
    vector::{Unknown, Vector},
    DuckDBType, Result,
};
use hickory_proto::rr::RecordType;
use hickory_resolver::config::*;
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::Resolver;
use once_cell::sync::Lazy;
use std::{error::Error, net::IpAddr, str::FromStr, sync::Arc};

/// Global DNS resolver state shared across all function invocations
///
/// This allows all DNS functions to share the same resolver instance,
/// enabling DNS query caching and dynamic configuration updates.
static GLOBAL_DNS_STATE: Lazy<DnsResolverState> = Lazy::new(|| DnsResolverState::default());

/// Shared state for DNS resolver functions
///
/// Contains a long-lived Tokio runtime and DNS resolver that are reused
/// across multiple function invocations to enable caching of DNS queries.
/// The resolver is wrapped in an ArcSwap for lock-free reads with atomic updates.
/// The concurrency semaphore limits the number of concurrent DNS requests.
/// The cache size determines how many DNS query results are cached.
struct DnsResolverState {
    runtime: tokio::runtime::Runtime,
    resolver: ArcSwap<Resolver<TokioRuntimeProvider>>,
    current_config: ArcSwap<ResolverConfig>,
    current_opts: ArcSwap<ResolverOpts>,
    concurrency_semaphore: ArcSwap<Arc<tokio::sync::Semaphore>>,
    cache_size: ArcSwap<u64>,
}

impl Default for DnsResolverState {
    /// Creates a new DnsResolverState with a Tokio runtime and DNS resolver
    fn default() -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

        // Default cache size of 4096 entries
        let cache_size = 4096;
        let (config, mut opts) = Self::default_config_and_options();
        opts.cache_size = cache_size;

        let resolver = ArcSwap::from_pointee(
            Resolver::builder_with_config(config.clone(), TokioRuntimeProvider::default())
                .with_options(opts.clone())
                .build()
                .expect("Failed to create DNS resolver"),
        );
        let concurrency_semaphore =
            ArcSwap::from_pointee(Arc::new(tokio::sync::Semaphore::new(50)));
        let cache_size_atomic = ArcSwap::from_pointee(cache_size);

        DnsResolverState {
            runtime,
            resolver,
            current_config: ArcSwap::from_pointee(config),
            current_opts: ArcSwap::from_pointee(opts),
            concurrency_semaphore,
            cache_size: cache_size_atomic,
        }
    }
}

impl DnsResolverState {
    /// Creates a new DnsResolverState with a Tokio runtime and DNS resolver
    fn new() -> std::result::Result<Self, Box<dyn Error>> {
        Ok(Self::default())
    }

    fn default_config_and_options() -> (ResolverConfig, ResolverOpts) {
        hickory_resolver::system_conf::read_system_conf().unwrap_or_else(|_| {
            (
                ResolverConfig::udp_and_tcp(&GOOGLE),
                ResolverOpts::default(),
            )
        })
    }

    /// Updates the resolver configuration
    ///
    /// Creates a new resolver with the specified configuration and atomically
    /// replaces the existing resolver. This clears the DNS cache.
    /// This operation is lock-free and extremely fast.
    fn update_config(&self, config: ResolverConfig) -> std::result::Result<(), Box<dyn Error>> {
        self.update_config_with_options(config, ResolverOpts::default())
    }

    fn update_default_config(&self) -> std::result::Result<(), Box<dyn Error>> {
        let (config, opts) = Self::default_config_and_options();
        self.update_config_with_options(config, opts)
    }

    fn update_config_with_options(
        &self,
        config: ResolverConfig,
        mut opts: ResolverOpts,
    ) -> std::result::Result<(), Box<dyn Error>> {
        // Use the stored cache size preference
        let cache_size = **self.cache_size.load();
        opts.cache_size = cache_size;

        let new_resolver =
            Resolver::builder_with_config(config.clone(), TokioRuntimeProvider::default())
                .with_options(opts.clone())
                .build()?;

        // Atomic swap - lock-free operation
        self.resolver.store(Arc::new(new_resolver));
        self.current_config.store(Arc::new(config));
        self.current_opts.store(Arc::new(opts));
        Ok(())
    }

    /// Updates the concurrency limit for DNS lookups
    ///
    /// Creates a new semaphore with the specified limit and atomically
    /// replaces the existing semaphore. This operation is lock-free and extremely fast.
    fn set_dns_concurrency_limit(&self, limit: usize) -> std::result::Result<(), Box<dyn Error>> {
        if limit == 0 {
            return Err("Concurrency limit must be greater than 0".into());
        }
        let new_semaphore = Arc::new(tokio::sync::Semaphore::new(limit));
        // Atomic swap - lock-free operation
        self.concurrency_semaphore.store(Arc::new(new_semaphore));
        Ok(())
    }

    /// Updates the DNS cache size
    ///
    /// Creates a new resolver with the specified cache size and atomically
    /// replaces the existing resolver. This clears the DNS cache.
    /// This operation is lock-free and extremely fast.
    fn set_dns_cache_size(&self, size: u64) -> std::result::Result<(), Box<dyn Error>> {
        if size == 0 {
            return Err("Cache size must be greater than 0".into());
        }

        // Store the new cache size preference
        self.cache_size.store(Arc::new(size));

        // Get the current config and rebuild resolver with new cache size
        let current_config = self.current_config.load_full();
        let mut opts = self.current_opts.load().as_ref().clone();
        opts.cache_size = size;

        let new_resolver = Resolver::builder_with_config(
            current_config.as_ref().clone(),
            TokioRuntimeProvider::default(),
        )
        .with_options(opts.clone())
        .build()?;

        // Atomic swap - existing queries continue with old resolver
        self.resolver.store(Arc::new(new_resolver));
        self.current_opts.store(Arc::new(opts));
        Ok(())
    }
}

/// Validates and parses an IPv4 or IPv6 address string
///
/// IPv4-mapped IPv6 addresses (e.g. `::ffff:8.8.8.8`) are converted to their
/// IPv4 form, so they are looked up in `in-addr.arpa` rather than `ip6.arpa`.
///
/// # Arguments
/// * `ip_str` - A string slice containing an IPv4 or IPv6 address
///
/// # Returns
/// * `Ok(IpAddr)` - Successfully parsed IP address
/// * `Err` - Invalid IP address format
fn validate_ip(ip_str: &str) -> std::result::Result<IpAddr, Box<dyn Error>> {
    match IpAddr::from_str(ip_str.trim()) {
        Ok(addr) => Ok(addr.to_canonical()),
        Err(_) => Err(format!("Invalid IP address format: {}", ip_str).into()),
    }
}

/// Performs an asynchronous reverse DNS lookup for an IPv4 or IPv6 address
///
/// Hickory builds the PTR query name (`in-addr.arpa` or `ip6.arpa`) from the address.
///
/// # Arguments
/// * `resolver` - Reference to the ArcSwap-wrapped Hickory DNS resolver
/// * `ip_str` - String containing the IPv4 or IPv6 address to resolve
///
/// # Returns
/// * `Ok(String)` - The resolved hostname
/// * `Err` - Lookup failed or invalid IP address
async fn reverse_dns_lookup_async(
    resolver: &ArcSwap<Resolver<TokioRuntimeProvider>>,
    ip_str: &str,
) -> std::result::Result<String, Box<dyn Error>> {
    let ip_addr = validate_ip(ip_str)?;

    // Lock-free load of the current resolver
    let resolver_guard = resolver.load();

    match resolver_guard.reverse_lookup(ip_addr).await {
        Ok(lookup) => {
            // Get the first hostname from the lookup result
            if let Some(record) = lookup.answers().first() {
                Ok(record.data.to_string().trim_end_matches('.').to_string())
            } else {
                Err("No hostname found for IP address".into())
            }
        }
        Err(e) => Err(format!("Reverse DNS lookup failed: {}", e).into()),
    }
}

/// Performs an asynchronous forward DNS lookup, returning the first IPv4 address
///
/// # Arguments
/// * `resolver` - Reference to the ArcSwap-wrapped Hickory DNS resolver
/// * `hostname` - String containing the hostname to resolve
///
/// # Returns
/// * `Ok(String)` - The first IPv4 address found
/// * `Err` - No IPv4 addresses found or lookup failed
async fn dns_lookup_async(
    resolver: &ArcSwap<Resolver<TokioRuntimeProvider>>,
    hostname: &str,
) -> std::result::Result<String, Box<dyn Error>> {
    let hostname = hostname.trim();

    // Lock-free load of the current resolver
    let resolver_guard = resolver.load();

    match resolver_guard.lookup_ip(hostname).await {
        Ok(lookup) => {
            // Find the first IPv4 address
            for ip in lookup.iter() {
                if let IpAddr::V4(ipv4) = ip {
                    return Ok(ipv4.to_string());
                }
            }
            Err("No IPv4 addresses found for hostname".into())
        }
        Err(e) => Err(format!("DNS lookup failed: {}", e).into()),
    }
}

/// Performs an asynchronous forward DNS lookup, returning all IPv4 addresses
///
/// # Arguments
/// * `resolver` - Reference to the RwLock-wrapped Hickory DNS resolver
/// * `hostname` - String containing the hostname to resolve
///
/// # Returns
/// * `Ok(Vec<String>)` - All IPv4 addresses found
/// * `Err` - No IPv4 addresses found or lookup failed
async fn dns_lookup_all_async(
    resolver: &ArcSwap<Resolver<TokioRuntimeProvider>>,
    hostname: &str,
) -> std::result::Result<Vec<String>, Box<dyn Error>> {
    let hostname = hostname.trim();

    // Lock-free load of the current resolver
    let resolver_guard = resolver.load();

    match resolver_guard.lookup_ip(hostname).await {
        Ok(lookup) => {
            let ips: Vec<String> = lookup
                .iter()
                .filter_map(|ip| {
                    // Only return IPv4 addresses
                    if let IpAddr::V4(ipv4) = ip {
                        Some(ipv4.to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if ips.is_empty() {
                Err("No IPv4 addresses found for hostname".into())
            } else {
                Ok(ips)
            }
        }
        Err(e) => Err(format!("DNS lookup failed: {}", e).into()),
    }
}

/// Parses a DNS record type string into a RecordType enum
///
/// # Arguments
/// * `record_type_str` - String containing the record type (case-insensitive)
///
/// # Supported Types
/// A, AAAA, CNAME, MX, NS, PTR, SOA, SRV, TXT, CAA
///
/// # Returns
/// * `Ok(RecordType)` - Successfully parsed record type
/// * `Err` - Unsupported or invalid record type
fn parse_record_type(record_type_str: &str) -> std::result::Result<RecordType, Box<dyn Error>> {
    let record_type_upper = record_type_str.trim().to_uppercase();

    match record_type_upper.as_str() {
        "A" => Ok(RecordType::A),
        "AAAA" => Ok(RecordType::AAAA),
        "CNAME" => Ok(RecordType::CNAME),
        "MX" => Ok(RecordType::MX),
        "NS" => Ok(RecordType::NS),
        "PTR" => Ok(RecordType::PTR),
        "SOA" => Ok(RecordType::SOA),
        "SRV" => Ok(RecordType::SRV),
        "TXT" => Ok(RecordType::TXT),
        "CAA" => Ok(RecordType::CAA),
        _ => Err(format!("Unsupported record type: {}", record_type_str).into()),
    }
}

/// Performs an asynchronous DNS lookup for a specific record type, returning the first record
///
/// # Arguments
/// * `resolver` - Reference to the RwLock-wrapped Hickory DNS resolver
/// * `hostname` - String containing the hostname to query
/// * `record_type` - The DNS record type to query for
///
/// # Returns
/// * `Ok(String)` - The first record of the specified type
/// * `Err` - No records found or lookup failed
async fn dns_lookup_with_type_async(
    resolver: &ArcSwap<Resolver<TokioRuntimeProvider>>,
    hostname: &str,
    record_type: RecordType,
) -> std::result::Result<String, Box<dyn Error>> {
    let hostname = hostname.trim();

    // Lock-free load of the current resolver
    let resolver_guard = resolver.load();

    match resolver_guard.lookup(hostname, record_type).await {
        Ok(lookup) => {
            if let Some(record) = lookup.answers().first() {
                Ok(record.data.to_string())
            } else {
                Err(format!("No {} records found for hostname", record_type).into())
            }
        }
        Err(e) => Err(format!("DNS lookup failed: {}", e).into()),
    }
}

/// Performs an asynchronous DNS lookup for a specific record type, returning all records
///
/// # Arguments
/// * `resolver` - Reference to the RwLock-wrapped Hickory DNS resolver
/// * `hostname` - String containing the hostname to query
/// * `record_type` - The DNS record type to query for
///
/// # Returns
/// * `Ok(Vec<String>)` - All records of the specified type
/// * `Err` - No records found or lookup failed
async fn dns_lookup_all_with_type_async(
    resolver: &ArcSwap<Resolver<TokioRuntimeProvider>>,
    hostname: &str,
    record_type: RecordType,
) -> std::result::Result<Vec<String>, Box<dyn Error>> {
    let hostname = hostname.trim();

    // Lock-free load of the current resolver
    let resolver_guard = resolver.load();

    match resolver_guard.lookup(hostname, record_type).await {
        Ok(lookup) => {
            let records: Vec<String> = lookup
                .answers()
                .iter()
                .map(|record| record.data.to_string())
                .collect();

            if records.is_empty() {
                Err(format!("No {} records found for hostname", record_type).into())
            } else {
                Ok(records)
            }
        }
        Err(e) => Err(format!("DNS lookup failed: {}", e).into()),
    }
}

/// Reverse DNS lookup scalar function
///
/// Performs reverse DNS lookups by converting IPv4 or IPv6 addresses to hostnames.
///
/// # Arguments
/// * `ip_address` - A VARCHAR containing an IPv4 or IPv6 address (e.g., "8.8.8.8" or "2001:4860:4860::8888")
///
/// # Returns
/// * VARCHAR - The resolved hostname (e.g., "dns.google"), or NULL on error
///
/// # Example
/// ```sql
/// SELECT reverse_dns_lookup('8.8.8.8') as hostname;
/// -- Returns: dns.google
/// SELECT reverse_dns_lookup('2001:4860:4860::8888') as hostname;
/// -- Returns: dns.google
/// ```
struct ReverseDnsLookup;

impl ScalarCallbacks for ReverseDnsLookup {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let values = input.get_vector_at::<String>(0)?.reader()?;
        let resolver = &GLOBAL_DNS_STATE.resolver;
        let semaphore = GLOBAL_DNS_STATE.concurrency_semaphore.load_full();
        let futures = values.iter().map(|ip_address| {
            let semaphore = &semaphore;
            async move {
                let ip_address = ip_address?;
                let _permit = semaphore.acquire().await.ok()?;
                reverse_dns_lookup_async(resolver, ip_address).await.ok()
            }
        });
        let results = GLOBAL_DNS_STATE
            .runtime
            .block_on(futures::future::join_all(futures));

        let mut output = output.cast::<String>()?;
        output.set_size(input.row_count()?)?;
        let mut writer = output.writer()?;
        for (i, result) in results.iter().enumerate() {
            writer.set(i, result.as_deref())?;
        }
        Ok(())
    }
}

/// Forward DNS lookup scalar function
///
/// Performs DNS lookups to resolve hostnames to IP addresses or other DNS record types.
///
/// # Arguments
/// * `hostname` - A VARCHAR containing the hostname to resolve (e.g., "google.com")
/// * `record_type` - Optional VARCHAR specifying the DNS record type (e.g., "A", "MX", "TXT", "CNAME")
///
/// # Returns
/// * VARCHAR - The first resolved record:
///   - Without record_type: First IPv4 address
///   - With record_type: First record of specified type
///   - Returns NULL on error
///
/// # Supported Record Types
/// A, AAAA, CNAME, MX, NS, PTR, SOA, SRV, TXT, CAA
///
/// # Examples
/// ```sql
/// -- Get first IPv4 address
/// SELECT dns_lookup('google.com') as ip;
/// -- Returns: 142.251.209.142
///
/// -- Get TXT record
/// SELECT dns_lookup('_dmarc.google.com', 'TXT') as txt;
/// -- Returns: v=DMARC1; p=reject; ...
///
/// -- Get MX record
/// SELECT dns_lookup('google.com', 'MX') as mx;
/// -- Returns: 10 smtp.google.com.
/// ```
struct DnsLookup;

impl ScalarCallbacks for DnsLookup {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let hostnames = input.get_vector_at::<String>(0)?.reader()?;
        let record_types = if input.col_count() > 1 {
            Some(input.get_vector_at::<String>(1)?.reader()?)
        } else {
            None
        };
        let resolver = &GLOBAL_DNS_STATE.resolver;
        let semaphore = GLOBAL_DNS_STATE.concurrency_semaphore.load_full();
        let futures = hostnames.iter().enumerate().map(|(i, hostname)| {
            let semaphore = &semaphore;
            async move {
                let hostname = hostname?;
                let _permit = semaphore.acquire().await.ok()?;
                if let Some(record_types) = record_types {
                    let record_type = parse_record_type(record_types.get(i).ok()??).ok()?;
                    dns_lookup_with_type_async(resolver, hostname, record_type)
                        .await
                        .ok()
                } else {
                    dns_lookup_async(resolver, hostname).await.ok()
                }
            }
        });
        let results = GLOBAL_DNS_STATE
            .runtime
            .block_on(futures::future::join_all(futures));

        let mut output = output.cast::<String>()?;
        output.set_size(input.row_count()?)?;
        let mut writer = output.writer()?;
        for (i, result) in results.iter().enumerate() {
            writer.set(i, result.as_deref())?;
        }
        Ok(())
    }
}

/// Forward DNS lookup scalar function (returns all records)
///
/// Performs DNS lookups to resolve hostnames to all matching IP addresses or DNS records.
///
/// # Arguments
/// * `hostname` - A VARCHAR containing the hostname to resolve (e.g., "google.com")
/// * `record_type` - Optional VARCHAR specifying the DNS record type (e.g., "A", "MX", "TXT", "NS")
///
/// # Returns
/// * VARCHAR[] - An array of all resolved records:
///   - Without record_type: All IPv4 addresses
///   - With record_type: All records of specified type
///   - Returns NULL on error
///
/// # Supported Record Types
/// A, AAAA, CNAME, MX, NS, PTR, SOA, SRV, TXT, CAA
///
/// # Examples
/// ```sql
/// -- Get all IPv4 addresses
/// SELECT dns_lookup_all('google.com') as ips;
/// -- Returns: [142.251.209.142, ...]
///
/// -- Get all MX records
/// SELECT dns_lookup_all('google.com', 'MX') as mx_records;
/// -- Returns: [10 smtp.google.com., ...]
///
/// -- Get all TXT records
/// SELECT dns_lookup_all('google.com', 'TXT') as txt_records;
/// -- Returns: [v=spf1 include:_spf.google.com ~all, ...]
/// ```
struct DnsLookupAll;

impl ScalarCallbacks for DnsLookupAll {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let hostnames = input.get_vector_at::<String>(0)?.reader()?;
        let record_types = if input.col_count() > 1 {
            Some(input.get_vector_at::<String>(1)?.reader()?)
        } else {
            None
        };
        let resolver = &GLOBAL_DNS_STATE.resolver;
        let semaphore = GLOBAL_DNS_STATE.concurrency_semaphore.load_full();
        let futures = hostnames.iter().enumerate().map(|(i, hostname)| {
            let semaphore = &semaphore;
            async move {
                let hostname = hostname?;
                let _permit = semaphore.acquire().await.ok()?;
                if let Some(record_types) = record_types {
                    let record_type = parse_record_type(record_types.get(i).ok()??).ok()?;
                    dns_lookup_all_with_type_async(resolver, hostname, record_type)
                        .await
                        .ok()
                } else {
                    dns_lookup_all_async(resolver, hostname).await.ok()
                }
            }
        });
        let results = GLOBAL_DNS_STATE
            .runtime
            .block_on(futures::future::join_all(futures));

        let mut output = output.cast::<List<String>>()?;
        output.set_size(input.row_count()?)?;
        let mut writer = output.writer()?;
        for (i, result) in results.iter().enumerate() {
            writer.set(
                i,
                result
                    .as_ref()
                    .map(|records| records.iter().map(|record| Some(record.as_str())).collect()),
            )?;
        }
        Ok(())
    }
}

/// Configuration update scalar function
///
/// Updates the DNS resolver configuration for all subsequent DNS queries.
/// Supported preset configurations: 'default', 'google', 'cloudflare', 'quad9'
///
/// # Arguments
/// * `preset` - A VARCHAR containing the configuration preset name
///
/// # Returns
/// * VARCHAR - Success message or error description
///
/// # Examples
/// ```sql
/// -- Use Google DNS servers (8.8.8.8, 8.8.4.4)
/// SELECT set_dns_config('google');
///
/// -- Use Cloudflare DNS servers (1.1.1.1, 1.0.0.1)
/// SELECT set_dns_config('cloudflare');
///
/// -- Use Quad9 DNS servers (9.9.9.9, 149.112.112.112)
/// SELECT set_dns_config('quad9');
///
/// -- Use default system configuration
/// SELECT set_dns_config('default');
/// ```
///
/// # Note
/// Changing configuration clears the DNS cache.
struct SetDnsConfig;

impl ScalarCallbacks for SetDnsConfig {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let values = input.get_vector_at::<String>(0)?.reader()?;
        let mut output = output.cast::<String>()?;
        output.set_size(input.row_count()?)?;
        let mut writer = output.writer()?;

        for (i, value) in values.iter().enumerate() {
            let Some(value) = value else {
                writer.set(i, None)?;
                continue;
            };
            let preset = value.trim().to_lowercase();
            let update_result = match preset.as_str() {
                "default" => GLOBAL_DNS_STATE.update_default_config(),
                "google" => GLOBAL_DNS_STATE.update_config(ResolverConfig::udp_and_tcp(&GOOGLE)),
                "cloudflare" => {
                    GLOBAL_DNS_STATE.update_config(ResolverConfig::udp_and_tcp(&CLOUDFLARE))
                }
                "quad9" => GLOBAL_DNS_STATE.update_config(ResolverConfig::udp_and_tcp(&QUAD9)),
                _ => {
                    let message = format!("Unknown preset '{}'. Supported presets: default, google, cloudflare, quad9", preset);
                    writer.set(i, Some(&message))?;
                    continue;
                }
            };
            let message = match update_result {
                Ok(()) => format!("DNS configuration updated to '{}'", preset),
                Err(e) => format!("Failed to update DNS configuration: {}", e),
            };
            writer.set(i, Some(&message))?;
        }
        Ok(())
    }
}

/// Concurrency limit configuration scalar function
///
/// Updates the concurrency limit for DNS lookup operations to prevent TCP connection exhaustion.
///
/// # Arguments
/// * `limit` - A BIGINT specifying the maximum number of concurrent DNS requests (must be > 0)
///
/// # Returns
/// * VARCHAR - Success message or error description
///
/// # Examples
/// ```sql
/// -- Set concurrency limit to 100
/// SELECT set_dns_concurrency_limit(100);
/// -- Returns: Concurrency limit updated to 100
///
/// -- Set concurrency limit to 500 for high-throughput scenarios
/// SELECT set_dns_concurrency_limit(500);
/// -- Returns: Concurrency limit updated to 500
///
/// -- Reset to default (50)
/// SELECT set_dns_concurrency_limit(50);
/// -- Returns: Concurrency limit updated to 50
///
/// -- Invalid limit returns error
/// SELECT set_dns_concurrency_limit(0);
/// -- Returns: Concurrency limit must be greater than 0
/// ```
///
/// # Note
/// This setting applies globally to all DNS lookup operations and takes effect immediately.
struct SetConcurrencyLimit;

impl ScalarCallbacks for SetConcurrencyLimit {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let values = input.get_vector_at::<i64>(0)?.reader()?;
        let mut output = output.cast::<String>()?;
        output.set_size(input.row_count()?)?;
        let mut writer = output.writer()?;

        for (i, value) in values.iter().enumerate() {
            let Some(&limit) = value else {
                writer.set(i, None)?;
                continue;
            };
            let message = if limit <= 0 {
                "Concurrency limit must be greater than 0".to_string()
            } else {
                match GLOBAL_DNS_STATE.set_dns_concurrency_limit(limit as usize) {
                    Ok(()) => format!("Concurrency limit updated to {}", limit),
                    Err(e) => format!("Failed to update concurrency limit: {}", e),
                }
            };
            writer.set(i, Some(&message))?;
        }
        Ok(())
    }
}

/// Cache size configuration scalar function
///
/// Updates the DNS cache size for the resolver. The cache stores DNS query results
/// to improve performance by avoiding repeated lookups for the same queries.
///
/// # Arguments
/// * `size` - A BIGINT specifying the maximum number of cached DNS queries (must be > 0)
///
/// # Returns
/// * VARCHAR - Success message or error description
///
/// # Examples
/// ```sql
/// -- Set cache size to 4096 (default)
/// SELECT set_dns_cache_size(4096);
/// -- Returns: DNS cache size updated to 4096
///
/// -- Set cache size to 8192 for larger workloads
/// SELECT set_dns_cache_size(8192);
/// -- Returns: DNS cache size updated to 8192
/// ```
///
/// # Note
/// Changing the cache size rebuilds the resolver and clears the existing cache.
/// This operation takes effect immediately for all subsequent DNS queries.
struct SetDnsCacheSize;

impl ScalarCallbacks for SetDnsCacheSize {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let values = input.get_vector_at::<i64>(0)?.reader()?;
        let mut output = output.cast::<String>()?;
        output.set_size(input.row_count()?)?;
        let mut writer = output.writer()?;

        for (i, value) in values.iter().enumerate() {
            let Some(&size) = value else {
                writer.set(i, None)?;
                continue;
            };
            let message = if size <= 0 {
                "Cache size must be greater than 0".to_string()
            } else {
                match GLOBAL_DNS_STATE.set_dns_cache_size(size as u64) {
                    Ok(()) => format!("DNS cache size updated to {}", size),
                    Err(e) => format!("Failed to update cache size: {}", e),
                }
            };
            writer.set(i, Some(&message))?;
        }
        Ok(())
    }
}

/// Table function for querying TXT DNS records
///
/// Returns all TXT records for a given hostname as a table with one row per record.
///
/// # Arguments
/// * `hostname` - A VARCHAR containing the hostname to query (e.g., "google.com", "_dmarc.example.com")
///
/// # Returns
/// A table with a single column:
/// * `txt_record` (VARCHAR) - Each TXT record as a separate row
///
/// # Examples
/// ```sql
/// -- Get all TXT records for google.com
/// SELECT * FROM corey('google.com');
/// -- Returns a table with multiple rows, one per TXT record
///
/// -- Filter TXT records
/// SELECT * FROM corey('google.com') WHERE txt_record LIKE '%spf%';
///
/// -- Count TXT records
/// SELECT COUNT(*) as record_count FROM corey('google.com');
///
/// -- Join with other tables
/// SELECT d.domain, c.txt_record
/// FROM domains d
/// CROSS JOIN corey(d.domain_name) c;
/// ```
///
/// # Notes
/// - Reports TXT lookup failures during binding
/// - Performs the DNS lookup during the bind phase for efficiency
/// - All records are fetched at once and then returned in chunks
struct CoreyBindData {
    txt_records: Vec<String>,
}

struct Corey;

impl TableFunctionCallbacks for Corey {
    type BindData = CoreyBindData;
    type LocalState = usize;
    type GlobalState = ();

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData> {
        bind_handle.add_result_column("txt_record", &String::logical_type(context)?)?;
        let hostname = arguments[0]
            .value
            .as_ref()
            .ok_or_else(|| duckdb_neo::Error::invalid_input("hostname must be constant"))?
            .get::<String>()?
            .ok_or_else(|| duckdb_neo::Error::invalid_input("hostname must not be NULL"))?;
        let resolver_state =
            DnsResolverState::new().map_err(|e| duckdb_neo::Error::invalid_input(e.to_string()))?;
        let txt_records = resolver_state.runtime.block_on(async {
            let resolver = resolver_state.resolver.load();
            resolver
                .lookup(hostname.trim(), RecordType::TXT)
                .await
                .map(|lookup| {
                    lookup
                        .answers()
                        .iter()
                        .map(|record| record.data.to_string())
                        .collect()
                })
                .map_err(|e| duckdb_neo::Error::invalid_input(format!("TXT lookup failed: {}", e)))
        })?;
        Ok(CoreyBindData { txt_records })
    }

    fn init_global_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        // One worker owns the cursor so each TXT record is emitted exactly once.
        Ok((None, Some(1)))
    }

    fn init_local_state(
        &self,
        _bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<Option<Self::LocalState>> {
        Ok(Some(0))
    }

    fn exec(
        &self,
        bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
        mut output: DataChunkRef<'_>,
        _column_info: ExecColumnInfo<'_>,
    ) -> Result<()> {
        let offset = local_state
            .ok_or_else(|| duckdb_neo::Error::api_error("TXT scan state is not initialized"))?;
        let records = &bind_data.txt_records[*offset..];
        let count = records.len().min(2048);
        let mut vector = output.get_vector_at_mut::<String>(0)?;
        vector.set_size(count)?;
        let mut writer = vector.writer()?;
        for (i, record) in records.iter().take(count).enumerate() {
            writer.set(i, Some(record.as_str()))?;
        }
        *offset += count;
        Ok(())
    }
}

fn register_scalar<T: ScalarCallbacks>(
    extension: &Extension,
    name: &str,
    signature: SignatureBuilder,
    implementation: T,
) -> Result<()> {
    ScalarFunctionBuilder::new(name, signature, implementation)
        // DNS depends on external state, and configuration calls have side effects.
        .set_property(FunctionProperty::Stability(
            FunctionPropertyStability::Volatile,
        ))
        .register(extension)
}

#[duckdb_entrypoint_c_api_v2(extension_name = "dns", api_version = "v2.0.0")]
pub fn extension_entrypoint(extension: &Extension) -> Result<()> {
    register_scalar(
        extension,
        "reverse_dns_lookup",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("ip_address"))
            .return_type(String::logical_type(extension)?),
        ReverseDnsLookup,
    )?;
    for with_record_type in [false, true] {
        let lookup_signature = || {
            let mut signature =
                SignatureBuilder::new().param(FunctionParameter::new::<String>("hostname"));
            if with_record_type {
                signature = signature.param(FunctionParameter::new::<String>("record_type"));
            }
            signature
        };
        register_scalar(
            extension,
            "dns_lookup",
            lookup_signature().return_type(String::logical_type(extension)?),
            DnsLookup,
        )?;
        register_scalar(
            extension,
            "dns_lookup_all",
            lookup_signature().return_type(Vec::<String>::logical_type(extension)?),
            DnsLookupAll,
        )?;
    }
    register_scalar(
        extension,
        "set_dns_config",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("preset"))
            .return_type(String::logical_type(extension)?),
        SetDnsConfig,
    )?;
    register_scalar(
        extension,
        "set_dns_concurrency_limit",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i64>("limit"))
            .return_type(String::logical_type(extension)?),
        SetConcurrencyLimit,
    )?;
    register_scalar(
        extension,
        "set_dns_cache_size",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i64>("size"))
            .return_type(String::logical_type(extension)?),
        SetDnsCacheSize,
    )?;
    TableFunctionBuilder::new(
        "corey",
        SignatureBuilder::new().param(FunctionParameter::new::<String>("hostname")),
        Corey,
    )
    .register(extension)?;
    Ok(())
}
