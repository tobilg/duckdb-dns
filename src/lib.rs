use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    types::DuckString,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::{arrow::WritableVector, BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use libduckdb_sys::duckdb_string_t;
use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::Arc,
};
use hickory_proto::rr::RecordType;
use hickory_resolver::config::*;
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::Resolver;
use once_cell::sync::Lazy;
use arc_swap::ArcSwap;

/// Global DNS resolver state shared across all function invocations
///
/// This allows all DNS functions to share the same resolver instance,
/// enabling DNS query caching and dynamic configuration updates.
static GLOBAL_DNS_STATE: Lazy<DnsResolverState> = Lazy::new(|| {
    DnsResolverState::default()
});

/// Shared state for DNS resolver functions
///
/// Contains a long-lived Tokio runtime and DNS resolver that are reused
/// across multiple function invocations to enable caching of DNS queries.
/// The resolver is wrapped in an ArcSwap for lock-free reads with atomic updates.
/// The concurrency semaphore limits the number of concurrent DNS requests.
/// The cache size determines how many DNS query results are cached.
struct DnsResolverState {
    runtime: tokio::runtime::Runtime,
    resolver: ArcSwap<Resolver<TokioConnectionProvider>>,
    concurrency_semaphore: ArcSwap<Arc<tokio::sync::Semaphore>>,
    cache_size: ArcSwap<usize>,
}

impl Default for DnsResolverState {
    /// Creates a new DnsResolverState with a Tokio runtime and DNS resolver
    fn default() -> Self {
        let runtime = tokio::runtime::Runtime::new()
            .expect("Failed to create Tokio runtime");
        
        // Default cache size of 4096 entries
        let cache_size = 4096;
        let mut opts = ResolverOpts::default();
        opts.cache_size = cache_size;
        
        let resolver = ArcSwap::from_pointee(
            Resolver::builder_with_config(
                ResolverConfig::default(),
                TokioConnectionProvider::default(),
            )
            .with_options(opts)
            .build()
        );
        let concurrency_semaphore = ArcSwap::from_pointee(
            Arc::new(tokio::sync::Semaphore::new(50))
        );
        let cache_size_atomic = ArcSwap::from_pointee(cache_size);
        
        DnsResolverState { 
            runtime, 
            resolver, 
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

    /// Updates the resolver configuration
    ///
    /// Creates a new resolver with the specified configuration and atomically
    /// replaces the existing resolver. This clears the DNS cache.
    /// This operation is lock-free and extremely fast.
    fn update_config(&self, config: ResolverConfig) -> std::result::Result<(), Box<dyn Error>> {
        // Use the stored cache size preference
        let cache_size = **self.cache_size.load();
        let mut opts = ResolverOpts::default();
        opts.cache_size = cache_size;
        
        let new_resolver = Resolver::builder_with_config(
            config,
            TokioConnectionProvider::default(),
        )
        .with_options(opts)
        .build();

        // Atomic swap - lock-free operation
        self.resolver.store(Arc::new(new_resolver));
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
    fn set_dns_cache_size(&self, size: usize) -> std::result::Result<(), Box<dyn Error>> {
        if size == 0 {
            return Err("Cache size must be greater than 0".into());
        }
        
        // Store the new cache size preference
        self.cache_size.store(Arc::new(size));
        
        // Get the current config and rebuild resolver with new cache size
        let current_config = self.resolver.load().config().clone();
        let mut opts = self.resolver.load().options().clone();
        opts.cache_size = size;
        
        let new_resolver = Resolver::builder_with_config(
            current_config,
            TokioConnectionProvider::default(),
        )
        .with_options(opts)
        .build();
        
        // Atomic swap - existing queries continue with old resolver
        self.resolver.store(Arc::new(new_resolver));
        Ok(())
    }
}

/// Validates and parses an IPv4 address string
///
/// # Arguments
/// * `ip_str` - A string slice containing an IPv4 address
///
/// # Returns
/// * `Ok(Ipv4Addr)` - Successfully parsed IPv4 address
/// * `Err` - Invalid IPv4 address format
fn validate_ipv4(ip_str: &str) -> std::result::Result<Ipv4Addr, Box<dyn Error>> {
    match Ipv4Addr::from_str(ip_str.trim()) {
        Ok(addr) => Ok(addr),
        Err(_) => Err(format!("Invalid IPv4 address format: {}", ip_str).into()),
    }
}

/// Performs an asynchronous reverse DNS lookup for an IPv4 address
///
/// # Arguments
/// * `resolver` - Reference to the ArcSwap-wrapped Hickory DNS resolver
/// * `ip_str` - String containing the IPv4 address to resolve
///
/// # Returns
/// * `Ok(String)` - The resolved hostname
/// * `Err` - Lookup failed or invalid IP address
async fn reverse_dns_lookup_async(
    resolver: &ArcSwap<Resolver<TokioConnectionProvider>>,
    ip_str: &str,
) -> std::result::Result<String, Box<dyn Error>> {
    let ipv4 = validate_ipv4(ip_str)?;
    let ip_addr = IpAddr::V4(ipv4);

    // Lock-free load of the current resolver
    let resolver_guard = resolver.load();

    match resolver_guard.reverse_lookup(ip_addr).await {
        Ok(lookup) => {
            // Get the first hostname from the lookup result
            if let Some(name) = lookup.iter().next() {
                Ok(name.to_string().trim_end_matches('.').to_string())
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
    resolver: &ArcSwap<Resolver<TokioConnectionProvider>>,
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
    resolver: &ArcSwap<Resolver<TokioConnectionProvider>>,
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
    resolver: &ArcSwap<Resolver<TokioConnectionProvider>>,
    hostname: &str,
    record_type: RecordType,
) -> std::result::Result<String, Box<dyn Error>> {
    let hostname = hostname.trim();

    // Lock-free load of the current resolver
    let resolver_guard = resolver.load();

    match resolver_guard.lookup(hostname, record_type).await {
        Ok(lookup) => {
            if let Some(record) = lookup.record_iter().next() {
                Ok(record.data().to_string())
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
    resolver: &ArcSwap<Resolver<TokioConnectionProvider>>,
    hostname: &str,
    record_type: RecordType,
) -> std::result::Result<Vec<String>, Box<dyn Error>> {
    let hostname = hostname.trim();

    // Lock-free load of the current resolver
    let resolver_guard = resolver.load();

    match resolver_guard.lookup(hostname, record_type).await {
        Ok(lookup) => {
            let records: Vec<String> = lookup
                .record_iter()
                .map(|record| record.data().to_string())
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
/// Performs reverse DNS lookups by converting IPv4 addresses to hostnames.
///
/// # Arguments
/// * `ip_address` - A VARCHAR containing an IPv4 address (e.g., "8.8.8.8")
///
/// # Returns
/// * VARCHAR - The resolved hostname (e.g., "dns.google"), or NULL on error
///
/// # Example
/// ```sql
/// SELECT reverse_dns_lookup('8.8.8.8') as hostname;
/// -- Returns: dns.google
/// ```
struct ReverseDnsLookup;

impl VScalar for ReverseDnsLookup {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let mut output_vector = output.flat_vector();

        // Get input strings
        let values = input_vector.as_slice_with_len::<duckdb_string_t>(size);
        let strings: Vec<String> = values
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string())
            .collect();

        // Use the global resolver state - load once for all lookups
        let resolver = &GLOBAL_DNS_STATE.resolver;
        let semaphore = GLOBAL_DNS_STATE.concurrency_semaphore.load();

        // Process all lookups concurrently with semaphore-controlled execution
        let futures: Vec<_> = strings
            .iter()
            .enumerate()
            .map(|(i, ip_address)| {
                let is_null = input_vector.row_is_null(i as u64);
                let ip_address = ip_address.clone();
                let sem = semaphore.clone();
                async move {
                    if is_null {
                        (i, None)
                    } else {
                        let _permit = sem.acquire().await.unwrap();
                        let result = reverse_dns_lookup_async(resolver, &ip_address).await;
                        (i, result.ok())
                    }
                }
            })
            .collect();

        let results = GLOBAL_DNS_STATE.runtime.block_on(async { futures::future::join_all(futures).await });

        // Write results to output
        for (i, result) in results.into_iter().take(size) {
            match result {
                Some(hostname) => output_vector.insert(i, hostname.as_str()),
                None => output_vector.set_null(i),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
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

impl VScalar for DnsLookup {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let size = input.len();
        let hostname_vector = input.flat_vector(0);
        let mut output_vector = output.flat_vector();

        // Get hostname strings
        let hostname_values = hostname_vector.as_slice_with_len::<duckdb_string_t>(size);
        let hostnames: Vec<String> = hostname_values
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string())
            .collect();

        // Check if we have a second parameter (record_type)
        let record_types: Option<Vec<Option<String>>> = if input.num_columns() > 1 {
            let record_type_vector = input.flat_vector(1);
            let record_type_values = record_type_vector.as_slice_with_len::<duckdb_string_t>(size);
            Some(
                (0..size)
                    .map(|i| {
                        if record_type_vector.row_is_null(i as u64) {
                            None
                        } else {
                            Some(
                                DuckString::new(&mut { record_type_values[i] })
                                    .as_str()
                                    .to_string(),
                            )
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };

        // Use the global resolver state - load once for all lookups
        let resolver = &GLOBAL_DNS_STATE.resolver;
        let semaphore = GLOBAL_DNS_STATE.concurrency_semaphore.load();

        // Process all lookups concurrently with semaphore-controlled execution
        let futures: Vec<_> = hostnames
            .iter()
            .enumerate()
            .map(|(i, hostname)| {
                let is_null = hostname_vector.row_is_null(i as u64);
                let hostname = hostname.clone();
                let record_type_opt = record_types.as_ref().and_then(|rt| rt[i].clone());
                let sem = semaphore.clone();
                async move {
                    if is_null {
                        (i, None)
                    } else {
                        let _permit = sem.acquire().await.unwrap();
                        let result = if let Some(record_type_str) = record_type_opt {
                            match parse_record_type(&record_type_str) {
                                Ok(record_type) => {
                                    dns_lookup_with_type_async(resolver, &hostname, record_type).await
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            dns_lookup_async(resolver, &hostname).await
                        };
                        (i, result.ok())
                    }
                }
            })
            .collect();

        let results = GLOBAL_DNS_STATE.runtime.block_on(async { futures::future::join_all(futures).await });

        // Write results to output
        for (i, result) in results.into_iter().take(size) {
            match result {
                Some(data) => output_vector.insert(i, data.as_str()),
                None => output_vector.set_null(i),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                ],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
        ]
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

impl VScalar for DnsLookupAll {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let size = input.len();
        let hostname_vector = input.flat_vector(0);
        let mut output_vector = output.list_vector();

        // Get hostname strings
        let hostname_values = hostname_vector.as_slice_with_len::<duckdb_string_t>(size);
        let hostnames: Vec<String> = hostname_values
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string())
            .collect();

        // Check if we have a second parameter (record_type)
        let record_types: Option<Vec<Option<String>>> = if input.num_columns() > 1 {
            let record_type_vector = input.flat_vector(1);
            let record_type_values = record_type_vector.as_slice_with_len::<duckdb_string_t>(size);
            Some(
                (0..size)
                    .map(|i| {
                        if record_type_vector.row_is_null(i as u64) {
                            None
                        } else {
                            Some(
                                DuckString::new(&mut { record_type_values[i] })
                                    .as_str()
                                    .to_string(),
                            )
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };

        // Use the global resolver state - load once for all lookups
        let resolver = &GLOBAL_DNS_STATE.resolver;
        let semaphore = GLOBAL_DNS_STATE.concurrency_semaphore.load();

        // Process all lookups concurrently with semaphore-controlled execution
        let futures: Vec<_> = hostnames
            .iter()
            .enumerate()
            .map(|(i, hostname)| {
                let is_null = hostname_vector.row_is_null(i as u64);
                let hostname = hostname.clone();
                let record_type_opt = record_types.as_ref().and_then(|rt| rt[i].clone());
                let sem = semaphore.clone();
                async move {
                    if is_null {
                        None
                    } else {
                        let _permit = sem.acquire().await.unwrap();
                        let result = if let Some(record_type_str) = record_type_opt {
                            match parse_record_type(&record_type_str) {
                                Ok(record_type) => {
                                    dns_lookup_all_with_type_async(resolver, &hostname, record_type).await
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            dns_lookup_all_async(resolver, &hostname).await
                        };
                        result.ok()
                    }
                }
            })
            .collect();

        let all_results = GLOBAL_DNS_STATE.runtime.block_on(async { futures::future::join_all(futures).await });

        // Calculate total number of records for capacity
        let total_capacity: usize = all_results.iter().map(|r| r.as_ref().map_or(0, |v| v.len())).sum();

        // Get the child vector with appropriate capacity
        let child_vector = output_vector.child(total_capacity);

        // Now insert the data
        let mut offset = 0;
        for (i, result) in all_results.iter().enumerate() {
            match result {
                Some(records) => {
                    output_vector.set_entry(i, offset, records.len());
                    for record in records {
                        child_vector.insert(offset, record.as_str());
                        offset += 1;
                    }
                }
                None => {
                    output_vector.set_null(i);
                }
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
                LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                ],
                LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ),
        ]
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

impl VScalar for SetDnsConfig {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let mut output_vector = output.flat_vector();

        // Get input strings
        let values = input_vector.as_slice_with_len::<duckdb_string_t>(size);

        for i in 0..size {
            if input_vector.row_is_null(i as u64) {
                output_vector.set_null(i);
                continue;
            }

            let preset = DuckString::new(&mut { values[i] }).as_str().trim().to_lowercase();

            // Determine which configuration to use
            let config = match preset.as_str() {
                "default" => ResolverConfig::default(),
                "google" => ResolverConfig::google(),
                "cloudflare" => ResolverConfig::cloudflare(),
                "quad9" => ResolverConfig::quad9(),
                _ => {
                    let error_msg = format!("Unknown preset '{}'. Supported presets: default, google, cloudflare, quad9", preset);
                    output_vector.insert(i, &error_msg);
                    continue;
                }
            };

            // Update the global resolver configuration
            // This is a lock-free atomic operation - extremely fast!
            match GLOBAL_DNS_STATE.update_config(config) {
                Ok(_) => {
                    let success_msg = format!("DNS configuration updated to '{}'", preset);
                    output_vector.insert(i, &success_msg);
                }
                Err(e) => {
                    let error_msg = format!("Failed to update DNS configuration: {}", e);
                    output_vector.insert(i, &error_msg);
                }
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
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

impl VScalar for SetConcurrencyLimit {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let mut output_vector = output.flat_vector();

        // Get input values
        let values = input_vector.as_slice_with_len::<i64>(size);

        for i in 0..size {
            if input_vector.row_is_null(i as u64) {
                output_vector.set_null(i);
                continue;
            }

            let limit = values[i];

            if limit <= 0 {
                let error_msg = "Concurrency limit must be greater than 0";
                output_vector.insert(i, error_msg);
                continue;
            }

            // Update the global concurrency limit
            // This is a lock-free atomic operation - extremely fast!
            match GLOBAL_DNS_STATE.set_dns_concurrency_limit(limit as usize) {
                Ok(_) => {
                    let success_msg = format!("Concurrency limit updated to {}", limit);
                    output_vector.insert(i, &success_msg);
                }
                Err(e) => {
                    let error_msg = format!("Failed to update concurrency limit: {}", e);
                    output_vector.insert(i, &error_msg);
                }
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Bigint)],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
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

impl VScalar for SetDnsCacheSize {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let mut output_vector = output.flat_vector();

        // Get input values
        let values = input_vector.as_slice_with_len::<i64>(size);

        for i in 0..size {
            if input_vector.row_is_null(i as u64) {
                output_vector.set_null(i);
                continue;
            }

            let cache_size = values[i];

            if cache_size <= 0 {
                let error_msg = "Cache size must be greater than 0";
                output_vector.insert(i, error_msg);
                continue;
            }

            // Update the global cache size
            // This is a lock-free atomic operation - extremely fast!
            match GLOBAL_DNS_STATE.set_dns_cache_size(cache_size as usize) {
                Ok(_) => {
                    let success_msg = format!("DNS cache size updated to {}", cache_size);
                    output_vector.insert(i, &success_msg);
                }
                Err(e) => {
                    let error_msg = format!("Failed to update cache size: {}", e);
                    output_vector.insert(i, &error_msg);
                }
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Bigint)],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
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
/// - Returns an empty result set if no TXT records are found
/// - Performs the DNS lookup during the bind phase for efficiency
/// - All records are fetched at once and then returned in chunks
struct CoreyBindData {
    #[allow(dead_code)]
    hostname: String,
    txt_records: Vec<String>,
    #[allow(dead_code)]
    resolver_state: DnsResolverState,
}

struct CoreyInitData {
    offset: std::sync::atomic::AtomicUsize,
}

struct Corey;

impl VTab for Corey {
    type InitData = CoreyInitData;
    type BindData = CoreyBindData;

    fn bind(bind: &BindInfo) -> std::result::Result<Self::BindData, Box<dyn Error>> {
        // Add result column for TXT records
        bind.add_result_column("txt_record", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        // Get hostname parameter
        let hostname = bind.get_parameter(0).to_string();

        // Create a long-lived resolver state
        let resolver_state = DnsResolverState::new()?;

        // Perform TXT lookup using the resolver state
        let txt_records = resolver_state.runtime.block_on(async {
            // Lock-free load of the current resolver
            let resolver_guard = resolver_state.resolver.load();
            match resolver_guard.lookup(hostname.trim(), RecordType::TXT).await {
                Ok(lookup) => {
                    let records: Vec<String> = lookup
                        .record_iter()
                        .map(|record| record.data().to_string())
                        .collect();
                    Ok(records)
                }
                Err(e) => Err(format!("TXT lookup failed: {}", e)),
            }
        })?;

        Ok(CoreyBindData {
            hostname,
            txt_records,
            resolver_state,
        })
    }

    fn init(_: &InitInfo) -> std::result::Result<Self::InitData, Box<dyn Error>> {
        Ok(CoreyInitData {
            offset: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let offset = init_data
            .offset
            .load(std::sync::atomic::Ordering::Relaxed);
        let remaining = bind_data.txt_records.len().saturating_sub(offset);

        if remaining == 0 {
            output.set_len(0);
            return Ok(());
        }

        // Determine how many records to return in this chunk
        let chunk_size = remaining.min(2048); // DuckDB default vector size
        output.set_len(chunk_size);

        let result_vector = output.flat_vector(0);

        // Write TXT records to output
        for (i, txt_record) in bind_data.txt_records[offset..offset + chunk_size]
            .iter()
            .enumerate()
        {
            result_vector.insert(i, txt_record.as_str());
        }

        // Update offset for next call
        init_data
            .offset
            .store(offset + chunk_size, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<ReverseDnsLookup>("reverse_dns_lookup")?;
    con.register_scalar_function::<DnsLookup>("dns_lookup")?;
    con.register_scalar_function::<DnsLookupAll>("dns_lookup_all")?;
    con.register_scalar_function::<SetDnsConfig>("set_dns_config")?;
    con.register_scalar_function::<SetConcurrencyLimit>("set_dns_concurrency_limit")?;
    con.register_scalar_function::<SetDnsCacheSize>("set_dns_cache_size")?;
    con.register_table_function::<Corey>("corey")?;
    Ok(())
}
