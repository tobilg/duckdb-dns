extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    ffi,
    types::DuckString,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection, Result,
};
use libduckdb_sys::duckdb_string_t;
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::Arc,
};
use trust_dns_resolver::config::*;
use trust_dns_resolver::TokioAsyncResolver;

// Validate IPv4 address format
fn validate_ipv4(ip_str: &str) -> std::result::Result<Ipv4Addr, Box<dyn Error>> {
    match Ipv4Addr::from_str(ip_str.trim()) {
        Ok(addr) => Ok(addr),
        Err(_) => Err(format!("Invalid IPv4 address format: {}", ip_str).into()),
    }
}

// Perform reverse DNS lookup asynchronously
async fn reverse_dns_lookup_async(
    resolver: &TokioAsyncResolver,
    ip_str: &str,
) -> std::result::Result<String, Box<dyn Error>> {
    let ipv4 = validate_ipv4(ip_str)?;
    let ip_addr = IpAddr::V4(ipv4);

    match resolver.reverse_lookup(ip_addr).await {
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

// Perform forward DNS lookup - returns first IPv4 address asynchronously
async fn dns_lookup_async(
    resolver: &TokioAsyncResolver,
    hostname: &str,
) -> std::result::Result<String, Box<dyn Error>> {
    let hostname = hostname.trim();

    match resolver.lookup_ip(hostname).await {
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

// Perform forward DNS lookup - returns all IPv4 addresses asynchronously
async fn dns_lookup_all_async(
    resolver: &TokioAsyncResolver,
    hostname: &str,
) -> std::result::Result<Vec<String>, Box<dyn Error>> {
    let hostname = hostname.trim();

    match resolver.lookup_ip(hostname).await {
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

// Reverse DNS lookup scalar function
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

        // Create tokio runtime and resolver
        let runtime = tokio::runtime::Runtime::new()?;
        let resolver = Arc::new(runtime.block_on(async {
            TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default())
        }));

        // Process all lookups concurrently
        let futures: Vec<_> = strings
            .iter()
            .enumerate()
            .map(|(i, ip_address)| {
                let is_null = input_vector.row_is_null(i as u64);
                let resolver = Arc::clone(&resolver);
                let ip_address = ip_address.clone();
                async move {
                    if is_null {
                        (i, None)
                    } else {
                        let result = reverse_dns_lookup_async(&resolver, &ip_address).await;
                        (i, result.ok())
                    }
                }
            })
            .collect();

        let results = runtime.block_on(async { futures::future::join_all(futures).await });

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

// Forward DNS lookup scalar function
struct DnsLookup;

impl VScalar for DnsLookup {
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

        // Create tokio runtime and resolver
        let runtime = tokio::runtime::Runtime::new()?;
        let resolver = Arc::new(runtime.block_on(async {
            TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default())
        }));

        // Process all lookups concurrently
        let futures: Vec<_> = strings
            .iter()
            .enumerate()
            .map(|(i, hostname)| {
                let is_null = input_vector.row_is_null(i as u64);
                let resolver = Arc::clone(&resolver);
                let hostname = hostname.clone();
                async move {
                    if is_null {
                        (i, None)
                    } else {
                        let result = dns_lookup_async(&resolver, &hostname).await;
                        (i, result.ok())
                    }
                }
            })
            .collect();

        let results = runtime.block_on(async { futures::future::join_all(futures).await });

        // Write results to output
        for (i, result) in results.into_iter().take(size) {
            match result {
                Some(ip_address) => output_vector.insert(i, ip_address.as_str()),
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

// Forward DNS lookup (all IPs) scalar function
struct DnsLookupAll;

impl VScalar for DnsLookupAll {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let mut output_vector = output.list_vector();

        // Get input strings
        let values = input_vector.as_slice_with_len::<duckdb_string_t>(size);
        let strings: Vec<String> = values
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string())
            .collect();

        // Create tokio runtime and resolver
        let runtime = tokio::runtime::Runtime::new()?;
        let resolver = Arc::new(runtime.block_on(async {
            TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default())
        }));

        // Process all lookups concurrently
        let futures: Vec<_> = strings
            .iter()
            .enumerate()
            .map(|(i, hostname)| {
                let is_null = input_vector.row_is_null(i as u64);
                let resolver = Arc::clone(&resolver);
                let hostname = hostname.clone();
                async move {
                    if is_null {
                        None
                    } else {
                        dns_lookup_all_async(&resolver, &hostname).await.ok()
                    }
                }
            })
            .collect();

        let all_results = runtime.block_on(async { futures::future::join_all(futures).await });

        // Calculate total number of IPs for capacity
        let total_capacity: usize = all_results.iter().map(|r| r.as_ref().map_or(0, |v| v.len())).sum();

        // Get the child vector with appropriate capacity
        let child_vector = output_vector.child(total_capacity);

        // Now insert the data
        let mut offset = 0;
        for (i, result) in all_results.iter().enumerate() {
            match result {
                Some(ip_addresses) => {
                    output_vector.set_entry(i, offset, ip_addresses.len());
                    for ip_address in ip_addresses {
                        child_vector.insert(offset, ip_address.as_str());
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
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        )]
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<ReverseDnsLookup>("reverse_dns_lookup")?;
    con.register_scalar_function::<DnsLookup>("dns_lookup")?;
    con.register_scalar_function::<DnsLookupAll>("dns_lookup_all")?;
    Ok(())
}
