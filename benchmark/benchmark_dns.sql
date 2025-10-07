-- DNS Extension Load Test and Benchmark
-- Tests request/sec performance with real-world domains

.timer on

-- Load the extension
LOAD './build/debug/extension/dns/dns.duckdb_extension';

-- Create a table with top 100 domains
CREATE TABLE domains AS
SELECT * FROM (VALUES
    ('fonts.googleapis.com'),
    ('facebook.com'),
    ('twitter.com'),
    ('google.com'),
    ('youtube.com'),
    ('instagram.com'),
    ('googletagmanager.com'),
    ('linkedin.com'),
    ('ajax.googleapis.com'),
    ('pinterest.com'),
    ('fonts.gstatic.com'),
    ('wordpress.org'),
    ('en.wikipedia.org'),
    ('youtu.be'),
    ('maps.google.com'),
    ('itunes.apple.com'),
    ('github.com'),
    ('bit.ly'),
    ('play.google.com'),
    ('docs.google.com'),
    ('cdnjs.cloudflare.com'),
    ('vimeo.com'),
    ('support.google.com'),
    ('google-analytics.com'),
    ('maps.googleapis.com'),
    ('flickr.com'),
    ('vk.com'),
    ('t.co'),
    ('reddit.com'),
    ('amazon.com'),
    ('medium.com'),
    ('sites.google.com'),
    ('drive.google.com'),
    ('creativecommons.org'),
    ('microsoft.com'),
    ('developers.google.com'),
    ('adobe.com'),
    ('soundcloud.com'),
    ('theguardian.com'),
    ('apis.google.com'),
    ('cloudflare.com'),
    ('nytimes.com'),
    ('support.microsoft.com'),
    ('blogger.com'),
    ('forbes.com'),
    ('s3.amazonaws.com'),
    ('code.jquery.com'),
    ('dropbox.com'),
    ('translate.google.com'),
    ('paypal.com'),
    ('apps.apple.com'),
    ('tinyurl.com'),
    ('etsy.com'),
    ('theatlantic.com'),
    ('archive.org'),
    ('cnn.com'),
    ('policies.google.com'),
    ('commons.wikimedia.org'),
    ('issuu.com'),
    ('wordpress.com'),
    ('businessinsider.com'),
    ('yelp.com'),
    ('mail.google.com'),
    ('support.apple.com'),
    ('t.me'),
    ('apple.com'),
    ('washingtonpost.com'),
    ('bbc.com'),
    ('gstatic.com'),
    ('imgur.com'),
    ('amazon.de'),
    ('bbc.co.uk'),
    ('mozilla.org'),
    ('eventbrite.com'),
    ('slideshare.net'),
    ('w3.org'),
    ('platform.twitter.com'),
    ('accounts.google.com'),
    ('wikipedia.org'),
    ('stackoverflow.com'),
    ('tumblr.com'),
    ('telegram.org'),
    ('whatsapp.com'),
    ('netflix.com'),
    ('spotify.com'),
    ('slack.com'),
    ('zoom.us'),
    ('trello.com'),
    ('notion.so'),
    ('figma.com'),
    ('canva.com'),
    ('shopify.com'),
    ('stripe.com'),
    ('twitch.tv'),
    ('discord.com'),
    ('gitlab.com'),
    ('bitbucket.org'),
    ('npmjs.com')
) AS t(domain);

.print ''
.print '═══════════════════════════════════════════════════════════'
.print 'DNS EXTENSION LOAD TEST'
.print '═══════════════════════════════════════════════════════════'
.print ''

-- Test 1: Single domain lookup (baseline)
.print '📊 Test 1: Single Domain Lookup'
.print '-----------------------------------------------------------'
SELECT dns_lookup('google.com') as ip;

-- Test 2: Batch lookup - 10 domains
.print ''
.print '📊 Test 2: Batch Lookup (10 domains)'
.print '-----------------------------------------------------------'
SELECT
    domain,
    dns_lookup(domain) as ip
FROM domains
LIMIT 10;

-- Test 3: Batch lookup - 50 domains
.print ''
.print '📊 Test 3: Batch Lookup (50 domains)'
.print '-----------------------------------------------------------'
SELECT
    COUNT(*) as resolved_count
FROM (
    SELECT
        domain,
        dns_lookup(domain) as ip
    FROM domains
    LIMIT 50
)
WHERE ip IS NOT NULL;

-- Test 4: Full batch - 100 domains
.print ''
.print '📊 Test 4: Full Batch Lookup (100 domains)'
.print '-----------------------------------------------------------'
SELECT
    COUNT(*) as total_domains,
    COUNT(ip) as resolved,
    COUNT(*) - COUNT(ip) as failed
FROM (
    SELECT
        domain,
        dns_lookup(domain) as ip
    FROM domains
);

-- Test 5: Repeated lookups (cache test) - 100 domains x 3 iterations
.print ''
.print '📊 Test 5: Cache Performance Test (300 lookups)'
.print '-----------------------------------------------------------'
SELECT
    COUNT(*) as total_lookups,
    COUNT(ip) as resolved
FROM (
    SELECT dns_lookup(domain) as ip FROM domains
    UNION ALL
    SELECT dns_lookup(domain) as ip FROM domains
    UNION ALL
    SELECT dns_lookup(domain) as ip FROM domains
);

-- Test 6: MX record lookups
.print ''
.print '📊 Test 6: MX Record Lookups (20 domains)'
.print '-----------------------------------------------------------'
SELECT
    COUNT(*) as mx_records_found
FROM (
    SELECT
        domain,
        dns_lookup(domain, 'MX') as mx
    FROM domains
    LIMIT 20
)
WHERE mx IS NOT NULL;

-- Test 7: Multiple record types for same domain
.print ''
.print '📊 Test 7: Multiple Record Types (google.com)'
.print '-----------------------------------------------------------'
SELECT
    'A' as record_type,
    dns_lookup('google.com', 'A') as result
UNION ALL
SELECT
    'MX' as record_type,
    dns_lookup('google.com', 'MX') as result
UNION ALL
SELECT
    'TXT' as record_type,
    dns_lookup('google.com', 'TXT') as result;

-- Test 8: dns_lookup_all for multiple IPs
.print ''
.print '📊 Test 8: dns_lookup_all (10 domains)'
.print '-----------------------------------------------------------'
SELECT
    domain,
    len(dns_lookup_all(domain)) as ip_count
FROM domains
LIMIT 10;

-- Test 9: Configuration change test
.print ''
.print '📊 Test 9: Dynamic Configuration Change'
.print '-----------------------------------------------------------'
SELECT set_dns_config('cloudflare') as config_result;
SELECT dns_lookup('google.com') as ip_after_cloudflare;
SELECT set_dns_config('google') as config_result;
SELECT dns_lookup('cloudflare.com') as ip_after_google;

-- Test 10: High concurrency test - Cross join for 1000 lookups
.print ''
.print '📊 Test 10: High Concurrency (1000 lookups)'
.print '-----------------------------------------------------------'
SELECT
    COUNT(*) as total_lookups,
    COUNT(ip) as successful
FROM (
    SELECT
        d1.domain,
        dns_lookup(d1.domain) as ip
    FROM domains d1
    CROSS JOIN (SELECT 1 as n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
                UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) multiplier
);

.print ''
.print '═══════════════════════════════════════════════════════════'
.print 'BENCHMARK COMPLETE'
.print '═══════════════════════════════════════════════════════════'
.print ''
.print 'NOTE: Times shown above include both network latency and'
.print 'processing overhead. The actual req/sec depends on:'
.print '- DNS response times from upstream servers'
.print '- Network latency'
.print '- Cache hit rates'
.print ''
