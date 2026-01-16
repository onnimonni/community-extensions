-- matsmart.sql
-- Extract product data from Matsmart.fi (Finnish grocery surplus store)
--
-- The site uses JSON-LD with Product schema containing:
-- - gtin, sku, productID
-- - name, url, image
-- - offers.price, offers.priceCurrency, offers.availability
--
-- KNOWN ISSUE: Crawler hangs on large pages (>500KB). Use timeout or test with smaller pages.

LOAD 'build/release/extension/crawler/crawler.duckdb_extension';

-- Crawl product pages (adjust LIMIT as needed)
CRAWL (SELECT 'https://www.matsmart.fi/')
INTO matsmart_raw
WITH (
    user_agent 'Mozilla/5.0 (compatible; PriceBot/1.0)',
    follow_links true,
    max_crawl_depth 3,
    timeout_seconds 30
)
WHERE url LIKE 'https://www.matsmart.fi/tuote/%'
LIMIT 10;

-- Extract products from JSON-LD
-- jsonld is keyed by @type: {"Product": {...}, "Organization": {...}}
CREATE OR REPLACE TABLE matsmart_products AS
SELECT
    jsonld->'Product'->>'sku' as sku,
    jsonld->'Product'->>'gtin' as gtin,
    jsonld->'Product'->>'productID' as product_id,
    trim(jsonld->'Product'->>'name') as name,
    jsonld->'Product'->>'url' as product_url,
    (jsonld->'Product'->'offers'->>'price')::DECIMAL(10,2) as price,
    jsonld->'Product'->'offers'->>'priceCurrency' as currency,
    jsonld->'Product'->'offers'->>'availability' as availability,
    jsonld->'Product'->'image'->>0 as image_url,
    url as source_url,
    crawled_at
FROM matsmart_raw
WHERE jsonld->>'Product' IS NOT NULL;

-- Results
SELECT 'Extracted ' || COUNT(*) || ' products' as status FROM matsmart_products;

SELECT
    sku,
    name,
    price || ' ' || currency as price,
    CASE
        WHEN availability LIKE '%InStock%' THEN 'In Stock'
        WHEN availability LIKE '%OutOfStock%' THEN 'Out of Stock'
        ELSE availability
    END as availability
FROM matsmart_products
ORDER BY name;
