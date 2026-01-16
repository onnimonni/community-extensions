-- matsmart.sql
-- Extract product data from Matsmart.fi using enhanced EXTRACT syntax
--
-- Uses the new Rust HTML parser for:
-- - JSON-LD extraction with dot notation
-- - Microdata fallback via COALESCE
-- - CSS selectors for prices not in structured data
-- - Type coercion and transforms

LOAD 'build/release/extension/crawler/crawler.duckdb_extension';

-- Create products table with inline extraction
-- No post-processing needed - data extracted during crawl
CRAWL (SELECT 'https://www.matsmart.fi/')
INTO matsmart_products
EXTRACT (
    -- Product identifiers (try JSON-LD first, fall back to microdata)
    COALESCE(jsonld.Product.sku, microdata.Product.sku) as sku,
    COALESCE(jsonld.Product.gtin, jsonld.Product.gtin13, microdata.Product.gtin) as gtin,
    jsonld.Product.productID as product_id,

    -- Product info
    jsonld.Product.name as name,
    jsonld.Product.description as description,
    jsonld.Product.url as product_url,
    jsonld.Product.brand.name as brand,

    -- Pricing (with type coercion)
    COALESCE(jsonld.Product.offers.price, microdata.Product.offers.price) as price,
    jsonld.Product.offers.priceCurrency as currency,
    jsonld.Product.offers.availability as availability,

    -- Unit pricing from CSS (Finnish sites often show €/kg etc)
    unit_price VARCHAR FROM css '.unit-price::text' | trim,

    -- Images
    jsonld.Product.image[0] as image_url
)
WITH (
    user_agent 'Mozilla/5.0 (compatible; PriceBot/1.0)',
    follow_links true,
    max_crawl_depth 2,
    timeout_seconds 30,
    crawl_delay_ms 500
)
WHERE url LIKE 'https://www.matsmart.fi/tuote/%'
LIMIT 5;

-- Show extraction results
SELECT 'Extracted ' || COUNT(*) || ' products' as status FROM matsmart_products;

-- Display products with formatted output
SELECT
    sku,
    LEFT(name, 50) as name,
    printf('%.2f %s', price::DECIMAL, COALESCE(currency, 'EUR')) as price,
    unit_price,
    CASE
        WHEN availability LIKE '%InStock%' THEN '✓ In Stock'
        WHEN availability LIKE '%OutOfStock%' THEN '✗ Out of Stock'
        ELSE COALESCE(availability, 'Unknown')
    END as status,
    brand
FROM matsmart_products
WHERE name IS NOT NULL
ORDER BY name
LIMIT 20;

-- Price analysis
SELECT
    'Price Statistics' as report,
    COUNT(*) as total_products,
    printf('%.2f', MIN(price::DECIMAL)) as min_price,
    printf('%.2f', AVG(price::DECIMAL)) as avg_price,
    printf('%.2f', MAX(price::DECIMAL)) as max_price
FROM matsmart_products
WHERE price IS NOT NULL;
