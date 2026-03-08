CREATE TABLE postgres_products (
    `product_id` STRING,
    `product_name` STRING,
    `description` STRING,
    `price` STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/ecommerce',
    'driver' = 'org.postgresql.Driver',
    'username' = 'admin',
    'password' = 'admin',
    'table-name' = 'products',
    'lookup.cache.max-rows' = '1000',
    'lookup.cache.ttl' = '10min'
)