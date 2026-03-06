CREATE TABLE postgres_user (
    `user_id` STRING,
    `username` STRING,
    `password` STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/ecommerce',
    'driver' = 'org.postgresql.Driver',
    'username' = 'admin',
    'password' = 'admin',
    'table-name' = 'users',
    'lookup.cache.max-rows' = '1000',
    'lookup.cache.ttl' = '10min'
)