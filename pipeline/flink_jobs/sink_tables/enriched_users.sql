
CREATE CATALOG nessie_catalog WITH (
    'type'                 = 'iceberg',
    'catalog-impl'         = 'org.apache.iceberg.nessie.NessieCatalog',
    'uri'                  = 'http://nessie:19120/api/v1',
    'ref'                  = 'main',
    'warehouse'            = 's3://lakehouse',
    'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'          = 'http://minio:9000',
    's3.access-key-id'     = 'admin',
    's3.secret-access-key' = 'admin123',
    's3.path-style-access' = 'true',
    'client.region'        = 'us-east-1'
);

USE CATALOG nessie_catalog;

CREATE DATABASE IF NOT EXISTS enriched_tables;
USE enriched_tables;


CREATE TABLE IF NOT EXISTS enriched_clicks (
    `click_id`      STRING,
    `user_id`       STRING,
    `product_id`    STRING,
    `product_url`   STRING,
    `user_agent`    STRING,
    `ip`            STRING,
    `event_time`    DOUBLE,
    `enqueued_at`   TIMESTAMP_LTZ(3),
    `partition_id`  BIGINT,
    `offset`  BIGINT,
    `topic_id`      STRING,
    `username`      STRING,
    `password`      STRING
);
