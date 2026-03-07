CREATE TABLE IF NOT EXISTS enriched_clicks (
    `click_id`    STRING,
    `user_id`     STRING,
    `product_id`  STRING,
    `product_url` STRING,
    `user_agent`  STRING,
    `ip`          STRING,
    `event_time`  DOUBLE,
    `enqueued_at` TIMESTAMP_LTZ(3),
    `partition_id` BIGINT,
    `offset` BIGINT,
    `topic_id` STRING,
    `proc_time` TIMESTAMP_LTZ(3),
    `product_name` STRING,
    `description` STRING,
    `price` STRING
);