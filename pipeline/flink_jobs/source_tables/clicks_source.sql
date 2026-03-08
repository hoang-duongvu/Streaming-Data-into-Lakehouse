CREATE TABLE kafka_clicks (

    `click_id`    STRING,
    `user_id`     STRING,
    `product_id`  STRING,
    `product_url` STRING,
    `user_agent`  STRING,
    `ip`          STRING,
    `event_time`  DOUBLE,
    `enqueued_at` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    `partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,
    `offset` BIGINT METADATA VIRTUAL,
    `topic_id` STRING METADATA FROM 'topic' VIRTUAL,
    `proc_time` AS PROCTIME()

) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks',
  'properties.bootstrap.servers' = 'k-01:9092',
  'scan.startup.mode' = 'earliest-offset',

  'key.format' = 'raw',
  'key.fields' = 'user_id',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://k-sr:8081',
  'value.fields-include' = 'ALL'
)