from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.expressions import col, lit

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10000)
t_env = StreamTableEnvironment.create(env)

# Schema Definition 
clicks_schema = Schema.new_builder() \
    .column("click_id"  , DataTypes.STRING()) \
    .column("user_id"   , DataTypes.STRING()) \
    .column("product_id" , DataTypes.STRING()) \
    .column("product_url" , DataTypes.STRING()) \
    .column("user_agent", DataTypes.STRING()) \
    .column("ip", DataTypes.STRING()) \
    .column("event_time", DataTypes.DOUBLE()) \
    .column_by_expression("event_ts", "TO_TIMESTAMP(FROM_UNIXTIME(CAST(event_time AS BIGINT)))") \
    .column_by_metadata("enqueued_at", DataTypes.TIMESTAMP_LTZ(3), "timestamp", True) \
    .column_by_metadata("partition_id", DataTypes.BIGINT(), "partition", True) \
    .column_by_metadata("offset", DataTypes.BIGINT(), "offset", True) \
    .column_by_metadata("topic_id", DataTypes.STRING(), "topic", True) \
    .column_by_expression("proc_time", "PROCTIME()") \
    .watermark("event_ts", "event_ts - INTERVAL '1' HOUR") \
    .build()

checkouts_schema = Schema.new_builder() \
    .column("checkout_id"     , DataTypes.STRING()) \
    .column("user_id"         , DataTypes.STRING()) \
    .column("product_id"      , DataTypes.STRING()) \
    .column("payment_method"  , DataTypes.STRING()) \
    .column("total_amount"    , DataTypes.STRING()) \
    .column("shipping_address", DataTypes.STRING()) \
    .column("billing_address" , DataTypes.STRING()) \
    .column("user_agent"      , DataTypes.STRING()) \
    .column("ip_address"      , DataTypes.STRING()) \
    .column("event_time"      , DataTypes.DOUBLE()) \
    .column_by_expression("event_ts", "TO_TIMESTAMP(FROM_UNIXTIME(CAST(event_time AS BIGINT)))") \
    .column_by_metadata("enqueued_at", DataTypes.TIMESTAMP_LTZ(3), "timestamp", True) \
    .column_by_metadata("partition_id", DataTypes.BIGINT(), "partition", True) \
    .column_by_metadata("offset", DataTypes.BIGINT(), "offset", True) \
    .column_by_metadata("topic_id", DataTypes.STRING(), "topic", True) \
    .column_by_expression("proc_time", "PROCTIME()") \
    .watermark("event_ts", "event_ts - INTERVAL '1' HOUR") \
    .build()

# Descriptor Definition
clicks_descriptor = TableDescriptor.for_connector("kafka") \
    .schema(clicks_schema) \
    .option("topic", "clicks") \
    .option("properties.bootstrap.servers", "k-01:9092") \
    .option("scan.startup.mode", "earliest-offset") \
    .option("key.format", "raw") \
    .option("key.fields", "user_id") \
    .option("value.format", "avro-confluent") \
    .option("value.fields-include", "ALL") \
    .option("value.avro-confluent.url", "http://k-sr:8081") \
    .build() \

checkouts_descriptor = TableDescriptor.for_connector("kafka") \
    .schema(checkouts_schema) \
    .option("topic", "checkouts") \
    .option("properties.bootstrap.servers", "k-01:9092") \
    .option("scan.startup.mode", "earliest-offset") \
    .option("key.format", "raw") \
    .option("key.fields", "user_id") \
    .option("value.format", "avro-confluent") \
    .option("value.fields-include", "ALL") \
    .option("value.avro-confluent.url", "http://k-sr:8081") \
    .build() \
    
# create table using TableDescriptor
t_env.create_temporary_table("kafka_clicks", clicks_descriptor)
t_env.create_temporary_table("kafka_checkouts", checkouts_descriptor)

clicks = t_env.from_path("kafka_clicks")

checkouts = t_env.from_path("kafka_checkouts")

clicks_columns = clicks.get_schema().get_field_names()
checkouts_columns = checkouts.get_schema().get_field_names()

clicks = clicks.rename_columns(*[col(name).alias(f"click_{name}") for name in clicks_columns])
checkouts = checkouts.rename_columns(*[col(name).alias(f"checkout_{name}") for name in checkouts_columns])

checkouts_clicks = checkouts \
    .left_outer_join(
        clicks, 
        clicks.click_user_id == checkouts.checkout_user_id) \
    .where(
        (clicks.click_event_ts >= checkouts.checkout_event_ts - lit(1).hours) & 
        (clicks.click_event_ts <= checkouts.checkout_event_ts)) \
    .select(
        checkouts.checkout_user_id.alias("user_id"),
        checkouts.checkout_checkout_id.alias("checkout_id"),
        checkouts.checkout_event_time.alias("checkout_time"),
        checkouts.checkout_product_id.alias("checkout_product_id"),
        clicks.click_click_id.alias("click_id"),
        clicks.click_event_time.alias("click_time"),
        clicks.click_product_id.alias("click_product_id")
    )

# Sink
# t_env.execute_sql('''USE CATALOG nessie_catalog;''')
t_env.execute_sql('''CREATE DATABASE IF NOT EXISTS stream_join_db;''')
t_env.execute_sql('''USE stream_join_db;''')

checkouts_clicks_schema = Schema.new_builder() \
    .column("user_id", DataTypes.STRING()) \
    .column("checkout_id", DataTypes.STRING()) \
    .column("checkout_time", DataTypes.DOUBLE()) \
    .column("checkout_product_id", DataTypes.STRING()) \
    .column("click_id", DataTypes.STRING()) \
    .column("click_time", DataTypes.DOUBLE()) \
    .column("click_product_id", DataTypes.STRING()) \
    .build()

iceberg_sink = TableDescriptor.for_connector('iceberg') \
    .schema(checkouts_clicks_schema) \
    .option("catalog-name", "nessie_catalog") \
    .option("catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .option("uri", "http://nessie:19120/api/v1") \
    .option("ref"                 , "main") \
    .option("warehouse"           , "s3://lakehouse") \
    .option("io-impl"             , "org.apache.iceberg.aws.s3.S3FileIO") \
    .option("s3.endpoint"         , "http://minio:9000") \
    .option("s3.access-key-id"    , "admin") \
    .option("s3.secret-access-key", "admin123") \
    .option("s3.path-style-access", "true") \
    .option("client.region"       , "us-east-1") \
    .build()

t_env.create_table("checkouts_clicks", iceberg_sink)

checkouts_clicks.execute_insert("checkouts_clicks")