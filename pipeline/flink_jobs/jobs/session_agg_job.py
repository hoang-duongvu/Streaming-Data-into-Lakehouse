from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.expressions import col, lit, if_then_else
from pyflink.table.window import Session

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
# watermark chỉ họat động trên kiểu dữ liệu timestamp, không phải double => tạo cột mới kiểu timestamp

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

clicks_selected = clicks.select(
    col("user_id"), 
    lit("click").alias("activity_type"),
    col("click_id").alias("activity_id"),
    col("event_ts").alias("activity_ts")
)

checkouts_selected = checkouts.select(
    col("user_id"), 
    lit("checkout").alias("activity_type"),
    col("checkout_id").alias("activity_id"),
    col("event_ts").alias("activity_ts")
)

# Intermediate Table
all_activities = clicks_selected.union_all(checkouts_selected)

# Final Table
session_activities = all_activities \
    .window(
        Session \
            .with_gap(lit(10).seconds) \
            .on(col("activity_ts")) \
            .alias("w")
    ) \
    .group_by(
        col("w"),
        col("user_id")
    ) \
    .select(
        col("user_id"),
        col("w").start.alias("session_start"),
        col("w").end.alias("session_end"),
        if_then_else(col("activity_type") == "checkout", 1, 0).sum.alias("num_checkouts"),
        if_then_else(col("activity_type") == "checkout", col("activity_id"), "null").list_agg(",").alias("list_checkouts"),
        if_then_else(col("activity_type") == "click", 1, 0).sum.alias("num_clicks"),
        if_then_else(col("activity_type") == "click", col("activity_id"), "null").list_agg(",").alias("list_clicks"))
    

# Sink
session_sink_schema = Schema.new_builder() \
    .column("user_id", DataTypes.STRING()) \
    .column("session_start", DataTypes.TIMESTAMP(3)) \
    .column("session_end", DataTypes.TIMESTAMP(3)) \
    .column("num_checkouts", DataTypes.BIGINT()) \
    .column("list_checkouts", DataTypes.STRING()) \
    .column("num_clicks", DataTypes.BIGINT()) \
    .column("list_clicks", DataTypes.STRING()) \
    .build()

iceberg_sink = TableDescriptor.for_connector("iceberg") \
    .schema(session_sink_schema) \
    .option("catalog-name", "nessie_catalog") \
    .option("catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .option("uri", "http://nessie:19120/api/v1") \
    .option("ref", "main") \
    .option("warehouse", "s3://lakehouse") \
    .option("io-impl"             , "org.apache.iceberg.aws.s3.S3FileIO") \
    .option("s3.endpoint", "http://minio:9000") \
    .option("s3.access-key-id", "admin") \
    .option("s3.secret-access-key", "admin123") \
    .option("s3.path-style-access", "true") \
    .option("client.region", "us-east-1") \
    .build()


t_env.execute_sql('''CREATE DATABASE IF NOT EXISTS session_activities;''')
t_env.execute_sql('''USE session_activities;''')
t_env.create_table("session_activities", iceberg_sink)
session_activities.execute_insert("session_activities")