from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.window import Tumble
from pyflink.table.expressions import col, lit

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10000)
t_env = StreamTableEnvironment.create(env)

# Source
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
    .watermark("event_ts", "event_ts - INTERVAL '5' SECOND") \
    .build()

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

t_env.create_temporary_table("kafka_clicks", clicks_descriptor)
clicks = t_env.from_path("kafka_clicks")

# Window Agg
clicks_5m = clicks \
    .window(
        Tumble \
            .over(lit(5).second) \
            .on(col("event_ts")) \
            .alias("w")) \
    .group_by(
        col("w"),
        col("product_id"))\
    .select(
        col("product_id"),
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("click_id").count.alias("num_clicks")
    )

# Sink
t_env.execute_sql('''CREATE DATABASE IF NOT EXISTS window_agg_db;''')
t_env.execute_sql('''USE window_agg_db;''')

clicks_5m_schema = Schema.new_builder() \
    .column("product_id", DataTypes.STRING()) \
    .column("window_start", DataTypes.TIMESTAMP(3)) \
    .column("window_end", DataTypes.TIMESTAMP(3)) \
    .column("num_clicks", DataTypes.BIGINT()) \
    .build()

iceberg_sink = TableDescriptor.for_connector('iceberg') \
    .schema(clicks_5m_schema) \
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

t_env.create_table("products_clicks_5min", iceberg_sink)
clicks_5m.execute_insert("products_clicks_5min")
