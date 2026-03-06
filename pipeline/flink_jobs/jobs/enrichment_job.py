from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pathlib import Path

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10000)  # checkpoint mỗi 10 giây → Iceberg commit data files
t_env = StreamTableEnvironment.create(env)

flink_jobs_path = Path(__file__).parent.parent

with open(f"{flink_jobs_path}/source_tables/clicks_source.sql", "r") as f:
  clicks_sql = f.read()

with open(f"{flink_jobs_path}/source_tables/checkouts_source.sql", "r") as f:
  checkouts_sql = f.read()

with open(f"{flink_jobs_path}/source_tables/postgres_users.sql", "r") as f:
  users_sql = f.read()

with open(f"{flink_jobs_path}/source_tables/postgres_products.sql", "r") as f:
  products_sql = f.read()

with open(f"{flink_jobs_path}/sink_tables/enriched_users.sql", "r") as f:
  sink_users_sql = f.read()

t_env.execute_sql(clicks_sql)
t_env.execute_sql(checkouts_sql)
t_env.execute_sql(users_sql)
t_env.execute_sql(products_sql)

t_env.execute_sql(
'''CREATE CATALOG nessie_catalog WITH (
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
);''')

t_env.execute_sql('''USE CATALOG nessie_catalog;''')

t_env.execute_sql('''CREATE DATABASE IF NOT EXISTS enriched_tables;''')
t_env.execute_sql('''USE enriched_tables;''')

t_env.execute_sql('''
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
);''')

enriched_clicks = t_env.sql_query('''
  SELECT kc.click_id, kc.user_id, kc.product_id, kc.product_url,
         kc.user_agent, kc.ip, kc.event_time,
         kc.enqueued_at, kc.partition_id, kc.`offset`, kc.topic_id,
         u.username, u.password 
  FROM default_catalog.default_database.kafka_clicks kc 
  JOIN default_catalog.default_database.postgres_user FOR SYSTEM_TIME AS OF kc.proc_time AS u
  ON kc.user_id = u.user_id
'''
)

enriched_clicks.execute_insert("nessie_catalog.enriched_tables.enriched_clicks")