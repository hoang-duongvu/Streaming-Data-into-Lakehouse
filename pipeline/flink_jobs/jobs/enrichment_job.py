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

with open(f"{flink_jobs_path}/source_tables/postgres_products.sql", "r") as f:
  products_sql = f.read()

with open(f"{flink_jobs_path}/sink_tables/enriched_clicks.sql", "r") as f:
  enriched_clicks_sql = f.read()

with open(f"{flink_jobs_path}/sink_tables/enriched_checkouts.sql", "r") as f:
  enriched_checkouts_sql = f.read()

with open(f"{flink_jobs_path}/sink_tables/nessie_catalog.sql", "r") as f:
  nessie_catalog_sql = f.read()


t_env.execute_sql(clicks_sql)
t_env.execute_sql(checkouts_sql)
t_env.execute_sql(products_sql)

t_env.execute_sql(nessie_catalog_sql)

t_env.execute_sql('''USE CATALOG nessie_catalog;''')

t_env.execute_sql('''CREATE DATABASE IF NOT EXISTS enriched_tables;''')
t_env.execute_sql('''USE enriched_tables;''')

t_env.execute_sql(enriched_clicks_sql)

t_env.execute_sql(enriched_checkouts_sql)

enriched_clicks = t_env.sql_query('''
  SELECT kc.*,
        p.product_name, p.description AS product_description, p.price AS product_price
  FROM default_catalog.default_database.kafka_clicks kc 
  JOIN default_catalog.default_database.postgres_products FOR SYSTEM_TIME AS OF kc.proc_time AS p
  ON kc.product_id = p.product_id
'''
)

enriched_checkouts = t_env.sql_query(
  '''
  SELECT kct.*,
        p.product_name, p.description AS product_description, p.price AS product_price
  FROM default_catalog.default_database.kafka_checkouts kct 
  JOIN default_catalog.default_database.postgres_products FOR SYSTEM_TIME AS OF kct.proc_time AS p
  ON kct.product_id = p.product_id
'''
)

statement_set = t_env.create_statement_set()
statement_set.add_insert("nessie_catalog.enriched_tables.enriched_clicks", enriched_clicks)
statement_set.add_insert("nessie_catalog.enriched_tables.enriched_checkouts", enriched_checkouts)
statement_set.execute()