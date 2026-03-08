# Ví dụ: Flink Table API

Các ví dụ thực tế sử dụng **Table API** — viết logic giống SQL nhưng dưới dạng **Python code** (method chaining), có type-safety và dễ refactor.

> **Khi nào dùng Table API thay SQL?**
> - Cần xây dựng query **động** (dynamic filters, conditional columns)
> - Muốn **tái sử dụng** logic qua functions/modules
> - Cần IDE hỗ trợ **autocomplete** và **type-checking**

---

## Mục lục

1. [Setup chung](#1-setup-chung)
2. [Ví dụ 1: Đọc Kafka → Print (Debug)](#2-ví-dụ-1-đọc-kafka--print-debug)
3. [Ví dụ 2: Filter + Select + Rename](#3-ví-dụ-2-filter--select--rename)
4. [Ví dụ 3: Computed Columns + Type Casting](#4-ví-dụ-3-computed-columns--type-casting)
5. [Ví dụ 4: Group By + Aggregation](#5-ví-dụ-4-group-by--aggregation)
6. [Ví dụ 5: Dynamic Query (xây filter động)](#6-ví-dụ-5-dynamic-query-xây-filter-động)
7. [Ví dụ 6: Mix SQL + Table API](#7-ví-dụ-6-mix-sql--table-api)

---

## 1. Setup chung

```python
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col, lit, call

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60000)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

t_env = StreamTableEnvironment.create(env)
```

> [!NOTE]
> Import `col`, `lit`, `call` từ `pyflink.table.expressions` — đây là 3 building blocks cốt lõi của Table API.
>
> | Function | Ý nghĩa | Tương đương SQL |
> |----------|---------|----------------|
> | `col("name")` | Tham chiếu cột | `name` |
> | `lit(value)` | Giá trị hằng số | `'value'` hoặc `123` |
> | `call("FN", args)` | Gọi built-in function | `FN(args)` |

---

## 2. Ví dụ 1: Đọc Kafka → Print (Debug)

> **Mục đích**: Dùng Table API để đọc và in data ra console.

```python
# Tạo source table bằng SQL (Table API không có cú pháp CREATE TABLE)
t_env.execute_sql("""
    CREATE TABLE kafka_clicks (
        click_id    STRING,
        user_id     STRING,
        product_id  STRING,
        url         STRING,
        user_agent  STRING,
        ip          STRING,
        event_time  DOUBLE
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'clicks',
        'properties.bootstrap.servers' = 'broker-1:29092,broker-2:29093,broker-3:29094',
        'properties.group.id'          = 'flink-table-debug',
        'scan.startup.mode'            = 'earliest-offset',
        'format'                       = 'avro-confluent',
        'avro-confluent.url'           = 'http://schema-registry:8081'
    )
""")

# Lấy Table object từ catalog
clicks = t_env.from_path("kafka_clicks")

# In ra console (blocking — dùng để debug)
clicks.execute().print()
```

**Output trên console:**
```
+----+----------+---------+------------+-----+------------+--------+------------+
| op | click_id | user_id | product_id | url | user_agent |   ip   | event_time |
+----+----------+---------+------------+-----+------------+--------+------------+
| +I | abc-123  | user-1  | prod-42    | ... | Mozilla... | 1.2..  | 1709...    |
| +I | def-456  | user-2  | prod-17    | ... | Chrome...  | 3.4..  | 1709...    |
```

> `+I` = Insert (thêm mới), `+U` = Update After, `-U` = Update Before, `-D` = Delete

---

## 3. Ví dụ 2: Filter + Select + Rename

> **Mục đích**: Lọc records và chọn cột cần thiết — tương đương `SELECT ... WHERE ...`

```python
clicks = t_env.from_path("kafka_clicks")

# ---- Filter + Select ----
cleaned = (clicks
    .filter(col("ip").is_not_null)                 # WHERE ip IS NOT NULL
    .filter(col("user_id") != lit(""))             # AND user_id != ''
    .select(
        col("click_id"),
        col("user_id"),
        col("product_id"),
        col("event_time").alias("click_epoch"),    # AS click_epoch
    )
)

# In kết quả để kiểm tra
cleaned.execute().print()
```

### Bảng so sánh operators:

| Table API | SQL tương đương |
|-----------|----------------|
| `.filter(col("x").is_not_null)` | `WHERE x IS NOT NULL` |
| `.filter(col("x") == lit("a"))` | `WHERE x = 'a'` |
| `.filter(col("x") > lit(100))` | `WHERE x > 100` |
| `.select(col("a"), col("b"))` | `SELECT a, b` |
| `.select(col("a").alias("new_name"))` | `SELECT a AS new_name` |
| `.filter(cond1 & cond2)` | `WHERE cond1 AND cond2` |
| `.filter(cond1 \| cond2)` | `WHERE cond1 OR cond2` |

---

## 4. Ví dụ 3: Computed Columns + Type Casting

> **Mục đích**: Thêm cột tính toán, chuyển kiểu dữ liệu — xử lý Silver layer.

```python
# Giả sử đã tạo kafka_checkouts table bằng SQL
checkouts = t_env.from_path("kafka_checkouts")

# ---- Transform: Clean + Enrich ----
silver_checkouts = (checkouts
    # Lọc null
    .filter(col("checkout_id").is_not_null)
    .filter(col("total_amount").is_not_null)

    # Chọn và transform cột
    .select(
        col("checkout_id"),
        col("user_id"),
        col("product_id"),

        # Chuẩn hóa text: UPPER(TRIM(payment_method))
        call("UPPER",
            call("TRIM", col("payment_method"))
        ).alias("payment_method"),

        # Chuyển string → decimal: CAST(REPLACE(amount, '$', '') AS DECIMAL)
        call("REGEXP_REPLACE", col("total_amount"), lit("[$,]"), lit(""))
            .cast(DataTypes.DECIMAL(10, 2))
            .alias("total_amount"),

        # Epoch → Timestamp
        call("TO_TIMESTAMP",
            call("FROM_UNIXTIME",
                col("event_time").cast(DataTypes.BIGINT())
            )
        ).alias("event_timestamp"),
    )
)

# In kết quả
silver_checkouts.execute().print()
```

> [!NOTE]
> Để dùng `.cast()`, cần import thêm:
> ```python
> from pyflink.table.types import DataTypes
> ```

---

## 5. Ví dụ 4: Group By + Aggregation

> **Mục đích**: Đếm clicks theo product — tương đương `GROUP BY ... COUNT(*)`.

```python
clicks = t_env.from_path("kafka_clicks")

# ---- Aggregation ----
product_stats = (clicks
    .group_by(col("product_id"))
    .select(
        col("product_id"),
        col("click_id").count.alias("total_clicks"),
        col("event_time").max.alias("last_click_time"),
    )
)

product_stats.execute().print()
```

### Các hàm aggregate có sẵn:

| Table API | SQL tương đương |
|-----------|----------------|
| `col("x").count` | `COUNT(x)` |
| `col("x").sum` | `SUM(x)` |
| `col("x").avg` | `AVG(x)` |
| `col("x").min` | `MIN(x)` |
| `col("x").max` | `MAX(x)` |

---

## 6. Ví dụ 5: Dynamic Query (xây filter động)

> **Mục đích**: Xây dựng query **dựa trên config** — thế mạnh chính của Table API so với SQL.

```python
# ---- Config (có thể đọc từ file, env var, database...) ----
config = {
    "filter_null_ip": True,
    "min_event_time": 1709000000.0,
    "selected_columns": ["click_id", "user_id", "product_id", "event_time"],
}

# ---- Xây query động ----
clicks = t_env.from_path("kafka_clicks")

# Bước 1: Apply filters động
if config["filter_null_ip"]:
    clicks = clicks.filter(col("ip").is_not_null)

if config["min_event_time"]:
    clicks = clicks.filter(col("event_time") > lit(config["min_event_time"]))

# Bước 2: Select columns động
selected_cols = [col(c) for c in config["selected_columns"]]
result = clicks.select(*selected_cols)

# Bước 3: In hoặc ghi ra sink
result.execute().print()
```

> [!TIP]
> Đây là lý do chính để chọn Table API thay SQL:
> - **SQL**: Phải dùng f-string để ghép query → dễ lỗi SQL injection, khó debug
> - **Table API**: Dùng Python logic bình thường → an toàn, dễ test, dễ maintain

---

## 7. Ví dụ 6: Mix SQL + Table API

> **Mục đích**: Dùng SQL để tạo source/sink, Table API để viết transform logic.

```python
# ---- Phần SQL: Tạo source + sink ----
t_env.execute_sql("""
    CREATE TABLE kafka_clicks (...) WITH ('connector' = 'kafka', ...)
""")

t_env.execute_sql("""
    CREATE TABLE print_output (
        user_id     STRING,
        click_count BIGINT
    ) WITH ('connector' = 'print')
""")

# ---- Phần Table API: Transform logic ----
clicks = t_env.from_path("kafka_clicks")

result = (clicks
    .filter(col("ip").is_not_null)
    .group_by(col("user_id"))
    .select(
        col("user_id"),
        col("click_id").count.alias("click_count"),
    )
)

# ---- Ghi kết quả vào sink đã tạo bằng SQL ----
result.execute_insert("print_output")
```

> [!NOTE]
> `execute_insert("table_name")` = ghi `Table` object vào một sink table đã đăng ký. Đây là cầu nối giữa Table API và SQL sink.
