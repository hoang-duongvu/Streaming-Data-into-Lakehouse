# Ví dụ: Flink SQL API

Các ví dụ thực tế sử dụng **Flink SQL** — API đơn giản nhất, phù hợp ~80% use cases.
Tất cả ví dụ dùng data từ project (clicks & checkouts topics).

---

## Mục lục

1. [Setup chung](#1-setup-chung)
2. [Ví dụ 1: Bronze Ingestion — Kafka → Print (Debug)](#2-ví-dụ-1-bronze-ingestion--kafka--print-debug)
3. [Ví dụ 2: Bronze Ingestion — Kafka → Iceberg](#3-ví-dụ-2-bronze-ingestion--kafka--iceberg)
4. [Ví dụ 3: Silver Transform — Cleaning & Enrichment](#4-ví-dụ-3-silver-transform--cleaning--enrichment)
5. [Ví dụ 4: Aggregation — Đếm clicks theo user](#5-ví-dụ-4-aggregation--đếm-clicks-theo-user)
6. [Ví dụ 5: Windowed Aggregation — Doanh thu theo giờ](#6-ví-dụ-5-windowed-aggregation--doanh-thu-theo-giờ)
7. [Ví dụ 6: JOIN hai streams — Clicks + Checkouts](#7-ví-dụ-6-join-hai-streams--clicks--checkouts)

---

## 1. Setup chung

Mọi ví dụ SQL đều bắt đầu với cùng một setup:

```python
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment

# ---- Tạo Environment ----
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60000)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

t_env = StreamTableEnvironment.create(env)
```

---

## 2. Ví dụ 1: Bronze Ingestion — Kafka → Print (Debug)

> **Mục đích**: Đọc data từ Kafka và **in ra console** để kiểm tra trước khi ghi vào Iceberg.

```python
# ---- Source: Kafka clicks ----
t_env.execute_sql("""
    CREATE TABLE kafka_clicks (
        click_id    STRING,
        user_id     STRING,
        product_id  STRING,
        url         STRING,
        user_agent  STRING,
        ip          STRING,
        event_time  DOUBLE,
        `partition` INT    METADATA FROM 'partition' VIRTUAL,
        `offset`    BIGINT METADATA FROM 'offset' VIRTUAL
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'clicks',
        'properties.bootstrap.servers' = 'broker-1:29092,broker-2:29093,broker-3:29094',
        'properties.group.id'          = 'flink-debug-clicks',
        'scan.startup.mode'            = 'earliest-offset',
        'format'                       = 'avro-confluent',
        'avro-confluent.url'           = 'http://schema-registry:8081'
    )
""")

# ---- Sink: Print (console output) ----
t_env.execute_sql("""
    CREATE TABLE print_clicks (
        click_id    STRING,
        user_id     STRING,
        product_id  STRING,
        url         STRING,
        user_agent  STRING,
        ip          STRING,
        event_time  DOUBLE
    ) WITH (
        'connector' = 'print'
    )
""")

# ---- Logic: đọc từ Kafka, in ra console ----
t_env.execute_sql("""
    INSERT INTO print_clicks
    SELECT
        click_id, user_id, product_id,
        url, user_agent, ip, event_time
    FROM kafka_clicks
""")
```

**Xem output:**
```bash
docker logs -f <flink-taskmanager-container>
```

> [!TIP]
> Luôn dùng `print` connector để debug **trước** khi đổi sang Iceberg sink. Tiết kiệm thời gian fix lỗi schema rất nhiều.

---

## 3. Ví dụ 2: Bronze Ingestion — Kafka → Iceberg

> **Mục đích**: Ingest raw data từ Kafka vào **Bronze layer** (Iceberg trên MinIO).

```python
# ---- Source: Kafka clicks (giống ví dụ 1) ----
t_env.execute_sql("""
    CREATE TABLE kafka_clicks (
        click_id    STRING,
        user_id     STRING,
        product_id  STRING,
        url         STRING,
        user_agent  STRING,
        ip          STRING,
        event_time  DOUBLE,
        `partition` INT    METADATA FROM 'partition' VIRTUAL,
        `offset`    BIGINT METADATA FROM 'offset' VIRTUAL
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'clicks',
        'properties.bootstrap.servers' = 'broker-1:29092,broker-2:29093,broker-3:29094',
        'properties.group.id'          = 'flink-bronze-clicks',
        'scan.startup.mode'            = 'earliest-offset',
        'format'                       = 'avro-confluent',
        'avro-confluent.url'           = 'http://schema-registry:8081'
    )
""")

# ---- Đăng ký Iceberg Catalog ----
t_env.execute_sql("""
    CREATE CATALOG lakehouse WITH (
        'type'                 = 'iceberg',
        'catalog-impl'         = 'org.apache.iceberg.nessie.NessieCatalog',
        'uri'                  = 'http://nessie:19120/api/v1',
        'ref'                  = 'main',
        'warehouse'            = 's3a://lakehouse',
        's3.endpoint'          = 'http://minio:9000',
        's3.access-key'        = 'minio_access_key',
        's3.secret-key'        = 'minio_secret_key',
        's3.path-style-access' = 'true'
    )
""")

# ---- Tạo Database + Sink Table ----
t_env.execute_sql("USE CATALOG lakehouse")
t_env.execute_sql("CREATE DATABASE IF NOT EXISTS bronze")
t_env.execute_sql("USE bronze")

t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS clicks (
        click_id        STRING,
        user_id         STRING,
        product_id      STRING,
        url             STRING,
        user_agent      STRING,
        ip              STRING,
        event_time      DOUBLE,
        ingestion_time  TIMESTAMP(3),
        kafka_partition INT,
        kafka_offset    BIGINT
    )
""")

# ---- Logic: Ingest với metadata bổ sung ----
t_env.execute_sql("""
    INSERT INTO bronze.clicks
    SELECT
        click_id,
        user_id,
        product_id,
        url,
        user_agent,
        ip,
        event_time,
        CURRENT_TIMESTAMP   AS ingestion_time,
        `partition`         AS kafka_partition,
        `offset`            AS kafka_offset
    FROM default_catalog.default_database.kafka_clicks
""")
```

> [!IMPORTANT]
> Source table nằm ở `default_catalog` nhưng sink table nằm ở `lakehouse` catalog. Phải dùng đường dẫn đầy đủ `default_catalog.default_database.kafka_clicks` khi SELECT.

---

## 4. Ví dụ 3: Silver Transform — Cleaning & Enrichment

> **Mục đích**: Đọc từ Bronze Iceberg → clean data → ghi vào **Silver layer**.

```python
# Giả sử đã USE CATALOG lakehouse

# ---- Tạo Silver table ----
t_env.execute_sql("CREATE DATABASE IF NOT EXISTS silver")

t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS silver.cleaned_checkouts (
        checkout_id      STRING,
        user_id          STRING,
        product_id       STRING,
        payment_method   STRING,
        total_amount     DECIMAL(10, 2),
        shipping_address STRING,
        event_timestamp  TIMESTAMP(3),
        process_date     STRING
    )
""")

# ---- Logic: Clean + Transform ----
t_env.execute_sql("""
    INSERT INTO silver.cleaned_checkouts
    SELECT
        checkout_id,
        user_id,
        product_id,

        -- Chuẩn hóa payment method
        UPPER(TRIM(payment_method))          AS payment_method,

        -- Chuyển string → decimal, xử lý ký tự đặc biệt
        CAST(
            REGEXP_REPLACE(total_amount, '[$,]', '')
            AS DECIMAL(10, 2)
        )                                    AS total_amount,

        -- Xử lý null
        COALESCE(shipping_address, 'N/A')    AS shipping_address,

        -- Chuyển epoch (double) → timestamp
        TO_TIMESTAMP(
            FROM_UNIXTIME(CAST(event_time AS BIGINT))
        )                                    AS event_timestamp,

        -- Tạo partition key (theo ngày)
        DATE_FORMAT(
            TO_TIMESTAMP(FROM_UNIXTIME(CAST(event_time AS BIGINT))),
            'yyyy-MM-dd'
        )                                    AS process_date

    FROM bronze.checkouts
    WHERE checkout_id IS NOT NULL
      AND total_amount IS NOT NULL
""")
```

### Tổng hợp các hàm transform hay dùng:

| Hàm | Tác dụng | Ví dụ |
|-----|----------|-------|
| `CAST(x AS TYPE)` | Chuyển kiểu | `CAST('123' AS INT)` |
| `COALESCE(a, b)` | Giá trị mặc định nếu null | `COALESCE(ip, 'unknown')` |
| `UPPER()` / `LOWER()` / `TRIM()` | Chuẩn hóa chuỗi | `UPPER(TRIM(name))` |
| `REGEXP_REPLACE()` | Thay thế bằng regex | `REGEXP_REPLACE(price, '[$]', '')` |
| `TO_TIMESTAMP(FROM_UNIXTIME())` | Epoch → Timestamp | Xem ví dụ trên |
| `DATE_FORMAT()` | Timestamp → String | `DATE_FORMAT(ts, 'yyyy-MM-dd')` |
| `CURRENT_TIMESTAMP` | Thời điểm xử lý | Dùng cho `ingestion_time` |

---

## 5. Ví dụ 4: Aggregation — Đếm clicks theo user

> **Mục đích**: Đếm tổng số clicks của mỗi user (continuous query).

```python
# ---- Sink: Bảng aggregation ----
t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS silver.user_click_counts (
        user_id      STRING,
        total_clicks BIGINT,
        last_click   DOUBLE,
        PRIMARY KEY (user_id) NOT ENFORCED
    )
""")

# ---- Logic: Group by + Aggregate ----
t_env.execute_sql("""
    INSERT INTO silver.user_click_counts
    SELECT
        user_id,
        COUNT(*)          AS total_clicks,
        MAX(event_time)   AS last_click
    FROM default_catalog.default_database.kafka_clicks
    GROUP BY user_id
""")
```

> [!NOTE]
> Aggregation trên unbounded stream sẽ **liên tục cập nhật** kết quả. Sink phải hỗ trợ **upsert** (ghi đè theo primary key). Iceberg hỗ trợ upsert mode.

---

## 6. Ví dụ 5: Windowed Aggregation — Doanh thu theo giờ

> **Mục đích**: Tính tổng doanh thu checkout **theo từng khung 1 giờ** (Tumble Window).

```python
# ---- Source: Kafka checkouts với watermark ----
t_env.execute_sql("""
    CREATE TABLE kafka_checkouts (
        checkout_id      STRING,
        user_id          STRING,
        product_id       STRING,
        payment_method   STRING,
        total_amount     STRING,
        shipping_address STRING,
        billing_address  STRING,
        user_agent       STRING,
        ip_address       STRING,
        event_time       DOUBLE,

        -- Khai báo event time + watermark (cho phép trễ 5 giây)
        event_ts AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(event_time AS BIGINT))),
        WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'checkouts',
        'properties.bootstrap.servers' = 'broker-1:29092,broker-2:29093,broker-3:29094',
        'properties.group.id'          = 'flink-window-checkouts',
        'scan.startup.mode'            = 'earliest-offset',
        'format'                       = 'avro-confluent',
        'avro-confluent.url'           = 'http://schema-registry:8081'
    )
""")

# ---- Sink: Doanh thu theo giờ ----
t_env.execute_sql("""
    CREATE TABLE print_hourly_revenue (
        window_start      TIMESTAMP(3),
        window_end        TIMESTAMP(3),
        payment_method    STRING,
        total_revenue     DECIMAL(12, 2),
        order_count       BIGINT
    ) WITH (
        'connector' = 'print'
    )
""")

# ---- Logic: Tumble Window 1 giờ ----
t_env.execute_sql("""
    INSERT INTO print_hourly_revenue
    SELECT
        TUMBLE_START(event_ts, INTERVAL '1' HOUR)   AS window_start,
        TUMBLE_END(event_ts, INTERVAL '1' HOUR)     AS window_end,
        payment_method,
        SUM(CAST(
            REGEXP_REPLACE(total_amount, '[$,]', '')
            AS DECIMAL(12, 2)
        ))                                           AS total_revenue,
        COUNT(*)                                     AS order_count
    FROM kafka_checkouts
    GROUP BY
        TUMBLE(event_ts, INTERVAL '1' HOUR),
        payment_method
""")
```

### Các loại Window trong Flink SQL:

| Window | Ý nghĩa | Ví dụ |
|--------|---------|-------|
| `TUMBLE(col, INTERVAL)` | Cửa sổ cố định, không chồng lấp | Mỗi 1 giờ |
| `HOP(col, slide, size)` | Cửa sổ trượt, có chồng lấp | 15 phút slide, 1 giờ size |
| `SESSION(col, gap)` | Cửa sổ theo phiên, kết thúc khi im lặng | Gap 30 phút |

---

## 7. Ví dụ 6: JOIN hai streams — Clicks + Checkouts

> **Mục đích**: JOIN click events với checkout events theo `user_id` + `product_id` trong **khung 30 phút** → xem user click rồi có mua không.

```python
# Giả sử đã tạo kafka_clicks và kafka_checkouts (có watermark)

# ---- Sink: Click-to-Checkout funnel ----
t_env.execute_sql("""
    CREATE TABLE print_funnel (
        user_id         STRING,
        product_id      STRING,
        click_time      TIMESTAMP(3),
        checkout_time   TIMESTAMP(3),
        time_to_buy_sec BIGINT,
        total_amount    STRING
    ) WITH (
        'connector' = 'print'
    )
""")

# ---- Logic: Interval JOIN (click rồi checkout trong 30 phút) ----
t_env.execute_sql("""
    INSERT INTO print_funnel
    SELECT
        c.user_id,
        c.product_id,
        c.event_ts                                         AS click_time,
        co.event_ts                                        AS checkout_time,
        TIMESTAMPDIFF(SECOND, c.event_ts, co.event_ts)     AS time_to_buy_sec,
        co.total_amount
    FROM kafka_clicks AS c
    JOIN kafka_checkouts AS co
        ON  c.user_id    = co.user_id
        AND c.product_id = co.product_id
        AND co.event_ts BETWEEN c.event_ts
                         AND     c.event_ts + INTERVAL '30' MINUTE
""")
```

> [!NOTE]
> **Interval JOIN** yêu cầu cả 2 bảng phải có **watermark** (khai báo `WATERMARK FOR` trong `CREATE TABLE`). Nếu thiếu watermark, Flink sẽ báo lỗi.
