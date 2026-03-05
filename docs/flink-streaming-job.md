# Hướng dẫn: Viết Flink Streaming Job (PyFlink)

Hướng dẫn từng bước cách viết một Flink Streaming Job bằng Python (PyFlink), đọc dữ liệu Avro từ Kafka và ghi vào Iceberg tables trên Lakehouse.

---

## Mục lục

1. [Tổng quan: Flink Streaming Job là gì?](#1-tổng-quan-flink-streaming-job-là-gì)
2. [Step 1 – Chuẩn bị môi trường](#2-step-1--chuẩn-bị-môi-trường)
3. [Step 2 – Tạo StreamExecutionEnvironment](#3-step-2--tạo-streamexecutionenvironment)
4. [Step 3 – Cấu hình TableEnvironment](#4-step-3--cấu-hình-tableenvironment)
5. [Step 4 – Khai báo Source Table (Kafka)](#5-step-4--khai-báo-source-table-kafka)
6. [Step 5 – Khai báo Sink Table (Iceberg)](#6-step-5--khai-báo-sink-table-iceberg)
7. [Step 6 – Viết logic xử lý (INSERT INTO ... SELECT)](#7-step-6--viết-logic-xử-lý-insert-into--select)
8. [Step 7 – Submit job lên Flink cluster](#8-step-7--submit-job-lên-flink-cluster)
9. [Tổng hợp: Template hoàn chỉnh](#9-tổng-hợp-template-hoàn-chỉnh)
10. [Checklist ghi nhớ nhanh](#10-checklist-ghi-nhớ-nhanh)

---

## 1. Tổng quan: Flink Streaming Job là gì?

Một Flink Streaming Job là một **chương trình chạy liên tục** (long-running), đọc dữ liệu từ source (Kafka), xử lý, và ghi vào sink (Iceberg/MinIO).

```
╔════════════════════════════════════════════════════════════════╗
║                      FLINK STREAMING JOB                      ║
║                                                                ║
║   ┌──────────┐     ┌──────────────┐     ┌──────────────────┐  ║
║   │  SOURCE  │────▶│  PROCESSING  │────▶│      SINK        │  ║
║   │ (Kafka)  │     │  (Transform) │     │ (Iceberg/MinIO)  │  ║
║   └──────────┘     └──────────────┘     └──────────────────┘  ║
║                                                                ║
║   Chạy liên tục — đọc từng message — xử lý — ghi ra           ║
╚════════════════════════════════════════════════════════════════╝
```

### Cấu trúc 1 Flink Job luôn gồm 7 bước cố định:

| Bước | Tên | Mô tả ngắn |
|------|-----|-------------|
| 1 | **Chuẩn bị môi trường** | Cài JARs, Dockerfile |
| 2 | **Tạo StreamExecutionEnvironment** | Khởi tạo "runtime" cho Flink |
| 3 | **Cấu hình TableEnvironment** | Setup môi trường Table/SQL |
| 4 | **Khai báo Source Table** | Bảng đầu vào (từ Kafka) |
| 5 | **Khai báo Sink Table** | Bảng đầu ra (Iceberg trên MinIO) |
| 6 | **Viết logic xử lý** | `INSERT INTO sink SELECT ... FROM source` |
| 7 | **Submit job** | Gửi job lên Flink cluster để chạy |

> [!TIP]
> Nhớ công thức: **Env → Table → Source → Sink → Logic → Submit**

---

## 2. Step 1 – Chuẩn bị môi trường

Trước khi viết code, Flink cần có các **JAR connectors** để biết cách đọc/ghi các hệ thống bên ngoài. JARs giống như "driver" — không có thì Flink không kết nối được.

### 2.1. JARs cần thiết

| JAR | Vai trò | Cần cho |
|-----|---------|---------|
| `flink-sql-connector-kafka` | Đọc/ghi Kafka topics | Source table |
| `flink-sql-avro-confluent` | Deserialize Avro từ Schema Registry | Đọc message Avro |
| `iceberg-flink-runtime` | Đọc/ghi Iceberg tables | Sink table |
| `hadoop-common` + `hadoop-aws` | Ghi file lên S3/MinIO | MinIO storage |
| `aws-java-sdk-bundle` | S3 SDK cho MinIO | MinIO storage |

### 2.2. Cài JARs ở đâu?

JARs được thêm vào **Dockerfile** của Flink, tải về thư mục `/opt/flink/lib/`:

```dockerfile
# Ví dụ: thêm JAR vào Dockerfile
RUN wget -P /opt/flink/lib/ <URL_CỦA_JAR>
```

> [!WARNING]
> **Phiên bản JAR phải tương thích với phiên bản Flink** (project dùng Flink 1.17.1). Sai version sẽ gây lỗi `ClassNotFoundException`.

### 2.3. Cách tìm JAR đúng version

1. Lên [Maven Central](https://repo.maven.apache.org/maven2/org/apache/flink/)
2. Tìm tên JAR (ví dụ `flink-sql-connector-kafka`)
3. Chọn version khớp với Flink version: `1.17.1`
4. Copy URL link `.jar` để dùng trong Dockerfile

---

## 3. Step 2 – Tạo StreamExecutionEnvironment

Đây là **bước đầu tiên** trong mọi Flink job. `StreamExecutionEnvironment` (gọi tắt là `env`) là "runtime" để chạy streaming.

```python
from pyflink.datastream import StreamExecutionEnvironment

# Tạo execution environment
env = StreamExecutionEnvironment.get_execution_environment()
```

### Cấu hình quan trọng: Checkpoint

**Checkpoint** là cơ chế Flink **lưu trạng thái định kỳ** để phục hồi nếu job bị crash. Không có checkpoint = mất dữ liệu khi restart.

```python
from pyflink.datastream import CheckpointingMode

# Bật checkpoint mỗi 60 giây
env.enable_checkpointing(60000)  # đơn vị: milliseconds

# Chế độ exactly-once: đảm bảo mỗi record được xử lý đúng 1 lần
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
```

> **Tại sao cần checkpoint?**
> - Flink job chạy liên tục — nếu crash, cần biết đã xử lý đến đâu
> - Checkpoint = "save game" — restart từ điểm cuối thay vì xử lý lại từ đầu
> - **Exactly-once**: mỗi message được ghi đúng 1 lần, không duplicate

---

## 4. Step 3 – Cấu hình TableEnvironment

`TableEnvironment` cho phép bạn sử dụng **Flink SQL** — viết SQL thay vì code Python thuần.

```python
from pyflink.table import StreamTableEnvironment

# Tạo TableEnvironment từ StreamExecutionEnvironment
t_env = StreamTableEnvironment.create(env)
```

### Tại sao dùng Table API / SQL?

| Cách viết | Ưu điểm | Khi nào dùng |
|-----------|---------|--------------|
| **Flink SQL** (Table API) | Dễ đọc, viết nhanh, giống SQL truyền thống | Hầu hết trường hợp |
| **DataStream API** (Python thuần) | Linh hoạt hơn, custom logic | Xử lý phức tạp |

Trong project này, chúng ta dùng **Flink SQL** vì phần lớn logic là đọc → transform → ghi — rất phù hợp với SQL.

---

## 5. Step 4 – Khai báo Source Table (Kafka)

Source table cho Flink biết **đọc dữ liệu từ đâu** và **định dạng dữ liệu như thế nào**.

### Cấu trúc câu lệnh

```sql
CREATE TABLE tên_source_table (
    -- Các cột dữ liệu (khớp với Avro schema)
    cột_1    KIỂU_DỮ_LIỆU,
    cột_2    KIỂU_DỮ_LIỆU,
    ...

    -- Metadata columns (thông tin từ Kafka)
    `partition`  INT    METADATA FROM 'partition' VIRTUAL,
    `offset`     BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
    'connector'                  = 'kafka',
    'topic'                      = 'tên_topic',
    'properties.bootstrap.servers' = 'broker1:9092,broker2:9092',
    'properties.group.id'        = 'flink-consumer-group',
    'scan.startup.mode'          = 'earliest-offset',
    'format'                     = 'avro-confluent',
    'avro-confluent.url'         = 'http://schema-registry:8081'
);
```

### Giải thích từng phần trong `WITH (...)`

| Thuộc tính | Ý nghĩa | Ghi nhớ |
|------------|---------|---------|
| `connector` | Loại kết nối | `'kafka'` = đọc từ Kafka |
| `topic` | Tên Kafka topic | Phải tồn tại trong Kafka |
| `properties.bootstrap.servers` | Địa chỉ Kafka brokers | Dùng tên container nếu chạy Docker |
| `properties.group.id` | Consumer group ID | Flink dùng để track offset |
| `scan.startup.mode` | Đọc từ đâu khi bắt đầu | `earliest-offset` = từ đầu, `latest-offset` = chỉ message mới |
| `format` | Định dạng message | `'avro-confluent'` = Avro với Schema Registry |
| `avro-confluent.url` | URL Schema Registry | Để deserialize Avro |

### Metadata Columns

```sql
`partition`  INT    METADATA FROM 'partition' VIRTUAL,
`offset`     BIGINT METADATA FROM 'offset' VIRTUAL,
`timestamp`  TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL
```

- Đây là thông tin **từ Kafka**, không phải từ message
- `VIRTUAL` = không ghi ra sink, chỉ đọc được
- Hữu ích cho debugging và audit

> [!NOTE]
> Các cột dữ liệu trong `CREATE TABLE` phải **khớp kiểu** với Avro schema đã đăng ký trong Schema Registry.

---

## 6. Step 5 – Khai báo Sink Table (Iceberg)

Sink table cho Flink biết **ghi dữ liệu ra đâu**. Trong project này, sink là Iceberg table trên MinIO, quản lý bởi Nessie catalog.

### Bước 6.1 – Đăng ký Iceberg Catalog

Trước khi tạo sink table, phải đăng ký **catalog** — cho Flink biết cách quản lý metadata của Iceberg tables.

```sql
CREATE CATALOG tên_catalog WITH (
    'type'          = 'iceberg',
    'catalog-impl'  = 'org.apache.iceberg.nessie.NessieCatalog',
    'uri'           = 'http://nessie:19120/api/v1',
    'ref'           = 'main',
    'warehouse'     = 's3a://lakehouse',

    -- Cấu hình MinIO (S3-compatible)
    's3.endpoint'   = 'http://minio:9000',
    's3.access-key' = 'minio_access_key',
    's3.secret-key' = 'minio_secret_key',
    's3.path-style-access' = 'true'
);
```

| Thuộc tính | Ý nghĩa |
|------------|---------|
| `type` | Loại catalog → `iceberg` |
| `catalog-impl` | Implementation class → dùng Nessie |
| `uri` | URL của Nessie server |
| `ref` | Branch trong Nessie (giống Git branch) |
| `warehouse` | Đường dẫn S3 chứa data files |
| `s3.*` | Cấu hình kết nối MinIO |

### Bước 6.2 – Tạo Sink Table

```sql
-- Chuyển sang dùng catalog vừa tạo
USE CATALOG tên_catalog;

CREATE DATABASE IF NOT EXISTS bronze;
USE bronze;

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
);
```

> [!IMPORTANT]
> Thứ tự quan trọng: **Tạo Catalog → Tạo Database → Tạo Table**. Nếu bỏ qua bước nào, Flink sẽ báo lỗi không tìm thấy catalog/database.

---

## 7. Step 6 – Viết logic xử lý (INSERT INTO ... SELECT)

Đây là **trái tim** của Flink job — câu lệnh SQL nói cho Flink cách xử lý dữ liệu.

### Công thức cơ bản

```sql
INSERT INTO sink_table
SELECT
    cột_1,
    cột_2,
    BIỂU_THỨC_TRANSFORM AS cột_mới,
    ...
FROM source_table;
```

### Ví dụ thực tế: Bronze Ingestion (Kafka → Iceberg)

```sql
INSERT INTO bronze.clicks
SELECT
    click_id,
    user_id,
    product_id,
    url,
    user_agent,
    ip,
    event_time,
    CURRENT_TIMESTAMP AS ingestion_time,
    `partition` AS kafka_partition,
    `offset` AS kafka_offset
FROM kafka_clicks;
```

### Các loại transform phổ biến trong Flink SQL

| Loại | Ví dụ | Khi nào dùng |
|------|-------|------------|
| **Type casting** | `CAST(event_time AS BIGINT)` | Chuyển đổi kiểu dữ liệu |
| **Timestamp conversion** | `TO_TIMESTAMP(FROM_UNIXTIME(epoch))` | Chuyển epoch → timestamp |
| **Null handling** | `COALESCE(ip, 'unknown')` | Set giá trị mặc định cho null |
| **String parsing** | `REGEXP_REPLACE(amount, '[$,]', '')` | Xử lý chuỗi |
| **Date formatting** | `DATE_FORMAT(ts, 'yyyy-MM-dd')` | Tạo partition key |
| **Windowed aggregation** | `TUMBLE(event_time, INTERVAL '1' HOUR)` | Tính tổng theo khung giờ |
| **Metadata enrichment** | `CURRENT_TIMESTAMP AS ingestion_time` | Thêm thông tin xử lý |

---

## 8. Step 7 – Submit job lên Flink cluster

Sau khi viết xong code, cần **submit job** lên Flink cluster để chạy.

### Cách 1: Dùng Flink CLI (khuyến nghị)

```bash
# Submit PyFlink job lên cluster
flink run \
    --jobmanager flink-jobmanager:8081 \
    --python /path/to/your_job.py
```

### Cách 2: Dùng Flink REST API

```bash
# Upload JAR hoặc Python file qua REST API
curl -X POST http://localhost:18081/jars/upload \
    -F "jarfile=@your_job.py"
```

### Cách 3: Qua Flink Web UI

1. Truy cập `http://localhost:18081`
2. Vào tab **Submit New Job**
3. Upload file Python
4. Click **Submit**

> [!TIP]
> Trong môi trường Docker, nên **mount volume** chứa file Python vào container Flink, sau đó dùng `docker exec` để chạy `flink run` từ bên trong container:
> ```bash
> docker exec -it flink-jobmanager flink run --python /opt/flink/jobs/your_job.py
> ```

---

## 9. Tổng hợp: Template hoàn chỉnh

Dưới đây là **template Python** hoàn chỉnh cho một Flink Streaming Job. Bạn chỉ cần thay đổi phần `CREATE TABLE` và `INSERT INTO ... SELECT` cho phù hợp với use case.

```python
# ============================================================
# TEMPLATE: Flink Streaming Job (PyFlink)
# ============================================================

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment

# ---- STEP 2: Tạo StreamExecutionEnvironment ----
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60000)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# ---- STEP 3: Tạo TableEnvironment ----
t_env = StreamTableEnvironment.create(env)

# ---- STEP 4: Khai báo Source Table (Kafka) ----
t_env.execute_sql("""
    CREATE TABLE kafka_source (
        -- Các cột dữ liệu (khớp Avro schema)
        ...
        -- Kafka metadata
        `partition`  INT    METADATA FROM 'partition' VIRTUAL,
        `offset`     BIGINT METADATA FROM 'offset' VIRTUAL
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'tên_topic',
        'properties.bootstrap.servers' = 'broker:9092',
        'properties.group.id'          = 'flink-group',
        'scan.startup.mode'            = 'earliest-offset',
        'format'                       = 'avro-confluent',
        'avro-confluent.url'           = 'http://schema-registry:8081'
    )
""")

# ---- STEP 5: Đăng ký Catalog + Sink Table (Iceberg) ----
t_env.execute_sql("""
    CREATE CATALOG iceberg_catalog WITH (
        'type'          = 'iceberg',
        'catalog-impl'  = 'org.apache.iceberg.nessie.NessieCatalog',
        'uri'           = 'http://nessie:19120/api/v1',
        'ref'           = 'main',
        'warehouse'     = 's3a://lakehouse',
        's3.endpoint'   = 'http://minio:9000',
        's3.access-key' = '...',
        's3.secret-key' = '...',
        's3.path-style-access' = 'true'
    )
""")

t_env.execute_sql("USE CATALOG iceberg_catalog")
t_env.execute_sql("CREATE DATABASE IF NOT EXISTS bronze")
t_env.execute_sql("USE bronze")

t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS target_table (
        -- Các cột dữ liệu
        ...
    )
""")

# ---- STEP 6: Viết logic xử lý ----
t_env.execute_sql("""
    INSERT INTO target_table
    SELECT
        ...
    FROM default_catalog.default_database.kafka_source
""")

# Flink streaming job tự chạy liên tục, không cần gọi env.execute()
# khi dùng Table API với execute_sql()
```

> [!IMPORTANT]
> Khi `INSERT INTO` sink table thuộc Iceberg catalog, mà source table thuộc default catalog, phải dùng **đường dẫn đầy đủ**: `default_catalog.default_database.kafka_source`. Nếu không, Flink sẽ tìm `kafka_source` trong Iceberg catalog và báo lỗi "table not found".

---

## 10. Checklist ghi nhớ nhanh

Mỗi khi viết một Flink Streaming Job mới, đi qua checklist này:

- [ ] **JARs**: Đã thêm đủ connectors vào Dockerfile? (Kafka, Avro, Iceberg, Hadoop, AWS SDK)
- [ ] **Checkpoint**: Đã bật checkpoint + chọn mode exactly-once?
- [ ] **Source Table**: Cột khớp với Avro schema? Format đúng (`avro-confluent`)? URL Schema Registry đúng?
- [ ] **Catalog**: Đã đăng ký Iceberg catalog với đầy đủ cấu hình Nessie + MinIO?
- [ ] **Sink Table**: Đã tạo database trước khi tạo table?
- [ ] **Logic SQL**: Source table dùng đường dẫn đầy đủ `default_catalog.default_database.tên_bảng`?
- [ ] **Docker networking**: Dùng **tên container** (không phải `localhost`) cho các kết nối?
- [ ] **JAR versions**: Tất cả JARs tương thích với phiên bản Flink (1.17.1)?

> [!TIP]
> **Quy trình ghi nhớ 6 chữ**: **E**nv → **T**able → **S**ource → **S**ink → **L**ogic → **S**ubmit (**ET-SS-LS**)
