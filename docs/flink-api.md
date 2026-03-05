# Hướng dẫn: 3 API của Apache Flink cho Stream Processing

Flink cung cấp **3 API** để viết streaming job. Mỗi API phù hợp với một mức độ phức tạp khác nhau. Hướng dẫn này giúp bạn hiểu **tư duy chọn đúng API** cho từng bài toán.

---

## Mục lục

1. [Hình dung tổng quan: 3 tầng API](#1-hình-dung-tổng-quan-3-tầng-api)
2. [Flink SQL — "Viết SQL, Flink lo phần còn lại"](#2-flink-sql--viết-sql-flink-lo-phần-còn-lại)
3. [Table API — "SQL nhưng viết bằng code"](#3-table-api--sql-nhưng-viết-bằng-code)
4. [DataStream API — "Toàn quyền kiểm soát"](#4-datastream-api--toàn-quyền-kiểm-soát)
5. [Tư duy chọn API đúng](#5-tư-duy-chọn-api-đúng)
6. [So sánh chi tiết 3 API](#6-so-sánh-chi-tiết-3-api)
7. [Mix API trong cùng một job](#7-mix-api-trong-cùng-một-job)

---

## 1. Hình dung tổng quan: 3 tầng API

Hãy tưởng tượng 3 API như **3 cách lái xe**:

```
┌─────────────────────────────────────────────────────────┐
│                                                         │
│   🚗 Flink SQL          = Xe tự lái (Autopilot)        │
│       → Bạn chỉ nói "đi đến đâu", xe tự tìm đường    │
│       → Ít kiểm soát, nhưng cực kỳ tiện                │
│                                                         │
│   🚙 Table API          = Xe số tự động                 │
│       → Bạn lái, nhưng hộp số tự xử lý                 │
│       → Linh hoạt hơn, vẫn dễ dùng                     │
│                                                         │
│   🏎️ DataStream API     = Xe số sàn (Manual)            │
│       → Toàn quyền kiểm soát mọi thứ                   │
│       → Mạnh nhất, nhưng phải biết nhiều hơn            │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

Về mặt kiến trúc, 3 API xếp chồng lên nhau:

```
┌───────────────────────────────┐  ← Dễ nhất, trừu tượng nhất
│          Flink SQL            │
├───────────────────────────────┤
│          Table API            │
├───────────────────────────────┤  ← Khó nhất, linh hoạt nhất
│       DataStream API          │
├───────────────────────────────┤
│    Flink Runtime (engine)     │  ← Tất cả đều chạy trên cùng engine
└───────────────────────────────┘
```

> [!NOTE]
> Cả 3 API đều biên dịch thành **cùng một execution plan** bên dưới. Hiệu năng tương đương — khác nhau chỉ ở **cách viết** và **mức độ kiểm soát**.

---

## 2. Flink SQL — "Viết SQL, Flink lo phần còn lại"

### Tư duy

Nếu bạn có thể diễn đạt logic bằng SQL → **dùng Flink SQL**. Đây là API được Flink khuyến nghị cho hầu hết use case.

### Cách hoạt động

Bạn viết các câu SQL dưới dạng **chuỗi string**, Flink parse và thực thi:

```python
t_env.execute_sql("""
    CREATE TABLE kafka_clicks (
        click_id STRING,
        user_id  STRING,
        event_time DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic'     = 'clicks',
        'format'    = 'avro-confluent',
        ...
    )
""")

t_env.execute_sql("""
    INSERT INTO bronze.clicks
    SELECT click_id, user_id, event_time,
           CURRENT_TIMESTAMP AS ingestion_time
    FROM kafka_clicks
""")
```

### Khi nào dùng ✅

| Use case | Ví dụ |
|----------|-------|
| ETL pipeline | Kafka → transform → Iceberg |
| Aggregations | Đếm clicks theo giờ, tổng doanh thu |
| Filtering & enrichment | Lọc null, JOIN với bảng dimension |
| Window operations | Tumbling, sliding, session windows |
| CDC (Change Data Capture) | Đọc binlog từ MySQL/PostgreSQL |

### Khi nào KHÔNG nên dùng ❌

| Tình huống | Lý do |
|------------|-------|
| Logic phức tạp không diễn đạt được bằng SQL | Ví dụ: ML inference, complex state machine |
| Cần custom serialization/deserialization | SQL chỉ hỗ trợ các format có sẵn |
| Side outputs (1 input → nhiều output khác nhau) | SQL không hỗ trợ |

### Ưu và nhược điểm

| ✅ Ưu điểm | ❌ Nhược điểm |
|------------|--------------|
| Ai biết SQL đều viết được | Logic phức tạp → SQL dài, khó đọc |
| Flink tự tối ưu query plan | Không custom được low-level behavior |
| Tích hợp sẵn mọi connector (Kafka, Iceberg, JDBC...) | Debug khó hơn (lỗi runtime, không có stack trace rõ ràng) |
| Ít code nhất | Không hỗ trợ side output, custom state |

---

## 3. Table API — "SQL nhưng viết bằng code"

### Tư duy

Table API là **phiên bản "code hóa" của SQL**. Thay vì viết SQL string, bạn gọi các method trên object `Table`. Kết quả hoàn toàn tương đương SQL, nhưng có thêm lợi ích của code: **autocomplete, type-checking, dễ refactor**.

### Cách hoạt động

```python
from pyflink.table.expressions import col, lit, call

# Lấy table từ catalog
clicks = t_env.from_path("kafka_clicks")

# Transform bằng method chaining (giống pandas)
result = (clicks
    .filter(col("ip").is_not_null)                          # WHERE ip IS NOT NULL
    .select(                                                 # SELECT ...
        col("click_id"),
        col("user_id"),
        call("TO_TIMESTAMP",                                 # Gọi built-in function
             call("FROM_UNIXTIME", col("event_time"))
        ).alias("event_ts"),
    )
    .group_by(col("user_id"))                                # GROUP BY user_id
    .select(
        col("user_id"),
        col("click_id").count.alias("total_clicks")          # COUNT(click_id)
    )
)

# Ghi kết quả
result.execute_insert("silver_user_clicks")
```

### So sánh trực quan với SQL

| Table API | SQL tương đương |
|-----------|----------------|
| `.filter(col("ip").is_not_null)` | `WHERE ip IS NOT NULL` |
| `.select(col("a"), col("b"))` | `SELECT a, b` |
| `.group_by(col("user_id"))` | `GROUP BY user_id` |
| `col("amount").sum.alias("total")` | `SUM(amount) AS total` |
| `.join(other_table).where(...)` | `JOIN other_table ON ...` |
| `.order_by(col("ts").desc)` | `ORDER BY ts DESC` |

### Khi nào dùng ✅

| Use case | Lý do |
|----------|-------|
| Logic SQL nhưng muốn type-safety | IDE báo lỗi ngay khi viết sai tên cột |
| Tái sử dụng logic | Dễ tách thành functions, modules |
| Xây dựng query động (dynamic) | Ghép điều kiện filter bằng code, không phải f-string |
| Mix với SQL trong cùng job | Một phần viết SQL, phần khác viết Table API |

### Khi nào KHÔNG nên dùng ❌

| Tình huống | Lý do |
|------------|-------|
| Logic đơn giản → SQL ngắn hơn, dễ đọc hơn | Table API verbose hơn cho cùng logic |
| Cần low-level control | Giống SQL, Table API cũng không hỗ trợ custom state |

### Ưu và nhược điểm

| ✅ Ưu điểm | ❌ Nhược điểm |
|------------|--------------|
| Type-safe, IDE hỗ trợ tốt | Verbose hơn SQL cho logic đơn giản |
| Dễ refactor, tái sử dụng | Cần học thêm API (col, expressions...) |
| Có thể mix với SQL | Cùng giới hạn như SQL (không custom state) |
| Dễ xây query động | Ít tài liệu/examples hơn SQL và DataStream |

---

## 4. DataStream API — "Toàn quyền kiểm soát"

### Tư duy

Khi logic xử lý **quá phức tạp** để diễn đạt bằng SQL — ví dụ: machine learning, custom state management, complex event patterns — bạn cần DataStream API.

### Cách hoạt động

DataStream API làm việc với **từng record** (hoặc nhóm records), cho phép bạn viết **bất kỳ logic Python/Java nào**:

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction
from pyflink.common import Types

env = StreamExecutionEnvironment.get_execution_environment()

# Đọc từ source
stream = env.from_source(kafka_source, watermark_strategy, "Kafka")

# Transform bằng custom functions
result = (stream
    .map(ParseAvroMessage())             # 1. Parse message
    .filter(lambda x: x["ip"] is not None)  # 2. Lọc null
    .key_by(lambda x: x["user_id"])      # 3. Nhóm theo user
    .process(DetectFraudFunction())      # 4. Custom stateful logic
)

# Ghi ra sink
result.sink_to(iceberg_sink)

env.execute("Fraud Detection Job")
```

### Các operators chính (dễ nhớ)

Hãy nghĩ DataStream như một **đường ống nước** — data chảy qua từng bước:

```
Source → map() → filter() → key_by() → window() → reduce() → Sink
  │        │         │          │           │          │
  │     Biến đổi   Lọc bỏ    Nhóm lại   Chia theo  Tổng hợp
  │     từng       record     theo key   thời gian   kết quả
  │     record     không                  (window)
  ▼                cần
 Data
```

| Operator | Ý nghĩa | Ví dụ tương tự |
|----------|---------|----------------|
| `map(fn)` | Biến đổi mỗi record → 1 record mới | Excel: thêm cột công thức |
| `flat_map(fn)` | 1 record → 0 hoặc nhiều records | Split text thành words |
| `filter(fn)` | Giữ lại records thỏa điều kiện | SQL `WHERE` |
| `key_by(fn)` | Nhóm records theo key | SQL `GROUP BY` |
| `window(spec)` | Chia stream theo thời gian | "Mỗi 1 giờ, xử lý 1 lần" |
| `reduce(fn)` | Gộp nhiều records → 1 record | SQL `SUM`, `COUNT` |
| `process(fn)` | Custom logic với state | Fraud detection, session tracking |
| `union(stream)` | Gộp nhiều streams | SQL `UNION ALL` |
| `connect(stream)` | Kết nối 2 streams khác kiểu | Enrichment từ 2 nguồn |

### Khái niệm đặc biệt của DataStream API

#### 4.1. Keyed State — "Bộ nhớ riêng theo key"

```
Ví dụ: Đếm số click của mỗi user

User A: click → count=1, click → count=2, click → count=3
User B: click → count=1, click → count=2
                   ↑
          Mỗi user có bộ đếm RIÊNG — đó là keyed state
```

#### 4.2. Watermark — "Đồng hồ cho event time"

```
Dữ liệu streaming thường đến KHÔNG ĐÚNG THỨ TỰ:

Timeline:    1:00  1:01  1:02  1:03  1:04
Đến Flink:   1:00  1:02  1:01  1:03  1:04
                          ↑
                    Record đến trễ!

Watermark = cơ chế để Flink biết:
  "Tôi đã nhận đủ data đến thời điểm X, có thể xử lý rồi"
```

#### 4.3. Side Output — "1 input, nhiều output"

```
                    ┌──▶ Output chính (valid records)
Input Stream ──▶ Process ──▶ Side Output 1 (invalid records → dead letter)
                    └──▶ Side Output 2 (late records → retry queue)
```

Đây là tính năng **chỉ DataStream API mới có** — SQL và Table API không hỗ trợ.

### Khi nào dùng ✅

| Use case | Ví dụ |
|----------|-------|
| Custom stateful processing | Fraud detection, session analysis |
| Complex event processing | Pattern matching trên chuỗi events |
| Side outputs | Tách valid/invalid records |
| Custom windowing | Window logic không standard |
| ML inference trên stream | Gọi model cho mỗi record |
| Async I/O | Call external API không blocking |

### Khi nào KHÔNG nên dùng ❌

| Tình huống | Lý do |
|------------|-------|
| ETL đơn giản (đọc-transform-ghi) | SQL ngắn gọn và dễ maintain hơn nhiều |
| Aggregation chuẩn | SQL đã có sẵn `SUM`, `COUNT`, window functions |
| Team không quen Flink concepts | Keyed state, watermark, checkpoint... cần thời gian học |

### Ưu và nhược điểm

| ✅ Ưu điểm | ❌ Nhược điểm |
|------------|--------------|
| Toàn quyền kiểm soát logic | Code nhiều hơn SQL rất nhiều |
| Custom state, side output, async I/O | Cần hiểu sâu Flink concepts |
| Viết bất kỳ logic nào | Phải tự lo tối ưu performance |
| Debug dễ hơn (có stack trace rõ) | Cần hiểu serialization, state backend |

---

## 5. Tư duy chọn API đúng

### Quy tắc quyết định nhanh (Decision Flowchart)

```
Bài toán của bạn là gì?
│
├── Diễn đạt được bằng SQL?
│   ├── CÓ → Logic tĩnh, không cần thay đổi runtime?
│   │         ├── CÓ  → ✅ Dùng FLINK SQL
│   │         └── KHÔNG → Cần xây query động?
│   │                     ├── CÓ  → ✅ Dùng TABLE API
│   │                     └── KHÔNG → ✅ Dùng FLINK SQL
│   │
│   └── KHÔNG → Cần custom state / side output / async I/O?
│               ├── CÓ  → ✅ Dùng DATASTREAM API
│               └── KHÔNG → Thử viết SQL trước, nếu không được
│                           → ✅ Dùng DATASTREAM API
```

### Bảng quyết định theo use case

| Bài toán | API khuyến nghị | Lý do |
|----------|:---:|-------|
| Kafka → transform → Iceberg | **SQL** | ETL thuần, SQL là đủ |
| Đếm clicks theo giờ | **SQL** | `TUMBLE` window + `COUNT` |
| JOIN stream với dimension table | **SQL** | Temporal join hỗ trợ sẵn |
| Lọc invalid records ra dead-letter topic | **DataStream** | Cần side output |
| Detect 3 login failures liên tiếp | **DataStream** | Cần keyed state |
| Build dashboard real-time từ Kafka | **SQL** | Aggregation + continuous query |
| Dynamic filter thay đổi theo config | **Table API** | Xây query điều kiện bằng code |
| Parse phức tạp + gọi external API | **DataStream** | Cần async I/O + custom logic |
| CDC từ MySQL → Iceberg | **SQL** | Connector hỗ trợ sẵn |

### Quy tắc vàng 🏆

> **Luôn bắt đầu bằng SQL. Chỉ chuyển sang DataStream khi SQL không đáp ứng được.**
>
> Trong thực tế, **~80% streaming jobs** có thể viết hoàn toàn bằng Flink SQL.

---

## 6. So sánh chi tiết 3 API

| Tiêu chí | Flink SQL | Table API | DataStream API |
|----------|:---------:|:---------:|:--------------:|
| **Cách viết** | SQL string | Method chaining | Operators + functions |
| **Độ khó** | ⭐ Dễ | ⭐⭐ Trung bình | ⭐⭐⭐ Khó |
| **Độ linh hoạt** | ⭐ | ⭐⭐ | ⭐⭐⭐ |
| **Code lượng** | Ít nhất | Trung bình | Nhiều nhất |
| **Type-safe** | ❌ (string) | ✅ | ✅ |
| **Custom state** | ❌ | ❌ | ✅ |
| **Side output** | ❌ | ❌ | ✅ |
| **Async I/O** | ❌ | ❌ | ✅ |
| **Window** | ✅ (built-in) | ✅ (built-in) | ✅ (built-in + custom) |
| **JOIN** | ✅ | ✅ | ✅ (manual) |
| **Mix được với nhau** | ✅ (với Table) | ✅ (với SQL + DataStream) | ✅ (với Table) |
| **Phổ biến thực tế** | 🔥🔥🔥 | 🔥 | 🔥🔥🔥 |
| **Dùng cho project này** | ✅ Chính | Mix khi cần | Khi SQL không đủ |

---

## 7. Mix API trong cùng một job

Một điểm **rất mạnh** của Flink là bạn có thể **kết hợp** cả 3 API trong cùng một job:

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# ---- Phần 1: Dùng SQL để đọc source ----
t_env.execute_sql("""
    CREATE TABLE kafka_clicks (...) WITH ('connector' = 'kafka', ...)
""")

# ---- Phần 2: Dùng Table API để transform ----
clicks_table = t_env.from_path("kafka_clicks")
cleaned = (clicks_table
    .filter(col("ip").is_not_null)
    .select(col("click_id"), col("user_id"), col("event_time"))
)

# ---- Phần 3: Chuyển sang DataStream cho custom logic ----
click_stream = t_env.to_data_stream(cleaned)

result_stream = (click_stream
    .key_by(lambda x: x["user_id"])
    .process(CustomFraudDetection())
)

# ---- Phần 4: Chuyển lại Table để ghi bằng SQL ----
result_table = t_env.from_data_stream(result_stream)
t_env.create_temporary_view("fraud_results", result_table)

t_env.execute_sql("""
    INSERT INTO iceberg_fraud_alerts
    SELECT * FROM fraud_results
""")
```

### Cách chuyển đổi giữa các API

```
                to_data_stream()
Table/SQL ──────────────────────▶ DataStream
    ▲                                  │
    │        from_data_stream()        │
    └──────────────────────────────────┘

Luồng phổ biến khi mix:
  SQL (đọc source) → Table API (transform) → DataStream (custom logic) → SQL (ghi sink)
```

> [!TIP]
> **Khi nào mix?** Khi job có cả phần "đơn giản" (dùng SQL) lẫn phần "phức tạp" (cần DataStream). Tận dụng điểm mạnh từng API thay vì ép dùng một API cho toàn bộ.
