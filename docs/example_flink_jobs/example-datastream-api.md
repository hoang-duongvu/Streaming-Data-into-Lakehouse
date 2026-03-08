# Ví dụ: Flink DataStream API

Các ví dụ thực tế sử dụng **DataStream API** — API mạnh nhất, cho phép viết **bất kỳ logic nào** bằng Python. Dùng khi SQL/Table API không đáp ứng được.

> **Khi nào dùng DataStream API?**
> - Cần **custom stateful logic** (fraud detection, session tracking)
> - Cần **side output** (tách valid/invalid records)
> - Cần **async I/O** (gọi external API)
> - Logic xử lý quá phức tạp cho SQL

---

## Mục lục

1. [Setup chung](#1-setup-chung)
2. [Ví dụ 1: Map — Biến đổi từng record](#2-ví-dụ-1-map--biến-đổi-từng-record)
3. [Ví dụ 2: Filter — Lọc records](#3-ví-dụ-2-filter--lọc-records)
4. [Ví dụ 3: FlatMap — 1 record → nhiều records](#4-ví-dụ-3-flatmap--1-record--nhiều-records)
5. [Ví dụ 4: KeyBy + Reduce — Aggregation theo key](#5-ví-dụ-4-keyby--reduce--aggregation-theo-key)
6. [Ví dụ 5: Process Function — Custom stateful logic](#6-ví-dụ-5-process-function--custom-stateful-logic)
7. [Ví dụ 6: Side Output — Tách valid/invalid records](#7-ví-dụ-6-side-output--tách-validinvalid-records)
8. [Ví dụ 7: Chuyển đổi giữa Table ↔ DataStream](#8-ví-dụ-7-chuyển-đổi-giữa-table--datastream)

---

## 1. Setup chung

```python
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment
from pyflink.common import Types

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60000)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

t_env = StreamTableEnvironment.create(env)
```

### Cách lấy DataStream từ Kafka

DataStream API **không có** cú pháp `CREATE TABLE`. Cách đơn giản nhất là dùng SQL tạo source table rồi chuyển sang DataStream:

```python
# Bước 1: Tạo source bằng SQL
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
        'properties.group.id'          = 'flink-ds-clicks',
        'scan.startup.mode'            = 'earliest-offset',
        'format'                       = 'avro-confluent',
        'avro-confluent.url'           = 'http://schema-registry:8081'
    )
""")

# Bước 2: Chuyển Table → DataStream
clicks_table = t_env.from_path("kafka_clicks")
click_stream = t_env.to_data_stream(clicks_table)
```

> [!NOTE]
> Mỗi record trong `click_stream` là một **Row** object. Truy cập field bằng `row["field_name"]` hoặc `row[index]`.

---

## 2. Ví dụ 1: Map — Biến đổi từng record

> **Mục đích**: Biến đổi mỗi record (1 input → 1 output). Tương đương `SELECT ... AS ...`

```python
from pyflink.datastream.functions import MapFunction

class EnrichClick(MapFunction):
    """Thêm trường mới và chuẩn hóa data."""

    def map(self, row):
        from datetime import datetime

        return {
            "click_id": row["click_id"],
            "user_id": row["user_id"],
            "product_id": row["product_id"],
            "url": row["url"],
            # Chuẩn hóa IP: null → 'unknown'
            "ip": row["ip"] if row["ip"] else "unknown",
            # Epoch → readable datetime
            "event_datetime": datetime.fromtimestamp(
                row["event_time"]
            ).strftime("%Y-%m-%d %H:%M:%S"),
            # Phân loại user_agent
            "device_type": self._detect_device(row["user_agent"]),
        }

    def _detect_device(self, user_agent: str) -> str:
        if not user_agent:
            return "unknown"
        ua = user_agent.lower()
        if "mobile" in ua or "android" in ua or "iphone" in ua:
            return "mobile"
        elif "tablet" in ua or "ipad" in ua:
            return "tablet"
        return "desktop"


# Áp dụng map
enriched_stream = click_stream.map(EnrichClick())

# In ra console
enriched_stream.print()
env.execute("Enrich Clicks Job")
```

> [!IMPORTANT]
> Khi dùng DataStream API, **phải gọi `env.execute()`** ở cuối để submit job. Khác với SQL API, `execute_sql()` tự submit.

---

## 3. Ví dụ 2: Filter — Lọc records

> **Mục đích**: Giữ lại records thỏa điều kiện.

```python
from pyflink.datastream.functions import FilterFunction

class ValidClickFilter(FilterFunction):
    """Chỉ giữ clicks hợp lệ."""

    def filter(self, row):
        # Bỏ clicks thiếu thông tin quan trọng
        if not row["click_id"] or not row["user_id"]:
            return False

        # Bỏ clicks có IP null
        if not row["ip"]:
            return False

        # Bỏ clicks quá cũ (trước 2024)
        if row["event_time"] < 1704067200.0:  # 2024-01-01
            return False

        return True


# Áp dụng filter
valid_clicks = click_stream.filter(ValidClickFilter())

# Hoặc dùng lambda cho filter đơn giản
valid_clicks = click_stream.filter(
    lambda row: row["ip"] is not None and row["user_id"] is not None
)

valid_clicks.print()
env.execute("Filter Clicks Job")
```

---

## 4. Ví dụ 3: FlatMap — 1 record → nhiều records

> **Mục đích**: Một input record tạo ra **0 hoặc nhiều** output records. Ví dụ: tách URL thành segments.

```python
from pyflink.datastream.functions import FlatMapFunction

class SplitUrlSegments(FlatMapFunction):
    """Tách mỗi click thành nhiều records theo URL path segments.

    Input:  {"click_id": "abc", "url": "/products/shoes/nike"}
    Output: {"click_id": "abc", "segment": "products", "depth": 0}
            {"click_id": "abc", "segment": "shoes",    "depth": 1}
            {"click_id": "abc", "segment": "nike",     "depth": 2}
    """

    def flat_map(self, row, collector):
        url = row["url"]
        if not url:
            return  # 0 output — record bị bỏ qua

        segments = [s for s in url.split("/") if s]

        for depth, segment in enumerate(segments):
            collector.collect({
                "click_id": row["click_id"],
                "user_id": row["user_id"],
                "segment": segment,
                "depth": depth,
            })


url_segments = click_stream.flat_map(SplitUrlSegments())
url_segments.print()
env.execute("URL Segments Job")
```

---

## 5. Ví dụ 4: KeyBy + Reduce — Aggregation theo key

> **Mục đích**: Nhóm records theo key và tổng hợp — tương đương `GROUP BY`.

```python
from pyflink.datastream.functions import ReduceFunction

class ClickCounter(ReduceFunction):
    """Đếm clicks và giữ event_time mới nhất."""

    def reduce(self, accumulated, new_value):
        return {
            "user_id": accumulated["user_id"],
            "click_count": accumulated["click_count"] + 1,
            "last_event_time": max(
                accumulated["last_event_time"],
                new_value["last_event_time"],
            ),
        }


# Chuẩn bị data (map trước để có schema đúng cho reduce)
prepared = click_stream.map(
    lambda row: {
        "user_id": row["user_id"],
        "click_count": 1,
        "last_event_time": row["event_time"],
    }
)

# KeyBy → Reduce
user_click_counts = (prepared
    .key_by(lambda x: x["user_id"])
    .reduce(ClickCounter())
)

user_click_counts.print()
env.execute("Click Counter Job")
```

### Hiểu `key_by` + `reduce`:

```
Input stream:
  {user: "A", count: 1}  →  {user: "B", count: 1}  →  {user: "A", count: 1}
                                                              │
                                                              ▼
Sau key_by("user"):
  Partition A: {count: 1}  →  {count: 1}   ── reduce ──▶  {count: 2}
  Partition B: {count: 1}                   ── reduce ──▶  {count: 1}
```

---

## 6. Ví dụ 5: Process Function — Custom stateful logic

> **Mục đích**: Logic phức tạp với **state** — ví dụ: phát hiện user click > 10 lần trong 5 phút.

```python
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types

class ClickBurstDetector(KeyedProcessFunction):
    """Phát hiện user click liên tục > threshold trong time_window."""

    def __init__(self, threshold=10, window_seconds=300):
        self.threshold = threshold
        self.window_seconds = window_seconds

    def open(self, runtime_context):
        # Khởi tạo state: đếm số clicks
        self.click_count = runtime_context.get_state(
            ValueStateDescriptor("click_count", Types.INT())
        )
        # State: thời điểm bắt đầu window
        self.window_start = runtime_context.get_state(
            ValueStateDescriptor("window_start", Types.DOUBLE())
        )

    def process_element(self, row, ctx):
        current_time = row["event_time"]
        count = self.click_count.value() or 0
        start = self.window_start.value() or current_time

        # Reset nếu window đã hết hạn
        if current_time - start > self.window_seconds:
            count = 0
            start = current_time
            self.window_start.update(start)

        # Tăng counter
        count += 1
        self.click_count.update(count)

        # Phát hiện burst
        if count >= self.threshold:
            yield {
                "alert": "CLICK_BURST",
                "user_id": row["user_id"],
                "click_count": count,
                "window_start": start,
                "current_time": current_time,
            }


# Áp dụng Process Function
alerts = (click_stream
    .key_by(lambda row: row["user_id"])
    .process(ClickBurstDetector(threshold=10, window_seconds=300))
)

alerts.print()
env.execute("Click Burst Detection Job")
```

### Keyed State giải thích:

```
User A click liên tục:
  click 1 → count=1  (chưa alert)
  click 2 → count=2  (chưa alert)
  ...
  click 10 → count=10 → 🚨 ALERT: CLICK_BURST!

User B click ít:
  click 1 → count=1  (chưa alert)
  (5 phút trôi qua, không click)
  click 2 → count=1  (reset vì hết window)
```

> [!NOTE]
> **Keyed State**: Mỗi key (user_id) có **bộ nhớ riêng**. State được quản lý bởi Flink, tự động lưu/khôi phục khi checkpoint.

---

## 7. Ví dụ 6: Side Output — Tách valid/invalid records

> **Mục đích**: Tách stream thành **2+ nhánh** — valid records đi một đường, invalid đi đường khác. Tính năng **chỉ DataStream API mới có**.

```python
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream import OutputTag

# Khai báo side output tag
INVALID_TAG = OutputTag("invalid-clicks", Types.STRING())

class ValidateClickStream(ProcessFunction):
    """Tách clicks thành valid (main output) và invalid (side output)."""

    def process_element(self, row, ctx):
        errors = []

        if not row["click_id"]:
            errors.append("missing_click_id")
        if not row["user_id"]:
            errors.append("missing_user_id")
        if not row["ip"]:
            errors.append("missing_ip")
        if row["event_time"] <= 0:
            errors.append("invalid_event_time")

        if errors:
            # → Side output: invalid records (gửi kèm lý do lỗi)
            import json
            ctx.output(INVALID_TAG, json.dumps({
                "record": {
                    "click_id": row["click_id"],
                    "user_id": row["user_id"],
                },
                "errors": errors,
            }))
        else:
            # → Main output: valid records
            yield row


# Áp dụng validation
validated = click_stream.process(ValidateClickStream())

# Lấy main output (valid clicks)
valid_clicks = validated
valid_clicks.print()  # → ghi vào Iceberg

# Lấy side output (invalid clicks)
invalid_clicks = validated.get_side_output(INVALID_TAG)
invalid_clicks.print()  # → ghi vào dead letter topic / log

env.execute("Validate Clicks Job")
```

### Side Output flow:

```
                          ┌──▶ Main Output (valid)
                          │    → ghi vào Iceberg (Bronze)
Input Stream ──▶ Process ─┤
                          │
                          └──▶ Side Output (invalid)
                               → ghi vào Kafka dead-letter topic
                               → hoặc log để phân tích sau
```

---

## 8. Ví dụ 7: Chuyển đổi giữa Table ↔ DataStream

> **Mục đích**: Kết hợp SQL (đọc source) + DataStream (custom logic) + SQL (ghi sink).

```python
# ==== PHẦN 1: SQL — Tạo source ====
t_env.execute_sql("""
    CREATE TABLE kafka_clicks (...) WITH ('connector' = 'kafka', ...)
""")

# ==== PHẦN 2: Table API → DataStream ====
clicks_table = t_env.from_path("kafka_clicks")
click_stream = t_env.to_data_stream(clicks_table)

# ==== PHẦN 3: DataStream — Custom logic ====
enriched = (click_stream
    .filter(lambda row: row["ip"] is not None)
    .map(EnrichClick())                              # Custom MapFunction
    .key_by(lambda row: row["user_id"])
    .process(ClickBurstDetector(threshold=5))         # Custom state logic
)

# ==== PHẦN 4: DataStream → Table → SQL Sink ====
result_table = t_env.from_data_stream(enriched)
t_env.create_temporary_view("enriched_alerts", result_table)

t_env.execute_sql("""
    INSERT INTO iceberg_catalog.silver.click_alerts
    SELECT * FROM enriched_alerts
""")
```

### Cheat sheet chuyển đổi:

```
Table/SQL ──── to_data_stream() ────▶ DataStream
    ▲                                      │
    │        from_data_stream()            │
    └──────────────────────────────────────┘

                  to_data_stream()
   SQL Table ─────────────────────▶ DataStream
                                       │
                                   map / filter /
                                   key_by / process
                                       │
                                       ▼
                 from_data_stream()
   SQL Sink ◀─────────────────────── DataStream
```

> [!TIP]
> **Pattern phổ biến nhất khi mix**: SQL (source) → DataStream (complex logic) → SQL (sink). Tận dụng SQL cho phần khai báo connector, DataStream cho phần logic phức tạp.
