# Hướng dẫn: Viết Avro Schema và Produce vào Kafka

Hướng dẫn từng bước cách viết file Avro schema (`.json`) cho bảng `clicks`, đọc schema đó trong Python, và produce message vào Kafka dưới dạng Avro thông qua Schema Registry.

---

## Mục lục

1. [Tổng quan luồng dữ liệu](#1-tổng-quan-luồng-dữ-liệu)
2. [Step 1 – Viết Avro Schema](#2-step-1--viết-avro-schema)
3. [Step 2 – Đọc Schema trong Python](#3-step-2--đọc-schema-trong-python)
4. [Step 3 – Kết nối Schema Registry](#4-step-3--kết-nối-schema-registry)
5. [Step 4 – Tạo Avro Producer](#5-step-4--tạo-avro-producer)
6. [Step 5 – Produce message vào Kafka](#6-step-5--produce-message-vào-kafka)
7. [Code hoàn chỉnh](#7-code-hoàn-chỉnh)
8. [Cách Avro Schema hoạt động](#8-cách-avro-schema-hoạt-động)

---

## 1. Tổng quan luồng dữ liệu

```
┌───────────────┐     ┌──────────────────┐     ┌───────────────┐
│  Python App   │────▶│  Schema Registry │────▶│  Kafka Broker  │
│  (Producer)   │     │  (validate Avro) │     │  (topic: clicks)│
└───────────────┘     └──────────────────┘     └───────────────┘
        │                      ▲
        │    Đọc schema từ     │
        │    file .json        │  Schema được đăng ký
        ▼                      │  tự động khi produce
  clicks_schema.json ──────────┘
```

**Quy trình:**
1. Viết schema Avro → file `.json`
2. Python đọc file `.json` → lấy schema string
3. `AvroSerializer` gửi schema lên Schema Registry → Registry trả về schema ID
4. Mỗi message được serialize theo Avro format + đính kèm schema ID
5. Message được gửi vào Kafka topic

---

## 2. Step 1 – Viết Avro Schema

### File: `pipeline/source/clicks_schema.json`

```json
{
  "type": "record",
  "name": "Click",
  "namespace": "com.s2lh.clicks",
  "fields": [
    {
      "name": "click_id",
      "type": "string",
      "doc": "Unique identifier for the click event (UUID)"
    },
    {
      "name": "user_id",
      "type": "string",
      "doc": "Unique identifier for the user (UUID)"
    },
    {
      "name": "product_id",
      "type": "string",
      "doc": "Unique identifier for the product (UUID)"
    },
    {
      "name": "url",
      "type": "string",
      "doc": "The URL that was clicked"
    },
    {
      "name": "user_agent",
      "type": "string",
      "doc": "Browser user agent string"
    },
    {
      "name": "ip",
      "type": "string",
      "doc": "Public IPv4 address of the user"
    },
    {
      "name": "event_time",
      "type": "double",
      "doc": "Unix timestamp of the click event"
    }
  ]
}
```

### Giải thích từng phần

| Trường | Ý nghĩa |
|---|---|
| `"type": "record"` | Đây là một Avro record (tương tự 1 row trong bảng) |
| `"name": "Click"` | Tên của schema, giống tên class |
| `"namespace": "com.s2lh.clicks"` | Namespace để tránh trùng tên giữa các schema |
| `"fields": [...]` | Danh sách các cột/field |

### Các kiểu dữ liệu Avro phổ biến

| Python type | Avro type | Ví dụ |
|---|---|---|
| `str` | `"string"` | UUID, URL, text |
| `int` | `"int"` | Số nguyên 32-bit |
| `int` (lớn) | `"long"` | Số nguyên 64-bit |
| `float` | `"float"` | Số thực 32-bit |
| `float` | `"double"` | Số thực 64-bit (dùng cho timestamp) |
| `bool` | `"boolean"` | True/False |
| `None` | `"null"` | Giá trị null |
| Cho phép null | `["null", "string"]` | Nullable string |

> **Quy tắc nhớ nhanh:** Avro type = kiểu dữ liệu viết thường bằng tiếng Anh, đặt trong dấu `""`.

---

## 3. Step 2 – Đọc Schema trong Python

```python
import json
from pathlib import Path

# Đọc Avro schema từ file JSON
schema_path = Path(__file__).parent / "clicks_schema.json"

with open(schema_path, "r") as f:
    schema_str = f.read()

# Kiểm tra: in ra schema
print(schema_str)
```

### Tại sao dùng `Path(__file__).parent`?

- `__file__` = đường dẫn tuyệt đối của file Python hiện tại
- `.parent` = thư mục chứa file đó
- `/ "clicks_schema.json"` = nối thêm tên file schema

→ Dù chạy script từ thư mục nào, nó luôn tìm đúng file schema nằm cạnh script.

---

## 4. Step 3 – Kết nối Schema Registry

```python
from confluent_kafka.schema_registry import SchemaRegistryClient

# Tạo client kết nối tới Schema Registry
sr_client = SchemaRegistryClient({
    "url": "http://localhost:28081"
})
```

> ⚠️ **Lưu ý:** Tham số phải là **dict** `{"url": "..."}`, KHÔNG phải 2 tham số riêng lẻ.
>
> ```python
> # ❌ SAI
> sr_client = SchemaRegistryClient("url", "http://localhost:28081")
>
> # ✅ ĐÚNG  
> sr_client = SchemaRegistryClient({"url": "http://localhost:28081"})
> ```

---

## 5. Step 4 – Tạo Avro Producer

```python
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer

# Tạo Avro serializer từ schema string
avro_serializer = AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=schema_str,
    to_dict=lambda obj, ctx: obj,  # message đã là dict → trả về nguyên
)

# Cấu hình producer
producer = SerializingProducer({
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
})
```

### Giải thích các thành phần

| Thành phần | Vai trò |
|---|---|
| `AvroSerializer` | Tự động serialize dict → Avro binary, đăng ký schema lên Registry |
| `StringSerializer` | Serialize key (string) thành bytes |
| `SerializingProducer` | Producer đặc biệt — tự động gọi serializer trước khi gửi message |
| `to_dict` | Hàm chuyển đổi object → dict. Vì message đã là dict nên chỉ cần `lambda obj, ctx: obj` |

---

## 6. Step 5 – Produce message vào Kafka

```python
from faker import Faker
import uuid
from time import time

faker = Faker()

def delivery_callback(err, msg):
    """Được gọi sau mỗi message để báo kết quả."""
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")

# Tạo message (phải khớp CHÍNH XÁC với schema)
message = {
    "click_id": str(uuid.uuid4()),
    "user_id": str(uuid.uuid4()),
    "product_id": str(uuid.uuid4()),
    "url": faker.url(),
    "user_agent": faker.user_agent(),
    "ip": faker.ipv4_public(),
    "event_time": time(),           # float → tương thích Avro "double"
}

# Produce message — KHÔNG cần json.dumps() hay .encode()
# AvroSerializer tự lo việc serialize
producer.produce(
    topic="clicks",
    key=message["click_id"],        # Dùng click_id làm key
    value=message,                  # Truyền dict trực tiếp
    on_delivery=delivery_callback,
)

# Đợi tất cả message được gửi xong
producer.flush()
```

### Những điểm QUAN TRỌNG

> **1. KHÔNG `json.dumps()`**: Khi dùng `SerializingProducer` + `AvroSerializer`, bạn truyền **dict gốc** — serializer tự chuyển thành binary Avro.
>
> **2. Dùng `on_delivery`** thay vì `callback`: `SerializingProducer` sử dụng parameter tên `on_delivery`.
>
> **3. Kiểu dữ liệu phải khớp schema**: Nếu schema ghi `"double"` mà bạn truyền `str`, sẽ lỗi serialize.

---

## 7. Code hoàn chỉnh

File: `pipeline/source/generate_stream_data.py`

```python
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from faker import Faker
from pathlib import Path
import uuid
from time import time


# ---- 1. Đọc Avro schema từ file ----
schema_path = Path(__file__).parent / "clicks_schema.json"
with open(schema_path, "r") as f:
    schema_str = f.read()


# ---- 2. Kết nối Schema Registry ----
sr_client = SchemaRegistryClient({
    "url": "http://localhost:28081"
})


# ---- 3. Tạo Avro serializer + Producer ----
avro_serializer = AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=schema_str,
    to_dict=lambda obj, ctx: obj,
)

producer = SerializingProducer({
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
})

faker = Faker()


# ---- 4. Callback ----
def delivery_callback(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


# ---- 5. Produce messages ----
for i in range(10):
    message = {
        "click_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "product_id": str(uuid.uuid4()),
        "url": faker.url(),
        "user_agent": faker.user_agent(),
        "ip": faker.ipv4_public(),
        "event_time": time(),
    }

    producer.produce(
        topic="clicks",
        key=message["click_id"],
        value=message,
        on_delivery=delivery_callback,
    )
    producer.poll(0)

producer.flush()
print("🏁 Done! All messages flushed.")
```

---

## 8. Cách Avro Schema hoạt động

### So sánh JSON vs Avro

| | JSON | Avro |
|---|---|---|
| Format | Text (con người đọc được) | Binary (compact) |
| Schema | Không bắt buộc | **Bắt buộc** |
| Kích thước | Lớn (tên field lặp lại) | Nhỏ (chỉ lưu value) |
| Tốc độ serialize | Chậm hơn | Nhanh hơn |
| Schema evolution | Không hỗ trợ | ✅ Hỗ trợ (thêm/xóa field) |

### Tại sao dùng Avro + Schema Registry?

```
Không có Schema Registry:
  Producer A: {"user": "Alice", "age": 25}
  Producer B: {"user": "Bob", "years_old": 30}    ← field khác tên!
  Consumer:   💥 lỗi vì data không nhất quán

Có Schema Registry:
  Producer A: serialize theo schema v1 → ✅
  Producer B: serialize theo schema v1 → ✅ (bắt buộc đúng format)
  Consumer:   đọc schema từ Registry → deserialize chính xác
```

### Luồng khi produce 1 message

```
1. Producer gửi schema lên Schema Registry
2. Registry kiểm tra:
   - Schema mới? → Lưu lại, trả về schema_id
   - Schema đã tồn tại? → Trả về schema_id có sẵn
3. Producer serialize message:
   [magic_byte (1B)] [schema_id (4B)] [avro_binary_data]
4. Message được gửi vào Kafka topic
```
