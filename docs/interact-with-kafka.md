# Interacting with Kafka Using `confluent-kafka` (Python)

A comprehensive best-practices guide for producing and consuming messages with the **confluent-kafka** Python client.

---

## Table of Contents

1. [Installation](#1-installation)
2. [Core Concepts](#2-core-concepts)
3. [Producer – Best Practices](#3-producer--best-practices)
4. [Consumer – Best Practices](#4-consumer--best-practices)
5. [Admin Client – Managing Topics](#5-admin-client--managing-topics)
6. [Schema Registry & Avro Serialization](#6-schema-registry--avro-serialization)
7. [Error Handling & Retries](#7-error-handling--retries)
8. [Performance Tuning Cheat Sheet](#8-performance-tuning-cheat-sheet)
9. [Graceful Shutdown Pattern](#9-graceful-shutdown-pattern)

---

## 1. Installation

```bash
# With pip
pip install confluent-kafka

# With Avro/Schema Registry support
pip install confluent-kafka[avro,schema-registry]

# With uv (this project)
uv add confluent-kafka
```

---

## 2. Core Concepts

| Concept | Description |
|---|---|
| **Broker** | A Kafka server that stores and serves messages. |
| **Topic** | A named category/feed to which messages are published. |
| **Partition** | A topic is split into partitions for parallelism. |
| **Offset** | A unique sequential ID for each message within a partition. |
| **Consumer Group** | A group of consumers that coordinate to consume from a topic — each partition is consumed by exactly one consumer in the group. |
| **Producer** | Publishes (writes) messages to a topic. |
| **Consumer** | Subscribes to (reads) messages from one or more topics. |

---

## 3. Producer – Best Practices

### 3.1 Basic Producer

```python
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "client.id": "my-producer",
}

producer = Producer(conf)


def delivery_callback(err, msg):
    """Called once per message to indicate delivery result."""
    if err is not None:
        print(f"❌ Delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ Delivered to {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"@ offset {msg.offset()}"
        )


# --- Produce messages ---
for i in range(10):
    producer.produce(
        topic="my-topic",
        key=str(i),            # key determines partitioning
        value=f"event-{i}",
        callback=delivery_callback,
    )

    # Trigger delivery callbacks from previous produce() calls.
    # poll(0) is non-blocking.
    producer.poll(0)

# Block until ALL messages are delivered (or timeout).
producer.flush(timeout=30)
```

### 3.2 Key Best Practices

| Practice | Why |
|---|---|
| **Always set a `callback`** | Without it, you silently lose messages on failure. |
| **Call `poll()` frequently** | It triggers delivery callbacks and keeps the internal queue flowing. |
| **Call `flush()` before exit** | Ensures all buffered messages are delivered. |
| **Use keys** | Messages with the same key go to the same partition → ordering guarantee. |
| **Enable idempotence** | Set `enable.idempotence=true` to avoid duplicate messages on retry. |

### 3.3 Production-Grade Config

```python
producer_conf = {
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "client.id": "my-producer",

    # Reliability
    "acks": "all",                   # Wait for ALL replicas to acknowledge
    "enable.idempotence": True,      # Exactly-once semantics (per partition)
    "max.in.flight.requests.per.connection": 5,  # Max with idempotence

    # Batching & throughput
    "linger.ms": 5,                  # Wait up to 5ms to batch messages
    "batch.size": 65536,             # 64 KB batch (default 16 KB)
    "compression.type": "lz4",       # lz4 = fast; zstd = best ratio

    # Retries
    "retries": 2147483647,           # Effectively infinite retries
    "retry.backoff.ms": 100,

    # Timeout
    "delivery.timeout.ms": 120000,   # 2 minutes total delivery timeout
}
```

---

## 4. Consumer – Best Practices

### 4.1 Basic Consumer

```python
from confluent_kafka import Consumer, KafkaError

conf = {
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "group.id": "my-consumer-group",
    "auto.offset.reset": "earliest",   # Start from beginning if no offset
}

consumer = Consumer(conf)
consumer.subscribe(["my-topic"])

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Block for up to 1 second

        if msg is None:
            continue  # No message available

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Reached end of partition — not an error
                print(f"End of partition {msg.partition()}")
            else:
                raise Exception(msg.error())
        else:
            print(
                f"Received: key={msg.key()}, "
                f"value={msg.value().decode('utf-8')}, "
                f"partition={msg.partition()}, "
                f"offset={msg.offset()}"
            )
finally:
    consumer.close()  # Leaves consumer group cleanly → triggers rebalance
```

### 4.2 Key Best Practices

| Practice | Why |
|---|---|
| **Always call `consumer.close()`** | Cleanly leaves the group so partitions are quickly reassigned. |
| **Handle `_PARTITION_EOF`** | It's informational, not an error. |
| **Use `try/finally`** | Guarantees cleanup even on exceptions. |
| **Set `auto.offset.reset`** | Without it, a new consumer group has no idea where to start. |

### 4.3 Manual Commit (Recommended for Critical Data)

By default, offsets are committed automatically every 5 seconds. This can cause **data loss** on crash (messages read but not processed). Manual commit gives full control:

```python
conf = {
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "group.id": "my-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,        # 👈 Disable auto-commit
}

consumer = Consumer(conf)
consumer.subscribe(["my-topic"])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        # ---- Process the message ----
        process(msg)

        # ---- Commit AFTER successful processing ----
        consumer.commit(asynchronous=False)  # Synchronous = safest
finally:
    consumer.close()
```

> **Tip:** For higher throughput, batch-commit every N messages or every T seconds
> instead of committing after every single message.

### 4.4 Production-Grade Config

```python
consumer_conf = {
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "group.id": "my-consumer-group",
    "auto.offset.reset": "earliest",

    # Commit strategy
    "enable.auto.commit": False,

    # Performance
    "fetch.min.bytes": 1024,            # Wait for at least 1 KB of data
    "fetch.wait.max.ms": 500,           # Max wait before returning fetch
    "max.poll.interval.ms": 300000,     # 5 min - max time between polls

    # Session & heartbeat
    "session.timeout.ms": 45000,        # Broker kicks consumer after 45s
    "heartbeat.interval.ms": 3000,      # Heartbeat every 3s (< 1/3 session)
}
```

---

## 5. Admin Client – Managing Topics

```python
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:19094,localhost:29094"})


# --- Create a topic ---
new_topic = NewTopic(
    topic="my-new-topic",
    num_partitions=3,
    replication_factor=2,          # Must be <= number of brokers
)
futures = admin.create_topics([new_topic])

for topic, future in futures.items():
    try:
        future.result()  # Block until topic is created
        print(f"✅ Topic '{topic}' created")
    except Exception as e:
        print(f"❌ Failed to create '{topic}': {e}")


# --- List existing topics ---
metadata = admin.list_topics(timeout=10)
for topic in metadata.topics:
    print(f"  📌 {topic}")


# --- Delete a topic ---
futures = admin.delete_topics(["my-new-topic"])
for topic, future in futures.items():
    try:
        future.result()
        print(f"🗑️  Topic '{topic}' deleted")
    except Exception as e:
        print(f"❌ Failed to delete '{topic}': {e}")
```

---

## 6. Schema Registry & Avro Serialization

Using Schema Registry ensures **producers and consumers agree on the data format**. This prevents broken data from entering your pipeline.

> Your cluster already has Schema Registry at `localhost:28081`.

### 6.1 Define an Avro Schema

```python
schema_str = """{
    "type": "record",
    "name": "User",
    "namespace": "com.example",
    "fields": [
        {"name": "id",    "type": "int"},
        {"name": "name",  "type": "string"},
        {"name": "email", "type": "string"}
    ]
}"""
```

### 6.2 Avro Producer

```python
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

sr_client = SchemaRegistryClient({"url": "http://localhost:28081"})

avro_serializer = AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=schema_str,
    to_dict=lambda user, ctx: user,  # Convert object → dict
)

producer = SerializingProducer({
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
})

# Produce a message
user = {"id": 1, "name": "Alice", "email": "alice@example.com"}
producer.produce(topic="users", key="user-1", value=user)
producer.flush()
```

### 6.3 Avro Consumer

```python
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

sr_client = SchemaRegistryClient({"url": "http://localhost:28081"})

avro_deserializer = AvroDeserializer(
    schema_registry_client=sr_client,
    schema_str=schema_str,
    from_dict=lambda data, ctx: data,  # dict → object
)

consumer = DeserializingConsumer({
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "group.id": "users-consumer-group",
    "auto.offset.reset": "earliest",
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": avro_deserializer,
})

consumer.subscribe(["users"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        user = msg.value()
        print(f"Received user: {user}")
finally:
    consumer.close()
```

### 6.4 JSON Schema (Alternative)

```python
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer

json_schema_str = """{
    "type": "object",
    "properties": {
        "id":    {"type": "integer"},
        "name":  {"type": "string"},
        "email": {"type": "string"}
    },
    "required": ["id", "name", "email"]
}"""

# Use JSONSerializer/JSONDeserializer the same way as Avro above.
```

---

## 7. Error Handling & Retries

### 7.1 Producer Error Handling

```python
from confluent_kafka import KafkaException

def delivery_callback(err, msg):
    if err is not None:
        # Log, alert, or push to a dead-letter queue
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()}[{msg.partition()}]")

try:
    producer.produce(
        topic="my-topic",
        value="hello",
        callback=delivery_callback,
    )
except BufferError:
    # Internal producer queue is full
    print("⚠️  Producer queue full — retrying after poll()")
    producer.poll(1)  # Free up space by triggering callbacks
    producer.produce(topic="my-topic", value="hello", callback=delivery_callback)
except KafkaException as e:
    print(f"Kafka error: {e}")
```

### 7.2 Consumer Error Handling

```python
from confluent_kafka import KafkaError

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue  # Informational, not an error
        elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            print(f"⚠️  Topic/partition not found: {msg.error()}")
            continue
        else:
            raise KafkaException(msg.error())

    # Process message...
```

---

## 8. Performance Tuning Cheat Sheet

### Producer

| Setting | Default | Recommended | Effect |
|---|---|---|---|
| `acks` | `all` | `all` | Durability guarantee |
| `linger.ms` | `0` | `5–100` | Higher = more batching = better throughput |
| `batch.size` | `16384` | `65536–131072` | Larger batches reduce network overhead |
| `compression.type` | `none` | `lz4` or `zstd` | Reduces network + disk usage |
| `buffer.memory` | `33554432` | Adjust per load | Total memory for unsent messages |

### Consumer

| Setting | Default | Recommended | Effect |
|---|---|---|---|
| `fetch.min.bytes` | `1` | `1024–65536` | Reduces fetch requests |
| `fetch.wait.max.ms` | `500` | `100–500` | Trade latency for throughput |
| `max.poll.interval.ms` | `300000` | Based on processing time | Avoid unnecessary rebalances |
| `session.timeout.ms` | `45000` | `10000–45000` | Faster failure detection |

---

## 9. Graceful Shutdown Pattern

Always ensure clean shutdown in production applications:

```python
import signal
import sys
from confluent_kafka import Consumer

running = True


def shutdown_handler(signum, frame):
    global running
    print("\n🛑 Shutdown signal received, closing gracefully...")
    running = False


# Register signal handlers
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

consumer = Consumer({
    "bootstrap.servers": "localhost:19094,localhost:29094",
    "group.id": "my-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["my-topic"])

try:
    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        # Process message
        process(msg)

        # Commit after processing
        consumer.commit(asynchronous=False)

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    # This will:
    # 1. Commit final offsets
    # 2. Leave the consumer group (trigger rebalance)
    # 3. Close network connections
    consumer.close()
    print("✅ Consumer shut down cleanly.")
```

---

## Quick Reference

```text
pip install confluent-kafka                      # Install
pip install confluent-kafka[avro,schema-registry] # With Schema Registry

Producer  → produce() + poll() + flush()
Consumer  → subscribe() + poll() + commit() + close()
Admin     → create_topics() / delete_topics() / list_topics()
```
