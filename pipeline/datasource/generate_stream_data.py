import random
import uuid
from faker import Faker 
from time import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from pathlib import Path

def generate_click_event(faker: Faker, users_df: list, products_df: list):
    return {
        "click_id": str(uuid.uuid4()),
        "user_id": random.choice(users_df),
        "product_id": random.choice(products_df),
        "product_url": faker.url(),
        "user_agent": faker.user_agent(),
        "ip": faker.ipv4(),
        "event_time": time()
    }

def generate_checkout_event(faker: Faker, click_event: dict):
    return {
        "checkout_id":     str(uuid.uuid4()),
        "user_id":         click_event["user_id"],
        "product_id":      click_event["product_id"],
        "payment_method":  random.choice(["Online Banking", "Cash"]),
        "total_amount":    faker.pricetag(),
        "shipping_address":faker.address(),
        "billing_address": faker.address(),
        "user_agent":      faker.user_agent(),
        "ip_address":      faker.ipv4(),
        "event_time":      time()
    }

def delivery_callback(err, msg):
    if err is not None:
        print(f"Error when produce {msg} to {msg.topic()}: {err}")
    else:
        print(f"Successfully produce {msg} to Kafka topic: {msg.topic()}")

def push_to_kafka(message: dict, topic_name: str, producer: SerializingProducer):
    print(f"{topic_name.upper()}: Pushing ...")
    producer.produce(
        topic=topic_name,
        key=message["user_id"],
        value=message,
        on_delivery=delivery_callback
    )
    producer.poll(0)

def get_schema():
    schema_path = Path(__file__).parent

    with open(f"{schema_path}/clicks_schema.json", "r") as f:
        clicks_schema = f.read()
    
    with open(f"{schema_path}/checkouts_schema.json", "r") as f:
        checkouts_schema = f.read()
    
    return clicks_schema, checkouts_schema

def get_kafka_avro_producer():
    clicks_schema, checkouts_schema = get_schema()

    sr_client = SchemaRegistryClient({
        "url": "http://localhost:28081"
    })

    clicks_avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=clicks_schema,
        to_dict=lambda obj,ctx: obj
    )

    checkouts_avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=checkouts_schema,
        to_dict=lambda obj,ctx: obj
    )

    clicks_producer = SerializingProducer({
        "bootstrap.servers": "localhost:19094,localhost:29094",
        "key.serializer": StringSerializer('utf-8'),
        "value.serializer": clicks_avro_serializer
    })

    checkouts_producer = SerializingProducer({
        "bootstrap.servers": "localhost:19094,localhost:29094",
        "key.serializer": StringSerializer('utf-8'),
        "value.serializer": checkouts_avro_serializer
    })

    return clicks_producer, checkouts_producer
