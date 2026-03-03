from faker import Faker
from sqlalchemy import create_engine
from pipeline.source.generate_static_data import generate_user_table, generate_product_table
from pipeline.source.generate_stream_data import (
    generate_click_event,
    generate_checkout_event,
    push_to_kafka,
    get_kafka_avro_producer,
)
import random


def main():
    faker = Faker()
    clicks_producer, checkouts_producer = get_kafka_avro_producer()
    engine = create_engine("postgresql+psycopg://admin:admin@localhost:5433/ecommerce")
    num_users = 100
    num_products = 50
    num_clicks = 1000000

    print("==== Generating static data in Postgres ====")

    users_id = generate_user_table(num_users, faker, engine)
    products_id = generate_product_table(num_products, faker, engine)

    print("==== Generating streaming data produce to Kafka ====")

    for i in range(num_clicks):
        print(f"Iteration {i}")
        click_event = generate_click_event(faker, users_id, products_id)
        push_to_kafka(click_event, "clicks", clicks_producer)

        if random.random() > 0.5:
            click_event = generate_click_event(faker, users_id, products_id)
            push_to_kafka(click_event, "clicks", clicks_producer)
            checkout_event = generate_checkout_event(faker, click_event)
            push_to_kafka(checkout_event, "checkouts", checkouts_producer)

    clicks_producer.flush()
    checkouts_producer.flush()
    print("Done")


if __name__ == "__main__":
    main()
