from faker import Faker
from sqlalchemy import create_engine
from pipeline.datasource.generate_static_data import generate_user_table, generate_product_table
from pipeline.datasource.generate_stream_data import (
    generate_click_event,
    generate_checkout_event,
    push_to_kafka,
    get_kafka_avro_producer,
)
import random
from time import time


def main():
    faker = Faker()
    clicks_producer, checkouts_producer = get_kafka_avro_producer()
    engine = create_engine("postgresql+psycopg://admin:admin@localhost:5433/ecommerce")
    num_users = 5
    num_products = 5
    num_clicks = 1000
    curr = time()

    print("==== Generating static data in Postgres ====")

    users_id = generate_user_table(num_users, faker, engine)
    products_id = generate_product_table(num_products, faker, engine)

    print("==== Generating streaming data produce to Kafka ====")

    for i in range(num_clicks):
        print(f"Iteration {i}")
        click_event = generate_click_event(faker, users_id, products_id, curr)
        push_to_kafka(click_event, "clicks", clicks_producer)
        curr = curr + random.randint(0, 10)

        if random.random() > 0.5:
            click_event = generate_click_event(faker, users_id, products_id, curr)
            push_to_kafka(click_event, "clicks", clicks_producer)
            curr = curr + random.randint(0, 10)

            checkout_event = generate_checkout_event(faker, click_event, curr)
            push_to_kafka(checkout_event, "checkouts", checkouts_producer)
            curr = curr + random.randint(0, 10)

    clicks_producer.flush()
    checkouts_producer.flush()
    print("Done")


if __name__ == "__main__":
    main()
