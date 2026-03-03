from sqlalchemy import create_engine
from faker import Faker
import uuid
import pandas as pd

faker = Faker()
engine = create_engine("postgresql+psycopg://admin:admin@localhost:5433/ecommerce")

def generate_user_table(num_users: int) -> pd.DataFrame:
    users = []
    for i in range(num_users):
        user_id = str(uuid.uuid4())
        username = faker.user_name()
        password = faker.password()

        users.append({
            "user_id": user_id,
            "username": username,
            "password": password
        })
    users_table = pd.DataFrame(users)
    return users_table

def generate_product_table(num_products: int) -> pd.DataFrame:
    products = []
    for i in range(num_products):
        product_id = str(uuid.uuid4())
        product_name = faker.name()
        description = faker.text()
        price = faker.pricetag()
        products.append({
            "product_id": product_id,
            "product_name": product_name,
            "description": description,
            "price": price
        })
    return pd.DataFrame(products)

def save_to_postgres(df: pd.DataFrame, table_name: str):
    # Create Table
    df.head(0).to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False
    )

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False,
        chunksize=10
    )

df = generate_user_table(20)
save_to_postgres(df, 'users')
