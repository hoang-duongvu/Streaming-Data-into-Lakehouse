from sqlalchemy import create_engine, Engine
from faker import Faker
import uuid
import pandas as pd

def generate_user_table(num_users: int, faker: Faker, engine: Engine) -> pd.DataFrame:
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
    return_values = save_to_postgres(users_table, "users", engine)

    return return_values

def generate_product_table(num_products: int, faker: Faker, engine: Engine) -> pd.DataFrame:
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
    products_table = pd.DataFrame(products)
    return_values = save_to_postgres(products_table, "products", engine)

    return return_values

def save_to_postgres(df: pd.DataFrame, table_name: str, engine):

    try:
        print(f"starting insert into {table_name.upper()} to postgres ...")

        # Create Table
        df.head(0).to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False
        )

        # Insert records 
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=10
        )

        print(f"Successfully inserted into {table_name.upper()}!")
        
        return df.iloc[:,0].to_list()
    except Exception as err:
        print(f"Error when insert into {table_name.upper()}: {err}")
        return []
