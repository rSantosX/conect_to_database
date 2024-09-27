import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv


# 1. Carregar variáveis ​​de ambiente do arquivo .env
#load_dotenv()
load_dotenv(dotenv_path=".env.development") # conexao em ambiente de dev
#load_dotenv(dotenv_path=".env.production") # conexao em ambiente producao

# 2. Recuperar as variáveis
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Step 1: Establish a database connection
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Step 2: Run the first SELECT query to get orders data
orders_query = "SELECT order_id, customer_ref, order_amount FROM orders"
orders_df = pd.read_sql(orders_query, conn)

# Step 3: Run the second SELECT query to get customer information
customers_query = "SELECT customer_id, customer_name, location FROM customer_info"
customers_df = pd.read_sql(customers_query, conn)

# Step 4: Fecho a conexão com o banco de dados
conn.close()

# Step 5: Merge the two DataFrames on the common ID
# Note: 'customer_ref' from the orders_df and 'customer_id' from the customers_df
combined_df = pd.merge(orders_df, customers_df, left_on='customer_ref', right_on='customer_id', how='inner')

# Step 6: Select specific columns from the combined DataFrame
# Example: Select 'customer_name' and 'order_amount'
filtered_df = combined_df[['customer_name', 'order_amount']]

# Step 7: Display the filtered DataFrame
print(filtered_df)