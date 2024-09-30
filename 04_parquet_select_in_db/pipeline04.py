import os
import warnings
import psycopg2 # type: ignore
import pandas as pd # type: ignore
from dotenv import load_dotenv # type: ignore

# Step 1: Carregar variáveis ​​de ambiente do arquivo .env
load_dotenv()

# Step 2: Recuperar as variáveis
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")


conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

columns: list[str]

# Step 3: Read data from the .parquet file
parquet_df = pd.read_parquet("../04_parquet_select_in_db/parquet/AmostraCNPJ.parquet")  # Replace with your .parquet file path

# Step 2: Extract the list of customer IDs from the parquet file
customer_ids = parquet_df['recpjcnscnpj'].astype(str).tolist()  # Convert the column to a list

# Step 4: Build the query dynamically using `%s` for the `IN` clause
placeholders = ', '.join(['%s'] * len(customer_ids))  # Use `%s` for PostgreSQL, MySQL
query = f"SELECT * FROM receita_cnpj.recpjcns WHERE recpjcnscnpj IN ({placeholders})"

# Step 5: Use `tuple` to pass the list of IDs as parameters
orders_df = pd.read_sql_query(query, conn, params=tuple(customer_ids))  # Pass `customer_ids` as a tuple

# Step 6: Output the resulting DataFrame
print(orders_df)

# Close the database connection
conn.close()