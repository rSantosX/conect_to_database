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

# Step 1: Leio os dados do CSV e tranformo em um DataFrame
path_csv = '../03_cvs_select_in_db/csv/AmostraCNPJ.csv'
csv_df = pd.read_csv(path_csv, sep=';', dtype={'recpjcnscnpj': str}) # Uso o dtype para definir o tipo da coluna recpjcnscnpj como str

# Step 2: Extraio a lista de Documentos do arquivo CSV
customer_docs = csv_df['recpjcnscnpj'].tolist()  # Converto a coluna para uma lista

# Step 4: Build the query dynamically using `%s` for the `IN` clause
placeholders = ', '.join(['%s'] * len(customer_docs))  # For PostgreSQL, MySQL, use %s
query = f"SELECT * FROM receita_cnpj.recpjcns WHERE recpjcnscnpj IN ({placeholders})"


# Step 5: Use `tuple` to pass the list of IDs as parameters
orders_df = pd.read_sql_query(query, conn, params=list(customer_docs))  # Pass `customer_ids` as a tuple

    # Step 6: Output the resulting DataFrame
print(orders_df)

conn.close()