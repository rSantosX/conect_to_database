import os
import psycopg2 # type: ignore
import pandas as pd # type: ignore
from dotenv import load_dotenv # type: ignore


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
cnt_query = "SELECT * FROM cnt LIMIT 10"
cnt_df = pd.read_sql(cnt_query, conn)

# Step 3: Run the second SELECT query to get customer information
cntfis_query = "SELECT customer_id, customer_name, location FROM cntfis WHERE = %s"
cntfis_df = pd.read_sql(cntfis_query, conn)

# Step 4: Fecho a conexão com o banco de dados
conn.close()

# Step 5: Merge the two DataFrames on the common ID
# Note: 'cntfis_df' from the cnt_df and 'cntid' from the cntfiscnt
combined_df = pd.merge(cnt_df, cntfis_df, left_on='cntfiscnt', right_on='cntid', how='inner')

# Step 6: Select specific columns from the combined DataFrame
# Example: Select 'cntnom' and 'cntfismae'
filtered_df = combined_df[['cntnom', 'order_amount']]

# Step 7: Display the filtered DataFrame
print(filtered_df)