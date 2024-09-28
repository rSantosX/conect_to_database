import os
import psycopg2 # type: ignore
import pandas as pd # type: ignore
import pyarrow as pa # type: ignore
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

#3. Estabeleça uma conexão com o banco de dados PostgreSQL usando psycopg2
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# 4. Execute uma consulta e carregue-a em um DataFrame do pandas
query = "SELECT * FROM cnt LIMIT 10"  # Query
orders_df = pd.read_sql(query, conn)

# Fecha a conexao
conn.close()

# Seleciono determinada colunas de um DataFrame
filtered_df = orders_df[['cntid','cntcpfcgc', 'cntnom']]

print(filtered_df.head())  # Exibir as primeiras linhas