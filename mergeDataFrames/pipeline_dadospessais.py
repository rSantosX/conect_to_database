import os
import warnings
import psycopg2 # type: ignore
import pandas as pd # type: ignore
from dotenv import load_dotenv # type: ignore

warnings.filterwarnings('ignore')

# 1. Carregar variáveis ​​de ambiente do arquivo .env
#load_dotenv()
load_dotenv(dotenv_path=".env") # conexao em ambiente de dev
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

# Step 2: Retrieve user details into a DataFrame
cnt_df = pd.read_sql_query("SELECT cntid, cntcpfcgc, cntnom FROM cnt WHERE cntcpfcgc = %s", conn, params=('38346436882',))  # Replace 'john_doe' as needed
cnt_id = cnt_df.iloc[0]['cntid'].item() # Extract the `id`

# Step 3: Use `user_id` in another query to get orders and load into a DataFrame
cntfis_df = pd.read_sql_query("SELECT cntfiscnt, cntfissxo, cntfisncm, cntfismae FROM cntfis WHERE cntfiscnt = %s", conn, params=(cnt_id,))

# Step 4: Merge the two DataFrames on the `user_id` column
merged_df = pd.merge(cntfis_df, cnt_df, left_on='cntfiscnt', right_on='cntid', how='inner')

# Step 5: Select desired columns from the merged DataFrame
final_df = merged_df[['cntcpfcgc', 'cntnom', 'cntfissxo', 'cntfisncm', 'cntfismae']]

# Step 6: Close the connection
conn.close()

# Step 7: Display the final DataFrame
print(final_df)