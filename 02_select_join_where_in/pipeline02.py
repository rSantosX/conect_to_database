import os
import warnings
import psycopg2 # type: ignore
import pandas as pd # type: ignore
from dotenv import load_dotenv # type: ignore

warnings.filterwarnings('ignore')

# Step 1: Carregar variáveis ​​de ambiente do arquivo .env
load_dotenv()
#load_dotenv(dotenv_path=".env") # conexao em ambiente de dev
#load_dotenv(dotenv_path=".env.production") # conexao em ambiente producao

# Step 2: Recuperar as variáveis
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Step 3: Estabelece a conexão com o banco de dados
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Step 4: Recuperar detalhes do usuário em um DataFrame
parametro = ('38346436882','44502706817') #Meus parametros de consulta
cnt_df = pd.read_sql_query("SELECT cntid, cntcpfcgc, cntnom FROM cnt WHERE cntcpfcgc in %s", conn, params=(parametro,)) #Transformo o retorno da consulta em um DataFrame
cnt_id = cnt_df['cntid'].iloc[:].tolist() #Separo uma lista de id retornados   

placeholders = ', '.join(['%s'] * len(cnt_id)) #Crio a cláusula SQL IN dinamicamente usando espaços reservados %s 

# Step 5: Crio um novo DataFrame com retorno da consulta usando o retorno de id's do Step 4 
cntfis_df = pd.read_sql_query(f"SELECT cntfiscnt, cntfissxo, cntfisncm, cntfismae FROM cntfis WHERE cntfiscnt in ({placeholders})", conn, params=list(cnt_id,))

# Step 6: Faço o Merge entre os dois DataFrames usando as colunas de ID
merged_df = pd.merge(cntfis_df, cnt_df, left_on='cntfiscnt', right_on='cntid', how='inner')

# Step 7: Seleciono as colunas desejadas do Merge DataFrame
final_df = merged_df[['cntcpfcgc', 'cntnom', 'cntfissxo', 'cntfisncm', 'cntfismae']]

# Step 8: Fecho a conexão com o banco de dados
conn.close()

# Step 9: Exibir o DataFrame final
print(final_df)