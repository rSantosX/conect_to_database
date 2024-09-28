import os
import warnings
import psycopg2 # type: ignore
import pandas as pd # type: ignore
from dotenv import load_dotenv # type: ignore

warnings.filterwarnings('ignore')

load_dotenv(dotenv_path=".env")

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Step 1: Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Step 2: Retrieve the user details into a DataFrame
try:
    user_df = pd.read_sql("SELECT cntid, cntcpfcgc, cntnom FROM cnt WHERE cntcpfcgc = %s", conn, params=('38346436882',))
except Exception as e:
    print(f"Error executing the first query: {e}")
    conn.close()
    exit()

# Check if the user exists
if user_df.empty:
    print("No user found with the given username.")
    conn.close()
    exit()

# Extract the user ID
#user_id = int(user_df.iloc[0]['cntid']) #Converto para integer
user_id = user_df.iloc[0]['cntid'].item()


# Step 3: Retrieve the orders for this user ID
try:
    orders_df = pd.read_sql("SELECT cntfiscnt, cntfissxo, cntfisncm, cntfismae FROM cntfis WHERE cntfiscnt = %s", conn, params=(user_id,))
except Exception as e:
    print(f"Error executing the second query: {e}")
    conn.close()
    exit()

# Step 4: Merge the two DataFrames on the `user_id` column
merged_df = pd.merge(orders_df, user_df, left_on='cntfiscnt', right_on='cntid', how='inner')

# Step 5: Select desired columns from the merged DataFrame
final_df = merged_df[['cntcpfcgc', 'cntnom', 'cntfissxo', 'cntfisncm', 'cntfismae']]

# Step 6: Close the connection
conn.close()

# Step 7: Display the final DataFrame
print(final_df)
