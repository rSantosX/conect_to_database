import os
import warnings
import psycopg2 # type: ignore
import pandas as pd # type: ignore
from dotenv import load_dotenv # type: ignore
import sqlite3  # Replace with your database connector (e.g., psycopg2 for PostgreSQL)

# Step 1: Read data from the CSV file
csv_df = pd.read_csv("customers.csv")  # Replace with your .csv file path

# Step 2: Extract the list of customer IDs from the CSV file
customer_ids = csv_df['customer_id'].tolist()  # Convert the column to a list

# Step 3: Ensure there are IDs to use in the query
if len(customer_ids) == 0:
    print("No customer IDs found in the CSV file.")
else:
    # Assuming `conn` is your database connection object (adjust the placeholder based on your database type)
    # For this example, I'm using a SQLite in-memory database, but replace this with your actual database connection
    conn = sqlite3.connect(":memory:")

    # Step 4: Build the query dynamically using `%s` for the `IN` clause
    placeholders = ', '.join(['%s'] * len(customer_ids))  # For PostgreSQL, MySQL, use %s
    query = f"SELECT * FROM orders WHERE customer_id IN ({placeholders})"

    # Step 5: Use `tuple` to pass the list of IDs as parameters
    orders_df = pd.read_sql_query(query, conn, params=tuple(customer_ids))  # Pass `customer_ids` as a tuple

    # Step 6: Output the resulting DataFrame
    print(orders_df)