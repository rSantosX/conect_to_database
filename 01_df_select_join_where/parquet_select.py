import pandas as pd
import psycopg2  # Replace with your database connector (e.g., sqlite3 for SQLite)

# Step 1: Read data from the .parquet file
parquet_df = pd.read_parquet("customers.parquet")  # Replace with your .parquet file path

# Step 2: Extract the list of customer IDs from the parquet file
customer_ids = parquet_df['customer_id'].tolist()  # Convert the column to a list

# Step 3: Ensure there are IDs to use in the query
if len(customer_ids) == 0:
    print("No customer IDs found in the .parquet file.")
else:
    # Assuming `conn` is your database connection object for PostgreSQL (adjust based on your database type)
    # Replace with your actual database connection details
    conn = psycopg2.connect(database="your_db", user="your_user", password="your_pass", host="localhost", port="5432")

    # Step 4: Build the query dynamically using `%s` for the `IN` clause
    placeholders = ', '.join(['%s'] * len(customer_ids))  # Use `%s` for PostgreSQL, MySQL
    query = f"SELECT * FROM orders WHERE customer_id IN ({placeholders})"

    # Step 5: Use `tuple` to pass the list of IDs as parameters
    orders_df = pd.read_sql_query(query, conn, params=tuple(customer_ids))  # Pass `customer_ids` as a tuple

    # Step 6: Output the resulting DataFrame
    print(orders_df)

    # Close the database connection
    conn.close()
