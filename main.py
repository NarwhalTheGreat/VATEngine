import pandas as pd
from sqlalchemy import create_engine

# Replace 'your_username', 'your_password', 'your_database', and 'your_host'
engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/witisi-ordering-local')

try:
    connection = engine.connect()
    print("Connection to PostgreSQL established successfully!")
except Exception as e:
    print(f"Connection failed! Error: {e}")

# Read the sql file and execute the query
with open('sql/Stripe_2_VAT_Org_1.sql', 'r') as query:
    # connection == the connection to your database, in your case prob_db
    df = pd.read_sql_query(query.read(),connection)