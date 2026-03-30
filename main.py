import pandas as pd
from sqlalchemy import create_engine, text

# Connect to the database
engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/witisi-ordering-local')

# Establish and check Connection
try:
    connection = engine.connect()
    print("Connection to PostgreSQL established successfully!")
except Exception as e:
    print(f"Connection failed! Error: {e}")

# ── Define the CatalogId dynamically ─────────────────────────────────────────
catalog_id = '57815a0f-2be4-4247-90dc-c25b85c04746'

#Query n1
query = text("""
    SELECT  o."OrderId",
            o."Number",
            o."Email",
            o."CreatedOn",
            o."Amount",
            o."Discount",
            '(''' || o."OrderId" || ''', ''' || o."Number" || ''', ''' || o."Email" || ''', ''' || o."CreatedOn" || ''', ''' || o."Amount" || ''', ''' || o."Discount" || '''),' as "OrderInfo"
    FROM "Order" o 
    JOIN "OrderItem" oi ON oi."OrderId" = o."OrderId" 
    JOIN "Catalog" c ON c."CatalogId" = oi."CatalogId" 
    WHERE oi."CatalogId" = :catalog_id
    AND o."State" = 'Completed'
    GROUP BY o."OrderId",
            o."Number",
            o."Email",
            o."CreatedOn",
            o."Amount",
            o."Discount"
""")

# Run query 1
eventOrderDf1 = pd.read_sql(query.bindparams(catalog_id=catalog_id), connection)

# All orders in a string for the next query f
allOrders = '\n'.join(eventOrderDf1['OrderInfo'].astype(str))

print(eventOrderDf1)

print(allOrders)