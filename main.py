import pandas as pd
from sqlalchemy import create_engine, text

#STEP1---------------------------------------
# Connect to the database
engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/witisi-ordering-local')

# Establish and check Connection
try:
    connection = engine.connect()
    print("Connection to PostgreSQL witisi-ordering established successfully!")
except Exception as e:
    print(f"Connection failed! Error: {e}")

# ── Define the CatalogId dynamically ─────────────────────────────────────────
catalog_id = '7ef69632-0ff6-48ba-b0d2-c53b5c4a7c7c'

#Query n1
query1 = text("""
-- DB host: 10.10.2.42
-- DB: witisi-ordering
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
eventOrderDf1 = pd.read_sql(query1.bindparams(catalog_id=catalog_id), connection)

# All orders in a string for the next query
all_orders = '\n'.join(eventOrderDf1['OrderInfo'].astype(str))

# Remove trailing comma
all_orders = all_orders[:-1]

#DEBUGS___________________________
print(eventOrderDf1)

#print(all_orders)

#STEP2 ---------------------------------------
#Query n2
# Connect to the database
engine2 = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/witisi-invoicing-local')

# Establish and check Connection
try:
    connection2 = engine2.connect()
    print("Connection to PostgreSQL witisi-invoicing established successfully!")
except Exception as e:
    print(f"Connection failed! Error: {e}")

query2 = text(f"""
-- DB host: 10.10.2.42
-- DB: witisi-invoicing
SELECT  o."OrderId",
		o."Number",
		o."Email",
		o."CreatedOn",
		p."PaymentId",
		o."Amount",
		o."Discount",
		p."Amount" as "PaymentAmount",
		'(''' || o."OrderId" || ''', ''' || o."Number" || ''', ''' || o."Email" || ''', ''' || o."CreatedOn" || ''', ''' || p."PaymentId" || ''', ''' || o."Amount" || ''', ''' || o."Discount" || ''', ''' || p."Amount" || '''),' as "OrderInfo"
FROM (VALUES
-- data begin
    {all_orders}
-- data end
) as o("OrderId", "Number", "Email", "CreatedOn", "Amount", "Discount")
join "Payment" p on p."InvoiceId"::text = o."OrderId"
where p."State" = 'Accepted'
and p."GatewayId" = 'stripe'
""")

# Run query 2
df2 = pd.read_sql(query2, connection2)
df2.to_csv("data/data2.csv")

# All orders in a string for the next query
all_accepted_orders = '\n'.join(df2['OrderInfo'].astype(str))

# Remove trailing comma
all_accepted_orders = all_accepted_orders[:-1]

#STEP3 ---------------------------------------
#Query n3
# Connect to the database
engine3 = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/witisi-stripe-payment-local')

# Establish and check Connection
try:
    connection3 = engine3.connect()
    print("Connection to PostgreSQL witisi-stripe established successfully!")
except Exception as e:
    print(f"Connection failed! Error: {e}")

# Establish even info
event_name = 'Piraeus Port Run 2026'

query3 = text(f"""
-- DB host: 10.10.2.42
-- DB: witisi-stripe

select  o."OrderId",
		o."Number",
		o."Email",
		o."CreatedOn",
		o."PaymentId",
		replace(o."Amount"::text, '.', ',') as "OrderAmount",
		replace(o."Discount"::text, '.', ',') as "OrderDiscount",
		p."CurrencyId",
		p2."IssuerCountry" as "Country",
		p3."PaymentIntentId",
		'{event_name}' as "AlbumTitle",
		replace(p."Amount"::text, '.', ',') as "PaymentAmount"
from (values
-- data begin
    {all_accepted_orders}
-- data end
	 ) as o("OrderId", "Number", "Email", "CreatedOn", "PaymentId", "Amount", "Discount", "PaymentAmount")
		 join "Payment" p on p."PaymentId"::text = o."PaymentId"
		 join "CardPayment" p2 on p."PaymentId" = p2."PaymentId"
		 join "ChargedPayment" p3 on p."PaymentId" = p3."PaymentId"
""")

# Run query 2
df3 = pd.read_sql(query3, connection3)
df3.to_csv("data/data3.csv", index=False)
print(df3)