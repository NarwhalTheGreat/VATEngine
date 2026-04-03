import numpy as np
import pandas as pd
from numpy.f2py.auxfuncs import throw_error
from sqlalchemy import create_engine, text

def report_by_event(username, password, catalog_id, catalog_name, hostname):


    # Connect to the databases
    port = '6432'
    #engine_ordering = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/witisi-ordering-local')
    engine_ordering = create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/witisi-ordering')

    # Establish and check Connection
    try:
        connection_ordering = engine_ordering.connect()
        print("Connection to PostgreSQL witisi-ordering established successfully!")
    except Exception as e:
        print(f"Connection failed! Error: {e}")
        raise ValueError(f"Connection failed! Error: {e}")

    engine_invoicing = create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/witisi-invoicing')

    try:
        connection_invoicing = engine_invoicing.connect()
        print("Connection to PostgreSQL witisi-invoicing established successfully!")
    except Exception as e:
        print(f"Connection failed! Error: {e}")
        raise ValueError(f"Connection failed! Error: {e}")

    engine_stripe = create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/witisi-stripe')

    try:
        connection_stripe = engine_stripe.connect()
        print("Connection to PostgreSQL witisi-stripe established successfully!")
    except Exception as e:
        print(f"Connection failed! Error: {e}")
        raise ValueError(f"Connection failed! Error: {e}")

    # Query n1
    query1 = text("""
                  -- DB host: 10.10.2.42
-- DB: witisi-ordering
                  SELECT o."OrderId",
                         o."Number",
                         o."Email",
                         o."CreatedOn",
                         o."Amount",
                         o."Discount",
                         '(''' || o."OrderId" || ''', ''' || o."Number" || ''', ''' || o."Email" || ''', ''' ||
                         o."CreatedOn" || ''', ''' || o."Amount" || ''', ''' || o."Discount" || '''),' as "OrderInfo"
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
    df1 = pd.read_sql(query1.bindparams(catalog_id=catalog_id), connection_ordering)
    connection_ordering.close()
    engine_ordering.dispose()

    # All orders in a string for the next query
    all_orders = '\n'.join(df1['OrderInfo'].astype(str))

    # Remove trailing comma
    all_orders = all_orders[:-1]

    # print(all_orders)

    # STEP2 ---------------------------------------
    # Query n2
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
    df2 = pd.read_sql(query2, connection_invoicing)

    connection_invoicing.close()
    engine_invoicing.dispose()

    # All orders in a string for the next query
    all_accepted_orders = '\n'.join(df2['OrderInfo'].astype(str))

    # Remove trailing comma
    all_accepted_orders = all_accepted_orders[:-1]

    # STEP3 ---------------------------------------
    # Query n3

    query3 = text(f"""
            SELECT  o."OrderId",
            o."Number",
            o."Email",
            o."CreatedOn",
            o."PaymentId",
            replace(o."Amount"::text, '.', ',')    as "OrderAmount",
            replace(o."Discount"::text, '.', ',')  as "OrderDiscount",
            p."CurrencyId",
            p2."IssuerCountry"                     as "Country",
            p3."PaymentIntentId",
            replace(p."Amount"::text, '.', ',')    as "PaymentAmount",
            cv."Vat"                               as "VatPercent",
            -- If Business Card payment use Business VAT formula, otherwise use regular VAT formula
            replace(
                case
                    when p."Attributes" is not null
                    then round(p."Amount" / 100 * cv."Vat", 2)
                    else round(p."Amount" / (100 + cv."Vat") * cv."Vat", 2)
                end::text, '.', ','
            )                                      as "VatAmount",
            p."Attributes"
    FROM (VALUES
        {all_accepted_orders}
    ) as o("OrderId", "Number", "Email", "CreatedOn", "PaymentId", "Amount", "Discount", "PaymentAmount")
        JOIN "Payment" p         ON p."PaymentId"::text = o."PaymentId"
        JOIN "CardPayment" p2    ON p."PaymentId" = p2."PaymentId"
        JOIN "ChargedPayment" p3 ON p."PaymentId" = p3."PaymentId"
        LEFT JOIN "CountryVat" cv ON cv."CountryCode" = p2."IssuerCountry"
        """)

    # Run query 3
    df3 = pd.read_sql(query3, connection_stripe)

    connection_stripe.close()
    engine_stripe.dispose()


    return df3
