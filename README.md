1. Set up Postgres Database -- For this I used Aiven
2. Create tables & load data  - I have created tables using the DBvisualizer and loaded the data into those tables.
○ Create tables in PostgreSQL (customers_raw, orders_raw, products_raw,
country_dim) using CREATE statements as defined in the schema above, and load
data into them using INSERT queries.

4. Set up the Hevo pipeline: - Connected my Hevo to Aiven postgress credentials and dumped the data from local database to Hevo.
○ Source = PostgreSQL
○ Destination = Snowflake
○ Ingestion mode must be Logical Replication 
5. Load data to Snowflake:

   
— Use Models from HERE —
Write SQL query/queries for the following tasks - Written SQL queries for creating these models
5. Deduplicate Customers
○ Keep only the most recent record for each customer_id.
○ Standardize emails to lowercase.
○ Standardize phone numbers into a 10-digit format, or mark as "Unknown" if invalid or
missing.

WITH ranked_customers AS (
    SELECT
        customer_id,

        LOWER(email) AS email,

        REGEXP_REPLACE(phone, '[^0-9]', '') AS raw_phone,

        country_code,
        updated_at,
        created_at,

        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM customers_raw
),

phone_cleaned AS (
    SELECT
        customer_id,
        email,

        CASE
            WHEN raw_phone IS NULL THEN 'Unknown'
            WHEN LENGTH(raw_phone) = 10 THEN raw_phone
            ELSE 'Unknown'
        END AS phone,

        country_code,
        updated_at,
        created_at
    FROM ranked_customers
    WHERE rn = 1
)

SELECT
    customer_id,
    email,
    phone,
    country_code,
    updated_at,
    created_at,

    CASE
        WHEN email IS NULL
         AND phone = 'Unknown'
         AND country_code IS NULL
         AND updated_at IS NULL
         AND created_at IS NULL
        THEN 'Invalid Customer'
        ELSE 'Valid Customer'
    END AS customer_status

FROM phone_cleaned


6. Fix <null>s & Country Issues
○ Standardize country_code values using country_dim.
○ Handle variations such as usa, UnitedStates, IND, SINGAPORE.
○ If created_at is <null>, replace with a default timestamp (1900-01-01).
WITH standardized_country AS (
    SELECT
        c.customer_id,
        c.email,
        c.phone,
        c.updated_at,

        COALESCE(c.created_at, '1900-01-01'::TIMESTAMP) AS created_at,

        CASE
            WHEN LOWER(c.country_code) IN ('us', 'usa', 'unitedstates', 'united states') THEN 'United States'
            WHEN LOWER(c.country_code) IN ('ind', 'india') THEN 'India'
            WHEN LOWER(c.country_code) IN ('sg', 'singapore') THEN 'Singapore'
            ELSE 'Unknown'
        END AS country_name,

        c.customer_status
    FROM Task_5 c
)

SELECT
    sc.customer_id,
    sc.email,
    sc.phone,
    cd.iso_code AS country_code,
    sc.created_at,
    sc.updated_at,
    sc.customer_status
FROM standardized_country sc
LEFT JOIN country_dim cd
    ON sc.country_name = cd.country_name
    
7. Clean Orders
○ Remove exact duplicate rows.
○ Handle invalid amounts:
■ If amount is negative, replace with 0.
■ If amount is <null>, replace with a reasonable fallback (e.g., median of
customer’s transactions).
○ Standardize currency codes to uppercase.
○ Create a derived column amount_usd by converting all amounts into USD using
predefined conversion rates.

○ Standardize category names (In “Title Case”).
○ If a product is inactive (active_flag = 'N'), mark it as "Discontinued Product".

WITH deduped_orders AS (
    SELECT DISTINCT
        order_id,
        customer_id,
        product_id,
        amount,
        created_at,
        UPPER(currency) AS currency
    FROM orders_raw
),

amount_standardized AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        created_at,
        currency,

        CASE
            WHEN amount < 0 THEN 0
            ELSE amount
        END AS amount_clean
    FROM deduped_orders
),

median_amounts AS (
    SELECT
        customer_id,
        MEDIAN(amount_clean) AS median_amount
    FROM amount_standardized
    WHERE amount_clean IS NOT NULL
    GROUP BY customer_id
),

amount_final AS (
    SELECT
        a.order_id,
        a.customer_id,
        a.product_id,
        a.created_at,
        a.currency,

        COALESCE(
            a.amount_clean,
            m.median_amount,
            0
        ) AS final_amount
    FROM amount_standardized a
    LEFT JOIN median_amounts m
        ON a.customer_id = m.customer_id
),

currency_conversion AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        created_at,
        currency,
        final_amount,

        CASE
            WHEN currency = 'USD' THEN final_amount * 1
            WHEN currency = 'INR' THEN final_amount * 0.012
            WHEN currency = 'SGD' THEN final_amount * 0.74
            WHEN currency = 'EUR' THEN final_amount * 1.08
            ELSE final_amount
        END AS amount_usd
    FROM amount_final
)

SELECT * FROM currency_conversion

8. Clean Products
○ Standardize product names (capitalize properly).
○ Standardize category names (In “Title Case”).
○ If a product is inactive (active_flag = 'N'), mark it as "Discontinued Product".

SELECT
    product_id,

    INITCAP(product_name) AS product_name,

    INITCAP(category) AS category,

    CASE
        WHEN active_flag = 'N' THEN 'Discontinued Product'
        ELSE 'Active Product'
    END AS product_status

FROM products_raw


9. JOIN the resultants
○ Produce a unified dataset joining customers, orders, and products.
○ If customer_id does not exist in cleaned customers, mark email as "Orphan
Customer".
○ If product_id is missing or invalid, mark it as "Unknown Product".


10. Edge Cases
○ Customers with completely <null> records should be marked as "Invalid Customer".
○ Orders referencing non-existent customers or products should still appear in the final
dataset, with appropriate placeholders.
○ Mixed currency handling should be consistent across all rows.
8. Clean Products
○ Standardize product names (capitalize properly).

SELECT
    o.order_id,
    o.created_at AS order_created_at,

    o.customer_id,
    CASE
        WHEN c.customer_id IS NULL THEN 'Orphan Customer'
        ELSE c.email
    END AS customer_email,

    c.phone,
    COALESCE(c.country_code, 'UNK') AS customer_country,
    COALESCE(c.customer_status, 'Invalid Customer') AS customer_status,

    o.product_id,
    COALESCE(p.product_name, 'Unknown Product') AS product_name,
    COALESCE(p.category, 'Unknown Category') AS product_category,
    COALESCE(p.product_status, 'Unknown Status') AS product_status,

    o.currency,
    o.final_amount AS amount_original,
    o.amount_usd

FROM Task_7 o
LEFT JOIN Task_6 c
    ON o.customer_id = c.customer_id
LEFT JOIN Task_8 p
    ON o.product_id = p.product_id


After creating these models, I have validated the data by running these querries and then saved the changes.
