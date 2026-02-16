{{ config(
    materialized='view',
    schema='gold'
) }}

WITH silver_sales AS (
    -- In a real project, this would reference models/silver/sales.sql
    -- For this minimal setup, we reference the seed directly
    SELECT 
        CAST(order_date AS DATE) AS sale_date,
        amount,
        item_count
    FROM {{ ref('raw_sales') }}
)

SELECT
    DATE_TRUNC('month', sale_date) AS sales_month,
    SUM(amount) AS total_revenue,
    SUM(item_count) AS total_items_sold,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount) / COUNT(*), 2) AS avg_transaction_value
FROM silver_sales
GROUP BY 1
ORDER BY 1 DESC