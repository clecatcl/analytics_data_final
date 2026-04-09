{{ config(materialized='table') }}

WITH product_orders AS (
    SELECT
        oi.product_id,
        oi.order_id,
        oi.quantity,
        oi.gross_revenue,
        oi.net_revenue,
        oi.delivery_days
    FROM {{ ref('int_local_bike__order_items') }} oi
),

aggregated_products AS (
    SELECT
        p.product_id,
        p.product_name,
        p.brand,
        p.category,
        p.model_year,
        p.list_price,
        p.is_premium,
        COUNT(DISTINCT product_orders.order_id) AS total_orders,
        SUM(product_orders.quantity) AS total_quantity_sold,
        SUM(product_orders.gross_revenue) AS total_gross_revenue,
        SUM(product_orders.net_revenue) AS total_net_revenue,
        AVG(product_orders.delivery_days) AS avg_delivery_days
    FROM {{ ref('int_local_bike__products') }} p
    LEFT JOIN product_orders
        ON p.product_id = product_orders.product_id
    GROUP BY
        p.product_id,
        p.product_name,
        p.brand,
        p.category,
        p.model_year,
        p.list_price,
        p.is_premium
)

SELECT *
FROM aggregated_products