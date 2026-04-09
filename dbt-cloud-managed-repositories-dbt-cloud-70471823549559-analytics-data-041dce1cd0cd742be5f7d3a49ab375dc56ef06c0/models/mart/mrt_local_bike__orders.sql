{{ config(materialized='table') }}

WITH base_orders AS (
    SELECT
        oi.order_id,
        oi.customer_id,
        oi.store_id,
        oi.staff_id,
        oi.product_id,
        oi.quantity,
        oi.list_price,
        oi.discount,
        oi.gross_revenue,
        oi.net_revenue,
        oi.delivery_days
    FROM {{ ref('int_local_bike__order_items') }} oi
),

aggregated_orders AS (
    SELECT
        order_id,
        customer_id,
        store_id,
        staff_id,
        COUNT(DISTINCT product_id) AS distinct_products,
        SUM(quantity) AS total_quantity,
        SUM(gross_revenue) AS total_gross_revenue,
        SUM(net_revenue) AS total_net_revenue,
        AVG(delivery_days) AS avg_delivery_days
    FROM base_orders
    GROUP BY
        order_id,
        customer_id,
        store_id,
        staff_id
)

SELECT *
FROM aggregated_orders