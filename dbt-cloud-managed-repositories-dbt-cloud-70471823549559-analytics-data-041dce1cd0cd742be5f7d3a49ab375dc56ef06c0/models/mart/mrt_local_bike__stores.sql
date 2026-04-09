{{ config(materialized='table') }}

WITH store_orders AS (
    SELECT
        oi.store_id,
        oi.order_id,
        oi.quantity,
        oi.gross_revenue,
        oi.net_revenue,
        oi.delivery_days
    FROM {{ ref('int_local_bike__order_items') }} oi
),

aggregated_stores AS (
    SELECT
        s.store_id,
        s.store_name,
        s.phone,
        s.email,
        s.street,
        s.city,
        s.state,
        s.zip_code,
        COUNT(DISTINCT store_orders.order_id) AS total_orders,
        SUM(store_orders.quantity) AS total_quantity_sold,
        SUM(store_orders.gross_revenue) AS total_gross_revenue,
        SUM(store_orders.net_revenue) AS total_net_revenue,
        AVG(store_orders.delivery_days) AS avg_delivery_days
    FROM {{ ref('int_local_bike__stores') }} s
    LEFT JOIN store_orders
        ON s.store_id = store_orders.store_id
    GROUP BY
        s.store_id,
        s.store_name,
        s.phone,
        s.email,
        s.street,
        s.city,
        s.state,
        s.zip_code
)

SELECT *
FROM aggregated_stores