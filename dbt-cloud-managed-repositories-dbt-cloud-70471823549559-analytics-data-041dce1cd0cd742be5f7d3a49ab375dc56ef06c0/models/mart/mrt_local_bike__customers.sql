{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT
        oi.customer_id,
        oi.order_id,
        oi.quantity,
        oi.gross_revenue,
        oi.net_revenue,
        oi.delivery_days
    FROM {{ ref('int_local_bike__order_items') }} oi
),

aggregated_customers AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.phone,
        c.email,
        c.street,
        c.city,
        c.state,
        c.zip_code,
        COUNT(DISTINCT customer_orders.order_id) AS total_orders,
        SUM(customer_orders.quantity) AS total_quantity,
        SUM(customer_orders.gross_revenue) AS total_gross_revenue,
        SUM(customer_orders.net_revenue) AS total_net_revenue,
        MIN(customer_orders.order_id) AS first_order_id,
        MAX(customer_orders.order_id) AS last_order_id,
        AVG(customer_orders.delivery_days) AS avg_delivery_days
    FROM {{ ref('int_local_bike__customers') }} c
    LEFT JOIN customer_orders
        ON c.customer_id = customer_orders.customer_id
    GROUP BY
        c.customer_id,
        c.full_name,
        c.phone,
        c.email,
        c.street,
        c.city,
        c.state,
        c.zip_code
)

SELECT *
FROM aggregated_customers