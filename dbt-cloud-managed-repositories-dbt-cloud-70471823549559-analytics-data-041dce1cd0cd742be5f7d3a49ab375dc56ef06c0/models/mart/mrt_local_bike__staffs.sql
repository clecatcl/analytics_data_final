{{ config(materialized='table') }}

WITH staff_orders AS (
    SELECT
        oi.staff_id,
        oi.order_id,
        oi.quantity,
        oi.gross_revenue,
        oi.net_revenue,
        oi.delivery_days
    FROM {{ ref('int_local_bike__order_items') }} oi
),

aggregated_staffs AS (
    SELECT
        st.staff_id,
        st.first_name,
        st.last_name,
        st.email,
        st.store_id,
        st.active,
        COUNT(DISTINCT staff_orders.order_id) AS total_orders,
        SUM(staff_orders.quantity) AS total_quantity_sold,
        SUM(staff_orders.gross_revenue) AS total_gross_revenue,
        SUM(staff_orders.net_revenue) AS total_net_revenue,
        AVG(staff_orders.delivery_days) AS avg_delivery_days
    FROM {{ ref('int_local_bike__staffs') }} st
    LEFT JOIN staff_orders
        ON st.staff_id = staff_orders.staff_id
    GROUP BY
        st.staff_id,
        st.first_name,
        st.last_name,
        st.email,
        st.store_id,
        st.active
)

SELECT *
FROM aggregated_staffs