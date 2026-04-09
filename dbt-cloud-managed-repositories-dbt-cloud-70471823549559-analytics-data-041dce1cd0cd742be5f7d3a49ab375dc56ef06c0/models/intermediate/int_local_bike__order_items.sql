{{ config(materialized='table') }}

WITH base_order_items AS (
    SELECT
        oi.order_id,
        oi.item_id,
        oi.product_id,
        o.customer_id,
        o.store_id,
        o.staff_id,
        o.order_date,
        o.shipped_date,
        oi.quantity,
        oi.list_price,
        oi.discount
    FROM {{ ref('stg_local_bike__order_items') }} oi
    LEFT JOIN {{ ref('stg_local_bike__orders') }} o
        ON oi.order_id = o.order_id
),

enriched_order_items AS (
    SELECT
        order_id,
        item_id,
        product_id,
        customer_id,
        store_id,
        staff_id,
        order_date,
        shipped_date,
        quantity,
        list_price,
        discount,
        quantity * list_price AS gross_revenue,
        quantity * list_price * (1 - discount) AS net_revenue,
        CASE
            WHEN SAFE_CAST(order_date AS DATE) IS NOT NULL
             AND SAFE_CAST(shipped_date AS DATE) IS NOT NULL
            THEN DATE_DIFF(SAFE_CAST(shipped_date AS DATE), SAFE_CAST(order_date AS DATE), DAY)
            ELSE NULL
        END AS delivery_days
    FROM base_order_items
)

SELECT *
FROM enriched_order_items