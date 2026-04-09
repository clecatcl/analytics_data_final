SELECT
    order_id,
    customer_id,
    order_status,
    CASE
        WHEN order_status = 1 THEN 'pending'
        WHEN order_status = 2 THEN 'processing'
        WHEN order_status = 3 THEN 'shipped'
        WHEN order_status = 4 THEN 'cancelled'
        ELSE 'unknown'
    END AS order_status_label,
    order_date,
    required_date,
    shipped_date,
    store_id,
    staff_id,
      CASE
        WHEN SAFE_CAST(order_date AS DATE) IS NOT NULL
         AND SAFE_CAST(shipped_date AS DATE) IS NOT NULL
        THEN DATE_DIFF(SAFE_CAST(shipped_date AS DATE), SAFE_CAST(order_date AS DATE), DAY)
        ELSE NULL
    END AS delivery_days
FROM {{ source('local_bike', 'orders') }}