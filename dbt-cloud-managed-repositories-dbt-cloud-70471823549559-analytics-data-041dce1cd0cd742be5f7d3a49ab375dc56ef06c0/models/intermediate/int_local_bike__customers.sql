{{ config(materialized='table') }}

WITH base_customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        phone,
        email,
        street,
        city,
        state,
        zip_code
    FROM {{ ref('stg_local_bike__customers') }}
),

enriched_customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        phone,
        email,
        street,
        city,
        state,
        zip_code,
        CONCAT(first_name, ' ', last_name) AS full_name,
        TRUE AS is_active
    FROM base_customers
)

SELECT *
FROM enriched_customers