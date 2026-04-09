{{ config(materialized='table') }}

WITH base_products AS (
    SELECT
        product_id,
        product_name,
        brand,
        category,
        model_year,
        list_price
    FROM {{ ref('stg_local_bike__products') }}
),

enriched_products AS (
    SELECT
        product_id,
        product_name,
        brand,
        category,
        model_year,
        list_price,
        CASE 
            WHEN list_price > 1000 THEN TRUE
            ELSE FALSE
        END AS is_premium
    FROM base_products
)

SELECT *
FROM enriched_products