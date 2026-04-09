{{ config(materialized='table') }}

WITH base_brands AS (
    SELECT
        brand_id,
        brand_name
    FROM {{ ref('stg_local_bike__brands') }}
),

enriched_brands AS (
    SELECT
        brand_id,
        brand_name
    FROM base_brands
)

SELECT *
FROM enriched_brands