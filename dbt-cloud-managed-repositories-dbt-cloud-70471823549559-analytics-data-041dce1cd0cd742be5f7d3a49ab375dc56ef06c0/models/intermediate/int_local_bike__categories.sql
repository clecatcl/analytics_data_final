{{ config(materialized='table') }}

WITH base_categories AS (
    SELECT
        category_id,
        category_name
    FROM {{ ref('stg_local_bike__categories') }}
),

enriched_categories AS (
    SELECT
        category_id,
        category_name
    FROM base_categories
)

SELECT *
FROM enriched_categories