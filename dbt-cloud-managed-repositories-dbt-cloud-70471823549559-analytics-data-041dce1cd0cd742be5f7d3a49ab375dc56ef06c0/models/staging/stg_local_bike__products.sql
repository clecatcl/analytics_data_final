SELECT
    product_id,
    product_name,
    brand_id AS brand,   
    category_id AS category,
    model_year,
    list_price
FROM {{ source('local_bike', 'products') }}