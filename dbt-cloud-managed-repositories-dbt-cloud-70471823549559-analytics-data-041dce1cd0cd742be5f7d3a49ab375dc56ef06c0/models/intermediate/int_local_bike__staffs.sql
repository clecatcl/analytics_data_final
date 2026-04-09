{{ config(materialized='table') }}

SELECT
    staff_id,
    first_name,
    last_name,
    email,
    store_id,
    active
FROM {{ ref('stg_local_bike__staffs') }}

