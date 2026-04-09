SELECT
    staff_id,
    first_name,
    last_name,
    email,
    store_id,
    active
FROM {{ source('local_bike', 'staffs') }}