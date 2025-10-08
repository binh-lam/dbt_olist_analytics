SELECT 
    customer_id,
    customer_unique_id

FROM {{ source('olist_raw', 'customers') }}