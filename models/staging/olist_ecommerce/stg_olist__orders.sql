SELECT 
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS DATE)    AS order_date

FROM {{ source('olist_raw', 'orders') }}