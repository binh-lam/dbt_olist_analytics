SELECT 
    order_id,
    payment_value

FROM {{ source('olist_raw', 'order_payments') }}