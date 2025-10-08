SELECT
    ord.order_id,
    ord.order_date,
    ord.order_status,
    cus.customer_unique_id

FROM {{ ref('stg_olist__orders') }}              AS ord
    LEFT JOIN {{ ref('stg_olist__customers') }}  AS cus ON ord.customer_id = cus.customer_id