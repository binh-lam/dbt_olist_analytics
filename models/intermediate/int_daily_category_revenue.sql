SELECT
    ord.order_date,
    prd.product_category_name,
    SUM(itm.price + itm.freight_value)    AS product_category_daily_revenue

FROM {{ ref('stg_olist__orders') }}                AS ord
    LEFT JOIN {{ ref('stg_olist__order_items') }}  AS itm ON ord.order_id = itm.order_id
    LEFT JOIN {{ ref('stg_olist__products') }}     AS prd ON itm.product_id = prd.product_id

WHERE
    ord.order_status NOT IN ('canceled', 'unavailable')

GROUP BY 1, 2