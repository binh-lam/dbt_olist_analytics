SELECT 
    prd.product_id,
    prt.product_category_name_english    AS product_category_name

FROM {{ source('olist_raw', 'products') }}                                    AS prd
    LEFT JOIN {{ source('olist_raw', 'product_category_name_translation') }}  AS prt ON prd.product_category_name = prt.product_category_name