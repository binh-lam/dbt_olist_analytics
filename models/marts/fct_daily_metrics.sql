WITH daily_aggregates AS (
    -- Calculating daily orders, customers, and revenue
    SELECT
        ord.order_date,
        COUNT(DISTINCT ord.order_id)              AS daily_orders, --multiple order_id in stg_olist__payments
        COUNT(DISTINCT ord.customer_unique_id)    AS daily_customers,
        COALESCE(SUM(pmt.payment_value), 0)       AS daily_revenue --order_id may not exist in stg_olist__payments

    FROM {{ ref('int_orders') }}                    AS ord
        LEFT JOIN {{ ref('stg_olist__payments') }}  AS pmt ON ord.order_id = pmt.order_id

    WHERE
        ord.order_status NOT IN ('canceled', 'unavailable')

    GROUP BY 1
),

daily_category_ranking AS (
    -- Ranking categories by revenue per order date
    SELECT
        order_date,
        product_category_name,
        product_category_daily_revenue,
        SUM(product_category_daily_revenue) OVER (PARTITION BY order_date)                          AS total_daily_revenue,
        ROW_NUMBER() OVER (PARTITION BY order_date ORDER BY product_category_daily_revenue DESC)    AS category_rank
    
    FROM {{ ref('int_daily_category_revenue') }}
),

top_categories AS (
    -- Getting the top 3 categories with their percentage of the day's revenue and aggregate them into an array
    SELECT
        order_date,
        ARRAY_AGG(product_category_name ORDER BY category_rank)    AS top_daily_categories,
        ARRAY_AGG(ROUND(product_category_daily_revenue / total_daily_revenue * 100, 2) 
                                        ORDER BY category_rank)    AS top_daily_categories_pct

    FROM daily_category_ranking

    WHERE category_rank <= 3

    GROUP BY 1
)

-- Final SELECT
SELECT
    agg.order_date                                              AS order_date,
    agg.daily_orders                                            AS daily_orders,
    agg.daily_customers                                         AS daily_customers,
    ROUND(agg.daily_revenue, 2)                                 AS daily_revenue,
    ROUND(agg.daily_revenue / NULLIF(agg.daily_orders,0), 2)    AS daily_avg_order_value, --returns NULL if there's no orders for the day
    cat.top_daily_categories                                    AS top_daily_categories,
    cat.top_daily_categories_pct                                AS top_daily_categories_pct

FROM daily_aggregates         AS agg
    LEFT JOIN top_categories  AS cat ON agg.order_date = cat.order_date

ORDER BY agg.order_date DESC