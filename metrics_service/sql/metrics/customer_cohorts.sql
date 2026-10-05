WITH customer_first_order AS (
    SELECT 
        customer_sk,
        MIN(order_date) as first_order_date,
        date_trunc('month', MIN(order_date))::date as cohort_month
    FROM warehouse.fact_orders
    GROUP BY customer_sk
),
customer_orders AS (
    SELECT DISTINCT
        fo.customer_sk,
        cfo.cohort_month,
        date_trunc('month', fo.order_date)::date as order_month
    FROM warehouse.fact_orders fo
    JOIN customer_first_order cfo ON fo.customer_sk = cfo.customer_sk
),
cohort_sizes AS (
    SELECT cohort_month, count(DISTINCT customer_sk) as cohort_size
    FROM customer_first_order
    GROUP BY cohort_month
)
SELECT 
    cs.cohort_month,
    co.order_month,
    cs.cohort_size,
    count(DISTINCT co.customer_sk) as active_customers,
    ROUND((count(DISTINCT co.customer_sk)::numeric / cs.cohort_size) * 100, 2) as retention_rate_pct
FROM customer_orders co
JOIN cohort_sizes cs ON co.cohort_month = cs.cohort_month
GROUP BY cs.cohort_month, co.order_month, cs.cohort_size
ORDER BY cs.cohort_month, co.order_month;
