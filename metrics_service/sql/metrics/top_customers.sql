WITH segment_customer_spend AS (
    SELECT 
        dc.segment,
        dc.customer_id,
        dc.region,
        sum(fo.amount_cents) as total_spent_cents,
        count(fo.order_id) as order_count,
        ROW_NUMBER() OVER (PARTITION BY dc.segment ORDER BY sum(fo.amount_cents) DESC) as rank_in_segment
    FROM warehouse.fact_orders fo
    JOIN warehouse.dim_customer dc ON fo.customer_sk = dc.customer_sk
    GROUP BY dc.segment, dc.customer_id, dc.region
)
SELECT 
    segment,
    customer_id,
    region,
    total_spent_cents,
    order_count,
    rank_in_segment
FROM segment_customer_spend
WHERE rank_in_segment <= :top_n
ORDER BY segment, rank_in_segment;
