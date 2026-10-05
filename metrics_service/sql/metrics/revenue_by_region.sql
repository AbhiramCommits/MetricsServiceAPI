WITH region_totals AS (
    SELECT 
        dc.region,
        count(fo.order_id) as order_count,
        sum(fo.amount_cents) as total_revenue_cents
    FROM warehouse.fact_orders fo
    JOIN warehouse.dim_customer dc ON fo.customer_sk = dc.customer_sk
    GROUP BY dc.region
),
grand_total AS (
    SELECT sum(total_revenue_cents) as global_revenue FROM region_totals
)
SELECT 
    rt.region,
    rt.order_count,
    rt.total_revenue_cents,
    ROUND((rt.total_revenue_cents::numeric / NULLIF(gt.global_revenue, 0)) * 100, 2) as share_of_total_pct,
    RANK() OVER (ORDER BY rt.total_revenue_cents DESC) as revenue_rank
FROM region_totals rt, grand_total gt
ORDER BY revenue_rank;
