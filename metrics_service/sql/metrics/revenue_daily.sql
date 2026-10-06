SELECT 
    fo.order_date,
    count(fo.order_id) as order_count,
    sum(fo.amount_cents) as total_revenue_cents,
    avg(sum(fo.amount_cents)) OVER (ORDER BY fo.order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_revenue_7d
FROM warehouse.fact_orders fo
WHERE (CAST(:start_date AS DATE) IS NULL OR fo.order_date >= CAST(:start_date AS DATE))
  AND (CAST(:end_date AS DATE) IS NULL OR fo.order_date <= CAST(:end_date AS DATE))
GROUP BY fo.order_date
ORDER BY fo.order_date DESC
LIMIT :limit;
