SELECT 
    fo.order_date,
    count(fo.order_id) as order_count,
    sum(fo.amount_cents) as total_revenue_cents,
    avg(sum(fo.amount_cents)) OVER (ORDER BY fo.order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_revenue_7d
FROM warehouse.fact_orders fo
WHERE (:start_date::date IS NULL OR fo.order_date >= :start_date::date)
  AND (:end_date::date IS NULL OR fo.order_date <= :end_date::date)
GROUP BY fo.order_date
ORDER BY fo.order_date DESC
LIMIT :limit;
