WITH staging_changes AS (
    SELECT order_id, customer_id, order_date, amount_cents, status, source_updated_at, op_type
    FROM staging.stg_orders
    WHERE source_updated_at > :watermark
),
resolved AS (
    SELECT DISTINCT ON (sc.order_id)
        sc.order_id,
        dc.customer_sk,
        sc.order_date,
        sc.amount_cents,
        sc.status,
        sc.source_updated_at,
        sc.op_type
    FROM staging_changes sc
    JOIN warehouse.dim_customer dc ON sc.customer_id = dc.customer_id AND dc.is_current = true
    ORDER BY sc.order_id, sc.source_updated_at DESC
),
deleted AS (
    DELETE FROM warehouse.fact_orders fo
    USING resolved r
    WHERE fo.order_id = r.order_id AND r.op_type = 'D'
    RETURNING fo.order_id
),
upserted AS (
    INSERT INTO warehouse.fact_orders (order_id, customer_sk, order_date, amount_cents, status, updated_at)
    SELECT order_id, customer_sk, order_date, amount_cents, status, source_updated_at
    FROM resolved
    WHERE op_type != 'D'
    ON CONFLICT (order_id) DO UPDATE 
    SET customer_sk = EXCLUDED.customer_sk,
        order_date = EXCLUDED.order_date,
        amount_cents = EXCLUDED.amount_cents,
        status = EXCLUDED.status,
        updated_at = EXCLUDED.updated_at
    RETURNING order_id
)
SELECT 
    (SELECT count(*) FROM resolved WHERE op_type = 'D') as deleted_count,
    (SELECT count(*) FROM resolved WHERE op_type = 'I') as inserted_count,
    (SELECT count(*) FROM resolved WHERE op_type = 'U') as updated_count,
    (SELECT MAX(source_updated_at) FROM staging_changes) as max_ts;
