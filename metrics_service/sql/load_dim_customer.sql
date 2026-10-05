WITH staging_changes AS (
    SELECT customer_id, region, segment, source_updated_at, op_type
    FROM staging.stg_customers
    WHERE source_updated_at > :watermark
),
expired AS (
    UPDATE warehouse.dim_customer dc
    SET valid_to = sc.source_updated_at, is_current = false
    FROM staging_changes sc
    WHERE dc.customer_id = sc.customer_id AND dc.is_current = true
    RETURNING dc.customer_id
),
inserted AS (
    INSERT INTO warehouse.dim_customer (customer_id, region, segment, valid_from, valid_to, is_current)
    SELECT 
        sc.customer_id,
        sc.region,
        sc.segment,
        sc.source_updated_at,
        NULL,
        CASE WHEN sc.op_type = 'D' THEN false ELSE true END
    FROM staging_changes sc
    WHERE sc.op_type != 'D'
    RETURNING customer_id
)
SELECT 
    (SELECT count(*) FROM staging_changes WHERE op_type = 'D') as deleted_count,
    (SELECT count(*) FROM inserted) as inserted_count,
    (SELECT count(*) FROM expired) as updated_count,
    (SELECT MAX(source_updated_at) FROM staging_changes) as max_ts;
