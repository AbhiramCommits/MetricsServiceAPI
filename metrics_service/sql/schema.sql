CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Staging Tables (CDC feed simulation)
CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id VARCHAR(64) NOT NULL,
    region VARCHAR(64),
    segment VARCHAR(64),
    source_updated_at TIMESTAMPTZ NOT NULL,
    op_type CHAR(1) NOT NULL CHECK (op_type IN ('I', 'U', 'D'))
);

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    order_date DATE NOT NULL,
    amount_cents INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    op_type CHAR(1) NOT NULL CHECK (op_type IN ('I', 'U', 'D'))
);

-- Warehouse Tables
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    region VARCHAR(64),
    segment VARCHAR(64),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_sk INT REFERENCES warehouse.dim_customer(customer_sk),
    order_date DATE NOT NULL,
    amount_cents INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- ETL Control Tables
CREATE TABLE IF NOT EXISTS warehouse.etl_watermark (
    table_name VARCHAR(64) PRIMARY KEY,
    last_watermark TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.etl_run_history (
    run_id VARCHAR(64) PRIMARY KEY,
    table_name VARCHAR(64) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status VARCHAR(32) NOT NULL,
    rows_inserted INT DEFAULT 0,
    rows_updated INT DEFAULT 0,
    rows_deleted INT DEFAULT 0,
    watermark_before TIMESTAMPTZ,
    watermark_after TIMESTAMPTZ,
    error_message TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_stg_customers_updated ON staging.stg_customers(source_updated_at);
CREATE INDEX IF NOT EXISTS idx_stg_orders_updated ON staging.stg_orders(source_updated_at);
CREATE INDEX IF NOT EXISTS idx_dim_customer_biz ON warehouse.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON warehouse.dim_customer(customer_id, is_current);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON warehouse.fact_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_fact_orders_cust ON warehouse.fact_orders(customer_sk);
