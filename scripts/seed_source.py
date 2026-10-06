import argparse
import random
from datetime import datetime, timedelta, timezone
import asyncpg
import asyncio


async def seed(database_url: str, cdc_batch: bool = False):
    # parse connection string or use direct asyncpg
    # database_url format: postgresql+asyncpg://user:pass@host:port/db
    conn_str = database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(conn_str)

    try:
        if not cdc_batch:
            print("Clearing staging tables for full seed...")
            await conn.execute(
                "TRUNCATE TABLE staging.stg_orders, staging.stg_customers;"
            )

            random.seed(42)
            regions = ["North", "South", "East", "West", "Central"]
            segments = ["Enterprise", "SMB", "Consumer", "Partner"]
            statuses = ["completed", "pending", "cancelled", "refunded"]

            start_date = datetime.now(timezone.utc) - timedelta(days=180)

            print("Generating 5,000 customers...")
            customers = []
            for i in range(1, 5001):
                cust_id = f"CUST_{i:05d}"
                region = random.choice(regions)
                segment = random.choice(segments)
                updated_at = start_date + timedelta(
                    days=random.randint(0, 150), hours=random.randint(0, 23)
                )
                customers.append((cust_id, region, segment, updated_at, "I"))

            await conn.executemany(
                "INSERT INTO staging.stg_customers (customer_id, region, segment, source_updated_at, op_type) VALUES ($1, $2, $3, $4, $5)",
                customers,
            )

            print("Generating 50,000 orders...")
            orders = []
            for i in range(1, 50001):
                order_id = f"ORD_{i:06d}"
                cust_id = f"CUST_{random.randint(1, 5000):05d}"
                ord_days = random.randint(0, 179)
                order_date = (start_date + timedelta(days=ord_days)).date()
                amount_cents = random.randint(1000, 250000)
                status = random.choice(statuses)
                updated_at = start_date + timedelta(
                    days=ord_days,
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                orders.append(
                    (
                        order_id,
                        cust_id,
                        order_date,
                        amount_cents,
                        status,
                        updated_at,
                        "I",
                    )
                )

            # Batch insert orders
            batch_size = 5000
            for i in range(0, len(orders), batch_size):
                await conn.executemany(
                    "INSERT INTO staging.stg_orders (order_id, customer_id, order_date, amount_cents, status, source_updated_at, op_type) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    orders[i : i + batch_size],
                )
            print("Full seed completed successfully.")

        else:
            print("Emitting CDC batch...")
            random.seed()
            now = datetime.now(timezone.utc)
            statuses = ["completed", "pending", "cancelled", "refunded"]

            # Insert/Update some customers
            cdc_customers = []
            for i in range(5001, 5100):
                cust_id = f"CUST_{i:05d}"
                region = random.choice(["North", "South", "East", "West", "Central"])
                segment = random.choice(["Enterprise", "SMB", "Consumer"])
                cdc_customers.append((cust_id, region, segment, now, "I"))

            # Update existing customer
            cdc_customers.append(("CUST_00001", "North", "Enterprise", now, "U"))

            await conn.executemany(
                "INSERT INTO staging.stg_customers (customer_id, region, segment, source_updated_at, op_type) VALUES ($1, $2, $3, $4, $5)",
                cdc_customers,
            )

            # Insert/Update/Delete orders
            cdc_orders = []
            for i in range(50001, 50500):
                order_id = f"ORD_{i:06d}"
                cust_id = f"CUST_{random.randint(1, 5000):05d}"
                order_date = datetime.now(timezone.utc).date()
                amount_cents = random.randint(2000, 300000)
                status = random.choice(statuses)
                cdc_orders.append(
                    (order_id, cust_id, order_date, amount_cents, status, now, "I")
                )

            # Update existing order
            cdc_orders.append(
                (
                    "ORD_00001",
                    "CUST_00001",
                    datetime.now(timezone.utc).date(),
                    99999,
                    "completed",
                    now,
                    "U",
                )
            )
            # Delete existing order (soft delete op_type = 'D')
            cdc_orders.append(
                (
                    "ORD_00002",
                    "CUST_00002",
                    datetime.now(timezone.utc).date(),
                    1000,
                    "cancelled",
                    now,
                    "D",
                )
            )

            await conn.executemany(
                "INSERT INTO staging.stg_orders (order_id, customer_id, order_date, amount_cents, status, source_updated_at, op_type) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                cdc_orders,
            )
            print("CDC batch emitted successfully.")

    finally:
        await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cdc-batch", action="store_true")
    args = parser.parse_args()

    from metrics_service.config import settings

    asyncio.run(seed(settings.DATABASE_URL, cdc_batch=args.cdc_batch))
