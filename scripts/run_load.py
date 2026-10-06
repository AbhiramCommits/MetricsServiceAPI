import argparse
import asyncio
import time
from metrics_service.db import AsyncSessionLocal
from metrics_service.etl.loader import load_dim_customer, load_fact_orders, run_all


async def main(table: str, full_refresh: bool):
    start = time.time()
    async with AsyncSessionLocal() as session:
        if full_refresh:
            print("Performing full refresh (resetting watermarks)...")
            await session.execute(text("DELETE FROM warehouse.etl_watermark;"))
            await session.commit()

        if table == "dim_customer":
            res = await load_dim_customer(session)
            print(f"Loaded dim_customer: {res}")
        elif table == "fact_orders":
            res = await load_fact_orders(session)
            print(f"Loaded fact_orders: {res}")
        else:
            res = await run_all(session)
            print(f"Loaded all: {res}")

    duration = time.time() - start
    print(f"ETL completed in {duration:.4f} seconds.")


if __name__ == "__main__":
    from sqlalchemy import text

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table", choices=["dim_customer", "fact_orders", "all"], default="all"
    )
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()

    asyncio.run(main(args.table, args.full_refresh))
