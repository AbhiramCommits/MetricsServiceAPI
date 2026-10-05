import asyncio
import asyncpg
from metrics_service.config import settings

async def main():
    conn_str = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(conn_str)
    sql = open("metrics_service/sql/schema.sql").read()
    await conn.execute(sql)
    await conn.close()
    print("Schema applied successfully.")

if __name__ == "__main__":
    asyncio.run(main())
