import os
import logging
import psycopg
from psycopg_pool import AsyncConnectionPool
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional # Added Optional here

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

db_pool: Optional[AsyncConnectionPool] = None

async def get_db_pool():
    global db_pool
    if db_pool is None:
        if not DATABASE_URL:
            logger.error("DATABASE_URL environment variable is not set.")
            raise ValueError("DATABASE_URL environment variable is not set.")
        try:
            db_pool = AsyncConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=10, open=psycopg.AsyncConnection.connect)
            async with db_pool.connection() as conn:
                await conn.execute("SELECT 1")
            logger.info("DB pool initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize DB pool: {e}", exc_info=True)
            raise
    return db_pool

async def get_all_active_sources() -> List[Dict[str, Any]]:
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute("SELECT * FROM sources WHERE status = 'active' ORDER BY id;")
            return await cur.fetchall()

# Для локального тестування (не запускається при імпорті в інший файл)
if __name__ == "__main__":
    import asyncio
    async def test_db_connection():
        try:
            pool = await get_db_pool()
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT version();")
                    version = await cur.fetchone()
                    print(f"PostgreSQL version: {version[0]}")
            print("Database connection successful.")
        except Exception as e:
            print(f"Database connection failed: {e}")
        finally:
            if db_pool:
                await db_pool.close()
    asyncio.run(test_db_connection())
