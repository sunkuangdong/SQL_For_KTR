import asyncio
import aiomysql
import json
import os

JSON_DIR = "C:/Users/KSUN5/Desktop/DATABASE/group_project2/SQL_For_KTR/staging-example/source-data/company-profile"  

async def connect_to_db():
    pool = await aiomysql.create_pool(
        host="localhost",          
        port=3306,                 
        user="root",               
        password="QwE456",
        db="staging",
        minsize=1,  # 连接池最小连接数
        maxsize=10  # 连接池最大连接数
    )
    print("✅ Database connection pool created")
    return pool

async def create_table_single(cur, sql):
    await cur.execute(sql)

async def create_tables(pool):
    async with pool.acquire() as con:
        async with con.cursor() as cur:
            await create_table_single(cur, 
                """
                    CREATE TABLE IF NOT EXISTS specialities_name (
                        specialities_id INT AUTO_INCREMENT PRIMARY KEY,
                        specialities_name VARCHAR(255)
                    )
                """
            )
            await create_table_single(cur, 
                """
                    CREATE TABLE IF NOT EXISTS location_information (
                        locations_id INT AUTO_INCREMENT PRIMARY KEY,
                        country VARCHAR(255),
                        city VARCHAR(255),
                        postal_code VARCHAR(255),
                        line_1 VARCHAR(255),
                        is_hq BOOLEAN,
                        state VARCHAR(255)
                    )
                """
            )
            await con.commit()
            print("✅ Tables created successfully!")

async def delete_tables(pool):
    async with pool.acquire() as con:
        async with con.cursor() as cur:
            await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            await cur.execute("SHOW TABLES")
            tables = await cur.fetchall()
            print("📊 Existing tables:", [table[0] for table in tables])

            for table in tables:
                await cur.execute(f"DROP TABLE IF EXISTS {table[0]}")

            await cur.execute("SET FOREIGN_KEY_CHECKS = 1")
            await con.commit()
            print("✅ All tables deleted successfully!")

async def read_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

async def insert_company_information(con, json_data):
    async with con.cursor() as cur:
        await cur.execute(
            """
                INSERT INTO company_information (
                    linkedin_internal_id, description, website, industry
                )
                VALUES (%s, %s, %s, %s)
            """,
            (
                json_data.get("linkedin_internal_id", None),
                json_data.get("description", None),
                json_data.get("website", None),
                json_data.get("industry", None),
            )
        )
    await con.commit()

async def insert_location_information(con, json_data):
    async with con.cursor() as cur:
        await cur.execute(
            """
                INSERT INTO location_information (locations_id, country, city, postal_code, line_1, is_hq, state)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                json_data.get("locations_id", None),
                json_data.get("country", None),
                json_data.get("city", None),
                json_data.get("postal_code", None),
                json_data.get("line_1", None),
                json_data.get("is_hq", None),
                json_data.get("state", None)
            )
        )
    await con.commit()

async def process_all_json(pool):
    async with pool.acquire() as con:  # 从连接池获取连接
        for file_name in os.listdir(JSON_DIR):
            if file_name.endswith(".json"):
                file_path = os.path.join(JSON_DIR, file_name)
                json_data = await read_json(file_path)
                if json_data:
                    # 依次执行插入操作，避免 `Cursor` 并发问题
                    await insert_company_information(con, json_data)
                    await insert_location_information(con, json_data)

        print("✅ All JSON data processed!")

async def main():
    pool = None
    try:
        pool = await connect_to_db()
        await delete_tables(pool)
        await create_tables(pool)
        await process_all_json(pool)
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if pool:
            pool.close()
            await pool.wait_closed()
            print("✅ Database connection pool closed")

if __name__ == "__main__":
    asyncio.run(main())
