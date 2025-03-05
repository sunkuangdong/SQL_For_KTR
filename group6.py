import asyncio
import aiomysql

async def connect_to_db():
    con = await aiomysql.connect(
        host="localhost",          
        port=3306,                 
        user="root",               
        password="QwE456",
        db="staging"
    )
    print("✅ Database connection successful")
    return con

async def create_table_single(cur, sql):
    await cur.execute(sql)

async def create_tables(con):
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

        await create_table_single(cur, 
            """
                CREATE TABLE IF NOT EXISTS updates_information (
                    updates_id INT AUTO_INCREMENT PRIMARY KEY,
                    article_link VARCHAR(255),
                    image_url VARCHAR(255),
                    text TEXT,
                    total_likes INT,
                    day INT,
                    month INT,
                    year INT
                )
            """
        )

        await create_table_single(cur,
            """
                CREATE TABLE IF NOT EXISTS similar_or_affiliated (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    link VARCHAR(255),
                    industry VARCHAR(255),
                    location VARCHAR(255),
                    company_rs_type VARCHAR(255)
                )
            """
        )

        await create_table_single(cur,
            """
                CREATE TABLE IF NOT EXISTS hq_information (
                    hq_id INT AUTO_INCREMENT PRIMARY KEY,
                    country VARCHAR(255),
                    city VARCHAR(255),
                    region VARCHAR(255),
                    metro VARCHAR(255),
                    street_address VARCHAR(255),
                    postal_code VARCHAR(255)   -- ✅ 修正错误：去掉最后的逗号
                )
            """
        )

        await create_table_single(cur,
            """
                CREATE TABLE IF NOT EXISTS specialities_relationship (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    linkedin_internal_id VARCHAR(255),
                    specialities_id INT,
                    FOREIGN KEY (specialities_id) REFERENCES specialities_name(specialities_id)
                )
            """
        )

        await create_table_single(cur,
            """
                CREATE TABLE IF NOT EXISTS company_size (
                    linkedin_internal_id VARCHAR(255) PRIMARY KEY,
                    company_size_min INT,
                    company_size_max INT
                )
            """
        )

        await create_table_single(cur,
            """
                CREATE TABLE IF NOT EXISTS company_information (
                    linkedin_internal_id VARCHAR(255) PRIMARY KEY,
                    description TEXT,
                    website TEXT,
                    industry TEXT,
                    company_size_on_linkedin INT,
                    company_type TEXT,
                    founded_year INT,
                    name TEXT,
                    tagline TEXT,
                    universal_name_id TEXT,
                    profile_pic_url TEXT,
                    background_cover_image_url TEXT,
                    search_id VARCHAR(255),
                    follower_count INT,
                    acquisitions VARCHAR(500),
                    exit_data VARCHAR(500),
                    extra VARCHAR(500),
                    categories JSON,
                    customer_list JSON,

                    hq_id INT UNIQUE,
                    locations_id INT UNIQUE,
                    similar_companies_id INT UNIQUE,
                    affiliated_companies_id INT UNIQUE,
                    updates_id INT UNIQUE,

                    FOREIGN KEY (hq_id) REFERENCES hq_information(hq_id),
                    FOREIGN KEY (locations_id) REFERENCES location_information(locations_id),
                    FOREIGN KEY (similar_companies_id) REFERENCES similar_or_affiliated(id),
                    FOREIGN KEY (affiliated_companies_id) REFERENCES similar_or_affiliated(id),
                    FOREIGN KEY (updates_id) REFERENCES updates_information(updates_id)
                )
            """
        )

        await create_table_single(cur,
            """
                ALTER TABLE specialities_relationship
                ADD CONSTRAINT fk_specialities_company
                FOREIGN KEY (linkedin_internal_id) REFERENCES company_information(linkedin_internal_id)
            """
        )

async def delete_tables(con):
    async with con.cursor() as cur:
        # First disable foreign key checks to avoid constraint issues
        await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        await cur.execute("SHOW TABLES")
        tables = await cur.fetchall()
        print("📊 Existing tables:", [table[0] for table in tables])

        for table in tables:
            await cur.execute(f"DROP TABLE IF EXISTS {table[0]}")

        # Re-enable foreign key checks
        await cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        print("✅ All tables deleted successfully!")

async def main():
    connection = None
    try:

        connection = await connect_to_db()
        await delete_tables(connection)
        await create_tables(connection)
        print("✅ Database connection closed")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    asyncio.run(main())
