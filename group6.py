import asyncio
import aiomysql
import json
import os
import time

JSON_DIR = "C:/Users/KSUN5/Desktop/DATABASE/group_project2/SQL_For_KTR/staging-example/source-data/company-profile"  

async def connect_to_db():
    con = await aiomysql.create_pool(
        host="localhost",          
        port=3306,                 
        user="root",               
        password="QwE456",
        db="staging",
        minsize=1,
        maxsize=10
    )
    print("✅ Database connection successful")
    return con

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
                    CREATE TABLE IF NOT EXISTS similar_or_affiliated (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        group_id INT NOT NULL,
                        name VARCHAR(255),
                        link VARCHAR(255),
                        industry VARCHAR(255),
                        location VARCHAR(255),
                        company_rs_type INT,
                        INDEX idx_group_id (group_id)
                    )
                """
            )

            await create_table_single(cur,
                """
                    CREATE TABLE IF NOT EXISTS hq_information (
                        hq_id INT AUTO_INCREMENT PRIMARY KEY,
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
                        similar_companies_id INT NULL,
                        affiliated_companies_id INT NULL,

                        FOREIGN KEY (hq_id) REFERENCES hq_information(hq_id),
                        FOREIGN KEY (similar_companies_id) REFERENCES similar_or_affiliated(group_id),
                        FOREIGN KEY (affiliated_companies_id) REFERENCES similar_or_affiliated(group_id)
                    )
                """
            )

            await create_table_single(cur, 
                """
                    CREATE TABLE IF NOT EXISTS updates_information (
                        updates_id INT AUTO_INCREMENT PRIMARY KEY,
                        linkedin_internal_id VARCHAR(255),
                        article_link TEXT,
                        image_url VARCHAR(255),
                        text TEXT,
                        total_likes INT,
                        day INT,
                        month INT,
                        year INT,
                        FOREIGN KEY (linkedin_internal_id) REFERENCES company_information(linkedin_internal_id)
                    )
                """
            )

            await create_table_single(cur, 
                """
                    CREATE TABLE IF NOT EXISTS location_information (
                        locations_id INT AUTO_INCREMENT PRIMARY KEY,
                        linkedin_internal_id VARCHAR(255),
                        country VARCHAR(255),
                        city VARCHAR(255),
                        postal_code VARCHAR(255),
                        line_1 VARCHAR(255),
                        is_hq BOOLEAN,
                        state VARCHAR(255),
                        FOREIGN KEY (linkedin_internal_id) REFERENCES company_information(linkedin_internal_id)
                    )
                """
            )

            await create_table_single(cur,
                """
                    CREATE TABLE IF NOT EXISTS specialities_relationship (
                        specialities_id INT PRIMARY KEY,
                        linkedin_internal_id VARCHAR(255),
                        FOREIGN KEY (linkedin_internal_id) REFERENCES company_information(linkedin_internal_id),
                        FOREIGN KEY (specialities_id) REFERENCES specialities_name(specialities_id)
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

            print("✅ Tables created successfully!")

async def delete_tables(pool):
    async with pool.acquire() as con:
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

async def read_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data

async def insert_company_information(pool, json_data):
    async with pool.acquire() as con:
        async with con.cursor() as cur:
            linkedin_internal_id = json_data.get("linkedin_internal_id", None)
            description = json_data.get("description", None)
            website = json_data.get("website", None)
            industry = json_data.get("industry", None)
            company_size_on_linkedin = json_data.get("company_size_on_linkedin", None)
            company_type = json_data.get("company_type", None)
            founded_year = json_data.get("founded_year", None)
            name = json_data.get("name", None)
            tagline = json_data.get("tagline", None)
            universal_name_id = json_data.get("universal_name_id", None)
            profile_pic_url = json_data.get("profile_pic_url", None)
            background_cover_image_url = json_data.get("background_cover_image_url", None)
            search_id = json_data.get("search_id", None)
            follower_count = json_data.get("follower_count", None)
            acquisitions = json_data.get("acquisitions", None)
            exit_data = json_data.get("exit_data", None)
            extra = json_data.get("extra", None)
            categories = json_data.get("categories", None)
            customer_list = json_data.get("customer_list", None)

            await cur.execute(
                """
                    INSERT INTO company_information (
                        linkedin_internal_id, description, website, industry, company_size_on_linkedin, 
                        company_type, founded_year, name, tagline, universal_name_id, profile_pic_url, 
                        background_cover_image_url, search_id, follower_count, 
                        acquisitions, exit_data, extra, categories, customer_list
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                        linkedin_internal_id, description, website, industry, company_size_on_linkedin, 
                        company_type, founded_year, name, tagline, universal_name_id, profile_pic_url, 
                        background_cover_image_url, search_id, follower_count, 
                        acquisitions, exit_data, extra, categories, customer_list
                )
            )
            await con.commit()

async def insert_location_information(pool, json_data):
    async with pool.acquire() as con:
        async with con.cursor() as cur:
            linkedin_internal_id = json_data.get("linkedin_internal_id")
            locations_json_data = json_data.get("locations")
            for location in locations_json_data:
                locations_id = location.get("locations_id")
                country = location.get("country", None)
                city = location.get("city", None)
                postal_code = location.get("postal_code", None)
                line_1 = location.get("line_1", None)
                is_hq = location.get("is_hq", None)
                state = location.get("state", None)
                await cur.execute(
                    """
                        INSERT INTO location_information (locations_id, linkedin_internal_id, country, city, postal_code, line_1, is_hq, state)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (locations_id, linkedin_internal_id, country, city, postal_code, line_1, is_hq, state)
                )
                await con.commit()

async def insert_updates_information(pool, json_data):
    async with pool.acquire() as con:
        async with con.cursor() as cur:
            linkedin_internal_id = json_data.get("linkedin_internal_id")
            updates_information = json_data.get("updates")
            for update in updates_information:
                updates_id = update.get("updates_id")
                article_link = update.get("article_link", None)
                image_url = update.get("image_url", None)
                text = update.get("text", None)
                total_likes = update.get("total_likes", None)

                posted_on = update.get("posted_on", None)
                if posted_on:
                    day = posted_on.get("day", None)
                    month = posted_on.get("month", None)
                    year = posted_on.get("year", None)
                else:
                    day = None
                    month = None
                    year = None
                await cur.execute(
                    """
                        INSERT INTO updates_information (updates_id, linkedin_internal_id, article_link, image_url, text, total_likes, day, month, year)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (updates_id, linkedin_internal_id, article_link, image_url, text, total_likes, day, month, year)
                )
                await con.commit()

async def insert_similar_or_affiliated(pool, json_data):
    async with pool.acquire() as con:
        async with con.cursor() as cur:
            linkedin_internal_id = json_data.get("linkedin_internal_id")
            similar_companies = json_data.get("similar_companies", [])
            affiliated_companies = json_data.get("affiliated_companies", [])

            similar_group_id = None
            affiliated_group_id = None

            await cur.execute("SELECT MAX(group_id) FROM similar_or_affiliated")
            result = await cur.fetchone()
            current_group_id = (result[0] or 0) + 1

            if similar_companies:
                similar_group_id = current_group_id
                for company in similar_companies:
                    name = company.get("name", None)
                    link = company.get("link", None)
                    industry = company.get("industry", None)
                    location = company.get("location", None)

                    await cur.execute(
                        """
                        INSERT INTO similar_or_affiliated 
                        (name, link, industry, location, company_rs_type, group_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (name, link, industry, location, 0, similar_group_id)
                    )
                current_group_id += 1

            if affiliated_companies:
                affiliated_group_id = current_group_id
                for company in affiliated_companies:
                    name = company.get("name", None)
                    link = company.get("link", None)
                    industry = company.get("industry", None)
                    location = company.get("location", None)

                    await cur.execute(
                        """
                        INSERT INTO similar_or_affiliated 
                        (name, link, industry, location, company_rs_type, group_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (name, link, industry, location, 1, affiliated_group_id)
                    )

            if similar_group_id or affiliated_group_id:
                await cur.execute(
                    """
                    UPDATE company_information
                    SET similar_companies_id = %s, affiliated_companies_id = %s
                    WHERE linkedin_internal_id = %s
                    """,
                    (similar_group_id, affiliated_group_id, linkedin_internal_id)
                )

            await con.commit()

async def insert_hq_information(pool, json_data):
    async with pool.acquire() as con:
        async with con.cursor() as cur:
            linkedin_internal_id = json_data.get("linkedin_internal_id")
            hq_information = json_data.get("hq")
            if not hq_information:
                return
            country = hq_information.get("country", None)
            city = hq_information.get("city", None)
            postal_code = hq_information.get("postal_code", None)
            line_1 = hq_information.get("line_1", None)
            is_hq = hq_information.get("is_hq", None)
            state = hq_information.get("state", None)
            await cur.execute(
                """
                    INSERT INTO hq_information (country, city, postal_code, line_1, is_hq, state)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (country, city, postal_code, line_1, is_hq, state)
            )

            await cur.execute("SELECT LAST_INSERT_ID()")
            hq_id = (await cur.fetchone())[0]

            await cur.execute(
                """
                UPDATE company_information
                SET hq_id = %s
                WHERE linkedin_internal_id = %s
                """,
                (hq_id, linkedin_internal_id)
            )
            await con.commit()

async def insert_specialities_relationship(pool, json_data):
    async with pool.acquire() as con:
        async with con.cursor() as cur:
            linkedin_internal_id = json_data.get("linkedin_internal_id")  
            specialities_list = json_data.get("specialities", [])

            for speciality_name in specialities_list:
                specialities_id = None
                await cur.execute(
                    "SELECT specialities_id FROM specialities_name WHERE specialities_name = %s", 
                    (speciality_name)
                )
                result = await cur.fetchone()
                
                if result:
                    specialities_id = result[0]
                else:
                        await cur.execute(
                            "INSERT INTO specialities_name (specialities_name) VALUES (%s)", 
                            (speciality_name)
                        )
                        specialities_id = cur.lastrowid
                await cur.execute(
                    """
                    INSERT INTO specialities_relationship (specialities_id, linkedin_internal_id)
                    VALUES (%s, %s)
                    AS new_values
                    ON DUPLICATE KEY UPDATE 
                    linkedin_internal_id = new_values.linkedin_internal_id
                    """,
                    (specialities_id, linkedin_internal_id)
                )
            await con.commit()

async def insert_company_size(pool, json_data):
    async with pool.acquire() as con:
        async with con.cursor() as cur:
            linkedin_internal_id = json_data.get("linkedin_internal_id", None)
            company_size_min = json_data.get("company_size", None)[0]
            company_size_max = json_data.get("company_size", None)[1]

            await cur.execute(
                """
                    INSERT INTO company_size (linkedin_internal_id, company_size_min, company_size_max)
                    VALUES (%s, %s, %s)
                """,
                (linkedin_internal_id, company_size_min, company_size_max)
            )
            await con.commit()

async def process_all_json(pool):
    for file_name in os.listdir(JSON_DIR):
        if file_name.endswith(".json"):
            file_path = os.path.join(JSON_DIR, file_name)
            if file_name == "null.json":
                continue
            json_data = await read_json(file_path)
            if json_data:
                await insert_company_information(pool, json_data)
                await insert_location_information(pool, json_data)
                await insert_updates_information(pool, json_data)
                await insert_similar_or_affiliated(pool, json_data)
                await insert_hq_information(pool, json_data)
                await insert_specialities_relationship(pool, json_data)
                await insert_company_size(pool, json_data)
    print("✅ All JSON data processed!")

async def main():
    start_time = time.time()
    pool = None
    try:
        pool = await connect_to_db()
        await delete_tables(pool)
        await create_tables(pool)
        await process_all_json(pool)
        end_time = time.time()
        print(f"✅ Time taken: {end_time - start_time} seconds")
        print("✅ Database connection closed")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        if pool:
            pool.close()
            await pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
