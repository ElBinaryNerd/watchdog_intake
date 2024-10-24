import aiomysql
import asyncio
from dotenv import load_dotenv
import os

# Load environment variables from the .env file at the root of the app
load_dotenv()

class DBManager:
    def __init__(self):
        # Load DB credentials from environment variables
        self.host = os.getenv("DB_HOST")
        self.port = int(os.getenv("DB_PORT"))
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.db = os.getenv("DB_NAME")
        self.minsize = 5  # default min connections
        self.maxsize = 20  # default max connections
        self.timeout = 10  # default connection timeout
        self.pool = None

    async def init_pool(self):
        """Initialize the connection pool with given parameters."""
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db,
            minsize=self.minsize,
            maxsize=self.maxsize,
            connect_timeout=self.timeout,
        )
        print("Database connection pool initialized.")

    async def close_pool(self):
        """Close the connection pool."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            print("Database connection pool closed.")

    async def find_duplicates(self, domains):
        """
        Check which domains already exist in the database.
        :param domains: List of domains to check.
        :return: List of booleans where True means the domain exists (is a duplicate).
        """
        duplicate_flags = []
        async with self.pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                try:
                    for domain in domains:
                        sql_query = "SELECT COUNT(*) AS count FROM domains WHERE domain = %s"
                        await cursor.execute(sql_query, (domain,))
                        result = await cursor.fetchone()
                        duplicate_flags.append(result['count'] > 0)
                except aiomysql.MySQLError as e:
                    print(f"Database error: {e}")
                    duplicate_flags = [False] * len(domains)  # Assume non-duplicate on error
        return duplicate_flags

    async def insert_non_duplicates(self, domains):
        """
        Insert only non-duplicate domains into the database, ignoring duplicates.
        :param domains: List of domains to check and insert if non-duplicate.
        :return: Dictionary {domain: id} of successfully inserted domains.
        """
        inserted_domains_ids = {}
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    for domain in domains:
                        sql_query = "INSERT IGNORE INTO domains (domain) VALUES (%s)"
                        await cursor.execute(sql_query, (domain,))
                        
                        # If lastrowid is not 0, it means the insert was successful
                        if cursor.lastrowid:
                            inserted_domains_ids[domain] = cursor.lastrowid
                            
                    await connection.commit()
                except aiomysql.MySQLError as e:
                    print(f"Database error: {e}")
        return inserted_domains_ids

    async def insert_domains_ns(self, data):
        """
        Insert nameserver (NS) data into the domains_ns table.
        :param data: List of tuples containing (domain_id, ns).
        """
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    # Prepare the bulk insert query for nameservers
                    sql_query = """
                        INSERT INTO domains_ns (domain_id, ns, timestamp) 
                        VALUES (%s, %s, NOW())
                    """
                    await cursor.executemany(sql_query, data)
                    await connection.commit()
                except aiomysql.MySQLError as e:
                    await connection.rollback()
                    print(f"Database error: {e}")

    async def insert_domains_ip(self, data):
        """
        Insert IP data into the domains_ip table.
        :param data: List of tuples containing (domain_id, ip).
        """
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    # Prepare the bulk insert query for IPs
                    sql_query = """
                        INSERT INTO domains_ip (domain_id, ip, timestamp) 
                        VALUES (%s, %s, NOW())
                    """
                    await cursor.executemany(sql_query, data)
                    await connection.commit()
                except aiomysql.MySQLError as e:
                    await connection.rollback()
                    print(f"Database error: {e}")
