import pymysql
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
        self.connection = None

    def init_connection(self):
        """Initialize the database connection."""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.db,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print("Database connection initialized.")
        except pymysql.MySQLError as e:
            print(f"Error connecting to the database: {e}")
    
    def close_connection(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def find_duplicates(self, domains):
        """
        Check which domains already exist in the database.
        :param domains: List of domains to check.
        :return: List of booleans where True means the domain exists (is a duplicate).
        """
        duplicate_flags = []
        try:
            with self.connection.cursor() as cursor:
                for domain in domains:
                    sql_query = "SELECT COUNT(*) AS count FROM domains WHERE domain = %s"
                    cursor.execute(sql_query, (domain,))
                    result = cursor.fetchone()
                    duplicate_flags.append(result['count'] > 0)
        except pymysql.MySQLError as e:
            print(f"Database error: {e}")
            duplicate_flags = [False] * len(domains)  # Assume non-duplicate on error
        return duplicate_flags

    def insert_non_duplicates(self, domains):
        """
        Insert only non-duplicate domains into the database, ignoring duplicates.
        :param domains: List of domains to check and insert if non-duplicate.
        :return: Dictionary {domain: id} of successfully inserted domains.
        """
        inserted_domains_ids = {}
        
        try:
            with self.connection.cursor() as cursor:
                for domain in domains:
                    sql_query = "INSERT IGNORE INTO domains (domain) VALUES (%s)"
                    cursor.execute(sql_query, (domain,))
                    
                    # If lastrowid is not 0, it means the insert was successful
                    if cursor.lastrowid:
                        inserted_domains_ids[domain] = cursor.lastrowid
                
                self.connection.commit()
        
        except pymysql.MySQLError as e:
            print(f"Database error: {e}")
        
        return inserted_domains_ids

    def insert_domains_ns(self, data):
        """
        Insert nameserver (NS) data into the domains_ns table.
        :param data: List of tuples containing (domain_id, ns).
        """
        try:
            with self.connection.cursor() as cursor:
                sql_query = """
                    INSERT INTO domains_ns (domain_id, ns, timestamp) 
                    VALUES (%s, %s, NOW())
                """
                cursor.executemany(sql_query, data)
                self.connection.commit()
        except pymysql.MySQLError as e:
            self.connection.rollback()
            print(f"Database error: {e}")

    def insert_domains_ip(self, data):
        """
        Insert IP data into the domains_ip table.
        :param data: List of tuples containing (domain_id, ip).
        """
        try:
            with self.connection.cursor() as cursor:
                sql_query = """
                    INSERT INTO domains_ip (domain_id, ip, timestamp) 
                    VALUES (%s, %s, NOW())
                """
                cursor.executemany(sql_query, data)
                self.connection.commit()
        except pymysql.MySQLError as e:
            self.connection.rollback()
            print(f"Database error: {e}")
