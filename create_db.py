import mysql.connector
from database import cursor

db_name = 'results'
cursor.execute(
    f"""
    DROP DATABASE IF EXISTS {db_name};
    """
)
cursor.execute(
    f"""
    CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET 'utf8';
    """
)
print(f"database {db_name} created")