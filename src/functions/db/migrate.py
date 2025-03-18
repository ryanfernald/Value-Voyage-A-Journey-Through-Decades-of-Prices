import os
import re
import subprocess
import sqlite3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

dump_file = '../../../data/db/mysql/mysql_dump.sql'

# Step 1: Dump the MySQL database to a file using mysqldump.
cmd = [
    'mysqldump',
    f'--host={DB_HOST}',
    f'--port={DB_PORT}',
    f'--user={DB_USER}',
    f'--password={DB_PASSWORD}',
    '--skip-extended-insert',  # Produces one INSERT per statement for easier processing.
    DB_NAME
]

print("Dumping MySQL database...")
with open(dump_file, 'w') as f:
    subprocess.run(cmd, stdout=f, check=True)
print("Dump completed.")

def convert_mysql_to_sqlite(sql):
    """
    Convert a MySQL SQL statement to SQLite-compatible SQL.
    This function handles basic adjustments such as:
      - Removing MySQL-specific options (AUTO_INCREMENT, ENGINE, etc.)
      - Replacing backticks with double quotes
      - Converting common MySQL data types to SQLite types
    Adjust this function further as needed for your schema.
    """
    # Remove MySQL-specific options
    sql = re.sub(r'\s+AUTO_INCREMENT=\d+', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s+ENGINE=\w+', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s+DEFAULT CHARSET=\w+', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s+COLLATE=\w+', '', sql, flags=re.IGNORECASE)
    # Replace backticks with double quotes
    sql = sql.replace('`', '"')
    # Adjust data types
    sql = sql.replace("int(", "INTEGER(")
    sql = sql.replace("bigint", "INTEGER")
    sql = sql.replace("tinyint", "INTEGER")
    sql = sql.replace("smallint", "INTEGER")
    sql = sql.replace("mediumint", "INTEGER")
    sql = sql.replace("unsigned", "")
    sql = sql.replace("double", "REAL")
    sql = sql.replace("float", "REAL")
    sql = sql.replace("datetime", "TEXT")
    sql = sql.replace("timestamp", "TEXT")
    return sql

# Step 2: Load and process the dump file.
with open(dump_file, 'r') as f:
    dump_sql = f.read()

# Split the dump into individual SQL statements using ';' as the delimiter.
# Note: This simplistic splitting may need refinement if your dump contains semicolons within statements.
statements = dump_sql.split(';')
sqlite_statements = []

for stmt in statements:
    stmt = stmt.strip()
    if not stmt:
        continue
    # Convert CREATE TABLE statements
    if stmt.upper().startswith("CREATE TABLE"):
        converted = convert_mysql_to_sqlite(stmt)
        sqlite_statements.append(converted)
    # Process INSERT statements (assuming values are compatible)
    elif stmt.upper().startswith("INSERT INTO"):
        sqlite_statements.append(stmt)
    # Other statements (like SET, DROP, etc.) can be skipped or handled as needed.

# Connect to (or create) the SQLite database.
sqlite_conn = sqlite3.connect('../../../data/db/sqlite/database.sqlite')
cursor = sqlite_conn.cursor()

print("Processing SQL statements and creating SQLite database...")
for stmt in sqlite_statements:
    try:
        cursor.execute(stmt)
    except sqlite3.OperationalError as e:
        print("Error executing statement:")
        print(stmt)
        print(e)

sqlite_conn.commit()
cursor.close()
sqlite_conn.close()

print("Dump loaded and processed successfully into 'database.sqlite'.")
