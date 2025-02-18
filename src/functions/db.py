import mysql.connector
from dotenv import load_dotenv
import os
import mysql.connector
from mysql.connector import Error
import json
import pandas as pd

# Load environment variables from .env
load_dotenv()

# Retrieve DB credentials from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT'))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

def test_db_connection():
    try:
        # Connect to MySQL using credentials
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM test;")
            row = cursor.fetchone()
            if row:
                # Assuming the table 'test' has one column, return its value
                result = row[0]
                return json.dumps({"result": result})
            else:
                return json.dumps({"error": "No data found in test table"})
    except Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def insert_good_price_entry(name, price, date, good_unit, data_source):
    """
    Inserts a good price entry into the good_prices table.

    Parameters:
        name (str): The name field.
        price (float): The price in dollars.
        date (str): The date in 'YYYY-MM-DD' format.
        good_unit (str): The unit associated with the good price.
        data_source (str): The source of the data.

    Returns:
        A JSON-formatted string indicating success or error.
    """
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,         # Defined in configuration
            port=DB_PORT,         # Defined in configuration
            user=DB_USER,         # Defined in configuration
            password=DB_PASSWORD, # Defined in configuration
            database=DB_NAME      # Defined in configuration
        )
        if connection.is_connected():
            cursor = connection.cursor()
            insert_query = """
                INSERT INTO goods_prices (name, price, date, good_unit, data_source)
                VALUES (%s, %s, %s, %s, %s);
            """
            values = (name, price, date, good_unit, data_source)
            cursor.execute(insert_query, values)
            connection.commit()
            return json.dumps({"result": "Good price entry inserted successfully."})
    except Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def bulk_insert_good_price_entries(df):
    """
    Performs a bulk insertion of good price entries into the good_prices table.

    Parameters:
        df (DataFrame): A DataFrame containing the following columns:
            'good name', 'price', 'date', 'good unit', 'source'

    Returns:
        A JSON-formatted string indicating success or error.
    """
    records = df[['good name', 'price', 'date', 'good unit', 'source']].values.tolist()

    for record in records:
        if pd.isna(record[1]):
            record[1] = None
    print(records)
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            cursor = connection.cursor()
            insert_query = """
                INSERT INTO goods_prices (name, price, date, good_unit, data_source)
                VALUES (%s, %s, %s, %s, %s);
            """
            cursor.executemany(insert_query, records)
            connection.commit()
            return json.dumps({"result": f"{cursor.rowcount} records inserted successfully."})
    except Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def create_good_prices_table():
    """
    Creates the 'good_prices' table with columns:
        - id (auto-increment primary key)
        - name (VARCHAR)
        - price (DECIMAL, representing dollars)
        - date (DATE)
        - good_unit (VARCHAR)
        - data_source (VARCHAR)

    Returns:
        A dictionary indicating success or error.
    """
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            cursor = connection.cursor()
            create_table_query = """
                CREATE TABLE IF NOT EXISTS goods_prices (
                    name VARCHAR(255) NOT NULL,
                    price DECIMAL(20,10),
                    date DATE NOT NULL,
                    good_unit VARCHAR(50),
                    data_source VARCHAR(255),
                    PRIMARY KEY(name, date, data_source)
                );
            """
            cursor.execute(create_table_query)
            connection.commit()
            result = {"result": "Table 'good_prices' created successfully."}
            return result
    except Error as e:
        return {"error": str(e)}
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    result = create_good_prices_table()
    print(json.dumps(result, indent=4))