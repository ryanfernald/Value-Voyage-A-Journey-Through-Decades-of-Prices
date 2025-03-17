import mysql.connector
from dotenv import load_dotenv
import os
import mysql.connector
from mysql.connector import Error
import json
import pandas as pd
from datetime import date
from decimal import Decimal
import matplotlib.pyplot as plt

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


def fetch_good_prices(output_format='json'):
    """
    Retrieves all entries from the goods_prices table.

    Returns:
        A JSON-formatted string containing the retrieved data or an error message.
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
            cursor = connection.cursor(dictionary=True)  # Fetch results as dictionaries
            fetch_query = """
                SELECT * FROM goods_prices
                WHERE MONTH(date) = 7
                AND DAY(date) = 2
                ORDER BY date ASC;
              """
            cursor.execute(fetch_query)
            results = cursor.fetchall()

            # Convert date and Decimal fields to JSON-serializable types
            for row in results:
                for key, value in row.items():
                    if isinstance(value, date):
                        row[key] = value.isoformat()  # Convert date to 'YYYY-MM-DD'
                    elif isinstance(value, Decimal):
                        row[key] = float(value)  # Convert Decimal to float

            if output_format == "df":
                df = pd.DataFrame(results)
                df['date'] = pd.to_datetime(df['date'])
                df_pivot = df.pivot(index='name', columns='date', values='price')
                df_pivot = df_pivot.transpose()
                return df_pivot
            elif output_format == "json":
                return results  # Convert result to JSON
            else:
                raise(ValueError, "Output formats supported: (df/json)")
    except mysql.connector.Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def fetch_good_price(good_name, year, output_format='json'):
    """
    Retrieves all entries from the goods_prices table.

    Returns:
        A JSON-formatted string containing the retrieved data or an error message.
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
            cursor = connection.cursor(dictionary=True)  # Fetch results as dictionaries
            fetch_query = """
                SELECT * FROM goods_prices
                WHERE MONTH(date) = 7
                AND DAY(date) = 2
                AND name %in% good_name
                AND year = %s
                ORDER BY date ASC;
              """
            cursor.execute(fetch_query)
            results = cursor.fetchall()

            # Convert date and Decimal fields to JSON-serializable types
            for row in results:
                for key, value in row.items():
                    if isinstance(value, date):
                        row[key] = value.isoformat()  # Convert date to 'YYYY-MM-DD'
                    elif isinstance(value, Decimal):
                        row[key] = float(value)  # Convert Decimal to float

            if output_format == "df":
                df = pd.DataFrame(results)
                df['date'] = pd.to_datetime(df['date'])
                df_pivot = df.pivot(index='name', columns='date', values='price')
                df_pivot = df_pivot.transpose()
                return df_pivot
            elif output_format == "json":
                return results  # Convert result to JSON
            else:
                raise(ValueError, "Output formats supported: (df/json)")
    except mysql.connector.Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def create_incomes_table():
    """
    Creates the 'incomes' table with columns:
        - year (INTEGER)
        - inflation_cpi (DECIMAL)
        - tax_units (INTEGER)
        - average_income_unadjusted (DECIMAL)
        - average_income_adjusted (DECIMAL)
        - source_link (VARCHAR)
        - source_name (VARCHAR)
        - region (VARCHAR)

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
                CREATE TABLE IF NOT EXISTS incomes (
                    year INT NOT NULL,
                    inflation_cpi DECIMAL(15,10),
                    tax_units INT,
                    average_income_unadjusted DECIMAL(20,10),
                    average_income_adjusted DECIMAL(20,10),
                    source_name VARCHAR(255),
                    source_link VARCHAR(511),
                    region VARCHAR(255),
                    PRIMARY KEY (year, source_name, region)
                );
            """
            cursor.execute(create_table_query)
            connection.commit()
            return {"result": "Table 'incomes' created successfully."}
    except mysql.connector.Error as e:
        return {"error": str(e)}
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def bulk_insert_incomes(df):
    """
    Performs a bulk insertion of income entries into the incomes table.
    """
    create_incomes_table()
    # Create list of records from required columns.
    records = df[['year', 'inflation_cpi', 'tax_units',
                  'average_income_unadjusted', 'average_income_adjusted',
                  'source_link', 'source_name', 'region']].values.tolist()

    # Convert any NaN values to None for SQL compatibility.
    records = [[None if pd.isna(value) else value for value in record] for record in records]

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
                INSERT INTO incomes 
                    (year, inflation_cpi, tax_units, average_income_unadjusted, 
                     average_income_adjusted, source_link, source_name, region)
                VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    inflation_cpi = VALUES(inflation_cpi),
                    tax_units = VALUES(tax_units),
                    average_income_unadjusted = VALUES(average_income_unadjusted),
                    average_income_adjusted = VALUES(average_income_adjusted);
            """
            # Bulk insert using executemany.
            cursor.executemany(insert_query, records)
            connection.commit()
            successful_inserts = cursor.rowcount
            return json.dumps({
                "result": f"{successful_inserts} records inserted/updated successfully out of {len(records)}."
            })
    except mysql.connector.Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()





def fetch_incomes_data(start_year, end_year, income_data_source):
    """
    Fetches income data for the given data source (IRS or BEA) for the United States.
    """
    connection = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = connection.cursor()
    income_query = """
        SELECT year, average_income_unadjusted
        FROM incomes
        WHERE year BETWEEN %s AND %s
          AND source_name = %s
          AND region = 'united states'
        ORDER BY year;
    """
    cursor.execute(income_query, (start_year, end_year, income_data_source))
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def compare_income_data_sources(start_year=1913, end_year=2024):
    sources = ['IRS', 'BEA', 'FRED']

    plt.figure(figsize=(12, 6))

    for source, marker in zip(sources, [".", "+", "x"]):
        data = fetch_incomes_data(start_year, end_year, source)
        if data:
            # Unpack fetched data into separate lists.
            years, incomes = zip(*data)
            plt.plot(years, incomes, marker=marker, label=source)
        else:
            print(f"No data found for {source} between {start_year} and {end_year}.")

    plt.xlabel("Year")
    plt.ylabel("Average Income Unadjusted")
    plt.title("United States Incomes (IRS vs. BEA vs FRED)")
    plt.legend()
    plt.grid(True)

    # Set the y-axis to logarithmic scale.
    plt.yscale('log')

    # Save the plot to a file.
    plt.savefig("../../doc/figures/compare_income_data_sources_2.png")
    plt.show()


import matplotlib.pyplot as plt
import pandas as pd

def get_connection():
    connection = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    return connection


if __name__ == "__main__":
    plot_incomes_inf_final_goods()
    # compare_income_data_sources()