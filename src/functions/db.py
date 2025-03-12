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
        - source (VARCHAR)

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
                    source VARCHAR(255),
                    PRIMARY KEY (year, source)
                );
            """
            cursor.execute(create_table_query)
            connection.commit()
            return {"result": "Table 'incomes' created successfully."}
    except Error as e:
        return {"error": str(e)}
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def bulk_insert_incomes(df):
    """
    Performs a bulk insertion of income entries into the incomes table.

    Parameters:
        df (DataFrame): A DataFrame containing the following columns:
            'year', 'inflation-cpi', 'tax-units', 'average-income-unadjusted', 'source'

    Returns:
        A JSON-formatted string indicating success or error.
    """
    records = df[['year', 'inflation_cpi', 'tax_units', 'average_income_unadjusted', 'average_income_adjusted', 'source']].values.tolist()

    for record in records:
        # Handle any NaN values in numeric columns
        for i in range(len(record)):
            if pd.isna(record[i]):
                record[i] = None

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
                INSERT INTO incomes (year, inflation_cpi, tax_units, average_income_unadjusted, average_income_adjusted, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    inflation_cpi = VALUES(inflation_cpi),
                    tax_units = VALUES(tax_units),
                    average_income_unadjusted = VALUES(average_income_unadjusted),
                    average_income_adjusted = VALUES(average_income_adjusted);
            """
            cursor.executemany(insert_query, records)
            connection.commit()
            return json.dumps({"result": f"{cursor.rowcount} records inserted/updated successfully."})
    except Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def get_final_goods_affordable_quantity(final_goods, year, income_interval='annual'):
    """
    Calculates how many units of each final good can be afforded based on the average income of a given year.

    Parameters:
        final_goods (list): A list of good names (strings) for which prices will be queried.
        year (int): The year for which the average income is retrieved and good prices are considered.
        income_interval (str): Specifies the salary frequency. Use 'annual' for the full annual salary
                               (default) or 'monthly' to use the monthly salary (annual salary divided by 12).

    Returns:
        dict: A dictionary mapping each good to a dictionary containing:
              - "quantity": The affordable quantity (average_income / price) as an integer.
              - "unit": The unit extracted from the good_unit field.
              If a good's price data is missing or invalid, its value will be set to None.
    """
    goods_affordable_quantity = {}
    try:
        # Establish a database connection using credentials from environment variables
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connection.cursor()

        # Retrieve the average income for the specified year.
        income_query = """
            SELECT average_income_unadjusted
            FROM incomes
            WHERE year = %s
            LIMIT 1;
        """
        cursor.execute(income_query, (year,))
        income_row = cursor.fetchone()
        if not income_row:
            raise ValueError("No income data found for year {}".format(year))
        average_income = income_row[0]

        # Adjust the average income based on the income_interval parameter.
        if income_interval.lower() == "monthly":
            average_income = average_income / 12
        elif income_interval.lower() != "annual":
            raise ValueError("Invalid income_interval. Use 'annual' or 'monthly'.")

        # For each final good, retrieve its price and unit, then compute the affordable quantity.
        for good in final_goods:
            price_query = """
                SELECT price, good_unit
                FROM goods_prices
                WHERE name = %s 
                  AND MONTH(date) = 7 
                  AND DAY(date) = 2 
                  AND YEAR(date) = %s
                LIMIT 1;
            """
            cursor.execute(price_query, (good, year))
            price_row = cursor.fetchone()
            if not price_row:
                goods_affordable_quantity[good] = None
            else:
                price, good_unit = price_row
                if price <= 0:
                    goods_affordable_quantity[good] = None
                else:
                    affordable_quantity = int(average_income / price)
                    # Extract the unit after '/' if available, otherwise use the whole string.
                    if '/' in good_unit:
                        parts = good_unit.split('/')
                        unit = parts[1].strip() if len(parts) > 1 else good_unit
                    else:
                        unit = good_unit
                    goods_affordable_quantity[good] = {
                        "quantity": affordable_quantity,
                        "unit": unit
                    }
    except mysql.connector.Error as e:
        print("Database error:", e)
        return None
    except Exception as e:
        print("Error:", e)
        return None
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

    return goods_affordable_quantity


if __name__ == "__main__":
    print(get_final_goods_affordable_quantity(['bacon'], 1970, "monthly"))