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


def get_final_goods_affordable_quantity(final_goods, start_year, end_year, income_interval='annual', output_format='json'):
    """
    Calculates how many units of each final good can be afforded for each year in the given range
    based on the average income of each year.

    Parameters:
        final_goods (list): A list of good names (strings) for which prices will be queried.
        start_year (int): The starting year for the analysis.
        end_year (int): The ending year for the analysis.
        income_interval (str): Specifies the salary frequency. Use 'annual' for the full annual salary
                               (default) or 'monthly' to use the monthly salary (annual salary divided by 12).
        output_format (str): Format of the output. Use 'json' (default) for a dictionary output or 'df'
                             for a pandas DataFrame in long format.

    Returns:
        dict or DataFrame: If output_format is 'json', returns a dictionary mapping each good to another
                           dictionary mapping each year to a dictionary containing:
                             - "quantity": The affordable quantity (average_income / price) as an integer.
                             - "unit": The unit extracted from the good_unit field.
                           If output_format is 'df', returns a pandas DataFrame in long format with columns:
                           ['year', 'good', 'quantity', 'unit'].
    """
    goods_affordable = {}
    try:
        # Establish database connection
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connection.cursor()

        # Fetch incomes for all years in the range.
        income_query = """
            SELECT year, average_income_unadjusted
            FROM incomes
            WHERE year BETWEEN %s AND %s;
        """
        cursor.execute(income_query, (start_year, end_year))
        incomes_data = cursor.fetchall()
        # Build a mapping: year -> adjusted average income
        incomes = {}
        for row in incomes_data:
            yr, income = row
            if income is None:
                continue
            if income_interval.lower() == 'monthly':
                income = income / 12
            elif income_interval.lower() != 'annual':
                raise ValueError("Invalid income_interval. Use 'annual' or 'monthly'.")
            incomes[yr] = income

        # Prepare placeholders for final_goods list in the IN clause.
        goods_placeholders = ','.join(['%s'] * len(final_goods))
        price_query = f"""
            SELECT name, YEAR(date) AS yr, price, good_unit
            FROM goods_prices
            WHERE name IN ({goods_placeholders})
              AND MONTH(date) = 7
              AND DAY(date) = 2
              AND YEAR(date) BETWEEN %s AND %s;
        """
        params = tuple(final_goods) + (start_year, end_year)
        cursor.execute(price_query, params)
        prices_data = cursor.fetchall()

        # Build a mapping: (good, year) -> (price, good_unit)
        prices = {}
        for row in prices_data:
            name, yr, price, good_unit = row
            prices[(name, yr)] = (price, good_unit)

        # Build result dictionary.
        # For each good, for each year in the range, calculate affordable quantity if data exists.
        for good in final_goods:
            goods_affordable[good] = {}
            for yr in range(start_year, end_year + 1):
                if yr not in incomes:
                    goods_affordable[good][yr] = None
                    continue

                income_for_year = incomes[yr]
                key = (good, yr)
                if key not in prices:
                    goods_affordable[good][yr] = None
                    continue

                price, good_unit = prices[key]
                if price is None or price <= 0:
                    goods_affordable[good][yr] = None
                    continue

                affordable_quantity = int(income_for_year / price)
                # Extract the unit after '/' if available, otherwise use the entire string.
                if isinstance(good_unit, str) and '/' in good_unit:
                    parts = good_unit.split('/')
                    unit = parts[1].strip() if len(parts) > 1 else good_unit
                else:
                    unit = good_unit

                goods_affordable[good][yr] = {
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

    # Return output in desired format.
    if output_format.lower() == 'df':
        import pandas as pd
        # Create a DataFrame in long format.
        rows = []
        for good, year_data in goods_affordable.items():
            for yr in range(start_year, end_year + 1):
                entry = year_data.get(yr)
                if entry is None:
                    rows.append({'year': yr, 'good': good, 'quantity': None, 'unit': None})
                else:
                    rows.append({
                        'year': yr,
                        'good': good,
                        'quantity': entry.get("quantity"),
                        'unit': entry.get("unit")
                    })
        df = pd.DataFrame(rows)
        return df

    return goods_affordable


import matplotlib.pyplot as plt
import pandas as pd

if __name__ == "__main__":
    # Assume get_final_goods_affordable_quantity is already defined and imported.
    # Retrieve the DataFrame in long format.
    df = get_final_goods_affordable_quantity(
        ['bacon', 'bread', 'butter', 'coffee', 'eggs', 'flour', 'milk', 'pork chop', 'round steak', 'sugar'],
        1900, 2000,
        "monthly",
        output_format='df'
    )
    print(df)

    plt.figure(figsize=(20, 10))

    # If the DataFrame contains data for only one year, use a bar chart.
    if df['year'].nunique() == 1:
        plt.bar(df['good'], df['quantity'])
        plt.xlabel("Good")
        plt.ylabel("Affordable Quantity")
        plt.title(f"Affordable Quantity in {df['year'].iloc[0]}")
    else:
        # For multiple years, plot a line for each good.
        for good, group in df.groupby('good'):
            plt.plot(group['year'], group['quantity'], marker='o', label=good)
        plt.xlabel("Year")
        plt.ylabel("Affordable Quantity")
        plt.title("Affordable Quantity Over Years")
        plt.legend()

    # Save the graph as a PNG file.
    plt.savefig("../../doc/figures/affordable_quantity.png")

    plt.show()

