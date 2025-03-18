
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

