
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




# new shit

def fetch_incomes(year_range=(1990, 2000), data_source_name='BEA', output_format='df'):
