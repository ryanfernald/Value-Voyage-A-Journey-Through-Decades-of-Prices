import sqlite3
import json
import pandas as pd


def fetch_incomes(db_path, year_range=(1990, 2000), data_source_name='BEA', regions=None, output_format='df'):
    import sqlite3
    import pandas as pd
    import json

    start_year, end_year = year_range

    if regions is None:
        regions = ['united states']

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    placeholders = ','.join('?' for _ in regions)
    region_filter = f"AND region IN ({placeholders})"

    income_query = f"""
        SELECT year, average_income_unadjusted, region
        FROM incomes
        WHERE year BETWEEN ? AND ?
          AND source_name = ?
          {region_filter}
        ORDER BY year;
    """
    params = (start_year, end_year, data_source_name, *regions)
    cursor.execute(income_query, params)

    rows = cursor.fetchall()

    cursor.close()
    connection.close()

    if output_format == 'df':
        df = pd.DataFrame([dict(row) for row in rows])
        return df
    else:
        data_with_field_names = [dict(row) for row in rows]
        json_output = json.dumps(data_with_field_names)
        return json_output


import sqlite3
import pandas as pd
import json


def fetch_goods_prices(year_range=(1990, 2000), goods_list=None, use_year_averages=True, output_format='df'):
    """
    Fetches goods prices from an SQLite database for a given year range and an optional list of goods.

    If use_year_averages is True, only the July 2nd entries (which already contain the average values)
    are fetched for every year. Otherwise, all entries except those on July 2nd are fetched.

    Args:
        year_range (tuple): (start_year, end_year) for filtering by year.
        goods_list (list or None): List of good names to filter on; if None, no filtering is applied.
        use_year_averages (bool): If True, fetch only July 2nd entries; if False, fetch all entries excluding July 2nd.
        output_format (str): 'df' returns a Pandas DataFrame (pivoted for averages), 'json' returns a JSON string.

    Returns:
        Either a Pandas DataFrame or a JSON string with the resulting data.
    """
    try:
        connection = sqlite3.connect('your_sqlite_database.sqlite')
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()

        start_year, end_year = year_range
        params = [start_year, end_year]
        where_clause = "WHERE CAST(strftime('%Y', date) AS INTEGER) BETWEEN ? AND ?"

        if goods_list:
            placeholders = ','.join('?' for _ in goods_list)
            where_clause += f" AND name IN ({placeholders})"
            params.extend(goods_list)

        if use_year_averages:
            where_clause += " AND strftime('%m', date) = '07' AND strftime('%d', date) = '02'"
        else:
            where_clause += " AND NOT (strftime('%m', date) = '07' AND strftime('%d', date) = '02')"

        query = f"""
            SELECT *
            FROM goods_prices
            {where_clause}
            ORDER BY date ASC;
        """

        cursor.execute(query, params)
        rows = cursor.fetchall()
        results = [dict(row) for row in rows]

        if output_format == 'df':
            df = pd.DataFrame(results)
            if use_year_averages and not df.empty:
                # Pivot the DataFrame: each good becomes a column, indexed by the year (extracted from the date).
                df['year'] = df['date'].apply(lambda d: int(d[:4]) if isinstance(d, str) else d.year)
                df_pivot = df.pivot(index='year', columns='name', values='price')
                return df_pivot
            else:
                if not df.empty and 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                return df
        elif output_format == 'json':
            return json.dumps(results)
        else:
            raise ValueError("Output formats supported: 'df' or 'json'")
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()


def fetch_purchasing_powers(year_range=(1990, 2000), goods_list=None, income_data_source='BEA', use_salary_interval='mounthly', use_year_averages=True, output_format='df'):
    pass

if __name__ == '__main__':

    data = fetch_incomes(
        db_path='../../../data/db/sqlite/database.sqlite',
        year_range=(1929, 2024),
        data_source_name='FRED',
        regions=['united states'],
        output_format='df'
    )

    print(data)
