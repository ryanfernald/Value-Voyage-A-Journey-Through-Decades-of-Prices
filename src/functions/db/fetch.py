import sqlite3
import json
import pandas as pd


def fetch_incomes(db_path, year_range=(1990, 2000), data_source_name='FRED', regions=None, output_format='df'):
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

def fetch_goods_prices(db_path, year_range=(1990, 2000), goods_list=None, use_year_averages=True, output_format='df'):
    """
    Fetches goods prices from an SQLite database for a given year range and optional goods filter.
    For years with multiple entries per good, only the latest date entry per year is retained.

    Args:
        db_path (str): Path to SQLite database.
        year_range (tuple): (start_year, end_year) for filtering.
        goods_list (list or None): List of good names; None fetches all goods.
        use_year_averages (bool): If True, fetch only July 2nd entries; else exclude July 2nd entries.
        output_format (str): 'df' returns DataFrame, 'json' returns JSON.

    Returns:
        DataFrame or JSON string.
    """
    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row

        start_year, end_year = year_range
        params = [start_year, end_year]

        where_conditions = [
            "CAST(strftime('%Y', date) AS INTEGER) BETWEEN ? AND ?"
        ]

        if goods_list:
            placeholders = ','.join('?' for _ in goods_list)
            where_conditions.append(f"name IN ({placeholders})")
            params.extend(goods_list)

        if use_year_averages:
            where_conditions.append("strftime('%m-%d', date) = '07-02'")
        else:
            where_conditions.append("strftime('%m-%d', date) != '07-02'")

        where_clause = ' AND '.join(where_conditions)

        query = f"""
            SELECT name, price, date, good_unit, data_source
            FROM goods_prices
            WHERE {where_clause}
            ORDER BY name ASC, date DESC
        """

        df = pd.read_sql_query(query, connection, params=params)
        df['year'] = pd.to_datetime(df['date']).dt.year

        # Keep only the latest entry per good per year
        df_unique = df.sort_values('date', ascending=False).drop_duplicates(subset=['name', 'year'], keep='first')

        if output_format == 'df':
            df_unique.reset_index(drop=True, inplace=True)
            return df_unique
        elif output_format == 'json':
            return df_unique.to_json(orient='records', date_format='iso')
        else:
            raise ValueError("Output formats supported: 'df' or 'json'")
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})
    finally:
        connection.close()


def fetch_final_goods_affordable(db_path, year_range=(1990, 2000), goods_list=None, regions=None, income_data_source='FRED', salary_interval='monthly', output_format='df'):
        incomes_df = fetch_incomes(
            db_path,
            year_range=year_range,
            data_source_name=income_data_source,
            regions=regions,
            output_format='df'
        )

        goods_df = fetch_goods_prices(
            db_path,
            year_range=year_range,
            goods_list=goods_list,
            use_year_averages=True,
            output_format='df'
        )

        merged_df = pd.merge(goods_df, incomes_df, on='year', how='inner')


        if salary_interval == 'monthly':
            merged_df['final_goods_affordable'] = (merged_df['average_income_unadjusted'] / 12) / merged_df['price']
        elif salary_interval == 'annually':
            merged_df['final_goods_affordable'] = merged_df['average_income_unadjusted'] / merged_df['price']

        merged_df['final_goods_affordable'] = merged_df['final_goods_affordable'].astype(int)

        merged_df = merged_df[['name', 'final_goods_affordable', 'good_unit', 'date', 'year', 'region']]

        if output_format == 'df':
            return merged_df
        else:
            return json.dumps(merged_df.to_dict(orient='records'))


if __name__ == '__main__':

    db_path = '../../../data/db/sqlite/database.sqlite'

    # fetch incomes sample use
    # data = fetch_incomes(
    #     db_path=db_path,
    #     year_range=(1929, 2024),
    #     data_source_name='FRED',
    #     regions=['united states', 'new york'],
    #     output_format='df'
    # )

    # fetch goods prices sample use
    # data = fetch_goods_prices(
    #     db_path=db_path,
    #     year_range=(1990, 2000),
    #     goods_list=['sugar', 'pork chop'],
    #     use_year_averages=True,
    #     output_format='df'
    # )

    data = fetch_final_goods_affordable(
        db_path,
        year_range=(1990, 2000),
        goods_list=['sugar', 'pork chop'],
        regions=['united states'],
        income_data_source='FRED',
        salary_interval='monthly',
        output_format='df'
    )

    print(data)
