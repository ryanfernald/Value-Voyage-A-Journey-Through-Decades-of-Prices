import sqlite3
import json
import pandas as pd

def insert_good_price_entry(db_path, name, price, date, good_unit, data_source):
    connection = cursor = None
    try:
        connection = sqlite3.connect(db_path, timeout=30)
        cursor = connection.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;')

        insert_query = """
            INSERT INTO goods_prices (name, price, date, good_unit, data_source)
            VALUES (?, ?, ?, ?, ?);
        """
        cursor.execute(insert_query, (name, price, date, good_unit, data_source))
        connection.commit()

        return json.dumps({"result": "Good price entry inserted successfully."})
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def bulk_insert_good_price_entries(db_path, df):
    records = df[['good name', 'price', 'date', 'good unit', 'source']].where(pd.notnull(df), None).values.tolist()
    connection = cursor = None
    try:
        connection = sqlite3.connect(db_path, timeout=30)
        cursor = connection.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;')

        insert_query = """
            INSERT INTO goods_prices (name, price, date, good_unit, data_source)
            VALUES (?, ?, ?, ?, ?);
        """
        cursor.executemany(insert_query, records)
        connection.commit()
        inserted_rows = cursor.rowcount

        return json.dumps({"result": f"{inserted_rows} records inserted successfully."})
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def create_good_prices_table(db_path):
    connection = cursor = None
    try:
        connection = sqlite3.connect(db_path, timeout=30)
        cursor = connection.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;')

        create_table_query = """
            CREATE TABLE IF NOT EXISTS goods_prices (
                name TEXT NOT NULL,
                price REAL,
                date TEXT NOT NULL,
                good_unit TEXT,
                data_source TEXT,
                PRIMARY KEY(name, date, data_source)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()

        return {"result": "Table 'goods_prices' created successfully."}
    except sqlite3.Error as e:
        return {"error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def create_incomes_table(db_path):
    connection = cursor = None
    try:
        connection = sqlite3.connect(db_path, timeout=30)
        cursor = connection.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;')

        create_table_query = """
            CREATE TABLE IF NOT EXISTS incomes (
                year INTEGER NOT NULL,
                inflation_cpi REAL,
                tax_units INTEGER,
                average_income_unadjusted REAL,
                average_income_adjusted REAL,
                source_name TEXT,
                source_link TEXT,
                region TEXT,
                PRIMARY KEY (year, source_name, region)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()

        return {"result": "Table 'incomes' created successfully."}
    except sqlite3.Error as e:
        return {"error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def bulk_insert_incomes(db_path, df):
    create_incomes_table(db_path)

    records = df[['year', 'inflation_cpi', 'tax_units',
                  'average_income_unadjusted', 'average_income_adjusted',
                  'source_link', 'source_name', 'region']].where(pd.notnull(df), None).values.tolist()

    connection = cursor = None
    try:
        connection = sqlite3.connect(db_path, timeout=30)
        cursor = connection.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;')

        insert_query = """
            INSERT INTO incomes 
                (year, inflation_cpi, tax_units, average_income_unadjusted, 
                 average_income_adjusted, source_link, source_name, region)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(year, source_name, region) DO UPDATE SET
                inflation_cpi = excluded.inflation_cpi,
                tax_units = excluded.tax_units,
                average_income_unadjusted = excluded.average_income_unadjusted,
                average_income_adjusted = excluded.average_income_adjusted;
        """
        cursor.executemany(insert_query, records)
        connection.commit()
        updated_rows = cursor.rowcount

        return json.dumps({
            "result": f"{updated_rows} records inserted/updated successfully out of {len(records)}."
        })
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
