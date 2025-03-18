import glob
import os
import numpy as np
import pandas as pd
from src.functions.db.insert import bulk_insert_incomes

def process_csv(db_path, csv_path):
    required_columns = ['year', 'average_income_unadjusted']

    df = pd.read_csv(csv_path)
    df.columns = [col.strip().lower() for col in df.columns]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    df['year'] = pd.to_numeric(df['year'], errors='raise', downcast='integer')

    df['source_name'] = 'FRED'
    df['source_link'] = 'https://fred.stlouisfed.org/series/A792RC0A052NBEA'
    df['region'] = 'united states'
    df['average_income_adjusted'] = np.nan
    df['tax_units'] = np.nan
    df['inflation_cpi'] = np.nan

    df.to_csv('ahan.csv', index=False)
    result = bulk_insert_incomes(db_path, df)
    print(result)

if __name__ == "__main__":
    csv_path = r"../../../data/raw/input_data_csv/incomes/fred_incomes.csv"
    db_path = r"../../../data/database/incomes.db"  # SQLite database path
    print(f"Processing {csv_path}")
    process_csv(db_path, csv_path)
