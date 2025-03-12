import glob
import os

import pandas as pd
from src.functions.db import bulk_insert_incomes


def process_csv(csv_path):
    required_columns = [
        'year', 'inflation_cpi', 'tax_units', 'average_income_adjusted', 'source'
    ]

    df = pd.read_csv(csv_path)
    # Normalize column names (trim whitespace and lower case)
    df.columns = [col.strip().lower() for col in df.columns]

    # Check for missing required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    # --- Data Constraint Checks ---

    # 1. Validate 'year' column: must be numeric and convertible to integer.
    try:
        df['year'] = pd.to_numeric(df['year'], errors='raise', downcast='integer')
    except Exception:
        raise ValueError("The 'Year' column must be numeric (integer).")

    df['average_income_unadjusted'] = pd.to_numeric(df['average_income_adjusted']) / pd.to_numeric(df['inflation_cpi'])
    df['tax_units'] = pd.to_numeric(df['tax_units']) * 1000

    result = bulk_insert_incomes(df)
    print(result)


if __name__ == "__main__":
    csv_dir_path = r"../../data/raw/input_data_csv/incomes"
    for csv_path in glob.glob(os.path.join(csv_dir_path, "*.csv")):
        print(f"Processing {csv_path}")
        process_csv(csv_path)
