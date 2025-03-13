import glob
import os
import pandas as pd
from src.functions.db import bulk_insert_incomes


def process_csv(csv_path):
    required_columns = [
        'year', 'inflation_cpi', 'tax_units', 'average_income_adjusted'
    ]

    df = pd.read_csv(csv_path)

    # Normalize column names: trim whitespace and convert to lower case.
    df.columns = [col.strip().lower() for col in df.columns]

    # Check for missing required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    # Validate 'year' column: must be numeric and convertible to integer.
    try:
        df['year'] = pd.to_numeric(df['year'], errors='raise', downcast='integer')
    except Exception:
        raise ValueError("The 'year' column must be numeric (integer).")

    # Ensure numeric conversion for required columns.
    df['average_income_adjusted'] = pd.to_numeric(df['average_income_adjusted'], errors='coerce')
    df['inflation_cpi'] = pd.to_numeric(df['inflation_cpi'], errors='coerce')
    df['tax_units'] = pd.to_numeric(df['tax_units'], errors='coerce')

    # Calculate average_income_adjusted = average_income_unadjusted * inflation_cpi.
    df['average_income_unadjusted'] = df['average_income_adjusted'] / df['inflation_cpi']

    # Multiply tax_units by 1000.
    df['tax_units'] = df['tax_units'] * 1000

    # Drop rows where critical numeric values are missing.
    df.dropna(subset=['year', 'average_income_unadjusted', 'inflation_cpi', 'tax_units'], inplace=True)

    # Set additional required columns.
    df['source_name'] = 'IRS'
    df['source_link'] = 'https://eml.berkeley.edu/~saez/pikettyqje.pdf'
    df['region'] = 'united states'

    df.to_csv('ahan.csv', index=False)
    result = bulk_insert_incomes(df)
    print(result)


if __name__ == "__main__":
    csv_path = r"../../data/raw/input_data_csv/incomes/irs_incomes.csv"
    print(f"Processing {csv_path}")
    process_csv(csv_path)
