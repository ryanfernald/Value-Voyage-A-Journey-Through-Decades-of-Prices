import pandas as pd
from src.functions.db.insert import bulk_insert_incomes

def process_csv(db_path, csv_path):
    required_columns = [
        'year', 'inflation_cpi', 'tax_units', 'average_income_adjusted'
    ]

    df = pd.read_csv(csv_path)
    df.columns = [col.strip().lower() for col in df.columns]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    df['year'] = pd.to_numeric(df['year'], errors='raise', downcast='integer')

    df['average_income_adjusted'] = pd.to_numeric(df['average_income_adjusted'], errors='coerce')
    df['inflation_cpi'] = pd.to_numeric(df['inflation_cpi'], errors='coerce')
    df['tax_units'] = pd.to_numeric(df['tax_units'], errors='coerce')

    df['average_income_unadjusted'] = df['average_income_adjusted'] / df['inflation_cpi']
    df['tax_units'] = df['tax_units'] * 1000

    df.dropna(subset=['year', 'average_income_unadjusted', 'inflation_cpi', 'tax_units'], inplace=True)

    df['source_name'] = 'IRS'
    df['source_link'] = 'https://eml.berkeley.edu/~saez/pikettyqje.pdf'
    df['region'] = 'united states'

    df.to_csv('ahan.csv', index=False)
    result = bulk_insert_incomes(db_path, df)
    print(result)

if __name__ == "__main__":
    csv_path = r"../../../data/raw/input_data_csv/incomes/irs_incomes.csv"
    db_path = r"../../../data/database/incomes.db"  # SQLite database path
    print(f"Processing {csv_path}")
    process_csv(db_path, csv_path)
