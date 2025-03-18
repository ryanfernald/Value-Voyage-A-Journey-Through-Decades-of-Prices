import glob
import os
import pandas as pd
from src.functions.db.insert import bulk_insert_good_price_entries

def process_csv(db_path, csv_path):
    required_columns = [
        'year', 'jan', 'feb', 'mar', 'apr', 'may', 'jun',
        'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'year avg',
        'good name', 'good unit', 'source', 'price unit'
    ]

    df = pd.read_csv(csv_path)
    df.columns = [col.strip().lower() for col in df.columns]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    df['year'] = pd.to_numeric(df['year'], errors='raise', downcast='integer')

    month_cols = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                  'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for col in month_cols:
        non_null = df[col].dropna()
        pd.to_numeric(non_null, errors='raise')

    if df['year avg'].isnull().any():
        raise ValueError("The 'Year Avg' column contains null values.")
    df['year avg'] = pd.to_numeric(df['year avg'], errors='raise')

    for col in ['good name', 'good unit', 'source']:
        if df[col].isnull().any() or (df[col].astype(str).str.strip() == '').any():
            raise ValueError(f"The '{col.title()}' column contains null or empty values.")

    allowed_units = {'cent', 'cents', 'dollar', 'dollars'}
    if not df['price unit'].str.lower().isin(allowed_units).all():
        raise ValueError("The 'Price Unit' column contains invalid values.")

    month_columns = month_cols + ['year avg']
    melted = pd.melt(
        df,
        id_vars=['year', 'good name', 'good unit', 'source', 'price unit'],
        value_vars=month_columns,
        var_name='month',
        value_name='price'
    )

    def convert_price(row, value):
        if str(row['price unit']).strip().lower() in ['cent', 'cents']:
            return value / 100 if pd.notnull(value) else value
        return value

    melted['price'] = melted.apply(lambda row: convert_price(row, row['price']), axis=1)
    melted.drop('price unit', axis=1, inplace=True)

    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
        'year avg': '07'
    }

    def convert_month(row):
        day = 2 if row['month'] == 'year avg' else 1
        return f"{int(row['year'])}-{month_map[row['month']]}-{day:02d}"

    melted['date'] = melted.apply(convert_month, axis=1)
    melted.drop('month', axis=1, inplace=True)

    result = bulk_insert_good_price_entries(db_path, melted)
    print(result)

if __name__ == "__main__":
    csv_dir_path = r"../../../data/raw/input_data_csv"
    db_path = r"../../../data/database/goods_prices.db"  # Define your SQLite database path here

    for csv_path in glob.glob(os.path.join(csv_dir_path, "*.csv")):
        print(f"Processing {csv_path}")
        process_csv(db_path, csv_path)
