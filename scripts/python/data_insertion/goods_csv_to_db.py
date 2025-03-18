import glob
import os
import pandas as pd
from src.functions.db.insert import bulk_insert_good_price_entries

def process_csv(db_path, csv_path):
    print(f"Starting processing file: {csv_path}")

    base_required_columns = [
        'year', 'year avg', 'good name', 'good unit', 'source', 'price unit'
    ]

    df = pd.read_csv(csv_path)
    df.columns = [col.strip().lower() for col in df.columns]
    print("Columns normalized to lowercase.")

    month_cols_present = [col for col in ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                                          'jul', 'aug', 'sep', 'oct', 'nov', 'dec'] if col in df.columns]

    # Ensure required columns are present
    required_columns = base_required_columns.copy()
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")
    print("All basic required columns are present.")

    # Year validation
    try:
        df['year'] = pd.to_numeric(df['year'], errors='raise', downcast='integer')
        print("Year column successfully validated as integer.")
    except Exception as e:
        raise ValueError(f"Error validating 'year' column: {e}")

    # Validate existing month columns if present
    if month_cols_present:
        for col in month_cols_present:
            non_null = df[col].dropna()
            try:
                pd.to_numeric(non_null, errors='raise')
            except Exception:
                raise ValueError(f"Column '{col}' contains non-numeric values.")
        print("Monthly price columns successfully validated as numeric.")
    else:
        print("No monthly columns present; proceeding with 'year avg' only.")

    # Year Avg validation
    if df['year avg'].isnull().any():
        raise ValueError("The 'Year Avg' column contains null values.")
    try:
        df['year avg'] = pd.to_numeric(df['year avg'], errors='raise')
        print("Year Avg column successfully validated.")
    except Exception as e:
        raise ValueError(f"Error validating 'Year Avg' column: {e}")

    # 'Good Name', 'Good Unit', 'Source' validation
    for col in ['good name', 'good unit', 'source']:
        if df[col].isnull().any() or (df[col].astype(str).str.strip() == '').any():
            raise ValueError(f"The '{col.title()}' column contains null or empty values.")
    print("Columns 'Good Name', 'Good Unit', and 'Source' successfully validated.")

    # Price unit validation
    allowed_units = {'cent', 'cents', 'dollar', 'dollars'}
    if not df['price unit'].str.lower().isin(allowed_units).all():
        raise ValueError("The 'Price Unit' column contains invalid values.")
    print("Price unit column successfully validated.")

    # Data transformation
    month_columns = month_cols_present + ['year avg']
    melted = pd.melt(
        df,
        id_vars=['year', 'good name', 'good unit', 'source', 'price unit'],
        value_vars=month_columns,
        var_name='month',
        value_name='price'
    )
    print(f"Melted dataframe created with {len(melted)} rows.")

    def convert_price(row):
        if pd.isnull(row['price']):
            return None
        unit = str(row['price unit']).strip().lower()
        return row['price'] / 100 if unit in ['cent', 'cents'] else row['price']

    melted['price'] = melted.apply(convert_price, axis=1)
    melted.drop('price unit', axis=1, inplace=True)
    melted.dropna(subset=['price'], inplace=True)  # Drop rows where price is None after conversion
    print("Price unit conversion complete and null prices removed.")

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
    print("Date conversion completed.")

    result = bulk_insert_good_price_entries(db_path, melted)
    print(f"Bulk insert result: {result}")

    print(f"Finished processing file: {csv_path}\n")

if __name__ == "__main__":
    csv_dir_path = r"../../../data/raw/input_data_csv/goods"
    db_path = r"../../../data/db/sqlite/database.sqlite"

    csv_files = glob.glob(os.path.join(csv_dir_path, "*.csv"))
    print(f"Found CSV files: {csv_files}")

    for csv_path in csv_files:
        print(f"Processing {csv_path}")
        process_csv(db_path, csv_path)
