import pandas as pd
from src.functions.db import bulk_insert_good_price_entries


def process_csv(csv_path):
    required_columns = [
        'year', 'jan', 'feb', 'mar', 'apr', 'may', 'jun',
        'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'year avg',
        'good name', 'good unit', 'source', 'price unit'
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

    # 2. Validate month columns (Jan...Dec): if not null, they must be numeric.
    month_cols = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                  'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for col in month_cols:
        non_null = df[col].dropna()
        try:
            pd.to_numeric(non_null, errors='raise')
        except Exception:
            raise ValueError(f"Column '{col}' contains non-numeric values.")

    # 3. Validate 'year avg': must not be null and must be numeric.
    if df['year avg'].isnull().any():
        raise ValueError("The 'Year Avg' column contains null values.")
    try:
        df['year avg'] = pd.to_numeric(df['year avg'], errors='raise')
    except Exception:
        raise ValueError("The 'Year Avg' column must be numeric.")

    # 4. Validate 'good name', 'good unit', and 'source': must not be null or empty.
    for col in ['good name', 'good unit', 'source']:
        if df[col].isnull().any():
            raise ValueError(f"The '{col.title()}' column contains null values.")
        if (df[col].astype(str).str.strip() == '').any():
            raise ValueError(f"The '{col.title()}' column contains empty strings.")

    # 5. Validate 'price unit': must be one of 'cent', 'cents', 'dollar', or 'dollars' (case insensitive)
    allowed_units = {'cent', 'cents', 'dollar', 'dollars'}
    if not df['price unit'].str.lower().isin(allowed_units).all():
        raise ValueError("The 'Price Unit' column contains invalid values. Allowed values are: cent, cents, dollar, dollars.")

    # --- End of Data Constraint Checks ---

    # Prepare for melting the DataFrame
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

    # Map months to their respective numeric representations
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
        'year avg': '07'  # Placeholder month for 'year avg'
    }

    def convert_month(row):
        # For 'year avg', assign day 2; otherwise day 1.
        day = 2 if row['month'] == 'year avg' else 1
        return f"{int(row['year'])}-{month_map[row['month']]}-{day:02d}"

    melted['date'] = melted.apply(convert_month, axis=1)
    melted.drop('month', axis=1, inplace=True)

    result = bulk_insert_good_price_entries(melted)
    print(result)


if __name__ == "__main__":
    # csv file path. make sure it has required columns (case insensitive)
    # columns required: [Year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,Year Avg,Good Name,Good Unit,Source,Price Unit]
    # Year= year of data (int)
    # [Jan...Dec]= price of good for a specific month of the year; can be None
    # Year Avg= average price for that year. Shouldnt be None
    # Good Name= name of the good (ex: eggs)
    # Good Unit= measurement unit (oz/lbs/dozen/sqft...)
    # Source= link to where data is coming from
    # Price unit= 'cent'/'cents'/'dollar'/'dollars'

    csv_path = "../data/raw/sample_csv_upload_for_db.csv"
    process_csv(csv_path)
