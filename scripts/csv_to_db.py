import pandas as pd
from src.functions.db import bulk_insert_good_price_entries


def process_csv(csv_path):
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

    month_columns = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                     'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'year avg']

    melted = pd.melt(
        df,
        id_vars=['year', 'good name', 'good unit', 'source', 'price unit'],
        value_vars=month_columns,
        var_name='month',
        value_name='price'
    )

    def convert_price(row, value):
        if str(row['price unit']).strip().lower() in ['cents', 'cent']:
            return value / 100
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
        day = 1
        if row['month'] == "year avg":
            day = 2
        # Use zero-padded day format; note that for days < 10, f"0{day}" works fine.
        return f"{int(row['year'])}-{month_map[row['month']]}-{day:02d}"

    melted['date'] = melted.apply(lambda row: convert_month(row), axis=1)
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