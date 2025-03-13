import glob
import os
import pandas as pd
from src.functions.db import bulk_insert_incomes

def process_csv(csv_path):
    # Read CSV file
    df = pd.read_csv(csv_path)

    # Normalize column names: trim whitespace and convert to lower case.
    df.columns = [col.strip().lower() for col in df.columns]

    # Ensure the 'year' column is present and numeric.
    if 'year' not in df.columns:
        raise ValueError("Missing required column: year")
    try:
        df['year'] = pd.to_numeric(df['year'], errors='raise', downcast='integer')
    except Exception:
        raise ValueError("The 'year' column must be numeric (integer).")

    # Pivot the data from wide to long format.
    # All columns except 'year' are assumed to represent regions.
    df_long = df.melt(id_vars='year', var_name='region', value_name='average_income_unadjusted')

    # Convert income values to numeric (non-numeric entries such as '(NA)' become NaN).
    df_long['average_income_unadjusted'] = pd.to_numeric(df_long['average_income_unadjusted'], errors='coerce')
    df_long.dropna(subset=['average_income_unadjusted'], inplace=True)

    # Set the additional required columns.
    df_long['inflation_cpi'] = None
    df_long['tax_units'] = None
    df_long['average_income_adjusted'] = None
    df_long['source_link'] = "https://apps.bea.gov/iTable/?reqid=70&step=30&isuri=1&major_area=0&area=xx&year=-1&tableid=21&category=421&area_type=0&year_end=-1&classification=non-industry&state=0&statistic=3&yearbegin=-1&unit_of_measure=levels#eyJhcHBpZCI6NzAsInN0ZXBzIjpbMSwyOSwyNSwzMSwyNiwzMCwzMF0sImRhdGEiOltbIm1ham9yX2FyZWEiLCIwIl0sWyJhcmVhIixbIlhYIl1dLFsieWVhciIsWyItMSJdXSxbInRhYmxlaWQiLCIyMSJdLFsieWVhcl9lbmQiLCItMSJdLFsic3RhdGUiLFsiMCJdXSxbInN0YXRpc3RpYyIsIjMiXSxbInllYXJiZWdpbiIsIi0xIl0sWyJ1bml0X29mX21lYXN1cmUiLCJMZXZlbHMiXV19"
    df_long['source_name'] = 'BEA'

    # Optional: Uncomment to review the transformed data.
    # df_long.to_csv("sample_long_df.csv", index=False)

    # Insert the data into the database.
    result = bulk_insert_incomes(df_long)
    print(result)


if __name__ == "__main__":
    csv_path = r"../../data/raw/input_data_csv/incomes/bea_incomes.csv"
    print(f"Processing {csv_path}")
    process_csv(csv_path)
