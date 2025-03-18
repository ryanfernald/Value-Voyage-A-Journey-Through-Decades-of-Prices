import glob
import os
import pandas as pd
from src.functions.db.insert import bulk_insert_incomes

def process_csv(db_path, csv_path):
    df = pd.read_csv(csv_path)
    df.columns = [col.strip().lower() for col in df.columns]

    if 'year' not in df.columns:
        raise ValueError("Missing required column: year")

    df['year'] = pd.to_numeric(df['year'], errors='raise', downcast='integer')

    df_long = df.melt(id_vars='year', var_name='region', value_name='average_income_unadjusted')
    df_long['average_income_unadjusted'] = pd.to_numeric(df_long['average_income_unadjusted'], errors='coerce')
    df_long.dropna(subset=['average_income_unadjusted'], inplace=True)

    df_long['inflation_cpi'] = None
    df_long['tax_units'] = None
    df_long['average_income_adjusted'] = None
    df_long['source_link'] = "https://apps.bea.gov/iTable/?reqid=70&step=30&isuri=1&major_area=0&area=xx&year=-1&tableid=21&category=421&area_type=0&year_end=-1&classification=non-industry&state=0&statistic=3&yearbegin=-1&unit_of_measure=levels#eyJhcHBpZCI6NzAsInN0ZXBzIjpbMSwyOSwyNSwzMSwyNiwzMCwzMF0sImRhdGEiOltbIm1ham9yX2FyZWEiLCIwIl0sWyJhcmVhIixbIlhYIl1dLFsieWVhciIsWyItMSJdXSxbInRhYmxlaWQiLCIyMSJdLFsieWVhcl9lbmQiLCItMSJdLFsic3RhdGUiLFsiMCJdXSxbInN0YXRpc3RpYyIsIjMiXSxbInllYXJiZWdpbiIsIi0xIl0sWyJ1bml0X29mX21lYXN1cmUiLCJMZXZlbHMiXV19"
    df_long['source_name'] = 'BEA'

    result = bulk_insert_incomes(db_path, df_long)
    print(result)

if __name__ == "__main__":
    csv_path = r"../../../data/raw/input_data_csv/incomes/bea_incomes.csv"
    db_path = r"../../../data/database/incomes.db"  # Define your SQLite database path here
    print(f"Processing {csv_path}")
    process_csv(db_path, csv_path)
