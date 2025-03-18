from matplotlib import pyplot as plt
from src.functions.db.fetch import fetch_incomes


def compare_income_data_sources(db_path, start_year, end_year, regions, sources, markers, output_format, output_file):
    plt.figure(figsize=(12, 6))

    for source, marker in zip(sources, markers):
        df = fetch_incomes(
            db_path=db_path,
            year_range=(start_year, end_year),
            data_source_name=source,
            regions=regions,
            output_format=output_format
        )
        if not df.empty:
            years = df['year']
            incomes = df['average_income_unadjusted']
            plt.plot(years, incomes, marker=marker, label=source)
        else:
            print(f"No data found for {source} between {start_year} and {end_year}.")

    plt.xlabel("Year")
    plt.ylabel("Average Income Unadjusted")
    plt.title(f"United States Incomes")
    plt.legend()
    plt.grid(True)
    plt.yscale('log')

    plt.savefig(output_file)
    plt.show()


if __name__ == "__main__":
    db_path = '../../../data/db/sqlite/database.sqlite'
    start_year = 1900
    end_year = 2024
    regions = ['united states']
    sources = ['IRS', 'BEA', 'FRED']
    markers = ['.', '+', 'x']
    output_format = 'df'
    output_file = f"../../../doc/figures/compare_income_data_sources_{start_year}_{end_year}.png"

    compare_income_data_sources(db_path, start_year, end_year, regions, sources, markers, output_format, output_file)
