from matplotlib import pyplot as plt
from src.functions.db import fetch_incomes_data



def compare_income_data_sources(start_year=1913, end_year=2024):
    sources = ['IRS', 'BEA', 'FRED']

    plt.figure(figsize=(12, 6))

    for source, marker in zip(sources, [".", "+", "x"]):
        data = fetch_incomes_data(start_year, end_year, source)
        if data:
            # Unpack fetched data into separate lists.
            years, incomes = zip(*data)
            plt.plot(years, incomes, marker=marker, label=source)
        else:
            print(f"No data found for {source} between {start_year} and {end_year}.")

    plt.xlabel("Year")
    plt.ylabel("Average Income Unadjusted")
    plt.title("United States Incomes (IRS vs. BEA vs FRED)")
    plt.legend()
    plt.grid(True)

    # Set the y-axis to logarithmic scale.
    plt.yscale('log')

    # Save the plot to a file.
    plt.savefig("../../../doc/figures/compare_income_data_sources_3.png")
    plt.show()

if __name__ == "__main__":
    compare_income_data_sources()