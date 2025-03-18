import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.colors as mcolors
from src.functions.db.fetch import fetch_goods_prices


def get_distinct_years_from_df(df):
    """
    Extract a full range of years from the DataFrame.
    """
    years = sorted(df['year'].unique())
    return list(range(min(years), max(years) + 1))


def get_distinct_goods_from_df(df):
    """
    Extract a sorted list of distinct goods (names) from the DataFrame.
    """
    goods = sorted(df['name'].unique())
    return goods


def create_binary_matrix(years, goods, df):
    """
    Create a DataFrame with years as rows and goods as columns.
    A cell is 1 if an entry exists for that good in that year, 0 otherwise.
    """
    matrix = pd.DataFrame(0, index=years, columns=goods)
    for _, row in df.iterrows():
        year = row['year']
        good = row['name']
        if year in matrix.index and good in matrix.columns:
            matrix.at[year, good] = 1
    return matrix


def plot_heatmap(matrix, output_file):
    """
    Plot and save the heatmap with reversed axes (X: Years, Y: Good Names).
    """
    matrix_transposed = matrix.T

    plt.figure(figsize=(40, 15))
    cmap = mcolors.ListedColormap(['red', 'green'])

    sns.heatmap(
        matrix_transposed,
        cmap=cmap,
        linewidths=0.5,
        linecolor="gray",
        cbar=False,
        square=True,
        annot=False,
        vmin=0,
        vmax=1
    )
    plt.xlabel("Year")
    plt.ylabel("Good Name")
    plt.title("July 2 Data Entry Heatmap (Green: Entry exists, Red: No entry) - Reversed Axes")
    plt.xticks(rotation=90, ha="center")
    plt.yticks(rotation=0)

    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    print(f"Heatmap saved as {output_file}")
    plt.show()


if __name__ == "__main__":
    year_range = (1929, 2024)

    df = fetch_goods_prices(
        db_path='../../../data/db/sqlite/database.sqlite',
        year_range=year_range,
        # If you want to restrict to a subset of goods, specify the list; otherwise, set to None.
        goods_list=None,
        use_year_averages=True,
        output_format='df'
    )

    output_file = f"../../../doc/diagrams/missing_data_heatmap_{year_range[0]}_{year_range[1]}.png"

    years = get_distinct_years_from_df(df)
    goods = get_distinct_goods_from_df(df)

    matrix = create_binary_matrix(years, goods, df)

    plot_heatmap(matrix, output_file)
