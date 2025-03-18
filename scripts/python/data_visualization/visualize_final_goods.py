from matplotlib import pyplot as plt
from src.functions.db.fetch import fetch_final_goods_affordable

def plot_incomes_inf_final_goods():
    df = fetch_final_goods_affordable(
        db_path='../../../data/db/sqlite/database.sqlite',
        year_range=(1929, 2024),
        goods_list=['bacon', 'bread', 'butter', 'coffee', 'eggs', 'flour', 'milk', 'pork chop', 'round steak', 'sugar'],
        regions=['united states'],
        income_data_source='FRED',
        salary_interval='monthly',
        output_format='df'
    )

    plt.figure(figsize=(20, 10))

    if df['year'].nunique() == 1:
        plt.bar(df['name'], df['final_goods_affordable'])
        plt.xlabel("Good")
        plt.ylabel("Affordable Quantity")
        plt.title(f"Affordable Quantity in {df['year'].iloc[0]}")
    else:
        # For multiple years, plot a line for each good.
        for good, group in df.groupby('name'):
            plt.plot(group['year'], group['final_goods_affordable'], marker='o', label=good)
        plt.xlabel("Year")
        plt.ylabel("Affordable Quantity")
        plt.title("Affordable Quantity Over Years FRED")
        plt.legend()

    # Save the graph as a PNG file.
    plt.savefig("../../../doc/figures/affordable_quantity_1929_2024_fred_incomes_sample_test.png")

    plt.show()


if __name__ == "__main__":
    plot_incomes_inf_final_goods()