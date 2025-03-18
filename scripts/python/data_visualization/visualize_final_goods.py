from matplotlib import pyplot as plt
from src.functions.db.fetch import fetch_final_goods_affordable


def plot_incomes_inf_final_goods(db_path, year_range, goods_list, regions, income_data_source, salary_interval, output_format):

    df = fetch_final_goods_affordable(
        db_path=db_path,
        year_range=year_range,
        goods_list=goods_list,
        regions=regions,
        income_data_source=income_data_source,
        salary_interval=salary_interval,
        output_format=output_format
    )

    plt.figure(figsize=(20, 10))

    if df['year'].nunique() == 1:
        labels = [f"{name} ({unit})" for name, unit in zip(df['name'], df['good_unit'])]
        plt.bar(labels, df['final_goods_affordable'])
        plt.xlabel("Good (Unit)")
        plt.ylabel("Affordable Quantity")
        plt.title(f"Affordable Quantity in {df['year'].iloc[0]}")
    else:
        for (good, unit), group in df.groupby(['name', 'good_unit']):
            plt.plot(
                group['year'],
                group['final_goods_affordable'],
                marker='o',
                label=f"{good} ({unit.strip("$/")})"
            )
        plt.xlabel("Year")
        plt.ylabel("Affordable Quantity")
        plt.title(f"Affordable Quantity Over Years ({income_data_source} Incomes)")
        plt.legend()

    filename = f"../../../doc/figures/affordable_quantity_{year_range[0]}_{year_range[1]}_{income_data_source.lower()}_incomes.png"
    plt.savefig(filename)
    plt.show()


if __name__ == "__main__":
    plot_incomes_inf_final_goods(
        db_path='../../../data/db/sqlite/database.sqlite',
        year_range=(1929, 2024),
        goods_list=['bacon', 'bread', 'butter', 'coffee', 'eggs', 'flour', 'milk', 'pork chop', 'round steak', 'sugar'],
        regions=['united states'],
        income_data_source='FRED',
        salary_interval='monthly',
        output_format='df',
    )
