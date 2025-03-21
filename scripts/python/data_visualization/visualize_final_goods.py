from plotly.graph_objects import Figure, Scatter
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

    fig = Figure()

    if df['year'].nunique() == 1:
        # If only one year, create a bar chart
        labels = [f"{name} ({unit})" for name, unit in zip(df['name'], df['good_unit'])]
        fig.add_trace(Scatter(x=labels, y=df['final_goods_affordable'], mode='markers', name='Affordable Quantity'))
        fig.update_layout(
            title=f"Affordable Quantity in {df['year'].iloc[0]}",
            xaxis_title="Good (Unit)",
            yaxis_title="Affordable Quantity"
        )
    else:
        # If multiple years, create a line chart
        for (good, unit), group in df.groupby(['name', 'good_unit']):
            fig.add_trace(Scatter(x=group['year'], y=group['final_goods_affordable'], mode='lines+markers', name=f"{good} ({unit})"))
        fig.update_layout(
            title=f"Affordable Quantity Over Years ({income_data_source} Incomes)",
            xaxis_title="Year",
            yaxis_title="Affordable Quantity",
            legend_title="Goods",
            hovermode="x unified"
        )

    return fig


if __name__ == "__main__":
    fig = plot_incomes_inf_final_goods(
        db_path='../../../data/db/sqlite/database.sqlite',
        year_range=(1929, 2024),
        goods_list=['bacon', 'bread', 'butter', 'coffee', 'eggs', 'flour', 'milk', 'pork chop', 'round steak', 'sugar', 'gas'],
        regions=['united states'],
        income_data_source='FRED',
        salary_interval='monthly',
        output_format='df'
    )
    fig.show()
