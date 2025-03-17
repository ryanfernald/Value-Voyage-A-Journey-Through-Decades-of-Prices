# pages/analysis.py
import dash
from dash import dcc, html
import pandas as pd
import plotly.graph_objects as go
import dash_bootstrap_components as dbc


# Define the Goods Prices Graph as a function
def get_goods_prices_graph():
    goods = pd.read_csv("https://raw.githubusercontent.com/ryanfernald/Value-Voyage-A-Journey-Through-Decades-of-Prices/refs/heads/main/data/good-prices.csv")
    goods = goods.sort_values("Date", ascending=True)

    goods_prices_graph = go.Figure()

    for good_name in goods["Good Name"].unique():
        filtered_data = goods[goods["Good Name"] == good_name]
        goods_prices_graph.add_trace(go.Scatter(
            x=filtered_data["Date"], 
            y=filtered_data["Price"], 
            mode="lines",  
            name=good_name  
        ))

    goods_prices_graph.update_layout(
        title="Price Trends Over Time",
        xaxis_title="Date",
        yaxis_title="Price",
        title_font_size=16,
        xaxis=dict(tickangle=45), 
        hovermode="x unified"  
    )
    return goods_prices_graph


# Define the Income Average Graph as a function
def get_income_averages_graph():
    income = pd.read_csv("https://raw.githubusercontent.com/ryanfernald/Value-Voyage-A-Journey-Through-Decades-of-Prices/refs/heads/main/data/income1913-1998.csv")

    income_graph_fig = go.Figure()

    income_graph_fig.add_trace(go.Scatter(x=income["year"], y=income["tax-units"],
                             mode='lines+markers',
                             name="Tax Units"))

    income_graph_fig.add_trace(go.Scatter(x=income["year"], y=income["Average Income Adjusted $ 1998"],
                             mode='lines+markers',
                             name="Avg Income Adjusted (1998 $)"))

    income_graph_fig.add_trace(go.Scatter(x=income["year"], y=income["Income Unadjusted"],
                             mode='lines+markers',
                             name="Income Unadjusted"))

    income_graph_fig.update_layout(
        title="Income Trends Over Years",
        xaxis_title="Year",
        yaxis_title="Income / Tax Units",
        template="plotly_white",
        hovermode="x"
    )
    return income_graph_fig


# Define the Income Shares By Percentage Graph as a function
def get_income_shares_graph():
    income = pd.read_csv("https://raw.githubusercontent.com/ryanfernald/Value-Voyage-A-Journey-Through-Decades-of-Prices/refs/heads/main/data/income1913-1998.csv")
    columns_to_plot = [
        "P90-100", "P90-95", "P95-99", "P99-100",
        "P99.5-100", "P99.9-100", "P99.99-100"
    ]

    income_shares = go.Figure()

    for col in columns_to_plot:
        income_shares.add_trace(go.Scatter(x=income["year"], y=income[col],
                             mode='lines+markers',
                             name=col))

    income_shares.update_layout(
        title="Top Income Shares by Percentage",
        xaxis_title="Year",
        yaxis_title="Income Share (%)",
        template="plotly_white",
        hovermode="x"
    )
    return income_shares


# Define the Income by Area Graph as a function
def get_income_by_area_graph():
    area_df = pd.read_csv("https://raw.githubusercontent.com/ryanfernald/Value-Voyage-A-Journey-Through-Decades-of-Prices/refs/heads/main/data/income-by-area.csv")
    regions = ["United States *", "Mideast", "Great Lakes", "Plains",
               "Southeast", "Southwest", "Rocky Mountain", "Far West *"]

    income_area = go.Figure()

    for region in regions:
        income_area.add_trace(go.Scatter(
            x=area_df["Year"],
            y=area_df[region],
            mode="lines",
            name=region
        ))

    income_area.update_layout(
        title="Regional Income Trends Over Time",
        xaxis_title="Year",
        yaxis_title="Income Value",
        legend_title="Regions",
        hovermode="x"
    )
    return income_area


# Define the layout for the analysis page
layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.Div([
                        html.H1("Price Trends Over Time"),
                        html.H2("Data Source:"),
                        html.P("This is a detailed explanation of the analysis. It can include multiple paragraphs and should provide context for the visualizations.")
                    ]),
                    width=6
                ),
                dbc.Col(
                    dcc.Graph(id="price-trends-graph", figure=get_goods_prices_graph()),  # Call the function
                    width=6
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(id="income-averages-graph", figure=get_income_averages_graph()),  # Call the function
                    width=6
                ),
                dbc.Col(
                    html.Div([
                        html.H1("Income Average"),
                        html.H2("Data Source:"),
                        html.P("Additional context or insights related to the second graph.")
                    ]),
                    width=6
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.Div([
                        html.H1("Income Shares by Percentage"),
                        html.H2("Data Source:"),
                        html.P("Additional context or insights related to the third graph.")
                    ]),
                    width=6
                ),
                dbc.Col(
                    dcc.Graph(id="income-shares-graph", figure=get_income_shares_graph()),  # Call the function
                    width=6
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(id="income-area-graph", figure=get_income_by_area_graph()),  # Call the function
                    width=6
                ),
                dbc.Col(
                    html.Div([
                        html.H1("Average Income by Area"),
                        html.H2("Data Source:"),
                        html.P("Additional context or insights related to the second graph.")
                    ]),
                    width=6
                )
            ]
        )
    ],
    fluid=True
)

# Export the layout
export_layout = layout
