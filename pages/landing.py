# pages/landing.py
from dash import dcc, html

layout = html.Div([
    html.H1("Welcome to Value Voyage - A Journey Through Decades of Prices"),
    html.P("This is the landing page with attractive visualizations and concise explanations."),
    dcc.Graph(
        id="example-graph",
        figure={
            "data": [{"x": [1, 2, 3], "y": [4, 1, 2], "type": "bar", "name": "Example"}],
            "layout": {"title": "Example Visualization"}
        }
    )
])
