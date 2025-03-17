import dash
from dash import html, dcc


layout = html.Div([
    html.H1("Findings Page"),
    html.P("Findings Page with some nice graphs and documentation"),
    dcc.Graph(
        id="example-graph",
        figure={
            "data": [{"x": [1,2,3,6,5,3,5,6,7,5,4,3], "y": [4,1,2,4,5,7,9,5,5,7,2,7], "type": "bar", "name": "Example"}],
            "layout": {"title": "Example Visualization"}
        }
    )
])