import dash_bootstrap_components as dbc
from dash import dcc, html

def create_navbar():
    navbar = dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Home", href="/")),
            dbc.NavItem(dbc.NavLink("Objectives", href="/objectives")),
            dbc.NavItem(dbc.NavLink("Analysis", href="/analysis")),
            dbc.NavItem(dbc.NavLink("Findings", href="/findings")),
            # dbc.DropdownMenu(
            #     dbc.DropdownMenuItem("About"),
            #     dbc.DropdownMenuItem("Contact"),
            #     dbc.DropdownMenuItem("Feedback"),),
        ],
        brand="Value Voyage - A Journey Through Decades of Prices",
        brand_href="/",
        color="darkcyan",
        dark=True,
        fluid=True,
    )
    return navbar
