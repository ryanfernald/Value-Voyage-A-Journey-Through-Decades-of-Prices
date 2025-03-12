import dash
from dash import dcc, html
import dash_bootstrap_components as dbc

layout = dbc.Container(
    [
            dbc.Row(
                [
                    dbc.Col(
                        html.Div([
                            html.H3("Project Summary"),
                            html.P(
                                "Price of goods is something that everyone is affected by. We all are well aware that the prices go up. To measure this, there is a CPI (consumer price index) indicator. That number gives insight to people with financial background into the rising prices trends. However, for an average consumer CPI is nothing but some number that verifies the claim “The prices go up!”"
                            ),
                            html.P(
                                "In this project, we aim to solve this issue, and make CPI and purchasing power concepts more sensible for an average consumer. To do this, we aim to transform CPI into visual and quantifiable insights measured in the final consumer goods (milk, eggs, sugar, flour, pork chops, butter, potatoes, real estate). Moreover, we aim to assess the average annual income across generations, to calculate how many/much of each good an average consumer was able to purchase with their monthly salary in the 1900-2020 period with 10 year steps."
                            ),
                        ]),
                        width=6
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardImg(src="/assets/income_indequality_washingtonpost.png", top=True),
                                dbc.CardBody(
                                    [
                                        html.H4("Learn More about our Motivation", className="card-title"),
                                        html.P(
                                            "Some quick example text to build on the card title and "
                                            "make up the bulk of the card's content.",
                                            className="card-text",
                                        ),
                                        dbc.Button("Go somewhere", color="success"),
                                    ]
                                ),
                            ], color = "success", outline = True
                        ),
                        width=3, 
                    )
                ]
            ),

        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardImg(src="/assets/housing_crash.jpg", top=True),
                            dbc.CardBody(
                                [
                                    html.H4("Our Take on Inflation", className="card-title"),
                                    html.P(
                                        "Some quick example text to build on the card title and "
                                        "make up the bulk of the card's content.",
                                        className="card-text",
                                    ),
                                    dbc.Button("Go somewhere", color="success"),
                                ]
                            ),
                        ], color = "success", outline = True
                    ),
                    width=3,
                ),
                dbc.Col(
                    html.Div([
                        html.H3("Broader Impacts"),
                        html.P(
                            "The project is meant to provide clear insight on how inflation affects the average consumer, and provide them with a clear understanding, even if they have no financial nor mathematical background. This will allow broader audiences to understand the impact of inflation on their day to day lives in a sensible way."
                        ),
                    ]),
                    width=6
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.Div([
                        html.H3("Data Sources"),
                        html.P(
                            "https://libraryguides.missouri.edu/pricesandwages\
                             https://usa.usembassy.de/etexts/his/e_prices1.htm\
                             https://babel.hathitrust.org/cgi/pt?id=umn.31951000014585x&seq=233 \
                             https://www.bls.gov/regions/mid-atlantic/data/AverageRetailFoodAndEnergyPrices_USandMidwest_Table.htm \
                             https://babel.hathitrust.org/cgi/pt?id=mdp.39015008856711&seq=527"
                        ),
                    ]),
                    width=6
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardImg(src="/assets/history_tax_returns.jpg", top=True),
                            dbc.CardBody(
                                [
                                    html.H4("Notes on Some of the Limitations behind the data", className="card-title"),
                                    html.P(
                                        "Some quick example text to build on the card title and "
                                        "make up the bulk of the card's content.",
                                        className="card-text",
                                    ),
                                    dbc.Button("Go somewhere", color="success"),
                                ]
                            ),
                        ], color = "success", outline = True
                    ),
                    width=3,
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardImg(src="/assets/inflation.png", top=True),
                            dbc.CardBody(
                                [
                                    html.H4("Check out our Analysis Page", className="card-title"),
                                    html.P(
                                        "Some quick example text to build on the card title and "
                                        "make up the bulk of the card's content.",
                                        className="card-text",
                                    ),
                                    dbc.Button("Go to Analysis", color="success", href="/analysis"),
                                ]
                            ),
                        ], color = "success", outline = True
                    ),
                    width=3,
                ),
                dbc.Col(
                    html.Div([
                        html.H3("Expected Major Findings"),
                        html.P(
                            "It’s been widely cited that the cost of living has drastically exceeded the average income across the United States for decades. So we can compare the cost of living and commodity items with the average incomes across generations to see which time periods had the best economic prosperity compared to modern day. We’re expecting to see that the earlier generations like the Silent, Boomer, and generation X had a much more prosperous economy, comfortable affordability and easier access to a life with upward mobility established across generations."
                        ),
                        html.P(
                            "Findings are expected but not limited to: \
                              - An average consumer can afford more food and clothing due to industrialization \
                              - An average consumer can afford less of average real estate, and college tuition \
                              - An average consumer can afford less of gold \
                              - An average consumers purchasing power today is generally higher than in 1900s "
                        ),
                        html.P(
                            "Besides the calculations and visualizations, we will attempt to train an ML model on the collected data. However, we expect the performance of the model to be poor, due to the extremely high number of factors that affect the real world prices."
                        )
                    ]),
                    width=6
                )
            ]
        )
    ],
    fluid=True
)

# If you need to export the layout, uncomment the following lines:
# from dash.exporter import Exporter
# exporter = Exporter(app)
# exporter.export_layout(layout, "pages/objective.html")
