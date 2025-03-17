import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='pandas')

from dash import Dash, dcc, html, Input, Output, callback, dash
import dash_bootstrap_components as dbc
from components import navbar
from pages import landing, objectives, analysis, findings
# from flask import Flask, request
import os

# from google.cloud import storage
# import os
# from io import StringIO


# # BLOB is an acronym for "Binary Large Object". It's a data type that stores binary data, such as images, videos, and audio.
# def get_csv_from_gcs(bucket_name, source_blob_name):
#     """Downloads a blob from the bucket."""
#     # The ID of your GCS bucket
#     bucket_name = "value-voyage-cs163.appspot.com"

#     # The ID of your GCS object
#     # source_blob_name = "storage-object-name"

#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)

#     # Construct a client side representation of a blob.
#     # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
#     # any content from Google Cloud Storage. As we don't need additional data,
#     # using `Bucket.blob` is preferred here.
#     blob = bucket.blob(source_blob_name)
#     data = blob.download_as_text()
#     return pd.read_csv(StringIO(data))


app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Define the app layout
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    navbar.create_navbar(),
    html.Div(id="page-content", style={"padding": "20px"}),
    dash.page_container

])

# Define the callback to update the page content
@callback(
    Output("page-content", "children"),
    [Input("url", "pathname")]
)
def display_page(pathname):
    if pathname == "/":
        return landing.layout
    elif pathname == "/objectives":
        return objectives.layout
    elif pathname == "/analysis":
        return analysis.layout
    elif pathname == "/findings":
        return findings.layout
    else:
        return html.Div([
            html.H1("404: Page Not Found"),
            html.P("The page you are looking for does not exist.")
        ])

# Define a readiness check endpoint
@app.server.route('/readiness_check')
def readiness_check():
    # Add logic to check if your app is ready to serve traffic
    if app.is_ready():
        return "App is ready", 200
    else:
        return "App is not ready", 503

# Check if the Dash app is ready
def app_is_ready():
    # Placeholder function to check if the app is ready
    # You should implement the actual logic here
    # For now, we assume the app is always ready
    return True

if __name__ == "__main__":
    app.run_server(debug=True, port=8080 if os.environ.get('SERVER_SOFTWARE') else 8050)
    warnings.filterwarnings("ignore", category=UserWarning, module='pandas')