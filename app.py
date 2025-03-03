from flask import Flask, render_template, request, jsonify
import plotly.express as px
import pandas as pd
import logging
from src.functions.db import fetch_good_prices

app = Flask(__name__)


data = fetch_good_prices() 
df = pd.DataFrame(data)

df['date'] = pd.to_datetime(df['date'])

logging.basicConfig(level=logging.DEBUG)

def create_plot(selected_items=None):
    """Generate a Plotly graph as an HTML string."""
    if not selected_items or "All" in selected_items:
        filtered_df = df  # Show all data
    else:
        filtered_df = df[df['name'].isin(selected_items)]
    
    # Ensure we have data to plot, otherwise return a message
    if filtered_df.empty:
        logging.warning("Filtered data is empty. No graph will be generated.")
        return "<p>No data available for the selected items.</p>"

    # Debugging logs
    logging.debug(f"Selected items: {selected_items}")
    logging.debug(f"Filtered Data Sample: {filtered_df.head()}")

    fig = px.line(
        filtered_df, x="date", y="price", color="name" if "name" in filtered_df.columns else None,
        title="Price Trends Over Time",
        labels={"date": "Year", "price": "Price ($)"}
    )

    fig.update_layout(width=1200, height=600)  # Increase figure size

    return fig.to_html(full_html=False)

@app.route('/')
def index():
    data = {
        1950: [{'label': 'Milk', 'price': '$0.50'}, {'label': 'Bread', 'price': '$0.20'}],
        1960: [{'label': 'Milk', 'price': '$0.60'}, {'label': 'Bread', 'price': '$0.25'}],
        # Add more decades as needed
    }
    return render_template('base.html', data=data)

@app.route('/price_graph', methods=['GET'])
def price_graph():
    """Render the graph page with a default graph."""
    item_names = df['name'].unique().tolist()
    initial_graph = create_plot(["All"])  
    
    return render_template("price_graph.html", item_names=item_names, graph_html=initial_graph)

@app.route('/update_graph', methods=['POST'])
def update_graph():
    """Update the graph based on selected checkboxes."""
    selected_items = request.json.get('selected_items', [])

    if not selected_items:
        logging.warning("No items selected, defaulting to 'All'.")
        selected_items = ["All"]

    logging.debug(f"Received update_graph request with selected_items: {selected_items}")

    try:
        graph_html = create_plot(selected_items)
        logging.debug("Graph successfully generated!")
        return jsonify(graph_html=graph_html)
    except Exception as e:
        logging.error(f"Error generating graph: {e}")
        return jsonify(error=str(e)), 500

if __name__ == '__main__':
    app.run(debug=True)
