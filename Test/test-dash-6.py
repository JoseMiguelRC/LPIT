from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import polars as pl

# ==================================
# Dash example with gapminder_unfiltered dataset @ https://raw.githubusercontent.com/plotly/datasets/master/gapminder_unfiltered.csv
# - Basic graph with Polars DataFrame
# ==================================

# Read data
df = pl.read_csv('gapminder_unfiltered.csv')

# Initialize the Dash app
app = Dash(__name__)

# App layout
app.layout = [
    html.H1(children='Dash app with data', style={'textAlign':'center'}),
    dcc.Dropdown(df["country"].unique().to_list(), 'Canada', id='dropdown-selection'),
    dcc.Graph(id='graph-content')
]

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    dff = df.filter(pl.col("country")==value)
    return px.line(dff, x='year', y='pop')

# Note: if app.run yields an error try changing the server port

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8061, debug=False)

