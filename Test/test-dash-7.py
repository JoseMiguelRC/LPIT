from dash import Dash, html, dcc, callback, Output, Input
import dash_ag_grid as dag
import polars as pl
import plotly.express as px

# ==================================
# Dash example with gapminder2007 dataset @ https://raw.githubusercontent.com/plotly/datasets/master/gapminder2007.csv
# - Polars DataFrame feeding:
# · Grid table
# · Graph
# ==================================

# https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.to_dicts.html

# Read data from file and inserts it into a Polars DataFrame
df = pl.read_csv('gapminder2007.csv')

# Create the Dash app
app = Dash(__name__)

# App layout
# - Receives updates through the callback update_graph
app.layout = [
    html.Div(children='App with Grid, Graph, and Controls'),
    html.Hr(),
    dcc.RadioItems(options=['pop', 'lifeExp', 'gdpPercap'], value='lifeExp', id='dash-radio-item'),
    dag.AgGrid(
        rowData=df.to_dicts(),
        columnDefs=[{"field": i} for i in df.columns]
    ),
    dcc.Graph(figure={}, id='dash-graph')
]

# Creates a histogram and embed it into dcc.Graph
# Inputs and outputs of the callback are properties of components of app.layout:
# - Input = 'dash-radio-item' ← dcc.RadioItems value
# - Output = 'dash-graph' → dcc.Graph figure
# - return fig → dcc.Graph figure
@callback(
    Output(component_id='dash-graph', component_property='figure'),
    Input(component_id='dash-radio-item', component_property='value')
)
def update_graph(col):
    fig = px.histogram(df, x='continent', y=col, histfunc='avg')
    return fig

# Run the Dash app
if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8062, debug=True)
