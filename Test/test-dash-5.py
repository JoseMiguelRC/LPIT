from dash import Dash, html
import dash_ag_grid as dag
import polars as pl

# ==================================
# Dash example with gapminder2007 dataset @ https://raw.githubusercontent.com/plotly/datasets/master/gapminder2007.csv
# - Basic grid table fed with Polars DataFrame
# ==================================

# Read data
df = pl.read_csv('gapminder2007.csv')

# Initialize the app
app = Dash(__name__)

# App layout
app.layout = [
    html.Div(children='Dash app with data (Polars DataFrame)'),
    dag.AgGrid(
        rowData=df.to_dicts(),
        columnDefs=[{"field": i} for i in df.columns]
    )
]

# Note: if app.run yields an error try changing the server port

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8060, debug=False)

