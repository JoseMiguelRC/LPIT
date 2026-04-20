
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

# ==================================
# Dash example with iris dataset @ https://github.com/pola-rs/polars/blob/main/docs/assets/data/iris.csv
# # - Scatter matrix
# ==================================

app = Dash(__name__)

app.layout = html.Div([
    html.H4('Analysis of Iris data using scatter matrix'),
    dcc.Dropdown(
        id="plotly-express-x-dropdown",
        options=['sepal_length', 'sepal_width', 
                 'petal_length', 'petal_width'],
        value=['sepal_length', 'sepal_width'],
        multi=True
    ),
    dcc.Graph(id="plotly-express-x-graph"),
])

@app.callback(
    Output("plotly-express-x-graph", "figure"),
    Input("plotly-express-x-dropdown", "value"))
def update_bar_chart(dims):
    df = px.data.iris()  # Iris dataset @ https://github.com/pola-rs/polars/blob/main/docs/assets/data/iris.csv
    fig = px.scatter_matrix(
        df, dimensions=dims, color="species")
    return fig

# Note: if app.run yields an error try changing the server port

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8057, debug=False)


