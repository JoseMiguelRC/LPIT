from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import polars as pl

# ==================================
# Dash example with basic dataset
# - Basic graph with two series
# ==================================

# Create Polars DataFrame
df = pl.DataFrame({
    "x": [1, 2, 3, 4, 5],
    "A": [10, 15, 13, 17, 20],
    "B": [7, 11, 14, 9, 18],
})

app = Dash(__name__)

app.layout = html.Div([
    html.H1("Dash + Polars Example"),
    
    dcc.Dropdown(
        id="series-dropdown",
        options=[
            {"label": "Series A", "value": "A"},
            {"label": "Series B", "value": "B"},
        ],
        value="A",
        clearable=False,
    ),
    
    dcc.Graph(id="line-chart"),
])

@app.callback(
    Output("line-chart", "figure"),
    Input("series-dropdown", "value"),
)
def update_chart(selected_series):
        
    fig = px.line(
        x=df.get_column("x").to_list(),
        y=df.get_column(selected_series).to_list(),
        markers=True,
        title=f"Showing {selected_series}",
    )
    return fig

# Note: if app.run yields an error try changing the server port

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8057, debug=False)