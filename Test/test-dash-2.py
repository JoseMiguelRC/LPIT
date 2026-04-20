from dash import Dash, html

# ==================================
# Dash 'Hello world'
# ==================================

app = Dash(__name__)
app.layout = html.Div("Hello Dash")

# Note: if app.run yields an error try changing the server port

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8056, debug=False)

