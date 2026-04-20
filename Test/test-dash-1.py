import plotly.graph_objects as go

# ==================================
# Plotly example
# ==================================

# Sample data
x = [1, 2, 3, 4, 5]
y = [10, 15, 13, 17, 20]

# Create a figure
fig = go.Figure()

# Add a line trace
fig.add_trace(go.Scatter(
    x=x,
    y=y,
    mode='lines+markers',
    name='Example Line'
))

# Customize layout
fig.update_layout(
    title='Basic Plotly Line Chart',
    xaxis_title='X Axis',
    yaxis_title='Y Axis'
)

# Show the plot
fig.show()