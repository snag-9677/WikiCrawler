import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import networkx as nx
import pickle
import numpy as np


# ############### LOAD DATA ############### #
with open(r'C:\Users\Sahir\Desktop\Network Science\Data Scrape\wiki_pickle\arhive_crawler\2.pkl', 'rb') as f:
    data = pickle.load(f)

# Keep only the nodes scanned
_nodes = data['nodes']
_graph = data['graph']
keys_to_keep = set(data['graph'].keys())

for key, vals in _graph.items():
    _graph[key] = list(set(vals).intersection(keys_to_keep).difference({key}))
    
index_diag_mapping = {i: key for i, key in enumerate(_graph.keys())}
# Create a sample directed graph (replace this with your own)
G_large = nx.DiGraph(data["graph"])
# Make a subset of G with 100 nodes. Take the nodes with the most connections
G = G_large.subgraph(sorted(G_large, key=lambda x: G_large.degree(x), reverse=True)[:5])


# Calculate degrees of nodes
degrees = dict(G.degree())

print("Data Loaded....")
# ############### LOAD DATA ############### #









# @app.callback(
#     Output('3d-network-plot', 'figure'),
#     Input('3d-network-plot', 'relayoutData')
# )
def update_graph():
    # Define a color scale for degrees (e.g., from blue to red)
    colors = [go.Scatter3d(x=[0, 1], y=[0, 1], z=[0, 1], mode='markers',
                        marker=dict(size=0.1, colorbar=dict(title='Degrees', tickvals=[0, 5, 10, 15, 20]),
                                    colorscale='Viridis'))]
    # Create 3D figure
    fig = go.Figure()
    print("Figure Initialized...")
    for edge in G.edges():
        # Get node positions
        node1, node2 = edge
        # Equi distribute points in the
        pos1 = [np.random.random(), np.random.random() , degrees[node1]]
        pos2 = [np.random.random(), np.random.random() , degrees[node2]]

        # Add edges
        fig.add_trace(go.Scatter3d(x=[pos1[0], pos2[0]], y=[pos1[1], pos2[1]], z=[pos1[2], pos2[2]], mode='lines', line=dict(color='gray', width=1)))

        # Add nodes
        fig.add_trace(go.Scatter3d(x=[pos1[0], pos2[0]], y=[pos1[1], pos2[1]], z=[pos1[2], pos2[2]],
                                mode='markers', marker=dict(size=5, color=degrees[node1], colorscale='Viridis')))
    print("Trace Added...")
    
    # Define layout for 3D plot
    fig.update_layout(scene=dict(aspectmode="cube"),
                    margin=dict(l=0, r=0, b=0, t=0),
                    uirevision='true',  # Disable updates
                    hovermode=False,   # Disable hover
                    scene_dragmode='orbit',  # Enable rotation
                    clickmode=None,   # Disable click events
                    showlegend=False,
                    coloraxis=dict(colorbar=dict(title='Degrees')),
                    template='plotly_dark'  # Set to dark mode
                    )  # Disable legend
    
    print("Layout Updated...")

    return fig

fig = update_graph()
# ############### APP CONFIGS ############### #
# Define additional CSS for dark background
external_stylesheets = ['style.css']

# Apply the CSS to the app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Define layout with inline CSS
app.layout = html.Div(children=[
    html.Div(children=[
        dcc.Graph(id='3d-network-plot', figure=fig),
    ], style={'background-color': '#111111', 'color': '#7FDBFF', 'width': '100%', 'height': '100vh'}),
], style={'background-color': '#111111', 'color': '#7FDBFF', 'width': '100%', 'height': '100vh'})
print("App Configured...")
# ############### APP CONFIGS ############### #

if __name__ == '__main__':
    app.run_server(debug=True)
