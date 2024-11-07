# from rdflib import Graph
# import networkx as nx
# import matplotlib.pyplot as plt

# # Load RDF data from a file
# g = Graph()
# g.parse("monster.ttl", format="turtle")

# # Create a NetworkX graph
# nx_graph = nx.Graph()

# # Add nodes and edges from RDF triples
# for subj, pred, obj in g:
#     nx_graph.add_edge(subj, obj, label=pred)

# # Draw the graph
# plt.figure(figsize=(12, 12))
# pos = nx.spring_layout(nx_graph, k=0.5)  # Adjust layout
# edge_labels = nx.get_edge_attributes(nx_graph, 'label')

# # Draw nodes, edges, and labels
# nx.draw(nx_graph, pos, with_labels=True, node_size=1500, node_color="skyblue", font_size=10)
# nx.draw_networkx_edge_labels(nx_graph, pos, edge_labels=edge_labels, font_color='red')

# plt.title("RDF Graph Visualization")
# plt.show()


# -----------------------------------
# -----------------------------------


# from rdflib import Graph
# import networkx as nx

# # Load RDF data
# g = Graph()
# g.parse("monster.ttl", format="turtle")

# # Create a NetworkX graph and add RDF data
# nx_graph = nx.DiGraph()  # Directed graph for RDF triples

# for subj, pred, obj in g:
#     nx_graph.add_edge(str(subj), str(obj), label=str(pred))

# # Save the NetworkX graph to GraphML format
# nx.write_graphml(nx_graph, "monster.graphml")


# -----------------------------------
# -----------------------------------


from rdflib import Graph
import csv

# Load RDF data
g = Graph()
g.parse("monster.ttl", format="turtle")

# Extract nodes and edges
nodes = set()
edges = []

for subj, pred, obj in g:
    nodes.add((subj,))
    nodes.add((obj,))
    edges.append((subj, obj, pred))

# Write nodes to CSV
with open("nodes.csv", "w", newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["Id", "Label"])
    writer.writerows([(str(node[0]).removeprefix('https://w3id.org/oc/meta/'), str(node[0]).removeprefix('https://w3id.org/oc/meta/')) for node in nodes])

# Write edges to CSV
with open("edges.csv", "w", newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["Source", "Target", "Type", "Label"])
    writer.writerows([(str(subj), str(obj), "Directed", str(pred)) for subj, obj, pred in edges])
