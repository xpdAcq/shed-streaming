"""Graphing utilities for EventStreams"""
import networkx as nx
import redsky.event_streams as es
from redsky.event_streams import dstar
import matplotlib.pyplot as plt


def create_graph(node, graph):
    """

    Parameters
    ----------
    node: EventStream instance
    graph: nx.Graph instance
    """
    t = hash(node)
    graph.add_node(t, str=str(node))
    for parent in node.parents:
        tt = hash(parent)
        graph.add_node(tt, str=str(node))
        graph.add_edge(t, tt)
        create_graph(parent, graph)
    if node.children == [None]:
        return graph


def plot_graph(starting_nodes, layout=nx.spring_layout):
    g = nx.DiGraph()
    for node in starting_nodes:
        g = create_graph(node, g)
    p = layout(g)
    nx.draw_networkx_labels(g, p, labels={k: g.node[k]['str'] for k in g})
    nx.draw_networkx_nodes(g, p, alpha=.3)
    nx.draw_networkx_edges(g, p)
    plt.show()

if __name__ == '__main__':
    def subs(x1, x2):
        return x1 - x2
    rds = es.EventStream(md={'name': 'Raw Data'})
    dark_data_stream = es.EventStream(md={'name': "Dark Data"})

    z = es.combine_latest(rds, dark_data_stream, emit_on=rds)
    img_stream = es.map(dstar(subs),
                        z,
                        input_info={'x1': 'pe1_image',
                                    'x2': 'pe1_image'},
                        output_info=[('image', {
                            'dtype': 'array',
                            'source': 'testing'})]
                        )
    L = img_stream.sink_to_list()
    plot_graph([rds, dark_data_stream])
