import importlib
from collections import MutableMapping, Hashable

import networkx as nx
from rapidz import Stream
from shed import SimpleFromEventStream


# One problem we're facing is that the various pipelines handle document
# filling external to the logic of the data processing (outside of the captured
# graph. This is a good thing since assuming that the documents have the data
# we're looking for is nice. However, we need to do this on the replay as well
# since our pipeline expects the data to be loaded. This could be done in the
# creation of the data dict but that requires us loading up all the data at
# once which is a major anti-pattern. We could use a filler but then we
# run into issues since we don't actually track the resource and datum
# documents in the FromEventModel nodes since they didn't exist until recently
# (and they aren't really used in the data processing).


def replay(db, hdr):
    """Replay data analysis

    Parameters
    ----------
    db : Broker instance
        The databroker to pull data from
    hdr : Header instance
        The analyzed data header

    Returns
    -------
    loaded_graph : DiGraph
        The data processing pipeline as a graph
    parent_nodes : dict
        The source nodes for the graph
    data : dict
        A map between the document uids and documents
    vs : list
        List of document uid in time order

    Notes
    -----
    >>> graph, parents, data, vs = replay(db, hdr)
    >>> for v in vs:
    ...     parents[v["node"]].update(data[v["uid"]])

    """
    data = {}
    parent_nodes = {}
    # TODO: try either raw or analysis db (or stash something to know who comes
    #  from where) Maybe take in a list of dbs?
    raw_hdrs = [db[u] for u in hdr["start"]["parent_node_map"].values()]
    # load data from raw/partially analyzed headers

    # TODO: figure out how to handle filling documents, since we don't want
    #  to fill them here but they need to be filled
    for raw_hdr in raw_hdrs:
        data.update(
            {
                d.get("uid", d.get("datum_id")): (n, d)
                for n, d in raw_hdr.documents()
            }
        )

    # get information from old analyzed header
    times = hdr["stop"]["times"]
    graph = hdr["start"]["graph"]

    loaded_graph = nx.node_link_graph(graph)
    for n in nx.topological_sort(loaded_graph):
        loaded_graph.nodes[n]["stream"] = rebuild_node(
            loaded_graph.nodes[n]["stream"], loaded_graph
        )

    for node_uid in hdr["start"]["parent_node_map"]:
        parent_nodes[node_uid] = loaded_graph.nodes[node_uid]["stream"]

    vs = [
        {'uid': dct['uid'], 'node': dct['node']}
        for dct in sorted(
            times, key=lambda d: d['time']
        )
    ]

    return loaded_graph, parent_nodes, data, vs


def rebuild_node(node_dict, graph):
    d = dict(node_dict)
    node = getattr(importlib.import_module(d["mod"]), d["name"])
    d.pop("name")
    d.pop("mod")

    aa = []
    for a in d["args"]:
        if isinstance(a, MutableMapping) and a.get("name") and a.get("mod"):
            aa.append(getattr(importlib.import_module(a["mod"]), a["name"]))
        elif isinstance(a, (tuple, list)):
            aa.append(a)
        elif a in graph.nodes:
            aa.append(graph.nodes[a]["stream"])
        else:
            aa.append(a)
    d["args"] = aa

    kk = {}
    for k, a in d["kwargs"].items():
        # We can't check if non hashables are in the graph (also I don't think
        # we can put non hashables as nodes in the graph)
        if isinstance(a, Hashable) and a in graph.nodes:
            kk[k] = graph.nodes[a]["stream"]
        elif isinstance(a, MutableMapping) and a.get("name") and a.get("mod"):
            kk[k] = getattr(importlib.import_module(a["mod"]), a["name"])
        # If there is an upstream node for our FromEventStream node then
        # it is out of scope, make a Placeholder node to keep the instantiation
        # happy
        elif issubclass(node, SimpleFromEventStream) and k == "upstream":
            kk[k] = Stream(stream_name="Placeholder")
        else:
            kk[k] = a
    d["kwargs"] = kk
    n = node(*d["args"], **d["kwargs"])
    return n
