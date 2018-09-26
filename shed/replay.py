import importlib
from pprint import pprint

import networkx as nx
from shed.translation import ToEventStream
from collections import MutableMapping


def replay(db, hdr, export=False):
    data = {}
    parent_nodes = {}
    # TODO: try either raw or analysis db (or stash something to know who comes
    # from where)
    raw_hdrs = [db[u] for u in hdr["start"]["parent_node_map"].values()]
    # load data from raw/partially analyzed headers
    for raw_hdr in raw_hdrs:
        data.update({d["uid"]: (n, d) for n, d in raw_hdr.documents()})

    # get information from old analyzed header
    times = hdr["stop"]["times"]
    graph = hdr["start"]["graph"]

    loaded_graph = nx.node_link_graph(graph)
    for n in nx.topological_sort(loaded_graph):
        loaded_graph.node[n]["stream"] = rebuild_node(
            loaded_graph.node[n]["stream"], loaded_graph
        )

    if export:
        for n, attrs in loaded_graph.node.items():
            if isinstance(attrs["stream"], ToEventStream):
                attrs["stream"].DBFriendly().starsink(db.insert)

    for node_uid in hdr["start"]["parent_node_map"]:
        parent_nodes[node_uid] = loaded_graph.node[node_uid]["stream"]
    print("parent_node_map", hdr["start"]["parent_node_map"])

    vs = sorted([(t, v) for t, v in times.items()], key=lambda x: x[0])
    vs = [v for t, v in vs]

    # push the data through the pipeline
    print(parent_nodes)
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
        elif a in graph.node:
            aa.append(graph.node[a]["stream"])
        else:
            aa.append(a)
    d["args"] = aa
    kk = {}
    for k, a in d["kwargs"].items():
        if a in graph.node:
            kk[k] = graph.node[a]["stream"]
        elif isinstance(a, MutableMapping) and a.get("name") and a.get("mod"):
            kk[k] = getattr(importlib.import_module(a["mod"]), a["name"])
        else:
            kk[k] = a
    d["kwargs"] = kk
    n = node(*d["args"], **d["kwargs"])
    return n
