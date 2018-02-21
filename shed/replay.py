from streamz_ext import Stream, map
from shed.translation import FromEventStream, ToEventStream, hash_or_uid
from shed.databroker_tools import GraphInsert
from shed.savers import GraphWriter
import uuid
from pprint import pprint
from databroker import Header
from databroker import Broker
import time
import operator as op
import inspect
import importlib
import networkx as nx


def replumb_data(product_header, raw_headers):
    """Reprocess the data

    Parameters
    ----------
    hdrs
    data

    Returns
    -------

    """
    data = {}
    times = {}
    nodes = {}
    # TODO: try either raw or analysis db (or stash something to know who comes
    # from where)

    # load data from raw/partially analyzed headers
    for hdr in raw_headers:
        data.update({d['uid']: (n, d) for n, d in hdr.documents()})

    # get information from old analyzed header
    times.update(product_header.stop['times'])
    graph = product_header.start['graph']
    for node_uid in product_header.start['parent_uids']:
        nodes[node_uid] = graph.node[node_uid]['stream']
    vs = sorted([(t, v) for t, v in times.items()], key=lambda x: x[0])
    vs = [v for t, v in vs]

    # push the data through the pipeline
    for v in vs:
        # TODO: fill the event from the databroker
        nodes[v['node']].update(data[v['uid']])


def db_friendly_node(node):
    """Extract data to make node db friendly"""
    d = dict(node.__dict__)
    d2 = {'name': node.__class__.__name__, 'mod': node.__module__}
    for f_name in ['func', 'predicate']:
        if f_name in d:
            d2[f_name] = {'name': d[f_name].__name__,
                          'mod': d[f_name].__module__}
            d[f_name] = {'name': d[f_name].__name__,
                         'mod': d[f_name].__module__}

    # TODO: Remove these? (They are captured by the graph, maybe)
    for k in ['upstreams', 'downstreams']:
        ups = []
        for up in d[k]:
            if up is not None:
                ups.append(hash_or_uid(up))
        d2[k] = ups
        d[k] = ups
    if len(d['upstreams']) == 1:
        d2['upstream'] = d2['upstreams'][0]
        d['upstream'] = d['upstreams'][0]

    sig = inspect.signature(node.__init__)
    params = sig.parameters
    constructed_args = []
    for p in params:
        q = params[p]
        # Exclude self
        if str(p) != 'self':
            if q.kind == q.POSITIONAL_OR_KEYWORD and p in d:
                if p == 'stream_name':
                    p = 'name'
                constructed_args.append(d[p])
            elif q.default is not q.empty:
                constructed_args.append(q.default)
    constructed_args.extend(d.get('args', ()))
    d2['args'] = constructed_args
    d2['kwargs'] = d.get('kwargs', {})
    pprint(d2)
    return d2


def rebuild_node(node_dict, graph):
    d = dict(node_dict)
    node = getattr(importlib.import_module(d['mod']),
                   d['name'])
    d.pop('name')
    d.pop('mod')
    for f_name in ['func', 'predicate']:
        if f_name in d:
            d[f_name] = getattr(importlib.import_module(d[f_name]['mod']),
                                d[f_name]['name'])
    # TODO: replace hash/uid in args with actual node (now built)
    for upstream in d['upstreams']:
        if upstream in d['args']:
            d['args'][d['args'].index(upstream)] = graph.node[upstream]['stream']
    print(node)
    pprint(d)
    return node(*d['args'], **d['kwargs'])


if __name__ == '__main__':
    from tempfile import TemporaryDirectory

    td = TemporaryDirectory()
    db = Broker.named('temp')


    def y():
        suid = str(uuid.uuid4())
        yield ('start', {'uid': suid,
                         'time': time.time()})
        duid = str(uuid.uuid4())
        yield ('descriptor', {'uid': duid,
                              'run_start': suid,
                              'name': 'primary',
                              'data_keys': {'det_image': {'dtype': 'int',
                                                          'units': 'arb'}},
                              'time': time.time()})
        yield ('event', {'uid': str(uuid.uuid4()),
                         'data': {'det_image': 1},
                         'timestamps': {'det_image': time.time()},
                         'seq_num': 1,
                         'time': time.time(),
                         'descriptor': duid})
        yield ('stop', {'uid': str(uuid.uuid4()),
                        'time': time.time(),
                        'run_start': suid})


    print('build graph')
    g1 = FromEventStream('event', ('data', 'det_image',), principle=True)
    g2 = g1.map(op.mul, 2)
    g = g2.ToEventStream(('img2',))
    l = g.sink_to_list()

    for yy in y():
        db.insert(*yy)
        g1.update(yy)
    graph = l[0][1]['graph']

    print('db friendly node')
    for n in nx.topological_sort(graph):
        graph.node[n]['stream'] = db_friendly_node(graph.node[n]['stream'])
    db_graph = nx.node_link_data(graph)
    loaded_graph = nx.node_link_graph(db_graph)

    print('rebuild nodes')
    for n in nx.topological_sort(loaded_graph):
        loaded_graph.node[n]['stream'] = rebuild_node(
            loaded_graph.node[n]['stream'], loaded_graph)

    for n, attrs in loaded_graph.node.items():
        print(n)
        pprint(attrs)
    # '''
    td.cleanup()
