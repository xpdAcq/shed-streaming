from shed.translation import FromEventStream
import uuid
from pprint import pprint
from databroker import Broker
import time
import operator as op
import importlib
import networkx as nx
from shed.translation import ToEventStream


def replay(db, hdr, export=False):
    data = {}
    parent_nodes = {}
    # TODO: try either raw or analysis db (or stash something to know who comes
    # from where)

    raw_hdrs = [db[u] for u in hdr['start']['parent_uids'].values()]
    # load data from raw/partially analyzed headers
    for raw_hdr in raw_hdrs:
        data.update({d['uid']: (n, d) for n, d in raw_hdr.documents()})

    # get information from old analyzed header
    times = hdr['stop']['times']
    graph = hdr['start']['graph']

    loaded_graph = nx.node_link_graph(graph)
    for n in nx.topological_sort(loaded_graph):
        loaded_graph.node[n]['stream'] = rebuild_node(
            loaded_graph.node[n]['stream'], loaded_graph)

    if export:
        for n, attrs in loaded_graph.node.items():
            if isinstance(attrs['stream'], ToEventStream):
                attrs['stream'].DBFriendly().starsink(db.insert)

    for node_uid in hdr['start']['parent_uids']:
        parent_nodes[node_uid] = loaded_graph.node[node_uid]['stream']
    yield loaded_graph
    vs = sorted([(t, v) for t, v in times.items()], key=lambda x: x[0])
    vs = [v for t, v in vs]

    # push the data through the pipeline
    for v in vs:
        # TODO: fill the event from the databroker
        # print(parent_nodes[v['node']], data[v['uid']])
        parent_nodes[v['node']].update(data[v['uid']])
    yield 'Done'


def rebuild_node(node_dict, graph):
    d = dict(node_dict)
    node = getattr(importlib.import_module(d['mod']), d['name'])
    d.pop('name')
    d.pop('mod')
    for f_name in ['func', 'predicate']:
        if f_name in d:
            idx = d['args'].index(d[f_name])
            d[f_name] = getattr(importlib.import_module(d[f_name]['mod']),
                                d[f_name]['name'])
            d['args'][idx] = d[f_name]
    for upstream in d['upstreams']:
        if upstream in d['args']:
            d['args'][d['args'].index(upstream)] = graph.node[upstream][
                'stream']
    return node(*d['args'], **d['kwargs'])


if __name__ == '__main__':
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
        for i in range(5):
            yield ('event', {'uid': str(uuid.uuid4()),
                             'data': {'det_image': i},
                             'timestamps': {'det_image': time.time()},
                             'seq_num': i+1,
                             'time': time.time(),
                             'descriptor': duid})
        yield ('stop', {'uid': str(uuid.uuid4()),
                        'time': time.time(),
                        'run_start': suid})


    print('build graph')
    g1 = FromEventStream('event', ('data', 'det_image',), principle=True,
                         stream_name='g1')
    g11 = FromEventStream('event', ('data', 'det_image',),
                          stream_name='g11')
    g11_1 = g1.zip(g11)
    g2 = g11_1.starmap(op.mul)
    g = g2.ToEventStream(('img2',))
    dbf = g.DBFriendly()
    l = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print('run experiment')
    for yy in y():
        db.insert(*yy)
        g11.update(yy)
        g1.update(yy)

    print(db[-1]['stop'])
    print('replay experiment')
    rp = replay(db, db[-1], export=True)
    lg = next(rp)
    ts = list(nx.topological_sort(lg))
    lg.node[ts[-1]]['stream'].sink(pprint)
    next(rp)
    print(db[-1]['stop'])
    # '''
