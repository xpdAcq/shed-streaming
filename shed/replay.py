from streamz_ext import Stream, map
from shed.translation import FromEventStream, ToEventStream
from shed.databroker_tools import GraphInsert
from shed.savers import GraphWriter
import uuid
from pprint import pprint
from databroker import Header
from databroker import Broker
import time
import operator as op


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
    # from where

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


    s = Stream()
    g1 = FromEventStream(s, 'event',
                         ('data', 'det_image',),
                         principle=True)
    g2 = g1.map(op.mul, 2)
    g = g2.ToEventStream(('img2',))
    l = g.sink_to_list()

    d = dict(g2.__dict__)
    d['name'] = g2.__class__.__name__
    d['mod'] = g2.__module__
    import importlib
    imp = importlib.import_module(d['mod'])
    c = getattr(imp, d['name'])
    d.pop('name')
    d.pop('mod')
    if len(d['upstreams']) == 1:
        d['upstream'] = d['upstreams'][0]
    print(d)
    print(c(**d))

    # d['upstreams'] = [hash(u) for u in d['upstreams']]
    # d['downstreams'] = [hash(u) for u in d['downstreams']]
    # print(d)
    # xyz = g.GraphInsert(db.fs, td.name)
    # xyz.sink(pprint)
    # xyz.sink(db.insert)

    for yy in y():
        db.insert(*yy)
        s.emit(yy)
    # gw = GraphWriter(db.fs, td.name)
    # graph = l[0][1]['graph']
    # gi = GraphInsert(g, db.fs, td.name)
    # gi.start(l[0][1])
    # print(gi.graph is graph)
    # print(gw.write(graph))
    # gw.write(gi.graph)

    # print(gw.write(graph))
    # import os
    # import dill
    # n = os.path.join(td.name, 'graph.pkl')
    # with open(n, 'wb') as f:
    #     dill.dump(graph, f)
    # del graph
    # with open(n, 'rb') as f:
    #     graph = dill.load(f)
    # for node, attrs in graph.node.items():
    #     print(attrs)

    # analyzed_header = db[-1]
    # raw_headers = [db[uid] for uid in analyzed_header['start']['parent_uids'].values()]
    # replumb_data(analyzed_header, raw_headers)
    #
    # '''
    td.cleanup()
