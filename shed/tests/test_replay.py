import operator as op
import time
import uuid

import networkx as nx
import numpy as np

from shed.replay import replay
from shed.translation import FromEventStream


def test_replay_export(db):
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
                             'seq_num': i + 1,
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
    g2 = g11_1.starmap(op.mul).map(np.log)
    g = g2.ToEventStream(('img2',))
    dbf = g.DBFriendly()
    dbf.starsink(db.insert)

    print('run experiment')
    for yy in y():
        db.insert(*yy)
        g11.update(yy)
        g1.update(yy)

    s1 = db[-1]['stop']
    print('replay experiment')
    rp = replay(db, db[-1], export=True)
    next(rp)
    next(rp)
    assert s1 != db[-1]['stop']


def test_replay(db):
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
                             'seq_num': i + 1,
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
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print('run experiment')
    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        g11.update(yy)
        g1.update(yy)

    print('replay experiment')
    rp = replay(db, db[-1])
    lg = next(rp)
    ts = list(nx.topological_sort(lg))
    l2 = lg.node[ts[-1]]['stream'].sink_to_list()
    next(rp)
    assert len(l1) == len(l2)
    assert len(l0) == len(l2)
    for nd1, nd2 in zip(l0, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == 'event':
            print()
            assert nd1[1]['data']['det_image']**2 == nd2[1]['data']['img2']
    for nd1, nd2 in zip(l1, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == 'event':
            print()
            assert nd1[1]['data']['img2'] == nd2[1]['data']['img2']
