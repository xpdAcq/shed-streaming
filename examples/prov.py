import operator as op
import time
import uuid
from pprint import pprint

import networkx as nx
from databroker import Broker
from shed.translation import FromEventStream
from shed.replay import replay

if __name__ == '__main__':
    db_path = '/home/christopher/ldrd_demo'
    config = {'description': 'lightweight personal database',
              'metadatastore': {'module': 'databroker.headersource.sqlite',
                                'class': 'MDS',
                                'config': {'directory': db_path,
                                           'timezone': 'US/Eastern'}},
              'assets': {'module': 'databroker.assets.sqlite',
                         'class': 'Registry',
                         'config': {'dbpath': db_path + '/database.sql'}}}
    db = Broker.from_config(config)


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
