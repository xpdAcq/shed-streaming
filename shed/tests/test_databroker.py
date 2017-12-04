import numpy as np

from numpy.testing import assert_equal
from streamz import Stream

from shed.databroker import AssetInsert
from shed.savers import NpyWriter
from shed.utils import to_event_model


def test_AssetInsert(db, tmp_dir):
    g = list(to_event_model([np.ones((5, 5)) for i in range(5)],
                            [('det', {'dtype': 'int'})]))

    source = Stream()
    l1 = source.sink_to_list()
    ai = AssetInsert(source, db.fs, tmp_dir,
                     external_writers={'det': NpyWriter})
    l2 = ai.sink_to_list()
    ai.sink(lambda x: db.insert(*x))

    for gg in g:
        source.emit(gg)

    ret = db[-1].documents()
    ret_fill = db[-1].documents(fill=True)
    for r, ll in zip(ret, l2):
        name, doc = r
        doc.pop('filled', None)
        doc.pop('_name', None)
        assert_equal(r, ll)
    for r, ll in zip(ret_fill, l1):
        name, doc = r
        if name == 'descriptor':
            for v in doc['data_keys'].values():
                assert v.pop('external', None)
        if name == 'event':
            for k in doc['filled']:
                doc['filled'][k] = True
        assert_equal(r, ll)
