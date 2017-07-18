from functools import partial

import numpy as np
from numpy.testing import assert_equal
from redsky import event_streams as es
from redsky.event_streams import star, dstar
from redsky.savers import NpyWriter
from redsky.sinks import StoreSink, StubStoreSink
from redsky.tests.utils import clean_databroker, tuple_doc
from streams.core import Stream


def test_map_databroker(exp_db, start_uid1, tmp_dir):
    source = Stream()

    def add5(img):
        return img + 5

    ii = {'img': 'pe1_image'}
    oi = [('img', {'dtype': 'array', 'source': 'testing'})]
    dp_stream = es.map(dstar(add5),
                       source,
                       input_info=ii,
                       output_info=oi)
    store_sink = StoreSink(
        db=exp_db,
        external_writers={'img': partial(NpyWriter, root=tmp_dir)})

    dp_stream.sink(star(store_sink))
    L = dp_stream.sink_to_list()

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for (n, d), (nn, dd) in zip(L, exp_db[-1].stream(fill=True)):
        assert_docs.add(n)
        if nn == 'event':
            assert dd['filled']
        assert n == nn
        assert_equal(tuple_doc(d), tuple_doc(clean_databroker(dd)))
        if n == 'stop':
            assert d['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_double_map(exp_db, start_uid1, tmp_dir):
    source = Stream()
    source2 = Stream()

    def add_imgs(img1, img2):
        return img1 + img2

    dp = es.map(dstar(add_imgs), es.zip(source, source2),
                input_info={'img1': ('pe1_image', 0),
                            'img2': ('pe1_image', 1)},
                output_info=[
                    ('img',
                     {'dtype': 'array',
                      'source': 'testing'})])
    store_sink = StoreSink(
        db=exp_db,
        external_writers={'img': partial(NpyWriter, root=tmp_dir)})

    dp.sink(star(store_sink))
    L = dp.sink_to_list()

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)
        source2.emit(a)

    assert_docs = set()
    for (n, d), (nn, dd) in zip(L, exp_db[-1].stream(fill=True)):
        assert_docs.add(n)
        if nn == 'event':
            assert dd['filled']
        assert n == nn
        assert_equal(tuple_doc(d), tuple_doc(clean_databroker(dd)))
        if n == 'stop':
            assert d['exit_status'] == 'success'


def test_filter(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return isinstance(img1, np.ndarray)

    dp = es.filter(f, source, input_info={'img1': 'pe1_image'})

    store_sink = StubStoreSink(db=exp_db)

    dp.sink(star(store_sink))
    L = dp.sink_to_list()

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for (n, d), (nn, dd) in zip(L, exp_db[-1].stream(fill=True)):
        assert_docs.add(n)
        if n not in ['descriptor', 'event']:
            assert n == nn
            assert tuple_doc(d) == tuple_doc(clean_databroker(dd))
            if n == 'stop':
                assert d['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs
