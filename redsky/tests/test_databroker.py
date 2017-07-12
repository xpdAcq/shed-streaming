from streams.core import Stream, no_default
from functools import partial

from redsky import event_streams as es
from redsky.savers import NpyWriter
from redsky.sinks import StoreSink, StubStoreSink
from redsky.event_streams import star, dstar

from numpy.testing import assert_equal
import numpy as np


def clean_databroker(doc):
    doc = dict(doc)
    for k2 in ['run_start', 'descriptor']:
        if k2 in doc.keys():
            doc[k2] = doc[k2]['uid']
    if '_name' in doc.keys():
        doc.pop('_name')
    if 'data_keys' in doc.keys():
        doc['data_keys'] = dict(doc['data_keys'])
        for k, v in doc['data_keys'].items():
            doc['data_keys'][k] = dict(doc['data_keys'][k])
            if 'external' in v.keys():
                doc['data_keys'][k].pop('external')
    return doc


def tuple_doc(doc):
    if isinstance(doc, dict):
        for k in doc.keys():
            doc[k] = tuple_doc(doc[k])
    elif isinstance(doc, list):
        for i in doc:
            if isinstance(i, list):
                doc[doc.index(i)] = tuple(i)
        return tuple(doc)
    else:
        return doc
    return doc


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

    for (n, d), (nn, dd) in zip(L, exp_db[-1].stream(fill=True)):
        if nn == 'event':
            assert dd['filled']
        assert n == nn
        assert_equal(tuple_doc(d), tuple_doc(clean_databroker(dd)))
        if n == 'stop':
            assert d['exit_status'] == 'success'


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

    for (n, d), (nn, dd) in zip(L, exp_db[-1].stream(fill=True)):
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

    for (n, d), (nn, dd) in zip(L, exp_db[-1].stream(fill=True)):
        if n not in ['descriptor', 'event']:
            assert n == nn
            assert tuple_doc(d) == tuple_doc(clean_databroker(dd))
            if n == 'stop':
                assert d['exit_status'] == 'success'
