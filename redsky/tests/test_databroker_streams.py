from streams.core import Stream, no_default
from functools import partial

from redsky import event_streams as es
from redsky.savers import NpyWriter
from redsky.sinks import StoreSink
from redsky.event_streams import star, dstar

from pprint import pprint


def clean_databroker(doc):
    doc = dict(doc)
    for k2 in ['run_start', 'descriptor']:
        if k2 in doc.keys():
            doc[k2] = doc[k2]['uid']
    if '_name' in doc.keys():
        doc.pop('_name')
    return doc


def tuple_doc(doc):
    if isinstance(doc, dict):
        for k in doc.keys():
            doc[k] = tuple_doc(doc[k])
    elif isinstance(doc, list):
        return tuple(doc)
    else:
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

    for (n, d), (nn, dd) in zip(L, exp_db[-1].stream()):
        assert n == nn
        pprint(clean_databroker(dd))
        assert tuple_doc(d) == tuple_doc(clean_databroker(dd))
        if n == 'stop':
            assert d['exit_status'] == 'success'

    for (n, d), (nn, dd) in zip(L, exp_db[-1].stream(fill=True)):
        assert n == nn
        assert tuple_doc(d) == tuple_doc(clean_databroker(dd))
        if n == 'stop':
            assert d['exit_status'] == 'success'


def test_double_access(exp_db, start_uid1):
    source = Stream()
    source2 = Stream()

    dp = es.eventify(source, 'name',
                     output_info=[('name', {
                         'dtype': 'str',
                         'source': 'testing'})],
                     )
    dp.sink(star(StoreSink(db=exp_db)))
    L = dp.sink_to_list()
    ih1 = exp_db[start_uid1]
    for a in exp_db.restream(ih1, fill=True):
        source.emit(a)

    dp2 = es.eventify(source2, 'name',
                      output_info=[('name', {
                          'dtype': 'str',
                          'source': 'testing'})],
                      db=exp_db)
    L2 = dp2.sink_to_list()

    for b in exp_db.restream(ih1, fill=True):
        source2.emit(b)

    assert len(L) == len(L2)
    for l, ll in zip(L, L2):
        assert l == ll


def test_double_access_override(exp_db, start_uid1):
    source = Stream()
    source2 = Stream()

    dp = es.eventify(source, 'name',
                     output_info=[('name', {
                         'dtype': 'str',
                         'source': 'testing'})],
                     )
    dp.sink(star(StoreSink(db=exp_db)))
    L = dp.sink_to_list()
    ih1 = exp_db[start_uid1]
    for a in exp_db.restream(ih1, fill=True):
        source.emit(a)

    dp2 = es.eventify(source2, 'name',
                      output_info=[('name', {
                          'dtype': 'str',
                          'source': 'testing'})],
                      db=exp_db, override_db=True)
    L2 = dp2.sink_to_list()

    for b in exp_db.restream(ih1, fill=True):
        source2.emit(b)

    assert len(L) == len(L2)
    for l, ll in zip(L, L2):
        assert l != ll


def test_bundle_insert(exp_db, start_uid1, start_uid3, tmp_dir):
    source = Stream()
    source2 = Stream()

    s = es.bundle(source, source2)
    store_sink = StoreSink(
        db=exp_db,
        external_writers={'pe1_image': partial(NpyWriter, root=tmp_dir)})
    s.sink(star(store_sink))
    L = s.sink_to_list()

    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s1 = list(exp_db.restream(ih1))
    s2 = list(exp_db.restream(ih2))

    for b in s2:
        source2.emit(b)
    for a in s1:
        source.emit(a)
    for l, ll in zip(L, exp_db[-1].stream()):
        d = dict(ll[1])
        for k2 in ['run_start', 'descriptor']:
            if k2 in d.keys():
                d[k2] = d[k2]['uid']
        if '_name' in d.keys():
            d.pop('_name')
        assert l[1] == d


def test_bundle_access(exp_db, start_uid1, start_uid3, tmp_dir):
    source = Stream()
    source2 = Stream()

    s = es.bundle(source, source2, db=exp_db)
    store_sink = StoreSink(
        db=exp_db,
        external_writers={'pe1_image': partial(NpyWriter, root=tmp_dir)})
    s.sink(print)
    L = s.sink_to_list()
    s.sink(star(store_sink))

    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s1 = list(exp_db.restream(ih1))
    s2 = list(exp_db.restream(ih2))

    for b in s2:
        source2.emit(b)
    for a in s1:
        source.emit(a)

    assert len(L) == len(list(exp_db[-1].stream()))
    ll = len(L)

    for b in s2:
        source2.emit(b)
    for a in s1:
        source.emit(a)

    assert 2 * ll == len(L)
    for a, b in zip(L[:ll], L[ll:]):
        assert a == b
