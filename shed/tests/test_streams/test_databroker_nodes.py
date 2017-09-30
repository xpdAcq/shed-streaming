import pytest
from numpy.testing import assert_equal
from shed import event_streams as es
from streamz import Stream


def test_query(exp_db, start_uid1):
    source = es.EventStream()

    def qf(db, docs):
        return db(uid=docs[0]['uid'])

    hdr = exp_db[start_uid1]
    s = hdr.documents()

    dp = es.Query(exp_db, source, qf,
                  query_decider=lambda x, y: [next(iter(x))])
    L = dp.sink_to_list()

    dp2 = es.QueryUnpacker(exp_db, dp)
    L2 = dp2.sink_to_list()

    for a in s:
        source.emit(a)

    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert l[1]['data']['hdr_uid'] == start_uid1
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs

    assert_docs = set()
    for l, ll in zip(L2, hdr.documents()):
        assert_docs.add(l[0])
        assert l[0] == ll[0]
        if l[0] is 'start':
            assert l[1] == ll[1]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_empty_query(exp_db, start_uid1):
    source = es.EventStream()

    def qf(db, docs):
        return db(hello='world')

    def qd(res, docs):
        try:
            rv = [next(iter(res))]
        except StopIteration:
            rv = []
        return rv

    hdr = exp_db[start_uid1]
    s = hdr.documents()

    dp = es.Query(exp_db, source, qf,
                  query_decider=qd)
    L = dp.sink_to_list()

    dp2 = es.QueryUnpacker(exp_db, dp)
    L2 = dp2.sink_to_list()

    for a in s:
        source.emit(a)

    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1]['n_hdrs'] == 0
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'stop']:
        assert n in assert_docs
    assert len(L2) == 0


def test_query_many_headers(exp_db):
    source = es.EventStream()

    def qf(db, docs):
        return db(sc_dk_field_uid={'$exists': True})

    s = [('start', None)]

    dp = es.Query(exp_db, source, qf)
    L = dp.sink_to_list()

    dp2 = es.QueryUnpacker(exp_db, dp)
    dp2.sink(print)
    L2 = dp2.sink_to_list()

    for a in s:
        source.emit(a)

    assert len(L) == 6
    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1]['n_hdrs'] == 3
        if l[0] == 'event':
            assert l[1]['data']['hdr_uid'] in list(d['start']['uid'] for d in
                                                   qf(exp_db, 'hi'))
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs

    assert_docs = set()
    assert len(L2) == 3 * 3 + 5 + 5 + 2
    for l in L2:
        assert_docs.add(l[0])
        assert l[0]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


@pytest.mark.xfail(raises=RuntimeError)
def test_query_too_many_headers(exp_db):
    source = es.EventStream()

    def qf(db, docs):
        return db(sc_dk_field_uid={'$exists': True})

    s = [('start', None)]

    q = es.Query(exp_db, source, qf, max_n_hdrs=1)

    for a in s:
        source.emit(a)


def test_fill_events(exp_db, start_uid1):
    source = Stream()
    dp = es.fill_events(exp_db, source)
    L = dp.sink_to_list()

    h1 = exp_db[start_uid1]
    for s in h1.documents():
        source.emit(s)

    for a, b in zip(L, h1.documents(fill=True)):
        assert_equal(a[1], b[1])


def test_bundle_single_stream(exp_db):
    source = es.EventStream()

    def qf(db, docs):
        return db(sc_dk_field_uid={'$exists': True})

    s = [('start', {})]

    dp = es.Query(exp_db, source, qf)

    dp2 = es.QueryUnpacker(exp_db, dp)

    dpf = es.BundleSingleStream(dp2, dp)

    L = dpf.sink_to_list()
    dpf.sink(print)

    for a in s:
        source.emit(a)

    assert_docs = set()
    assert len(L) == 3 + 5 + 5 + 2
    for l in L:
        assert_docs.add(l[0])
        assert l[0]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_bundle_single_stream_control_int(exp_db):
    source = es.EventStream()

    def qf(db, docs):
        return db(sc_dk_field_uid={'$exists': True})

    s = [('start', {})]

    dp = es.Query(exp_db, source, qf)

    dp2 = es.QueryUnpacker(exp_db, dp)

    dpf = es.BundleSingleStream(dp2, 2)

    L = dpf.sink_to_list()
    dpf.sink(print)

    for a in s:
        source.emit(a)

    assert_docs = set()
    assert len(L) == 3 + 5 + 2
    for l in L:
        assert_docs.add(l[0])
        assert l[0]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs
