import numpy as np
import pytest
from numpy.testing import assert_allclose
from streamz import Stream

from shed import event_streams as es
from shed.event_streams import star
from shed.tests.utils import SinkAssertion
from shed.utils import to_event_model

source = Stream()


def test_filter():
    def f(img1):
        return img1 is not None

    dp = es.filter(f, source,
                   input_info={'img1': 'pe1_image'}, stream_name='test')
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ins = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))

    for _ in range(2):
        L.clear()
        for a in ins:
            source.emit(a)
        assert len(L) == len(ins)
        assert_docs = set()
        for l, s in zip(L, ins):
            assert_docs.add(l[0])
            if l[0] == 'event':
                assert_allclose(l[1]['data']['pe1_image'],
                                s[1]['data']['pe1_image'])
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'
        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


def test_filter_descriptor(exp_db, start_uid1):
    source = Stream()

    def f(d):
        return d[0]['name'] == 'primary'

    dp = es.filter(f, source,
                   stream_name='test',
                   document_name='descriptor')
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1] != s[1]
        if l[0] == 'descriptor':
            assert l[1] == s[1]
        if l[0] == 'event':
            assert_allclose(l[1]['data']['pe1_image'],
                            s[1]['data']['pe1_image'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs

    L.clear()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1] != s[1]
        if l[0] == 'descriptor':
            assert l[1] == s[1]
        if l[0] == 'event':
            assert_allclose(l[1]['data']['pe1_image'],
                            s[1]['data']['pe1_image'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_filter_descriptor_negative(exp_db, start_uid1):
    source = Stream()

    def f(d):
        tv = d[0]['name'] != 'primary'
        return tv

    dp = es.filter(f, source,
                   stream_name='test',
                   document_name='descriptor')
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False, expected_docs={'start', 'stop'})))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1] != s[1]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    assert {'start', 'stop'} == assert_docs

    L.clear()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1] != s[1]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    assert {'start', 'stop'} == assert_docs


def test_filter_full_header(exp_db, start_uid1):
    source = Stream()

    def f(d):
        return d['sample_name'] != 'kapton'

    def g(d):
        return d['sample_name'] == 'kapton'

    dp = es.filter(f, source, input_info={0: ()}, document_name='start',
                   full_event=True)
    dp2 = es.filter(g, source, input_info={0: ()}, document_name='start',
                    full_event=True)

    L = dp.sink_to_list()
    L2 = dp2.sink_to_list()

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert L == []

    assert L2 != []


def test_filter_args_kwargs(exp_db, start_uid1):
    source = Stream()

    def f(img1, a):
        return img1 is not None and a == 1

    dp = es.filter(f, source,
                   input_info={0: 'pe1_image'}, a=1)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['pe1_image'],
                            s[1]['data']['pe1_image'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_filter_args(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return img1 is not None

    dp = es.filter(f, source,
                   input_info={0: 'pe1_image'})
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['pe1_image'],
                            s[1]['data']['pe1_image'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


@pytest.mark.xfail(raises=TypeError)
def test_filter_fail(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return img1 is not None

    dp = es.filter(f, source,
                   input_info={'i': 'pe1_image'}, raise_upon_error=True)
    dp.sink(star(SinkAssertion()))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)


def test_filter_fail_no_except(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return img1 is not None

    dp = es.filter(f, source,
                   input_info={'img1': 'no_such_key'},
                   document_name='start', full_event=True,
                   raise_upon_error=False)
    dp.sink(star(SinkAssertion(expected_docs={'start', 'stop'})))
    L = dp.sink_to_list()

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)
    for n, d in L:
        if n == 'stop':
            assert d['exit_status'] == 'failure'


def test_filter_full_event(exp_db, start_uid1):
    source = Stream()

    def f(i):
        return i > 1

    dp = es.filter(f, source,
                   input_info={'i': 'seq_num'},
                   full_event=True)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert l[1]['seq_num'] > 1
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs
