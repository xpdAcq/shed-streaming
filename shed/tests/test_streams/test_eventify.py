from streamz.core import Stream
import numpy as np
import pytest

from shed import event_streams as es
from shed.event_streams import star
from shed.tests.utils import SinkAssertion
from shed.utils import to_event_model


def test_eventify(exp_db, start_uid1):
    source = Stream()

    dp = es.Eventify(source, 'name',
                     output_info=[('name', {
                         'dtype': 'str',
                         'source': 'testing'})])
    # try two outputs
    dp2 = es.Eventify(source, 'name', 'name',
                      output_info=[
                          ('name', {'dtype': 'str', 'source': 'testing'}),
                          ('name2', {'dtype': 'str', 'source': 'testing'})])

    # try without output_info, no keys
    dp3 = es.Eventify(source)

    dps = [dp, dp2, dp3]
    Ls = []
    for ddp in dps:
        Ls.append(ddp.sink_to_list())
        ddp.sink(star(SinkAssertion(False)))
        ddp.sink(print)

    ih1 = exp_db[start_uid1]
    s = list(exp_db.restream(ih1, fill=True))
    for _ in range(2):
        for LL in Ls:
            LL.clear()
        for a in s:
            source.emit(a)

        assert all(len(L) == 4 for L in Ls)
        assert_docs_l = [set() for _ in range(3)]
        # zip them since we know they're same length and order
        for l, l2, l3 in zip(*Ls):
            for i, ll in enumerate([l, l2, l3]):
                assert_docs_l[i].add(ll[0])
            if l[0] == 'event':
                assert l[1]['data']['name'] == 'test'
                assert l2[1]['data']['name'] == 'test'
                assert l2[1]['data']['name2'] == 'test'
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'
                assert l2[1]['exit_status'] == 'success'
            if l3[0] == 'descriptor':
                assert l3[1]['data_keys']['name'] == {}

        assert all(
            (set(assert_docs) == {'start', 'descriptor', 'event',
                                  'stop'} for assert_docs in assert_docs_l))


def test_eventify_descriptor():
    source = Stream()

    dp = es.Eventify(source, 'data_keys',
                     output_info=[('name', {
                         'dtype': 'str',
                         'source': 'testing'})],
                     document='descriptor')
    # try two outputs
    # dp2 = es.Eventify(source, 'data_keys', 'data_keys',
    #                   output_info=[
    #                       ('name', {'dtype': 'str', 'source': 'testing'}),
    #                       ('name2', {'dtype': 'str', 'source': 'testing'})],
    #                   document='descriptor')
    # dp3 = es.Eventify(source, document='descriptor')

    dps = [dp,
           # dp2, dp3
           ]
    Ls = []
    for ddp in dps:
        Ls.append(ddp.sink_to_list())
        ddp.sink(star(SinkAssertion(False)))

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(1)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for _ in range(2):
        for LL in Ls:
            LL.clear()
        for a in s:
            source.emit(a)

        dk = None
        for a in s:
            if a[0] == 'descriptor':
                dk = a[1]['data_keys']
            source.emit(a)

        print(_, [l[0] for l in Ls[0]])
        assert all([len(L) == 4 for L in Ls])
        assert_docs_l = [set() for _ in range(3)]
        # zip them since we know they're same length and order
        for l, l2, l3 in zip(*Ls):
            for i, ll in enumerate([l, l2, l3]):
                assert_docs_l[i].add(ll[0])
            if l[0] == 'event':
                assert l[1]['data']['name'] == dk
                assert l2[1]['data']['name'] == dk
                assert l2[1]['data']['name2'] == dk
                for k in set(dk.keys()) | set(l[1]['data'].keys()):
                    assert l3[1]['data'][k] == dk[k]
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'
                assert l2[1]['exit_status'] == 'success'
        assert all(
            (set(assert_docs) == {'start', 'descriptor', 'event',
                                  'stop'} for assert_docs in assert_docs_l))


def test_eventify_all_descriptor(exp_db, start_uid1):
    source = Stream()

    dp = es.Eventify(source, document='descriptor')
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))
    dp.sink(print)

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    dk = None
    for a in s:
        if a[0] == 'descriptor':
            dk = a[1]
        source.emit(a)

    assert len(L) == 4
    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'event':
            for k in set(dk.keys()) | set(l[1]['data'].keys()):
                assert l[1]['data'][k] == dk[k]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs

    L.clear()
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert len(L) == 4
    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'event':
            for k in set(dk.keys()) | set(l[1]['data'].keys()):
                assert l[1]['data'][k] == dk[k]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_eventify_no_event():
    source = Stream()

    dp = es.Eventify(source, 'source',
                     output_info=[('name', {
                         'dtype': 'str',
                         'source': 'testing'})])
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))
    dp.sink(print)

    for a in to_event_model([], output_info=[('img', {})]):
        source.emit(a)

    assert len(L) == 4
    assert_docs = set()
    # zip them since we know they're same length and order
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert l[1]['data']['name'] == 'to_event_model'
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs
