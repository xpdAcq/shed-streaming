from streamz.core import Stream
import numpy as np
import pytest

from shed import event_streams as es
from shed.event_streams import star
from shed.tests.utils import SinkAssertion
from shed.utils import to_event_model

s1 = list(to_event_model(
    [np.random.random((10, 10)) for _ in range(5)],
    output_info=[('pe1_image', {'dtype': 'array'})],
    md={'name': 'test'}
))

s2 = list(to_event_model([], output_info=[('pe1_image', {'dtype': 'array'})],
                         md={'name': 'test'}))


@pytest.mark.parametrize('s', [s1, s2])
def test_eventify_single(s):
    source = Stream()

    dp = es.Eventify(source, 'name',
                     output_info=[('name', {
                         'dtype': 'str',
                         'source': 'testing'})])
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))
    dp.sink(print)

    for _ in range(2):
        L.clear()
        for a in s:
            source.emit(a)

        assert len(L) == 4
        assert_docs = set()
        # zip them since we know they're same length and order
        for l in L:
            assert_docs.add(l[0])
            if l[0] == 'event':
                assert l[1]['data']['name'] == 'test'
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'

        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


@pytest.mark.parametrize('s', [s1, s2])
def test_eventify_double(s):
    source = Stream()

    # try two outputs
    dp = es.Eventify(source, 'name', 'name',
                     output_info=[
                         ('name', {'dtype': 'str', 'source': 'testing'}),
                         ('name2', {'dtype': 'str', 'source': 'testing'})])

    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))
    dp.sink(print)

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})],
        md={'name': 'test'}
    ))
    for _ in range(2):
        L.clear()
        for a in s:
            source.emit(a)

        assert len(L) == 4
        assert_docs = set()
        # zip them since we know they're same length and order
        for l in L:
            assert_docs.add(l[0])
            if l[0] == 'event':
                assert l[1]['data']['name'] == s[0][1]['name']
                assert l[1]['data']['name2'] == s[0][1]['name']
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'

        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


@pytest.mark.parametrize('s', [s1, s2])
def test_eventify_all(s):
    source = Stream()

    # try without output_info, no keys
    dp = es.Eventify(source)

    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))
    dp.sink(print)

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})],
        md={'name': 'test'}
    ))
    for _ in range(2):
        L.clear()
        for a in s:
            source.emit(a)

        assert len(L) == 4
        assert_docs = set()
        # zip them since we know they're same length and order
        for l, ll in zip(L, s):
            assert_docs.add(l[0])
            if l[0] == 'descriptor':
                assert l[1]['data_keys']['name'] == {}
            if l[0] == 'event':
                assert l[1]['data'] == s[0][1]
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'

        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


@pytest.mark.parametrize('s', [s1, s2])
def test_eventify_descriptor_single(s):
    source = Stream()

    dp = es.Eventify(source, 'data_keys',
                     output_info=[('name', {
                         'dtype': 'str',
                         'source': 'testing'})],
                     document='descriptor',
                     stream_name='eventify')

    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(2)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for _ in range(2):
        L.clear()
        dk = None
        for a in s:
            if a[0] == 'descriptor':
                dk = a[1]['data_keys']
            source.emit(a)

        assert len(L) == 4
        assert_docs = set()
        # zip them since we know they're same length and order
        for l in L:
            assert_docs.add(l[0])
            if l[0] == 'event':
                assert l[1]['data']['name'] == dk
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'
        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


@pytest.mark.parametrize('s', [s1, s2])
def test_eventify_descriptor_double(s):
    source = Stream()

    # try two outputs
    dp = es.Eventify(source, 'data_keys', 'data_keys',
                     output_info=[
                         ('name', {'dtype': 'str', 'source': 'testing'}),
                         ('name2', {'dtype': 'str', 'source': 'testing'})],
                     document='descriptor')

    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(2)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for _ in range(2):
        L.clear()
        dk = None
        for a in s:
            if a[0] == 'descriptor':
                dk = a[1]['data_keys']
            source.emit(a)

        assert len(L) == 4
        assert_docs = set()
        # zip them since we know they're same length and order
        for l in L:
            assert_docs.add(l[0])
            if l[0] == 'event':
                assert l[1]['data']['name'] == dk
                assert l[1]['data']['name2'] == dk
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'
        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


@pytest.mark.parametrize('s', [s1, s2])
def test_eventify_descriptor_all(s):
    source = Stream()

    dp = es.Eventify(source, document='descriptor')

    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(2)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for _ in range(2):
        L.clear()
        dk = None
        for a in s:
            if a[0] == 'descriptor':
                dk = a[1]
            source.emit(a)

        assert len(L) == 4
        assert_docs = set()
        # zip them since we know they're same length and order
        for l in L:
            assert_docs.add(l[0])
            if l[0] == 'event':
                print(dk)
                print(l[1]['data'])
                for k in set(dk.keys()) | set(l[1]['data'].keys()):
                    assert l[1]['data'][k] == dk[k]
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'
        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


@pytest.mark.parametrize('s', [s1, s2])
def test_eventify_no_event(s):
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
