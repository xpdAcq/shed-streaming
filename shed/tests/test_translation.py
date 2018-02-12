import operator as op
import networkx as nx
import uuid

from streamz_ext import Stream

from shed.translation import (FromEventStream, ToEventStream,
                              walk_to_translation)
from shed.utils import to_event_model


def test_from_event_model():
    g = to_event_model(range(10), [('ct', {'units': 'arb'})])

    source = Stream()
    t = FromEventStream(source, 'event', ('data', 'ct'))
    L = t.sink_to_list()

    for gg in g:
        source.emit(gg)

    for i, ll in enumerate(L):
        assert i == ll


def test_from_event_model_stream_name():
    def data():
        suid = str(uuid.uuid4())
        duid = str(uuid.uuid4())
        yield 'start', {'hi': 'world', 'uid': suid}
        yield 'descriptor', {'name': 'hi', 'data_keys': {'ct'},
                             'uid': duid, 'run_start': suid}
        for i in range(10):
            yield 'event', {'uid': str(uuid.uuid4()),
                            'ct': {'data': i}, 'descriptor': duid}
        duid = str(uuid.uuid4())
        yield 'descriptor', {'name': 'not hi', 'data_keys': {'ct'},
                             'uid': duid, 'run_start': suid}
        for i in range(100, 110):
            yield 'event', {'uid': str(uuid.uuid4()),
                            'data': {'ct': i}, 'descriptor': duid}
        yield 'stop', {'uid': str(uuid.uuid4()), 'run_start': suid}

    g = data()
    source = Stream()
    t = FromEventStream(source, 'event', ('data', 'ct'),
                        event_stream_name='hi')
    L = t.sink_to_list()

    for gg in g:
        source.emit(gg)

    for i, ll in enumerate(L):
        assert i == ll


def test_from_event_model_stream_name2():
    def data():
        suid = str(uuid.uuid4())
        duid = str(uuid.uuid4())
        yield 'start', {'hi': 'world', 'uid': suid}
        yield 'descriptor', {'name': 'hi', 'data_keys': {'ct'},
                             'uid': duid, 'run_start': suid}
        for i in range(10):
            yield 'event', {'uid': str(uuid.uuid4()),
                            'ct': {'data': i}, 'descriptor': duid}
        duid = str(uuid.uuid4())
        yield 'descriptor', {'name': 'not hi', 'data_keys': {'ct'},
                             'uid': duid, 'run_start': suid}
        for i in range(100, 110):
            yield 'event', {'uid': str(uuid.uuid4()),
                            'data': {'ct': i}, 'descriptor': duid}
        yield 'stop', {'uid': str(uuid.uuid4()), 'run_start': suid}

    g = data()
    source = Stream()
    t = FromEventStream(source, 'event', ('data', 'ct'),
                        event_stream_name='not hi')
    L = t.sink_to_list()

    for gg in g:
        source.emit(gg)

    for i, ll in enumerate(L):
        assert i + 100 == ll


def test_walk_up():
    raw = Stream()
    a_translation = FromEventStream(raw, 'start', ('time',))
    b_translation = FromEventStream(raw, 'event', ('data', 'pe1_image'))

    d = b_translation.zip_latest(a_translation)
    dd = d.map(op.truediv)
    e = ToEventStream(dd, ('data',))

    g = nx.DiGraph()
    walk_to_translation(e, g)
    att = []
    for node, attrs in g.node.items():
        att.append(attrs['stream'])
    s = {a_translation, b_translation, d, dd, e}
    assert s == set(att)
    assert {hash(k) for k in s} == set(g.nodes)


def test_to_event_model():
    g = to_event_model(range(10), [('ct', {'units': 'arb'})])

    source = Stream()
    t = FromEventStream(source, 'event', ('data', 'ct'), principle=True)

    n = ToEventStream(t, ('ct',))
    p = n.pluck(0).sink_to_list()

    n.sink(print)
    for gg in g:
        source.emit(gg)

    assert set(p) == {'start', 'stop', 'event', 'descriptor'}
