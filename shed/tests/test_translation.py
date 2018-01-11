import operator as op
import networkx as nx

from streamz_ext import Stream

from shed.translation import FromEventStream, ToEventStream, walk_to_translation
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
