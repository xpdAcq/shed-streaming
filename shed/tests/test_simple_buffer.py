import pytest
from streamz.utils_test import gen_test
from tornado import gen

pytest.importorskip('streamz_ext.thread')

import time

from streamz_ext import Stream

from shed.simple import (
    SimpleFromEventStream as FromEventStream,
    SimpleToEventStream as ToEventStream,
)
from shed.utils import to_event_model


@gen_test()
def test_slow_to_event_model():
    """This doesn't use threads so it should be slower due to sleep"""
    def slow_inc(x):
        time.sleep(.5)
        return x + 1

    g = to_event_model(range(10), [("ct", {"units": "arb"})])

    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "ct"), source, principle=True)
    assert t.principle
    a = (t
         .map(slow_inc)
         )
    b = (a.buffer(100)
         .gather()
         )
    L = b.sink_to_list()
    futures_L = a.sink_to_list()
    n = ToEventStream(b, ("ct",))
    tt = t.sink_to_list()
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()
    t0 = time.time()
    for gg in g:
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 > .5 * 10

    assert tt
    assert p == ['start', 'descriptor'] + ['event'] * 10 + ['stop']
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}


@gen_test()
def test_to_event_model():
    def slow_inc(x):
        time.sleep(.5)
        return x + 1

    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "ct"), source, principle=True)
    assert t.principle
    a = (t
         .thread_scatter()
         .map(slow_inc)
         )
    b = (a.buffer(100)
         .gather()
         )
    L = b.sink_to_list()
    futures_L = a.sink_to_list()
    n = ToEventStream(b, ("ct",))
    tt = t.sink_to_list()
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()
    t0 = time.time()
    for gg in to_event_model(range(10), [("ct", {"units": "arb"})]):
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 < .5 * 10

    assert tt
    assert p == ['start', 'descriptor'] + ['event'] * 10 + ['stop']
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}

    for gg in to_event_model(range(100, 110), [("ct", {"units": "arb"})]):
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 < .5 * 10

    assert tt
    assert p == (['start', 'descriptor'] + ['event'] * 10 + ['stop'])*2
    assert d[14]["hints"] == {"analyzer": {"fields": ["ct"]}}
    for i, j in zip([0, 1, 12], [13, 14, 25]):
        assert p[i] == p[j]
        assert d[i] != d[j]


@gen_test()
def test_double_buffer_to_event_model():
    def slow_inc(x):
        time.sleep(.5)
        return x + 1

    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "ct"), source, principle=True)
    assert t.principle
    ts = (t
         .thread_scatter()
         )
    a = ts.map(slow_inc)
    aa = ts.map(slow_inc)
    b = (a.buffer(100)
         .gather()
         )
    bb = aa.buffer(100).gather()
    L = b.sink_to_list()
    futures_L = a.sink_to_list()
    n = ToEventStream(b.zip(bb), ("ct",))
    assert len(n.buffers) == 2
    tt = t.sink_to_list()
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()
    t0 = time.time()
    for gg in to_event_model(range(10), [("ct", {"units": "arb"})]):
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 < .5 * 10
    print(d)

    assert tt
    assert p == ['start', 'descriptor'] + ['event'] * 10 + ['stop']
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}

    for gg in to_event_model(range(100, 110), [("ct", {"units": "arb"})]):
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 < .5 * 10

    assert tt
    assert p == (['start', 'descriptor'] + ['event'] * 10 + ['stop'])*2
    assert d[14]["hints"] == {"analyzer": {"fields": ["ct"]}}
    for i, j in zip([0, 1, 12], [13, 14, 25]):
        assert p[i] == p[j]
        assert d[i] != d[j]
