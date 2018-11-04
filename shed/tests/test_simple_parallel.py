from rapidz.utils_test import gen_test
from tornado import gen

import time

from shed.simple_parallel import SimpleFromEventStream
from shed.simple import (
    SimpleFromEventStream as FromEventStream,
    SimpleToEventStream as ToEventStream,
)
from rapidz import Stream

from shed.tests.utils import y, slow_inc, slow_filter0, slow_filter1


from distributed.utils_test import gen_cluster  # flake8: noqa


@gen_test(20)
def test_slow_to_event_model():
    """This doesn't use threads so it should be slower due to sleep"""

    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "det_image"), source, principle=True)
    assert t.principle
    a = t.map(slow_inc)
    L = a.sink_to_list()
    futures_L = a.sink_to_list()
    n = a.SimpleToEventStream(("ct",))
    n.sink(print)
    tt = t.sink_to_list()
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()
    t0 = time.time()
    for gg in y(10):
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    td = t1 - t0
    ted = .5 * 10
    assert td > ted

    assert tt
    assert p == ["start", "descriptor"] + ["event"] * 10 + ["stop"]
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}


@gen_test(20)
def test_slow_to_event_model_filter():
    """This doesn't use threads so it should be slower due to sleep"""

    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "det_image"), source, principle=True)
    assert t.principle
    a = t.filter(slow_filter0)
    b = t.filter(slow_filter1)

    futures_L = source.sink_to_list()
    n = a.SimpleToEventStream(("ct",))
    nn = b.SimpleToEventStream(("ct",))
    L = n.sink_to_list()
    LL = nn.sink_to_list()
    n.sink(print)
    nn.sink(print)
    tt = t.sink_to_list()
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()
    pp = nn.pluck(0).sink_to_list()
    dd = nn.pluck(1).sink_to_list()
    t0 = time.time()
    for gg in y(10):
        yield source.emit(gg)
    while (len(L) + len(LL)) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    td = t1 - t0
    ted = .5 * 10 * 2
    assert td > ted

    assert tt
    assert p == ["start", "descriptor"] + ["event"] * 5 + ["stop"]
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}
    assert pp == ["start", "descriptor"] + ["event"] * 5 + ["stop"]
    assert dd[1]["hints"] == {"analyzer": {"fields": ["ct"]}}


@gen_test(40)
def test_double_buffer_to_event_model():
    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "det_image"), source, principle=True)
    assert t.principle
    ts = t
    a = ts.map(slow_inc)
    aa = ts.map(slow_inc)
    n = a.zip(aa).SimpleToEventStream(("ct",))

    b = n
    b.sink(print)
    L = b.sink_to_list()

    futures_L = []

    tt = t.sink_to_list()
    p = b.pluck(0).sink_to_list()
    d = b.pluck(1).sink_to_list()
    t0 = time.time()
    for gg in y(10):
        futures_L.append(gg)
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 > .5 * 10

    assert tt
    assert p == ["start", "descriptor"] + ["event"] * 10 + ["stop"]
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}

    t0 = time.time()
    for gg in y(10):
        futures_L.append(gg)
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    print(len(L), len(futures_L))
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 > .5 * 10

    assert tt
    assert p == (["start", "descriptor"] + ["event"] * 10 + ["stop"]) * 2
    assert d[14]["hints"] == {"analyzer": {"fields": ["ct"]}}
    for i, j in zip([0, 1, 12], [13, 14, 25]):
        assert p[i] == p[j]
        assert d[i] != d[j]


@gen_test()
def test_slow_to_event_model_parallel():
    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "det_image"), source, principle=True)
    assert t.principle
    ts = t.scatter(backend="thread")
    # futures_L = t.sink_to_list()
    a = ts.map(slow_inc)
    n = a.SimpleToEventStream(("ct",))

    b = n.buffer(100).gather()
    b.sink(print)
    L = b.sink_to_list()

    tt = t.sink_to_list()
    p = b.pluck(0).sink_to_list()
    d = b.pluck(1).sink_to_list()

    t0 = time.time()
    futures_L = []
    for gg in y(10):
        futures_L.append(gg)
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    td = t1 - t0
    ted = .5 * 10
    assert td < ted

    assert tt
    assert p == ["start", "descriptor"] + ["event"] * 10 + ["stop"]
    assert "uid" in d[0]
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}
    assert d[1]["data_keys"]["ct"]["dtype"] == "number"
    assert d[2]["data"]["ct"] == 2


@gen_test()
def test_double_buffer_to_event_model_parallel():
    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "det_image"), source, principle=True)
    assert t.principle
    ts = t.scatter(backend="thread")
    a = ts.map(slow_inc)
    aa = ts.map(slow_inc)
    n = a.zip(aa).SimpleToEventStream(("ct",))

    b = n.buffer(100).gather()
    b.sink(print)
    L = b.sink_to_list()

    futures_L = []

    tt = t.sink_to_list()
    p = b.pluck(0).sink_to_list()
    d = b.pluck(1).sink_to_list()
    t0 = time.time()
    for gg in y(10):
        futures_L.append(gg)
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 < .5 * 10

    assert tt
    assert p == ["start", "descriptor"] + ["event"] * 10 + ["stop"]
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}

    for gg in y(10):
        futures_L.append(gg)
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    print(len(L), len(futures_L))
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 < .5 * 10

    assert tt
    assert p == (["start", "descriptor"] + ["event"] * 10 + ["stop"]) * 2
    assert d[14]["hints"] == {"analyzer": {"fields": ["ct"]}}
    for i, j in zip([0, 1, 12], [13, 14, 25]):
        assert p[i] == p[j]
        assert d[i] != d[j]


@gen_cluster(client=True)
def test_slow_to_event_model_parallel_dask(c, s, a, b):
    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "det_image"), source, principle=True)
    assert t.principle
    ts = t.scatter(backend="dask")
    # futures_L = t.sink_to_list()
    a = ts.map(slow_inc)
    n = a.SimpleToEventStream(("ct",))

    b = n.buffer(100).gather()
    b.sink(print)
    L = b.sink_to_list()

    tt = t.sink_to_list()
    p = b.pluck(0).sink_to_list()
    d = b.pluck(1).sink_to_list()

    t0 = time.time()
    futures_L = []
    for gg in y(10):
        futures_L.append(gg)
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    td = t1 - t0
    ted = .5 * 10
    assert td < ted

    assert tt
    assert p == ["start", "descriptor"] + ["event"] * 10 + ["stop"]
    assert "uid" in d[0]
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}
    assert d[1]["data_keys"]["ct"]["dtype"] == "number"
    assert d[2]["data"]["ct"] == 2


@gen_cluster(client=True)
def test_double_buffer_to_event_model_parallel_dask(c, s, a, b):
    source = Stream(asynchronous=True)
    t = FromEventStream("event", ("data", "det_image"), source, principle=True)
    assert t.principle
    ts = t.scatter(backend="dask")
    a = ts.map(slow_inc)
    aa = ts.map(slow_inc)
    n = a.zip(aa).SimpleToEventStream(("ct",))

    b = n.buffer(100).gather()
    b.sink(print)
    L = b.sink_to_list()

    futures_L = []

    tt = t.sink_to_list()
    p = b.pluck(0).sink_to_list()
    d = b.pluck(1).sink_to_list()
    t0 = time.time()
    for gg in y(10):
        futures_L.append(gg)
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 < .5 * 10

    assert tt
    assert p == ["start", "descriptor"] + ["event"] * 10 + ["stop"]
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}

    for gg in y(10):
        futures_L.append(gg)
        yield source.emit(gg)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)
    print(len(L), len(futures_L))
    t1 = time.time()
    # check that this was faster than running in series
    assert t1 - t0 < .5 * 10

    assert tt
    assert p == (["start", "descriptor"] + ["event"] * 10 + ["stop"]) * 2
    assert d[14]["hints"] == {"analyzer": {"fields": ["ct"]}}
    for i, j in zip([0, 1, 12], [13, 14, 25]):
        assert p[i] == p[j]
        assert d[i] != d[j]
