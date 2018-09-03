import networkx as nx
import operator as op
import time
import uuid

from streamz_ext import Stream

from shed.simple import (
    SimpleFromEventStream as FromEventStream,
    SimpleToEventStream as ToEventStream,
    walk_to_translation,
    _hash_or_uid,
)
from shed.utils import to_event_model


def test_from_event_model():
    g = to_event_model(range(10), [("ct", {"units": "arb"})])

    source = Stream()
    t = FromEventStream("event", ("data", "ct"), source)
    L = t.sink_to_list()

    for gg in g:
        source.emit(gg)

    assert len(L) == 10
    for i, ll in enumerate(L):
        assert i == ll


def test_from_event_model_stream_name():
    def data():
        suid = str(uuid.uuid4())
        duid = str(uuid.uuid4())
        yield "start", {"hi": "world", "uid": suid}
        yield "descriptor", {
            "name": "hi",
            "data_keys": {"ct"},
            "uid": duid,
            "run_start": suid,
        }
        for i in range(10):
            yield "event", {
                "uid": str(uuid.uuid4()),
                "data": {"ct": i},
                "descriptor": duid,
            }
        duid = str(uuid.uuid4())
        yield "descriptor", {
            "name": "not hi",
            "data_keys": {"ct"},
            "uid": duid,
            "run_start": suid,
        }
        for i in range(100, 110):
            yield "event", {
                "uid": str(uuid.uuid4()),
                "data": {"ct": i},
                "descriptor": duid,
            }
        yield "stop", {"uid": str(uuid.uuid4()), "run_start": suid}

    g = data()
    source = Stream()
    t = FromEventStream(
        "event", ("data", "ct"), source, event_stream_name="hi"
    )
    L = t.sink_to_list()

    for gg in g:
        source.emit(gg)

    assert len(L) == 10
    for i, ll in enumerate(L):
        assert i == ll


def test_from_event_model_stream_name2():
    def data():
        suid = str(uuid.uuid4())
        duid = str(uuid.uuid4())
        yield "start", {"hi": "world", "uid": suid}
        yield "descriptor", {
            "name": "hi",
            "data_keys": {"ct"},
            "uid": duid,
            "run_start": suid,
        }
        for i in range(10):
            yield "event", {
                "uid": str(uuid.uuid4()),
                "data": {"ct": i},
                "descriptor": duid,
            }
        duid = str(uuid.uuid4())
        yield "descriptor", {
            "name": "not hi",
            "data_keys": {"ct"},
            "uid": duid,
            "run_start": suid,
        }
        for i in range(100, 110):
            yield "event", {
                "uid": str(uuid.uuid4()),
                "data": {"ct": i},
                "descriptor": duid,
            }
        yield "stop", {"uid": str(uuid.uuid4()), "run_start": suid}

    g = data()
    source = Stream()
    t = FromEventStream(
        "event", ("data", "ct"), source, event_stream_name="not hi"
    )
    L = t.sink_to_list()

    for gg in g:
        source.emit(gg)

    assert len(L) == 10
    for i, ll in enumerate(L):
        assert i + 100 == ll


def test_walk_up():
    raw = Stream()
    a_translation = FromEventStream("start", ("time",), raw)
    b_translation = FromEventStream("event", ("data", "pe1_image"), raw)

    d = b_translation.zip_latest(a_translation)
    dd = d.map(op.truediv)
    e = ToEventStream(dd, ("data",))

    g = nx.DiGraph()
    walk_to_translation(e, g)
    att = []
    for node, attrs in g.node.items():
        att.append(attrs["stream"])
    s = {a_translation, b_translation, d, dd, e}
    assert s == set(att)
    assert {_hash_or_uid(k) for k in s} == set(g.nodes)


def test_walk_up_partial():
    raw = Stream()
    a_translation = FromEventStream("start", ("time",), raw)
    b_translation = FromEventStream("event", ("data", "pe1_image"), raw)

    d = b_translation.zip_latest(a_translation)
    ddd = ToEventStream(d, ("data",))
    dd = d.map(op.truediv)
    e = ToEventStream(dd, ("data",))

    g = nx.DiGraph()
    walk_to_translation(e, g)
    att = []
    for node, attrs in g.node.items():
        att.append(attrs["stream"])
    s = {ddd, dd, e, d}
    assert s == set(att)
    assert {_hash_or_uid(k) for k in s} == set(g.nodes)


def test_to_event_model():
    g = to_event_model(range(10), [("ct", {"units": "arb"})])

    source = Stream()
    t = FromEventStream("event", ("data", "ct"), source, principle=True)
    assert t.principle

    n = ToEventStream(t, ("ct",))
    tt = t.sink_to_list()
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()

    for gg in g:
        source.emit(gg)

    assert tt
    assert set(p) == {"start", "stop", "event", "descriptor"}
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}


def test_execution_order():
    def data():
        suid = str(uuid.uuid4())
        duid = str(uuid.uuid4())
        yield "start", {"hi": "world", "uid": suid}
        yield "descriptor", {
            "name": "hi",
            "data_keys": {"ct"},
            "uid": duid,
            "run_start": suid,
        }
        for i in range(10):
            yield "event", {
                "uid": str(uuid.uuid4()),
                "data": {"ct": i},
                "descriptor": duid,
            }
        duid = str(uuid.uuid4())
        yield "descriptor", {
            "name": "not hi",
            "data_keys": {"ct"},
            "uid": duid,
            "run_start": suid,
        }
        for i in range(100, 110):
            yield "event", {
                "uid": str(uuid.uuid4()),
                "data": {"ct": i},
                "descriptor": duid,
            }
        yield "stop", {"uid": str(uuid.uuid4()), "run_start": suid}

    source = FromEventStream("event", ("data", "ct"))
    p = source.map(op.add, 1)
    pp = p.SimpleToEventStream("ctp1")
    ppp = p.map(op.mul, 2)
    l1 = ppp.sink_to_list()
    pppp = ppp.SimpleToEventStream("ctp2")
    l2 = ppp.map(lambda *x: time.time()).sink_to_list()
    assert next(iter(p.downstreams)) is pp
    assert next(iter(ppp.downstreams)) is pppp
    ex_l = [(i + 1) * 2 for i in range(10)] + [
        (i + 1) * 2 for i in range(100, 110)
    ]
    for d in data():
        source.update(d)
    assert l1 == ex_l
    assert all((v == pppp.start_uid for v in pppp.times.values()))
    t = sorted(pppp.times.keys())
    # ToEventStream executed first
    assert all((v < v2 for v, v2 in zip(t, l2)))


def test_align():
    a = Stream()
    b = Stream()
    z = a.AlignEventStreams(b)
    sl = z.sink_to_list()
    for n, d, dd in zip(
        ["start", "descriptor", "event", "stop"],
        [
            {"a": "hi", "b": {"hi": "world"}},
            {"bla": "foo"},
            {"data": "now"},
            {"stop": "doc"},
        ],
        [
            {"a": "hi2", "b": {"hi2": "world"}},
            {"bla": "foo"},
            {"data": "now"},
            {"stop": "doc"},
        ],
    ):
        a.emit((n, d))
        b.emit((n, dd))

    assert len(sl) == 4
    assert sl[0][1].get("b") == {"hi": "world", "hi2": "world"}


def test_to_event_model_dict():
    g = to_event_model(range(10), [("ct", {"units": "arb"})])

    source = Stream()
    t = FromEventStream("event", ("data",), source, principle=True)

    n = ToEventStream(t)
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()

    n.sink(print)
    for gg in g:
        source.emit(gg)

    assert set(p) == {"start", "stop", "event", "descriptor"}
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}
    assert d[2]["data"] == {"ct": 0}
