import operator as op
import time
import uuid

import networkx as nx
import numpy as np
from bluesky.plans import scan
from rapidz import Stream
from shed.simple import walk_to_translation, _hash_or_uid
from shed.translation import FromEventStream, ToEventStream, merkle_hash
from shed.utils import unstar
from databroker import Broker


def test_from_event_model(RE, hw):
    source = Stream()
    t = FromEventStream("event", ("data", "motor"), source, principle=True)
    L = t.sink_to_list()

    RE.subscribe(unstar(source.emit))
    RE.subscribe(print)

    RE(scan([hw.motor], hw.motor, 0, 9, 10))

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
    a_translation = FromEventStream("start", ("time",), raw, principle=True)
    b_translation = FromEventStream("event", ("data", "pe1_image"), raw)

    d = b_translation.zip_latest(a_translation)
    dd = d.map(op.truediv)
    e = ToEventStream(dd, ("data",))

    g = nx.DiGraph()
    walk_to_translation(e, g)
    att = []
    for node, attrs in g.nodes.items():
        att.append(attrs["stream"])
    s = {a_translation, b_translation, d, dd, e}
    assert s == set(att)
    assert {_hash_or_uid(k) for k in s} == set(g.nodes)


def test_walk_up_partial():
    raw = Stream()
    a_translation = FromEventStream("start", ("time",), raw, principle=True)
    b_translation = FromEventStream("event", ("data", "pe1_image"), raw)

    d = b_translation.zip_latest(a_translation)
    ddd = ToEventStream(d, ("data",))
    dd = d.map(op.truediv)
    e = ToEventStream(dd, ("data",))

    g = nx.DiGraph()
    walk_to_translation(e, g)
    att = []
    for node, attrs in g.nodes.items():
        att.append(attrs["stream"])
    s = {ddd, dd, e, d}
    assert s == set(att)
    assert {_hash_or_uid(k) for k in s} == set(g.nodes)


def test_to_event_model(RE, hw):
    source = Stream()
    t = FromEventStream("event", ("data", "motor"), source, principle=True)
    assert t.principle

    n = ToEventStream(t, ("ct",))
    tt = t.sink_to_list()
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()

    RE.subscribe(unstar(source.emit))
    RE.subscribe(print)

    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    assert tt
    assert set(p) == {"start", "stop", "event", "descriptor"}
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}
    assert d[-1]["run_start"]


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

    source = FromEventStream("event", ("data", "ct"), principle=True)
    p = source.map(op.add, 1)
    pp = p.ToEventStream("ctp1")
    ppp = p.map(op.mul, 2)
    l1 = ppp.sink_to_list()
    pppp = ppp.ToEventStream("ctp2")
    l2 = ppp.map(lambda *x: time.time()).sink_to_list()
    assert next(iter(p.downstreams)) is pp
    assert next(iter(ppp.downstreams)) is pppp
    for d in data():
        source.update(d)
    ex_l = [(i + 1) * 2 for i in range(10)] + [
        (i + 1) * 2 for i in range(100, 110)
    ]
    assert l1 == ex_l
    assert all((v == pppp.start_uid for _, v in pppp.times))
    t = sorted([t for t, _ in pppp.times])
    # ToEventStream executed first
    assert all((v < v2 for v, v2 in zip(t, l2)))


def test_to_event_model_dict(RE, hw):
    source = Stream()
    t = FromEventStream("event", ("data",), source, principle=True)

    n = ToEventStream(t)
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()

    n.sink(print)
    RE.subscribe(unstar(source.emit))
    RE.subscribe(print)

    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    print(d[1]["hints"])
    # AAA
    assert set(p) == {"start", "stop", "event", "descriptor"}
    assert d[1]["hints"] == {
        "analyzer": {"fields": ["motor", "motor_setpoint"]}
    }
    assert d[2]["data"] == {"motor_setpoint": 0, "motor": 0}
    assert d[-1]["run_start"]


def test_replay_export_test():
    def y():
        suid = str(uuid.uuid4())
        yield ("start", {"uid": suid, "time": time.time()})
        duid = str(uuid.uuid4())
        yield (
            "descriptor",
            {
                "uid": duid,
                "run_start": suid,
                "name": "primary",
                "data_keys": {"det_image": {"dtype": "int", "units": "arb"}},
                "time": time.time(),
            },
        )
        for i in range(5):
            yield (
                "event",
                {
                    "uid": str(uuid.uuid4()),
                    "data": {"det_image": i},
                    "timestamps": {"det_image": time.time()},
                    "seq_num": i + 1,
                    "time": time.time(),
                    "descriptor": duid,
                },
            )
        yield (
            "stop",
            {"uid": str(uuid.uuid4()), "time": time.time(), "run_start": suid},
        )

    print("build graph")
    g1 = FromEventStream(
        "event", ("data", "det_image"), principle=True, stream_name="g1"
    )
    g11 = FromEventStream("event", ("data", "det_image"), stream_name="g11")
    g11_1 = g1.zip(g11)
    g2 = g11_1.starmap(op.mul).map(np.log)
    g = g2.SimpleToEventStream(("img2",))
    from pprint import pprint

    g.sink(pprint)
    L = g.sink_to_list()

    print("run experiment")
    for yy in y():
        print(yy[0])
        g11.update(yy)
        g1.update(yy)
    assert L[-1][1]["run_start"]


def test_no_stop(hw, RE):

    source = Stream().filter(lambda x: x[0] != "stop")
    t = FromEventStream("event", ("data",), source, principle=True)

    n = ToEventStream(t)
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()

    RE.subscribe(unstar(source.emit))
    RE.subscribe(print)

    RE(scan([hw.motor], hw.motor, 0, 9, 10))
    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    assert set(p) == {"start", "stop", "event", "descriptor"}
    assert d[1]["hints"] == {
        "analyzer": {"fields": ["motor", "motor_setpoint"]}
    }
    assert d[2]["data"] == {"motor_setpoint": 0, "motor": 0}


def test_merkle_hash():
    source = Stream()
    t = FromEventStream("event", ("data", "motor"), source, principle=True)
    assert t.principle

    n = ToEventStream(t, ("ct",), data_key_md={"ct": {"units": "arb"}})
    h = merkle_hash(n)
    assert h

    tt = FromEventStream("event", ("data", "motor"), source, principle=True)

    nn = ToEventStream(tt, ("ct",), data_key_md={"ct": {"units": "arb"}})
    assert h == merkle_hash(nn)
    assert h != merkle_hash(tt)

    tt = FromEventStream("event", ("data", "motor"), source, principle=True)

    z = tt.map(op.add, 1)
    zz = tt.map(op.sub, 1)
    j = z.zip(zz)

    nn = ToEventStream(j, ("ct",), data_key_md={"ct": {"units": "arb"}})
    order_1_hash = merkle_hash(nn)

    tt = FromEventStream("event", ("data", "motor"), source, principle=True)

    zz = tt.map(op.sub, 1)
    z = tt.map(op.add, 1)
    j = z.zip(zz)

    nn = ToEventStream(j, ("ct",), data_key_md={"ct": {"units": "arb"}})
    order_2_hash = merkle_hash(nn)
    assert order_1_hash != order_2_hash

    tt = FromEventStream("event", ("data", "motor"), source, principle=True)

    z = tt.map(op.add, 1)
    zz = tt.map(op.sub, 1)
    j = zz.zip(z)

    nn = ToEventStream(j, ("ct",), data_key_md={"ct": {"units": "arb"}})
    order_3_hash = merkle_hash(nn)
    assert order_1_hash != order_3_hash


def test_dbfriendly(RE, hw):
    source = Stream()
    t = FromEventStream("event", ("data", "motor"), source, principle=True)
    z = t.map(op.add, 1)
    n = ToEventStream(z, "out").DBFriendly()
    d = n.pluck(1).sink_to_list()

    RE.subscribe(unstar(source.emit))

    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    assert isinstance(d[0]["graph"], dict)
    h1 = d[0].get("graph_hash")
    assert h1

    d.clear()
    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    h2 = d[0].get("graph_hash")
    assert h1 == h2
    assert len(d) == 10 + 3

    d.clear()
    z.args = (2,)
    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    h2 = d[0].get("graph_hash")
    assert h1 != h2
    assert len(d) == 10 + 3


def test_db_insertion(RE, hw):
    db = Broker.named("temp")

    source = Stream()
    n0 = FromEventStream("event", ("data", "motor"), source, principle=True)
    n1 = ToEventStream(n0, "motor")
    n1.DBFriendly().starsink(db.v1.insert)

    RE.subscribe(lambda *x: source.emit(x))
    RE(scan([hw.motor], hw.motor, 0, 1, 2))

    assert db[-1]
