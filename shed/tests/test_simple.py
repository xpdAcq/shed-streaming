import operator as op
import time
import uuid

import networkx as nx
import numpy as np
import pytest
from bluesky.plan_stubs import checkpoint, abs_set, trigger_and_read
from bluesky.plans import scan, count
from shed import (
    SimpleFromEventStream as FromEventStream,
    SimpleToEventStream as ToEventStream,
    walk_to_translation,
)
from shed.simple import _hash_or_uid
from shed.tests.utils import y
from shed.utils import unstar
from rapidz import Stream, move_to_first


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


def test_from_event_model_single(RE, hw):
    source = Stream()
    t = FromEventStream("event", "data", source, principle=True)
    L = t.sink_to_list()

    RE.subscribe(unstar(source.emit))
    RE.subscribe(print)

    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    assert len(L) == 10
    for i, ll in enumerate(L):
        assert i == ll["motor"]


def test_from_event_model_multi(RE, hw):
    source = Stream()
    t = FromEventStream(
        "event", ("data", ("motor", "motor_setpoint")), source, principle=True
    )
    L = t.sink_to_list()

    RE.subscribe(unstar(source.emit))
    RE.subscribe(print)

    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    assert len(L) == 10
    for i, ll in enumerate(L):
        assert i == ll[0]
        assert i == ll[1]


def test_from_event_model_all(RE, hw):
    source = Stream()
    t = FromEventStream("event", (), source, principle=True)
    L = t.sink_to_list()

    RE.subscribe(unstar(source.emit))
    RE.subscribe(print)

    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    assert len(L) == 10
    for i, ll in enumerate(L):
        assert i == ll["data"]["motor"]


def test_from_event_model_stream_syntax(RE, hw):
    source = Stream()
    t = source.simple_from_event_stream(
        "event", ("data", "motor"), principle=True
    )
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
    for node, attrs in g.node.items():
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
    for node, attrs in g.node.items():
        att.append(attrs["stream"])
    s = {ddd, dd, e, d}
    assert s == set(att)
    assert {_hash_or_uid(k) for k in s} == set(g.nodes)


def test_to_event_model(RE, hw):
    source = Stream()
    t = FromEventStream("event", ("data", "motor"), source, principle=True)
    assert t.principle

    n = ToEventStream(t, ("ct",), data_key_md={"ct": {"units": "arb"}})
    tt = t.sink_to_list()
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()

    RE.subscribe(unstar(source.emit))
    RE.subscribe(print)

    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    assert tt
    assert set(p) == {"start", "stop", "event", "descriptor"}
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}
    assert d[1]["data_keys"]["ct"]["units"] == "arb"
    assert d[-1]["run_start"]


def test_to_event_model_stream_syntax(RE, hw):
    source = Stream()
    t = FromEventStream("event", ("data", "motor"), source, principle=True)
    assert t.principle

    n = t.simple_to_event_stream(("ct",), data_key_md={"ct": {"units": "arb"}})
    tt = t.sink_to_list()
    p = n.pluck(0).sink_to_list()
    d = n.pluck(1).sink_to_list()

    RE.subscribe(unstar(source.emit))
    RE.subscribe(print)

    RE(scan([hw.motor], hw.motor, 0, 9, 10))

    assert tt
    assert set(p) == {"start", "stop", "event", "descriptor"}
    assert d[1]["hints"] == {"analyzer": {"fields": ["ct"]}}
    assert d[1]["data_keys"]["ct"]["units"] == "arb"
    assert d[-1]["run_start"]


def test_align():
    a = Stream()
    b = Stream()
    z = a.AlignEventStreams(b)
    sl = z.sink_to_list()
    # TODO: use real run engine here
    for n, d, dd in zip(
        ["start", "descriptor", "event", "stop"],
        [
            {"a": "hi", "b": {"hi": "world"}, "uid": "hi", "time": 123},
            {"bla": "foo", "uid": "abc"},
            {"data": "now", "descriptor": "abc"},
            {"stop": "doc"},
        ],
        [
            {"a": "hi2", "b": {"hi2": "world"}},
            {"bla": "foo", "uid": "123"},
            {"data": "now", "descriptor": "123"},
            {"stop": "doc"},
        ],
    ):
        a.emit((n, d))
        b.emit((n, dd))

    assert len(sl) == 4
    assert sl[0][1].get("b") == {"hi": "world", "hi2": "world"}


def test_align_stream_syntax():
    a = Stream()
    b = Stream()
    z = a.align_event_streams(b)
    sl = z.sink_to_list()
    # TODO: use real run engine here
    for n, d, dd in zip(
        ["start", "descriptor", "event", "stop"],
        [
            {"a": "hi", "b": {"hi": "world"}, "uid": "hi", "time": 123},
            {"bla": "foo", "uid": "abc"},
            {"data": "now", "descriptor": "abc"},
            {"stop": "doc"},
        ],
        [
            {"a": "hi2", "b": {"hi2": "world"}},
            {"bla": "foo", "uid": "123"},
            {"data": "now", "descriptor": "123"},
            {"stop": "doc"},
        ],
    ):
        a.emit((n, d))
        b.emit((n, dd))

    assert len(sl) == 4
    assert sl[0][1].get("b") == {"hi": "world", "hi2": "world"}


def test_align_interrupted(RE, hw):
    a = Stream()
    b = FromEventStream("event", ("data", "img"), a, principle=True).map(
        op.add, 1
    )
    b.sink(print)
    c = ToEventStream(b, ("out",))
    z = move_to_first(a.AlignEventStreams(c))
    sl = z.sink_to_list()

    L = []

    RE.subscribe(lambda *x: L.append(x))

    RE(count([hw.img]))

    for nd in L:
        name, doc = nd
        # cause an exception
        if name == "event":
            doc["data"]["img"] = "hi"
        try:
            a.emit((name, doc))
        except TypeError:
            pass
    assert {"start", "stop"} == set(list(zip(*sl))[0])
    # check that buffers are not cleared, yet
    assert any([b for n, tb in z.true_buffers.items() for u, b in tb.items()])
    sl.clear()
    # If there are elements in the buffer they need to be cleared when all
    # start docs come in.
    for nd in L:
        name, doc = nd
        # cause an exception
        if name == "event":
            doc["data"]["img"] = 1
        a.emit((name, doc))
        if name == "start":
            # now buffers should be clear
            assert not any(
                [b for n, tb in z.true_buffers.items() for u, b in tb.items()]
            )
    assert {"start", "descriptor", "event", "stop"} == set(list(zip(*sl))[0])
    # now buffers should be clear (as all docs were emitted)
    assert not any(
        [b for n, tb in z.true_buffers.items() for u, b in tb.items()]
    )


def test_align_res_dat(RE, hw):
    a = Stream()
    b = FromEventStream("event", ("data", "motor"), a, principle=True).map(
        op.add, 1
    )
    c = ToEventStream(b, ("out",))
    z = a.AlignEventStreams(c)
    sl = z.sink_to_list()

    RE.subscribe(lambda *x: a.emit(x))

    osu = RE(scan([hw.img], hw.motor, 0, 10, 10))

    for n, d in sl:
        if n == "start":
            assert d["original_start_uid"] == osu[0]
        if n == "event":
            assert d["data"]["out"] == d["data"]["motor"] + 1


def test_align_multi_stream(RE, hw):
    a = Stream()
    b = FromEventStream(
        "event",
        ("data", "motor"),
        a,
        principle=True,
        event_stream_name="primary",
    ).map(op.add, 1)
    c = ToEventStream(b, ("out",))
    c.sink(print)
    z = a.AlignEventStreams(c, event_stream_name="primary")
    sl = z.sink_to_list()

    RE.subscribe(lambda *x: a.emit(x))

    def one_1d_step(detectors, motor, step):
        """
        Inner loop of a 1D step scan

        This is the default function for ``per_step`` param in 1D plans.
        """
        yield from checkpoint()
        yield from abs_set(motor, step, wait=True)
        yield from trigger_and_read(list(detectors) + [motor], name="dark")
        return (yield from trigger_and_read(list(detectors) + [motor]))

    osu = RE(scan([hw.img], hw.motor, 0, 10, 10, per_step=one_1d_step))

    assert len(sl) == 10 + 3

    for n, d in sl:
        if n == "start":
            assert d["original_start_uid"] == osu[0]
        if n == "event":
            print(d)
            assert d["data"]["out"] == d["data"]["motor"] + 1


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


def test_parent_nodes():
    # build the graph
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g11 = FromEventStream(
        "event", ("data", "det_image"), stream_name="g11", asynchronous=True
    )
    g2 = g1.zip(g11).starmap(op.mul, stream_name="mul")
    g = g2.SimpleToEventStream(("img2",))
    l1 = g.sink_to_list()
    # g.sink(print)
    assert len(g.translation_nodes) == 2
    print("start experiment")

    # run the experiment
    l0 = []
    for yy in y(5):
        l0.append(yy)
        g11.update(yy)
        g1.update(yy)
        print(g11.start_uid)

    assert len(l1[0][1]["parent_node_map"]) == 2


@pytest.mark.xfail(raises=RuntimeError)
def test_no_parent_nodes():
    # build the graph
    g1 = FromEventStream(
        "event", ("data", "det_image"), stream_name="g1", asynchronous=True
    )
    g11 = FromEventStream(
        "event", ("data", "det_image"), stream_name="g11", asynchronous=True
    )
    g2 = g1.zip(g11).starmap(op.mul, stream_name="mul")
    g2.SimpleToEventStream(("img2",))


def test_multi_path_principle(hw, RE):
    source = Stream()
    fes1 = FromEventStream("start", ("number",), source, principle=True)
    fes2 = FromEventStream("event", ("data", "motor"), source, principle=True)

    out1 = fes1.map(op.add, 1)
    out2 = fes2.combine_latest(out1, emit_on=0).starmap(op.mul)

    a = ToEventStream(out1, ("out1",))
    b = ToEventStream(out2, ("out2",))

    la = a.sink_to_list()
    lb = b.sink_to_list()

    RE.subscribe(lambda *x: source.emit(x))

    for i in range(1, 3):
        RE(count([hw.motor], md={"number": 5}))

        for l in [la, lb]:
            o1 = [z[0] for z in l]
            o2 = ["start", "descriptor", "event", "stop"] * i
            assert o1 == o2


def test_same_hdr_many_times(hw, RE):
    source = Stream()
    fes1 = FromEventStream("start", ("number",), source, principle=True)
    fes2 = FromEventStream("event", ("data", "motor"), source, principle=True)

    out1 = fes1.map(op.add, 1)
    out2 = fes2.combine_latest(out1, emit_on=0).starmap(op.mul)

    a = ToEventStream(out1, ("out1",))
    b = ToEventStream(out2, ("out2",))

    la = a.sink_to_list()
    lb = b.sink_to_list()

    L = []
    RE.subscribe(lambda *x: L.append(x))
    RE(count([hw.motor], md={"number": 5}))

    for i in range(1, 3):
        for ll in L:
            source.emit(ll)
        for l in [la, lb]:
            o1 = [z[0] for z in l]
            o2 = ["start", "descriptor", "event", "stop"] * i
            assert o1 == o2
