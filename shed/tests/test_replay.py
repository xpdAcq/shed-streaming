import operator as op
import time
import uuid
from pprint import pprint

import networkx as nx
import pytest
from shed.replay import replay
from shed.translation import FromEventStream
from tornado import gen


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
                "data": {"det_image": i + 1},
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


def test_replay(db):
    # build the graph
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g11_1 = g1
    g2 = g11_1.map(op.mul, 5, stream_name="mul")
    g = g2.ToEventStream(("img2",))
    graph = g.graph
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    # run the experiment
    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        g1.update(yy)

    # generate the replay
    lg, parents, data, vs = replay(db, db[-1])

    assert set(graph.nodes) == set(lg.nodes)
    l2 = lg.node[list(nx.topological_sort(lg))[-1]]["stream"].sink_to_list()
    # run the replay
    for v in vs:
        parents[v["node"]].update(data[v["uid"]])

    # check that all the things are ok
    assert len(l1) == len(l2)
    assert len(l0) == len(l2)
    for nd1, nd2 in zip(l0, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert nd1[1]["data"]["det_image"] * 5 == nd2[1]["data"]["img2"]
    for nd1, nd2 in zip(l1, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert nd1[1]["data"]["img2"] == nd2[1]["data"]["img2"]


def test_replay_double(db):
    # build the graph
    g1 = FromEventStream(
        "event", ("data", "det_image"), principle=True, stream_name="g1"
    )
    g11 = FromEventStream("event", ("data", "det_image"), stream_name="g11")
    g11_1 = g1.zip(g11)
    g2 = g11_1.starmap(op.mul)
    g = g2.ToEventStream(("img2",))
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    graph = g.graph

    # run the experiment
    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        g11.update(yy)
        g1.update(yy)

    # replay the experiment
    lg, parents, data, vs = replay(db, db[-1])
    assert set(graph.nodes) == set(lg.nodes)

    ts = list(nx.topological_sort(lg))
    l2 = lg.node[ts[-1]]["stream"].sink_to_list()

    for v in vs:
        parents[v["node"]].update(data[v["uid"]])

    assert len(l1) == len(l2)
    assert len(l0) == len(l2)
    for nd1, nd2 in zip(l0, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert nd1[1]["data"]["det_image"] ** 2 == nd2[1]["data"]["img2"]
    for nd1, nd2 in zip(l1, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert nd1[1]["data"]["img2"] == nd2[1]["data"]["img2"]


def test_replay_export(db):
    g1 = FromEventStream(
        "event", ("data", "det_image"), principle=True, stream_name="g1"
    )
    g11 = FromEventStream("event", ("data", "det_image"), stream_name="g11")
    g11_1 = g1.zip(g11)
    g2 = g11_1.starmap(op.mul)
    g = g2.ToEventStream(("img2",))
    L = g.sink_to_list()
    dbf = g.DBFriendly()
    dbf.starsink(db.insert)

    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        g11.update(yy)
        g1.update(yy)

    s1 = db[L[0][1]["uid"]]["stop"]

    lg, parents, data, vs = replay(db, db[-1], export=True)

    for v in vs:
        parents[v["node"]].update(data[v["uid"]])
    assert s1 != db[-1]["stop"]

    f = False
    for (_, a), (_, b) in zip(db[-2].documents(), db[-1].documents()):
        assert a["uid"] != b["uid"]
        if "data" in a:
            assert a["data"] == b["data"]
            f = True
    assert f


@pytest.mark.gen_test
def test_replay_thread(db):
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g11_1 = g1.scatter(backend="thread")
    g2 = g11_1.map(op.mul, 5, stream_name="mul")
    g = (
        g2.buffer(10, stream_name="buff")
        .gather(stream_name="gather")
        .ToEventStream(("img2",))
    )
    graph = g.graph
    futures_L = g2.sink_to_list()
    L = g.sink_to_list()
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    l0 = []
    t0 = None
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        yield g1.update(yy)
        t1 = list(g1.times.keys())
        if t0:
            assert max(t0) < max(t1)
        t0 = t1
    while len(L) < len(futures_L):
        yield gen.sleep(.01)

    lg, parents, data, vs = replay(db, db[-1])

    assert set(graph.nodes) == set(lg.nodes)

    l2 = lg.node[list(nx.topological_sort(lg))[-1]]["stream"].sink_to_list()

    for v in vs:
        yield parents[v["node"]].update(data[v["uid"]])
    while len(l2) < len(l1):
        yield gen.sleep(.01)

    assert len(l1) == len(l2)
    assert len(l0) == len(l2)
    for nd1, nd2 in zip(l0, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert nd1[1]["data"]["det_image"] * 5 == nd2[1]["data"]["img2"]
    for nd1, nd2 in zip(l1, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert nd1[1]["data"]["img2"] == nd2[1]["data"]["img2"]


@pytest.mark.gen_test
def test_replay_double_thread(db):
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g11 = FromEventStream("event", ("data", "det_image"), stream_name="g11")
    g11_1 = g1
    g2s = g11_1.scatter(backend="thread").zip(
        g11.scatter(backend="thread"))
    g2 = (
        g2s
        .starmap(op.mul, stream_name="mul")
    )
    g = (
        g2.buffer(10, stream_name="buff")
        .gather(stream_name="gather")
        .ToEventStream(("img2",))
    )
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    graph = g.graph

    l0 = []
    t0 = None
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        yield g11.update(yy)
        yield g1.update(yy)
        t1 = list(g1.times.keys())
        if t0:
            assert max(t0) < max(t1)
        t0 = t1
    while len(l1) < len(l0):
        yield gen.sleep(.01)

    for name, l in zip(['l1'], [l1]):
        print(name)
        assert len(l0) == len(l)
        for (n1, d1), (n2, d2) in zip(l0, l):
            pprint(d1)
            pprint(d2)
            assert n1 == n2
            if n1 == "event":
                assert d1["data"]["det_image"] ** 2 == d2["data"]["img2"]

    lg, parents, data, vs = replay(db, db[-1])
    assert set(graph.nodes) == set(lg.nodes)

    l2 = lg.node[list(nx.topological_sort(lg))[-1]]["stream"].sink_to_list()

    for v in vs:
        p = parents[v["node"]]
        yield p.update(data[v["uid"]])
    while len(l2) < len(l0):
        yield gen.sleep(.01)

    for name, l in zip(['l1', 'l2'], [l1, l2]):
        print(name)
        assert len(l0) == len(l)
        for (n1, d1), (n2, d2) in zip(l0, l):
            assert n1 == n2
            if n1 == "event":
                assert d1["data"]["det_image"] ** 2 == d2["data"]["img2"]
    for nd1, nd2 in zip(l1, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert nd1[1]["data"]["img2"] == nd2[1]["data"]["img2"]


@pytest.mark.gen_test
def test_replay_export(db):
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g11 = FromEventStream("event", ("data", "det_image"), stream_name="g11")
    g11_1 = g1
    g2 = (
        g11_1.scatter(backend="thread")
            .zip(g11.scatter(backend="thread"))
            .starmap(op.mul, stream_name="mul")
    )
    g = (
        g2.buffer(10, stream_name="buff")
            .gather(stream_name="gather")
            .ToEventStream(("img2",))
    )
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    futures_L = g2.sink_to_list()
    L = g.sink_to_list()

    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        yield g11.update(yy)
        yield g1.update(yy)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)

    s1 = db[L[0][1]["uid"]]["stop"]

    lg, parents, data, vs = replay(db, db[-1], export=True)

    l2 = lg.node[list(nx.topological_sort(lg))[-1]]["stream"].sink_to_list()

    for v in vs:
        yield parents[v["node"]].update(data[v["uid"]])
    while len(l2) < len(l1):
        yield gen.sleep(.01)
    assert s1 != db[-1]["stop"]

    f = False
    for (_, a), (_, b) in zip(db[-2].documents(), db[-1].documents()):
        assert a["uid"] != b["uid"]
        if "data" in a:
            assert a["data"] == b["data"]
            f = True
    assert f
