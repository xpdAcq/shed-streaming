import operator as op
import time
import uuid

import networkx as nx
import numpy as np
import pytest

from shed.replay import replay
from shed.translation import FromEventStream
from tornado import gen
from streamz_ext import Stream


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


def test_replay_thread_single(db):
    print("build graph")
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g11_1 = g1
    g2 = g11_1.starmap(op.mul, 5, stream_name="mul")
    g = g2.ToEventStream(("img2",))
    graph = g.graph
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print("run experiment")
    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        print(yy)
        g1.update(yy)

    print("replay experiment")
    lg, parents, data, vs = replay(db, db[-1])
    assert set(graph.nodes) == set(lg.nodes)
    ts = list(nx.topological_sort(lg))
    lg.node[ts[-1]]["stream"].sink(print)

    l2 = lg.node[ts[-1]]["stream"].sink_to_list()
    for v in vs:
        parents[v["node"]].update(data[v["uid"]])

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


def test_replay(db):
    print("build graph")
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

    print("run experiment")
    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        g11.update(yy)
        g1.update(yy)

    print("replay experiment")
    lg, parents, data, vs = replay(db, db[-1])
    assert set(graph.nodes) == set(lg.nodes)
    ts = list(nx.topological_sort(lg))
    lg.node[ts[-1]]["stream"].sink(print)

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
    print("build graph")
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

    print("run experiment")
    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        g11.update(yy)
        g1.update(yy)

    s1 = db[L[0][1]["uid"]]["stop"]

    print("replay experiment")
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
def test_replay_thread_single(db):
    print("build graph")
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g11_1 = g1.scatter(backend="thread")
    g2 = g11_1.starmap(op.mul, 5, stream_name="mul")
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

    print("run experiment")
    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        print(yy)
        yield g1.update(yy)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)

    print("replay experiment")
    lg, parents, data, vs = replay(db, db[-1])
    assert set(graph.nodes) == set(lg.nodes)
    ts = list(nx.topological_sort(lg))
    lg.node[ts[-1]]["stream"].sink(print)

    l2 = lg.node[ts[-1]]["stream"].sink_to_list()
    for v in vs:
        yield parents[v["node"]].update(data[v["uid"]])

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
def test_replay_thread(db):
    print("build graph")
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

    graph = g.graph
    futures_L = g2.sink_to_list()
    L = g.sink_to_list()

    print("run experiment")
    l0 = []
    for yy in y():
        l0.append(yy)
        db.insert(*yy)
        yield g1.update(yy)
        yield g11.update(yy)
    while len(L) < len(futures_L):
        yield gen.sleep(.01)

    print("replay experiment")
    lg, parents, data, vs = replay(db, db[-1])
    assert set(graph.nodes) == set(lg.nodes)
    ts = list(nx.topological_sort(lg))
    lg.node[ts[-1]]["stream"].sink(print)

    l2 = lg.node[ts[-1]]["stream"].sink_to_list()
    print(len(parents))
    for v in vs:
        p = parents[v["node"]]
        yield p.update(data[v["uid"]])

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
