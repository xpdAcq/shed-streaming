import operator as op

import networkx as nx
import numpy as np
import pytest
from rapidz import Stream
from shed_streaming import FromEventStream
from shed_streaming.replay import replay
from shed_streaming.tests.utils import y
from tornado import gen


@pytest.mark.gen_test
def test_replay(db):
    # XXX: what to do if you have a source?
    # build the graph
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g2 = g1.map(op.mul, 5, stream_name="mul")
    g = g2.ToEventStream(("img2",))
    graph = g.graph
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print("start experiment")

    # run the experiment
    l0 = []
    for yy in y(5):
        l0.append(yy)
        db.insert(*yy)
        yield g1.update(yy)

    assert "env" in db[-1].start
    print("start replay")

    # generate the replay
    lg, parents, data, vs = replay(db, db[-1])

    assert set(graph.nodes) == set(lg.nodes)
    l2 = lg.nodes[list(nx.topological_sort(lg))[-1]]["stream"].sink_to_list()
    # run the replay
    lg.nodes[g1.uid]["stream"].sink(print)
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


@pytest.mark.gen_test
def test_replay_dummy_node(db):
    # XXX: what to do if you have a source?
    # build the graph
    source = Stream()
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        upstream=source,
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g2 = g1.map(op.mul, 5, stream_name="mul")
    g = g2.ToEventStream(("img2",))
    graph = g.graph
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print("start experiment")

    # run the experiment
    l0 = []
    for yy in y(5):
        l0.append(yy)
        db.insert(*yy)
        yield source.emit(yy)

    print("start replay")

    # generate the replay
    lg, parents, data, vs = replay(db, db[-1])

    assert set(graph.nodes) == set(lg.nodes)
    l2 = lg.nodes[list(nx.topological_sort(lg))[-1]]["stream"].sink_to_list()
    # run the replay
    lg.nodes[g1.uid]["stream"].sink(print)
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


@pytest.mark.gen_test
def test_replay_parallel(db):
    print("build graph")
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g2 = g1.scatter(backend="thread").map(op.mul, 5, stream_name="mul")
    g = g2.ToEventStream(("img2",))
    graph = g.graph
    dbf = g.buffer(10).gather().DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print("start experiment")

    # run the experiment
    l0 = []
    for yy in y(5):
        l0.append(yy)
        db.insert(*yy)
        yield g1.update(yy)
    while len(l1) < len(l0):
        yield gen.sleep(.01)

    print("start replay")

    # generate the replay
    lg, parents, data, vs = replay(db, db[-1])

    assert set(graph.nodes) == set(lg.nodes)
    l2 = (
        lg.nodes[list(nx.topological_sort(lg))[-1]]["stream"]
        .buffer(10)
        .gather()
        .sink_to_list()
    )
    # run the replay
    lg.nodes[g1.uid]["stream"].sink(print)
    for v in vs:
        parents[v["node"]].update(data[v["uid"]])

    while len(l2) < len(l0):
        yield gen.sleep(.01)

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


@pytest.mark.gen_test
def test_replay_numpy(db):
    # XXX: what to do if you have a source?
    # build the graph
    g1 = FromEventStream(
        "event",
        ("data", "det_image"),
        principle=True,
        stream_name="g1",
        asynchronous=True,
    )
    g2 = g1.map(np.exp, stream_name="mul")
    g = g2.ToEventStream(("img2",))
    g.sink(print)
    graph = g.graph
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print("start experiment")

    # run the experiment
    l0 = []
    for yy in y(5):
        l0.append(yy)
        db.insert(*yy)
        yield g1.update(yy)

    print("start replay")

    # generate the replay
    lg, parents, data, vs = replay(db, db[-1])

    assert set(graph.nodes) == set(lg.nodes)
    l2 = lg.nodes[list(nx.topological_sort(lg))[-1]]["stream"].sink_to_list()
    # run the replay
    lg.nodes[g1.uid]["stream"].sink(print)
    print(graph.nodes)
    print(parents)
    for v in vs:
        print(v["node"])
        parents[v["node"]].update(data[v["uid"]])

    # check that all the things are ok
    assert len(l1) == len(l2)
    assert len(l0) == len(l2)
    for nd1, nd2 in zip(l0, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert (
                np.exp(nd1[1]["data"]["det_image"]) == nd2[1]["data"]["img2"]
            )
    for nd1, nd2 in zip(l1, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert nd1[1]["data"]["img2"] == nd2[1]["data"]["img2"]


@pytest.mark.gen_test
def test_replay_parallel_numpy(db):
    # XXX: what to do if you have a source?
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
    g2 = g1.scatter(backend="thread").map(np.exp, stream_name="mul")
    g = g2.ToEventStream(("img2",))
    graph = g.graph
    dbf = g.buffer(10).gather().DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print("start experiment")

    # run the experiment
    l0 = []
    for yy in y(5):
        l0.append(yy)
        db.insert(*yy)
        yield g11.update(yy)
        yield g1.update(yy)
    while len(l1) < len(l0):
        yield gen.sleep(.01)

    print("start replay")

    # generate the replay
    lg, parents, data, vs = replay(db, db[-1])

    assert set(graph.nodes) == set(lg.nodes)
    l2 = (
        lg.nodes[list(nx.topological_sort(lg))[-1]]["stream"]
        .buffer(10)
        .gather()
        .sink_to_list()
    )
    # run the replay
    lg.nodes[g1.uid]["stream"].sink(print)
    print(graph.nodes)
    print(parents)
    for v in vs:
        print(v["node"])
        parents[v["node"]].update(data[v["uid"]])

    while len(l2) < len(l0):
        yield gen.sleep(.01)

    # check that all the things are ok
    assert len(l1) == len(l2)
    assert len(l0) == len(l2)
    for nd1, nd2 in zip(l0, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert (
                np.exp(nd1[1]["data"]["det_image"]) == nd2[1]["data"]["img2"]
            )
    for nd1, nd2 in zip(l1, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            assert nd1[1]["data"]["img2"] == nd2[1]["data"]["img2"]
