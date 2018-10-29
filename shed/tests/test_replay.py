import operator as op
import time
import uuid

import networkx as nx
import numpy as np

from shed.replay import replay
from shed.translation import FromEventStream
from shed.translation_parallel import ToEventStream

from shed.tests.utils import y, slow_inc, slow_mul
from distributed.utils_test import gen_cluster  # flake8: noqa
import pytest
from tornado import gen


@pytest.mark.gen_test
def test_replay(db):
    print("build graph")
    g1 = FromEventStream(
        "event", ("data", "det_image"), principle=True, stream_name="g1"
    )
    g11 = FromEventStream("event", ("data", "det_image"), stream_name="g11")
    g11_1 = g1.zip(g11)
    g2 = g11_1.starmap(slow_mul)
    g = g2.ToEventStream(("img2",))
    dbf = g.DBFriendly()
    g.sink(print)
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print("run experiment")
    l0 = []
    for yy in y(5):
        l0.append(yy)
        db.insert(*yy)
        yield g11.update(yy)
        yield g1.update(yy)

    print("replay experiment")
    rp = replay(db, db[-1])
    lg = next(rp)
    ts = list(nx.topological_sort(lg))
    l2 = lg.node[ts[-1]]["stream"].sink_to_list()
    next(rp)
    print(l2)
    assert len(l1) == len(l2)
    assert len(l0) == len(l2)
    for nd1, nd2 in zip(l0, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            print()
            assert nd1[1]["data"]["det_image"] ** 2 == nd2[1]["data"]["img2"]
    for nd1, nd2 in zip(l1, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            print()
            assert nd1[1]["data"]["img2"] == nd2[1]["data"]["img2"]


@pytest.mark.gen_test
def test_replay_parallel(db):
    print("build graph")
    g1 = FromEventStream(
        "event", ("data", "det_image"), principle=True, stream_name="g1"
    )
    g11 = FromEventStream("event", ("data", "det_image"), stream_name="g11"
                          )
    g11_1 = g1.scatter(backend='thread').zip(g11.scatter(backend='thread'))
    g2 = g11_1.starmap(slow_mul)
    to_event_stream = g2.ToEventStream(("img2",))
    g = to_event_stream.buffer(10).gather()
    g.sink(print)
    dbf = g.DBFriendly()
    l1 = dbf.sink_to_list()
    dbf.starsink(db.insert)

    print("run experiment")
    l0 = []
    for yy in y(5):
        l0.append(yy)
        db.insert(*yy)
        yield g11.update(yy)
        yield g1.update(yy)
    while len(l1) < len(l0):
        yield gen.sleep(.01)

    print("replay experiment")
    rp = replay(db, db[-1])
    lg = next(rp)
    ts = list(nx.topological_sort(lg))
    l2 = lg.node[ts[-1]]["stream"].sink_to_list()
    next(rp)
    print(l2)
    assert len(l1) == len(l2)
    assert len(l0) == len(l2)
    for nd1, nd2 in zip(l0, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            print()
            assert nd1[1]["data"]["det_image"] ** 2 == nd2[1]["data"]["img2"]
    for nd1, nd2 in zip(l1, l2):
        assert nd1[0] == nd2[0]
        if nd1[0] == "event":
            print()
            assert nd1[1]["data"]["img2"] == nd2[1]["data"]["img2"]


def test_replay_export(db):
    print("build graph")
    g1 = FromEventStream(
        "event", ("data", "det_image"), principle=True, stream_name="g1"
    )
    g11 = FromEventStream("event", ("data", "det_image"), stream_name="g11")
    g11_1 = g1.zip(g11)
    g2 = g11_1.starmap(op.mul).map(np.log)
    g = g2.ToEventStream(("img2",))
    L = g.sink_to_list()
    dbf = g.DBFriendly()
    dbf.sink(print)
    dbf.starsink(db.insert)

    print("run experiment")
    for yy in y():
        db.insert(*yy)
        g11.update(yy)
        g1.update(yy)

    print(L[0][1]["uid"])
    s1 = db[L[0][1]["uid"]]["stop"]
    print("replay experiment")
    rp = replay(db, db[L[0][1]["uid"]], export=True)
    next(rp)
    next(rp)
    assert s1 != db[-1]["stop"]
    for (_, a), (_, b) in zip(db[-2].documents(), db[-1].documents()):
        assert a["uid"] != b["uid"]
        if "data" in a:
            assert a["data"] == b["data"]



