import operator as op

from numpy.testing import assert_array_equal
from streamz_ext import Stream

from shed.translation import FromEventStream

from shed.writers import NpyWriter, Store


def test_storage(exp_db, tmp_dir):
    s = Store(external_writers={"out": NpyWriter(exp_db.reg, root=tmp_dir)})
    source = Stream()
    (
        FromEventStream("event", ("data", "pe1_image"), source, principle=True)
        .map(op.mul, 2)
        .ToEventStream(("out",))
        .starmap(s)
        .DBFriendly()
        .starsink(exp_db.insert)
    )
    for n, d in exp_db[-1].documents(fill=True):
        source.emit((n, d))

    for a, b in zip(
        exp_db[-2].events(fill=True), exp_db[-1].events(fill=True)
    ):
        aa = a["data"]["pe1_image"]
        bb = b["data"]["out"]
        assert_array_equal(aa * 2, bb)
