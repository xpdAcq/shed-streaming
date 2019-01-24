import bluesky.plans as bp
from rapidz import Stream
from shed.writers import NpyWriter
from xpdan.vend.callbacks.core import Retrieve


def test_storage(RE, hw, db, tmpdir):
    source = Stream()
    z = source.Store(str(tmpdir), NpyWriter)
    z.starsink(db.insert)

    L = []
    RE.subscribe(lambda *x: source.emit(x))
    RE.subscribe(lambda *x: L.append(x))
    RE(bp.count([hw.direct_img]))

    rt = Retrieve(handler_reg=db.reg.handler_reg)
    for i, nd in enumerate(db[-1].documents()):
        n2, d2 = rt(*nd)
        print(n2)
        if n2 == 'event':
            print(d2['data']['img'])
            assert d2['data']['img'].shape == (10, 10)


def test_double_storage(RE, hw, db, tmpdir):
    source = Stream()
    z = source.Store(str(tmpdir), NpyWriter)
    z.starsink(db.insert)

    L = []
    RE.subscribe(lambda *x: L.append(x))
    RE(bp.count([hw.direct_img], 2))

    LL = []
    RE.subscribe(lambda *x: LL.append(x))
    RE(bp.count([hw.direct_img], 2))
    for nd1, nd2 in zip(L, LL):
        source.emit(nd1)
        source.emit(nd2)

    rt = Retrieve(handler_reg=db.reg.handler_reg)
    for ii in [-2, -1]:
        for i, nd in enumerate(db[ii].documents()):
            n2, d2 = rt(*nd)
            print(n2)
            if n2 == 'event':
                print(d2['data']['img'])
                assert d2['data']['img'].shape == (10, 10)
