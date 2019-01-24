import operator as op

from numpy.testing import assert_array_equal
from rapidz import Stream

from shed.translation import FromEventStream

from shed.writers import NpyWriter, Store
import bluesky.plans as bp
from numpy.testing import assert_allclose
from xpdan.vend.callbacks.core import Retrieve


def test_storage(RE, hw, db, tmpdir):
    source = Stream()
    z = source.Store(str(tmpdir), NpyWriter)
    lz = z.sink_to_list()
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
