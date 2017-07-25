from bluesky.examples import (motor, det, stepscan)

import redsky.event_streams as es

from operator import add
from numpy.testing import assert_allclose


def test_all_add_5(fresh_RE):
    RE = fresh_RE
    source = es.Stream()
    source.sink(print)

    def add5(img):
        return add(5, img)

    dp = es.map(es.dstar(add5), source, input_info={'img': 'det'},
                output_info=[('det5', {'dtype': 'float', 'source': 'test'})])
    dp.sink(print)
    L = dp.sink_to_list()
    s2 = es.EventStream()
    L2 = s2.sink_to_list()

    RE.subscribe('all', es.istar(s2.emit))
    RE(stepscan(det, motor), subs={'all': es.istar(source.emit)})

    assert_docs = set()
    for l, s in zip(L, L2):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['det5'],
                            s[1]['data']['det'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs
