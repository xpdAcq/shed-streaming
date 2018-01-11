import numpy as np
import pytest
from numpy.testing import assert_raises, assert_equal
from shed import event_streams as es
from shed.utils import to_event_model
from streamz_ext.core import Stream


@pytest.mark.parametrize(('n', 'n2', 'order', 'kwargs'), [
    (5, 5, 'forward', {}),
    (5, 2, 'forward', {}),
    (2, 5, 'forward', {}),
    (5, 5, 'reverse', {}),
    (5, 2, 'reverse', {}),
    (2, 5, 'reverse', {}),
    (5, 5, 'interleaved', {}),
    (5, 2, 'interleaved', {}),
    (2, 5, 'interleaved', {}),

])
def test_zip_latest(n, n2, order, kwargs):
    source = Stream()
    source2 = Stream()

    L = es.zip_latest(source, source2, **kwargs).sink_to_list()
    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(n)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    s2 = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(n2)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for i in range(2):
        L.clear()
        if order == 'forward':
            for b in s2:
                source2.emit(b)
            for a in s:
                source.emit(a)
        if order == 'reverse':
            for a in s:
                source.emit(a)
            for b in s2:
                source2.emit(b)
        if order == 'interleaved':
            if i % 2 == 0:
                for b in s2:
                    source2.emit(b)
                for a in s:
                    source.emit(a)
            else:
                for a in s:
                    source.emit(a)
                for b in s2:
                    source2.emit(b)

        assert_docs = set()
        for name, (l1, l2) in L:
            assert_docs.add(name)
            assert_raises(AssertionError, assert_equal, l1, l2)
        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}
        assert len(L) == len(s)


def test_zip_latest_double_clear():
    source = Stream()
    source2 = Stream()

    dp = es.zip_latest(source, source2, clear_on_lossless_stop=True)
    L = dp.sink_to_list()
    g1 = list(to_event_model([1, 2, 3], [('det', {})]))
    g2 = list(to_event_model(['a', 'b', 'c'], [('det', {})]))
    g3 = list(to_event_model([5], [('det', {})]))
    for b in g1:
        source.emit(b)
    for a in g2:
        source.emit(a)
    for c in g3:
        source2.emit(c)

    assert_docs = set()
    assert len(L) == len(g2)
    for (name, (l1, l2)), (_, ll1) in zip(L, g2):
        assert_docs.add(name)
        assert l1 != l2
        assert l1 == ll1
        if name == 'event':
            assert l2['data']['det'] == 5
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs

    L.clear()
    g1 = list(to_event_model([10, 20, 30], [('det', {})]))
    g2 = list(to_event_model(['aa', 'bb', 'cc'], [('det', {})]))
    g3 = list(to_event_model([50], [('det', {})]))
    for b in g1:
        source.emit(b)
    for a in g2:
        source.emit(a)
    for c in g3:
        source2.emit(c)

    assert_docs = set()
    assert len(L) == len(g2)
    for (name, (l1, l2)), (_, ll1) in zip(L, g2):
        assert_docs.add(name)
        assert l1 != l2
        assert l1 == ll1
        if name == 'event':
            assert l2['data']['det'] == 50
    assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}
