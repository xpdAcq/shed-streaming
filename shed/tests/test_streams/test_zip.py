import numpy as np
import pytest
from numpy.testing import assert_raises, assert_equal
from streamz import Stream

from shed import event_streams as es
from shed.utils import to_event_model


@pytest.mark.parametrize(('n', 'n2', 'kwargs', 'expected'), [
    (5, 5, {}, None),
    (5, 5, {'zip_type': 'extend'}, None),
    (5, 2, {'zip_type': 'extend'}, None),
    (5, 5, {'zip_type': 'strict'}, None),
    (3, 5, {'zip_type': 'truncate'}, None),
    pytest.param(3, 5, {'zip_type': 'strict'}, True, marks=pytest.mark.xfail),
    pytest.param(3, 5, {'zip_type': 'extend'}, True, marks=pytest.mark.xfail),
    pytest.param(5, 3, {'zip_type': 'truncate'}, True,
                 marks=pytest.mark.xfail),
    pytest.param(5, 3, {'zip_type': 'not implemented'}, True,
                 marks=pytest.mark.xfail),
])
def test_zip(n, n2, kwargs, expected):
    source = Stream()
    source2 = Stream()

    dp = es.zip(source, source2, **kwargs)
    L = dp.sink_to_list()
    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(n)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    s2 = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(n2)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))

    for clear in [True, False]:
        for _ in range(2):
            if clear:
                source.emit(('clear', None))
                assert all([not buf for buf in dp.buffers])
            L.clear()
            for b in s2:
                source2.emit(b)
            for a in s:
                source.emit(a)
            assert_docs = set()
            for name, (l1, l2) in L:
                assert_docs.add(name)
                assert_raises(AssertionError, assert_equal, l1, l2)
            assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}
