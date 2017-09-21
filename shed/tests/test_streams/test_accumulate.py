import pytest
from numpy.testing import assert_allclose

from shed import event_streams as es
from shed.event_streams import dstar, star
from shed.tests.utils import SinkAssertion
from streamz import Stream
import numpy as np
from shed.utils import to_event_model


source = Stream()


@pytest.mark.parametrize('kwargs', [
    dict(state_key='img1', input_info={'img2': 'pe1_image'},),
    dict(start=dstar(lambda img2: img2), state_key='img1',
         input_info={'img2': 'pe1_image'},),
    dict(state_key='img1', input_info={'img2': 'pe1_image'},
         across_start=True),
])
def test_scan(kwargs):

    def add(img1, img2):
        return img1 + img2

    dp = es.accumulate(dstar(add), source,
                       output_info=[('img', {
                           'dtype': 'array',
                           'source': 'testing'})],
                       **kwargs)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ins = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    state = None
    for _ in range(2):
        L.clear()
        for a in ins:
            source.emit(a)

        assert_docs = set()
        if not kwargs.get('across_start', False):
            state = None
        for o, l in zip(ins, L):
            assert_docs.add(l[0])
            if l[0] == 'event':
                print(o[1]['seq_num'])
                if state is None:
                    state = o[1]['data']['pe1_image'].copy()
                else:
                    state += o[1]['data']['pe1_image']
                assert_allclose(state, l[1]['data']['img'])
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'
        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


def test_scan_full_event():
    source = Stream()

    def add(i, j):
        return i + j

    dp = es.accumulate(dstar(add), source,
                       state_key='i',
                       input_info={'j': 'seq_num'},
                       output_info=[('total', {
                           'dtype': 'int',
                           'source': 'testing'})],
                       full_event=True)

    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ins = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for a in ins:
        source.emit(a)

    for _ in range(2):
        for a in ins:
            source.emit(a)

        assert_docs = set()
        state = None
        for o, l in zip(ins, L):
            assert_docs.add(l[0])
            if l[0] == 'event':
                if state is None:
                    state = o[1]['seq_num']
                else:
                    state += o[1]['seq_num']
                assert_allclose(state, l[1]['data']['total'])
            if l[0] == 'stop':
                assert l[1]['exit_status'] == 'success'
        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}
    assert_docs = set()
    state = None
    for o, l in zip(ins, L):
        assert_docs.add(l[0])
        if l[0] == 'event':
            if state is None:
                state = o[1]['seq_num']
            else:
                state += o[1]['seq_num']
            assert_allclose(state, l[1]['data']['total'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


@pytest.mark.parametrize('kwargs', [
    dict(state_key='i', input_info={'i': 'pe1_image'}),
    dict(state_key='i', input_info={'i': 'pe1_image', 'i2': 'pe2_image'},)
])
@pytest.mark.xfail()
def test_scan_fail(kwargs):

    def add(img1, img2):
        return img1 + img2

    dp = es.accumulate(dstar(add), source,
                       output_info=[('img', {
                           'dtype': 'array',
                           'source': 'testing'})],
                       **kwargs)
    sa = SinkAssertion()
    sa.expected_docs = {'start', 'descriptor', 'event', 'stop'}
    dp.sink(star(sa))

    ins = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for _ in range(2):
        for a in ins:
            source.emit(a)
