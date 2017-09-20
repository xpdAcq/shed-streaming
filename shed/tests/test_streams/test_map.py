import numpy as np
import pytest
from numpy.testing import assert_allclose
from streamz import Stream

from shed import event_streams as es
from shed.event_streams import star
from shed.tests.utils import SinkAssertion
from shed.utils import to_event_model

source = Stream()


skv = [
    {'input_info': {'img': 'pe1_image'}},
    {'input_info': {'img': (('data', 'pe1_image',), 0)}},
    {'input_info': {'img': (('data', 'pe1_image',), source)}},
    {'input_info': {'img': 'pe1_image'}, 'adder': 5},
    {'input_info': {0: 'pe1_image'}}
]


@pytest.mark.parametrize(('stream_kwargs', 'stream_args'),
                         zip(skv, [(), (), (), (), (5, )])
                         )
def test_map(stream_kwargs, stream_args):

    def add5(img, adder=5):
        return img + adder

    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    prov = dict(stream_class='map',
                function=dict(function_module=add5.__module__,
                              function_name=add5.__name__),
                stream_class_module=es.map.__module__,
                output_info=oi, input_info=stream_kwargs.get('input_info'))
    dp = es.map(add5,
                source,
                *stream_args,
                output_info=oi, stream_name='test', **stream_kwargs)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for _ in range(2):
        L.clear()
        for a in s:
            source.emit(a)

        assert_docs = set()
        assert len(L) == len(s)
        for (name, doc), (_, raw_doc) in zip(L, s):
            assert_docs.add(name)
            if name == 'event':
                assert_allclose(doc['data']['image'],
                                raw_doc['data']['pe1_image'] + 5)
            if name == 'stop':
                assert doc['exit_status'] == 'success'
                assert doc['provenance'] == prov
            assert doc != raw_doc
        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


@pytest.mark.parametrize('full_event', [False, True])
def test_map_no_input_info(full_event):
    dp = es.map(lambda **x: x, source, full_event=full_event)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for _ in range(2):
        L.clear()
        for a in s:
            source.emit(a)

        assert_docs = set()
        assert len(L) == len(s)
        for (name, doc), (_, raw_doc) in zip(L, s):
            assert_docs.add(name)
            if name == 'event':
                if full_event:
                    assert doc['data'] == raw_doc
                else:
                    assert doc['data'] == raw_doc['data']
            if name == 'stop':
                assert doc['exit_status'] == 'success'
        assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


def test_map_full_event():
    source = Stream()

    def add5(i):
        return i + 5

    ii = {'i': 'seq_num'}
    oi = [('i', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(add5,
                source,
                input_info=ii,
                output_info=oi,
                full_event=True)
    dp.sink(star(SinkAssertion(False)))
    L = dp.sink_to_list()

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map',
                function=dict(function_module=add5.__module__,
                              function_name=add5.__name__),
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi,
                full_event=True)

    assert_docs = set()
    for l, sz in zip(L, s):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['i'],
                            sz[1]['seq_num'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
            assert l[1]['provenance'] == prov
        assert l[1] != sz[1]
    assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


def test_double_map():
    source = Stream()
    source2 = Stream()

    def add_imgs(img1, img2):
        return img1 + img2

    dp = es.map((add_imgs), es.zip(source, source2),
                input_info={'img1': ('pe1_image', 0),
                            'img2': ('pe1_image', 1)},
                output_info=[
                    ('img',
                     {'dtype': 'array',
                      'source': 'testing'})])
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for a in s:
        source.emit(a)
        source2.emit(a)

    assert_docs = set()
    for l, sz in zip(L, s):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'],
                            add_imgs(sz[1]['data']['pe1_image'],
                                     sz[1]['data']['pe1_image']))
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


def test_double_internal_map():
    source = Stream()

    def div(img1, ct):
        return img1 / ct

    dp = es.map(div, source,
                input_info={'img1': 'pe1_image', 'ct': 'I0'},
                output_info=[
                    ('img',
                     {'dtype': 'array',
                      'source': 'testing'})])

    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    s = list(to_event_model(
        [(_, np.random.random((10, 10))) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'}),
                     ('I0', {'dtype': 'float'})]
    ))
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, sz in zip(L, s):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'],
                            div(sz[1]['data']['pe1_image'],
                                sz[1]['data']['I0']))
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    assert set(assert_docs) == {'start', 'descriptor', 'event', 'stop'}


@pytest.mark.xfail(raises=TypeError)
def test_map_fail():
    source = Stream()

    def add5(img):
        return img + 5

    ii = {'i': 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map((add5),
                source,
                input_info=ii,
                output_info=oi)
    dp.sink(star(SinkAssertion()))

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for a in s:
        source.emit(a)


def test_map_fail_dont_except():
    source = Stream()

    def add5(img):
        return img + 5

    ii = {'i': 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(add5,
                source,
                input_info=ii,
                output_info=oi, raise_upon_error=False)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion()))

    s = list(to_event_model(
        [np.random.random((10, 10)) for _ in range(5)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, sz in zip(L, s):
        assert_docs.add(l[0])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'failure'
        assert l[1] != sz[1]
    assert set(assert_docs) == {'start', 'descriptor', 'stop'}
