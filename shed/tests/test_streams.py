##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2017 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright (CJ-Wright)
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
from numpy.testing import assert_allclose, assert_equal, assert_raises
from streamz.core import Stream

import shed.event_streams as es
from ..event_streams import dstar, star
import pytest
from bluesky.callbacks.core import CallbackBase


class SinkAssertion(CallbackBase):
    def __init__(self, fail=True):
        self.fail = fail
        self.docs = []
        if fail:
            self.expected_docs = {'start', 'descriptor', 'stop'}
        else:
            self.expected_docs = {'start', 'descriptor', 'event', 'stop'}

    def __call__(self, name, doc):
        """Dispatch to methods expecting particular doc types."""
        self.docs.append(name)
        return getattr(self, name)(doc)

    def stop(self, doc):
        if self.fail:
            assert doc['exit_status'] == 'failure'
            assert doc.get('reason')
        else:
            assert doc['exit_status']
            assert not doc.get('reason', None)
        assert self.expected_docs == set(self.docs)


def test_map(exp_db, start_uid1):
    source = Stream()

    def add5(img):
        return img + 5

    ii = {'img': 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(add5,
                source,
                input_info=ii,
                output_info=oi)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map',
                function=dict(function_module=add5.__module__,
                              function_name=add5.__name__),
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi)
    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['image'],
                            s[1]['data']['pe1_image'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
            assert l[1]['provenance'] == prov
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_map_args(exp_db, start_uid1):
    source = Stream()

    from operator import add

    ii = {0: 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(add,
                source,
                5,
                input_info=ii,
                output_info=oi)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map',
                function=dict(function_module=add.__module__,
                              function_name=add.__name__),
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi)
    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['image'],
                            s[1]['data']['pe1_image'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
            assert l[1]['provenance'] == prov
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_map_args_kwargs(exp_db, start_uid1):
    source = Stream()

    def add(img, adder):
        return img + adder

    ii = {0: 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(add,
                source,
                adder=5,
                input_info=ii,
                output_info=oi)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map',
                function=dict(function_module=add.__module__,
                              function_name=add.__name__),
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi)
    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['image'],
                            s[1]['data']['pe1_image'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
            assert l[1]['provenance'] == prov
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_map_two_runs(exp_db, start_uid1):
    source = Stream()

    def add5(img):
        return img + 5

    ii = {'img': 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(add5,
                source,
                input_info=ii,
                output_info=oi)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map',
                function=dict(function_module=add5.__module__,
                              function_name=add5.__name__),
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi)
    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['image'],
                            s[1]['data']['pe1_image'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
            assert l[1]['provenance'] == prov
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs

    L_original = L.copy()
    del L[:]
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s, ll in zip(L, exp_db.restream(ih1, fill=True), L_original):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['image'],
                            ll[1]['data']['image'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
            assert l[1]['provenance'] == ll[1]['provenance']
        assert l[1] != s[1]
        assert_raises(AssertionError, assert_equal, l[1], ll[1])
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_map_full_event(exp_db, start_uid1):
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

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map',
                function=dict(function_module=add5.__module__,
                              function_name=add5.__name__),
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi,
                full_event=True)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            print(l[1])
            print(s[1]['seq_num'])
            assert_allclose(l[1]['data']['i'],
                            s[1]['seq_num'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
            assert l[1]['provenance'] == prov
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_map_stream_input(exp_db, start_uid1):
    source = Stream()

    def add5(img):
        return img + 5

    ii = {'img': ('pe1_image', source)}
    oi = [('img', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map((add5),
                source,
                input_info=ii,
                output_info=oi)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map',
                function=dict(function_module=add5.__module__,
                              function_name=add5.__name__),
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'], s[1]['data']['pe1_image'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
            assert l[1]['provenance'] == prov
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_double_map(exp_db, start_uid1):
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

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)
        source2.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'],
                            add_imgs(s[1]['data']['pe1_image'],
                                     s[1]['data']['pe1_image']))
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_double_internal_map(exp_db, start_uid1):
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

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'], div(s[1]['data']['pe1_image'],
                                                     s[1]['data']['I0']))
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


@pytest.mark.xfail(raises=TypeError)
def test_map_fail(exp_db, start_uid1):
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

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)


def test_map_fail_dont_except(exp_db, start_uid1):
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

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'failure'
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'stop']:
        assert n in assert_docs


def test_filter(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return img1 is not None

    dp = es.filter(f, source,
                   input_info={'img1': 'pe1_image'})
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['pe1_image'],
                            s[1]['data']['pe1_image'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_filter_full_header(exp_db, start_uid1):
    source = Stream()

    def f(docs):
        d = docs[0]
        return d['sample_name'] != 'hi'

    def g(docs):
        d = docs[0]
        return d['sample_name'] == 'hi'

    dp = es.filter(f, source, input_info=None, document_name='start')
    dp2 = es.filter(g, source, input_info=None, document_name='start')

    L = dp.sink_to_list()
    L2 = dp2.sink_to_list()

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert dp.bypass is True
    assert L == []

    assert dp2.bypass is False
    assert L2 != []


def test_filter_args_kwargs(exp_db, start_uid1):
    source = Stream()

    def f(img1, a):
        return img1 is not None and a == 1

    dp = es.filter(f, source,
                   input_info={0: 'pe1_image'}, a=1)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['pe1_image'],
                            s[1]['data']['pe1_image'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_filter_args(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return img1 is not None

    dp = es.filter(f, source,
                   input_info={0: 'pe1_image'})
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert_allclose(l[1]['data']['pe1_image'],
                            s[1]['data']['pe1_image'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


@pytest.mark.xfail(raises=TypeError)
def test_filter_fail(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return img1 is not None

    dp = es.filter(f, source,
                   input_info={'i': 'pe1_image'})
    dp.sink(star(SinkAssertion()))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)


def test_filter_full_event(exp_db, start_uid1):
    source = Stream()

    def f(i):
        return i > 1

    dp = es.filter(f, source,
                   input_info={'i': 'seq_num'},
                   full_event=True)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert l[1]['seq_num'] > 1
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_scan(exp_db, start_uid1):
    source = Stream()

    def add(img1, img2):
        return img1 + img2

    dp = es.accumulate(dstar(add), source,
                       state_key='img1',
                       input_info={'img2': 'pe1_image'},
                       output_info=[('img', {
                           'dtype': 'array',
                           'source': 'testing'})])
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    state = None
    for o, l in zip(exp_db.restream(ih1, fill=True), L):
        assert_docs.add(l[0])
        if l[0] == 'event':
            if state is None:
                state = o[1]['data']['pe1_image']
            else:
                state += o[1]['data']['pe1_image']
            assert_allclose(state, l[1]['data']['img'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


@pytest.mark.xfail(raises=TypeError)
def test_scan_fail(exp_db, start_uid1):
    source = Stream()

    def add(img1, img2):
        return img1 + img2

    dp = es.accumulate(dstar(add), source,
                       state_key='i',
                       input_info={'i': 'pe1_image'},
                       output_info=[('img', {
                           'dtype': 'array',
                           'source': 'testing'})])
    sa = SinkAssertion()
    sa.expected_docs = {'start', 'descriptor', 'event', 'stop'}
    dp.sink(star(sa))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)


def test_scan_start_func(exp_db, start_uid1):
    source = Stream()

    def add(img1, img2):
        return img1 + img2

    def get_array(img2):
        return img2

    dp = es.accumulate(dstar(add), source,
                       start=dstar(get_array),
                       state_key='img1',
                       input_info={'img2': 'pe1_image'},
                       output_info=[('img', {
                           'dtype': 'array',
                           'source': 'testing'})])
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    state = None
    for o, l in zip(exp_db.restream(ih1, fill=True), L):
        assert_docs.add(l[0])
        if l[0] == 'event':
            if state is None:
                state = o[1]['data']['pe1_image']
            else:
                state += o[1]['data']['pe1_image']
            assert_allclose(state, l[1]['data']['img'])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_scan_full_event(exp_db, start_uid1):
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

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    state = None
    for o, l in zip(exp_db.restream(ih1, fill=True), L):
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


def test_zip(exp_db, start_uid1, start_uid3):
    source = Stream()
    source2 = Stream()

    L = es.zip(source, source2).sink_to_list()
    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s = exp_db.restream(ih1)
    s2 = exp_db.restream(ih2)
    for b in s2:
        source2.emit(b)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l1, l2 in L:
        assert_docs.add(l1[0])
        assert l1 != l2
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_bundle(exp_db, start_uid1, start_uid3):
    source = Stream()
    source2 = Stream()

    s = es.Bundle(source, source2)
    s.sink(star(SinkAssertion(False)))
    L = s.sink_to_list()

    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s1 = list(exp_db.restream(ih1))
    s2 = list(exp_db.restream(ih2))
    uids = set([doc['uid'] for name, doc in s1] + [doc['uid'] for name, doc in
                                                   s2])
    for b in s2:
        source2.emit(b)
    for a in s1:
        source.emit(a)
    assert len(L) == len(list(exp_db.get_events(ih1))) + len(
        list(exp_db.get_events(ih2))) + 3

    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        assert l[1]['uid'] not in uids
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_combine_latest(exp_db, start_uid1, start_uid3):
    source = Stream()
    source2 = Stream()

    L = es.combine_latest(source, source2, emit_on=source).sink_to_list()
    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s = exp_db.restream(ih1)
    s2 = exp_db.restream(ih2)
    for b in s2:
        source2.emit(b)
        print(b)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l1, l2 in L:
        assert_docs.add(l1[0])
        assert l1 != l2
        if l1[0] == 'event':
            assert l2[1]['seq_num'] == 2
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_lossless_combine_latest(exp_db, start_uid1, start_uid3):
    source = Stream()
    source2 = Stream()

    L = es.zip_latest(source, source2).sink_to_list()
    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s = exp_db.restream(ih1)
    s2 = exp_db.restream(ih2)
    for b in s2:
        source2.emit(b)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l1, l2 in L:
        assert l1[0] == l2[0]
        assert_docs.add(l1[0])
        assert l1 != l2
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs
    assert len(L) == len(list(exp_db.restream(ih1)))


def test_lossless_combine_latest_reverse(exp_db, start_uid1, start_uid3):
    source = Stream()
    source2 = Stream()

    L = es.zip_latest(source, source2).sink_to_list()
    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s = exp_db.restream(ih1)
    s2 = exp_db.restream(ih2)
    for a in s:
        source.emit(a)
    for b in s2:
        source2.emit(b)

    assert_docs = set()
    for l1, l2 in L:
        assert l1[0] == l2[0]
        assert_docs.add(l1[0])
        assert l1 != l2
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs
    assert len(L) == len(list(exp_db.restream(ih1)))


def test_eventify(exp_db, start_uid1):
    source = Stream()

    dp = es.Eventify(source, 'name', 'name',
                     output_info=[('name', {
                         'dtype': 'str',
                         'source': 'testing'})])
    # try two outputs
    dp2 = es.Eventify(source, 'name', 'name',
                      output_info=[('name', {
                                   'dtype': 'str',
                                   'source': 'testing'}),
                                   ('name2',
                                    {'dtype': 'str', 'source': 'testing'})])
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))
    dp.sink(print)
    L2 = dp2.sink_to_list()
    dp2.sink(star(SinkAssertion(False)))
    dp2.sink(print)

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert len(L) == 4
    assert len(L2) == 4
    assert_docs = set()
    assert_docs2 = set()
    # zip them since we know they're same length and order
    for l, l2 in zip(L, L2):
        assert_docs.add(l[0])
        assert_docs2.add(l2[0])
        if l[0] == 'event':
            assert l[1]['data']['name'] == ['test', 'test']
            assert l2[1]['data']['name'] == 'test'
            assert l2[1]['data']['name2'] == 'test'
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
            assert l2[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_eventify_all(exp_db, start_uid1):
    source = Stream()

    dp = es.Eventify(source)
    L = dp.sink_to_list()
    dp.sink(star(SinkAssertion(False)))
    dp.sink(print)

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert len(L) == 4
    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert l[1]['data']['name'] == 'test'
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_query(exp_db, start_uid1):
    source = es.EventStream()

    def qf(db, docs):
        return db(uid=docs[0]['uid'])

    hdr = exp_db[start_uid1]
    s = hdr.documents()

    dp = es.Query(exp_db, source, qf,
                  query_decider=lambda x, y: next(iter(x)))
    L = dp.sink_to_list()

    dp2 = es.QueryUnpacker(exp_db, dp)
    L2 = dp2.sink_to_list()

    for a in s:
        source.emit(a)

    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert l[1]['data']['hdr_uid'] == start_uid1
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs

    assert_docs = set()
    for l, ll in zip(L2, hdr.documents()):
        assert_docs.add(l[0])
        assert l[0] == ll[0]
        if l[0] is 'start':
            assert l[1] == ll[1]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_query_many_headers(exp_db):
    source = es.EventStream()

    def qf(db, docs):
        return db(sc_dk_field_uid={'$exists': True})

    s = [('start', None)]

    dp = es.Query(exp_db, source, qf)
    L = dp.sink_to_list()

    dp2 = es.QueryUnpacker(exp_db, dp)
    dp2.sink(print)
    L2 = dp2.sink_to_list()

    for a in s:
        source.emit(a)

    assert len(L) == 6
    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1]['n_hdrs'] == 3
        if l[0] == 'event':
            assert l[1]['data']['hdr_uid'] in list(d['start']['uid'] for d in
                                                   qf(exp_db, 'hi'))
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs

    assert_docs = set()
    assert len(L2) == 3 * 3 + 5 + 5 + 2
    for l in L2:
        assert_docs.add(l[0])
        assert l[0]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


@pytest.mark.xfail(raises=RuntimeError)
def test_query_too_many_headers(exp_db):
    source = es.EventStream()

    def qf(db, docs):
        return db(sc_dk_field_uid={'$exists': True})

    s = [('start', None)]

    es.Query(exp_db, source, qf, max_n_hdrs=1)

    for a in s:
        source.emit(a)


def test_bundle_single_stream(exp_db):
    source = es.EventStream()

    def qf(db, docs):
        return db(sc_dk_field_uid={'$exists': True})

    s = [('start', {})]

    dp = es.Query(exp_db, source, qf)

    dp2 = es.QueryUnpacker(exp_db, dp)

    dpf = es.BundleSingleStream(dp2, dp)

    L = dpf.sink_to_list()
    dpf.sink(print)

    for a in s:
        source.emit(a)

    assert_docs = set()
    assert len(L) == 3 + 5 + 5 + 2
    for l in L:
        assert_docs.add(l[0])
        assert l[0]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_bundle_single_stream_control_int(exp_db):
    source = es.EventStream()

    def qf(db, docs):
        return db(sc_dk_field_uid={'$exists': True})

    s = [('start', {})]

    dp = es.Query(exp_db, source, qf)

    dp2 = es.QueryUnpacker(exp_db, dp)

    dpf = es.BundleSingleStream(dp2, 2)

    L = dpf.sink_to_list()
    dpf.sink(print)

    for a in s:
        source.emit(a)

    assert_docs = set()
    assert len(L) == 3 + 5 + 2
    for l in L:
        assert_docs.add(l[0])
        assert l[0]
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_workflow(exp_db, start_uid1):
    def subs(x1, x2):
        return x1 - x2

    hdr = exp_db[start_uid1]

    raw_data = list(hdr.documents(fill=True))
    dark_data = list(
        exp_db[hdr['start']['sc_dk_field_uid']][0].documents(fill=True))
    rds = Stream()
    dark_data_stream = Stream()

    z = es.combine_latest(rds, dark_data_stream, emit_on=rds)
    img_stream = es.map((subs),
                        z,
                        input_info={'x1': 'pe1_image',
                                    'x2': 'pe1_image'},
                        output_info=[('image', {
                            'dtype': 'array',
                            'source': 'testing'})]
                        )
    L = img_stream.sink_to_list()

    for d in dark_data:
        dark_data_stream.emit(d)
    for d in raw_data:
        rds.emit(d)

    assert_docs = set()
    for (n, d) in L:
        assert_docs.add(n)
        # just a smoke test for now
        if n == 'stop':
            assert d['exit_status'] == 'success'
        assert d
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


@pytest.mark.xfail(raises=ValueError)
def test_event_contents_fail(exp_db, start_uid1):
    source = Stream()

    from operator import add

    ii = {1: 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(add,
                source,
                5,
                input_info=ii,
                output_info=oi)
    dp.sink(star(SinkAssertion(True)))

    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)


def test_curate_streams():
    """ Ensure that stream curation works as intended"""
    s = es.EventStream()
    # try both dict and None type
    doc1 = ('start', None)
    doc2 = ('start', {})

    doc3 = (('start', {}), ('start', {}))

    doc4 = (('start', {}), ('start', ({}, {})))

    doc5 = ('start', ({}, {}))

    doc1_curated = s.curate_streams(doc1, False)
    doc2_curated = s.curate_streams(doc2, False)
    doc3_curated = s.curate_streams(doc3, False)
    doc4_curated = s.curate_streams(doc4, False)
    doc5_curated = s.curate_streams(doc5, False)

    # try nesting
    doc1_curated2 = s.curate_streams(doc1_curated, True)
    doc2_curated2 = s.curate_streams(doc2_curated, True)
    doc3_curated2 = s.curate_streams(doc3_curated, True)
    doc4_curated2 = s.curate_streams(doc4_curated, True)
    doc5_curated2 = s.curate_streams(doc5_curated, True)

    assert doc1_curated == ('start', (None,))
    assert doc1_curated2 == ('start', None,)

    assert doc2_curated == ('start', ({},))
    assert doc2_curated2 == ('start', {})

    assert doc3_curated == ('start', ({}, {}))
    assert doc3_curated2 == ('start', ({}, {}))

    assert doc4_curated == ('start', ({}, {}, {}))
    assert doc4_curated2 == ('start', ({}, {}, {}))

    assert doc5_curated == ('start', ({}, {}))
    assert doc5_curated2 == ('start', ({}, {}))


def test_outputinfo_default(exp_db, start_uid1):
    def empty_function(x):
        return None

    def bad_function(x):
        # it doesn't return None or a dict,
        # and when used by stream, output_info is not defined
        return (1,)

    hdr = exp_db[start_uid1]

    raw_data = list(hdr.stream(fill=True))
    s = Stream()
    es.map(empty_function, s, input_info={'x': 'pe1_image'})

    s2 = Stream()
    s2_1 = es.map(bad_function, s2, input_info={'x': 'pe1_image'})
    L = list()
    es.map(L.append, s2_1)

    # should not raise any exception
    for d in raw_data:
        s.emit(d)

    for d in raw_data:
        # the Exception should be raised in the stop document, not in the
        # events themselves
        if d[0] == 'stop':
            with pytest.raises(TypeError):
                s2.emit(d)
        else:
            s2.emit(d)
