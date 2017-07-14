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
from numpy.testing import assert_allclose
from streams.core import Stream

import redsky.event_streams as es
from ..event_streams import dstar


def test_map(exp_db, start_uid1):
    source = Stream()

    def add5(img):
        return img + 5

    ii = {'img': 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(dstar(add5),
                source,
                input_info=ii,
                output_info=oi)
    L = dp.sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map', function_name=add5.__name__,
                function_module=add5.__module__,
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi)
    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1]['provenance'] == prov
        if l[0] == 'event':
            assert_allclose(l[1]['data']['image'],
                            s[1]['data']['pe1_image'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_map_full_event(exp_db, start_uid1):
    source = Stream()

    def add5(i):
        return i + 5

    ii = {'i': 'seq_num'}
    oi = [('i', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(dstar(add5),
                source,
                input_info=ii,
                output_info=oi,
                full_event=True)
    L = dp.sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map', function_name=add5.__name__,
                function_module=add5.__module__,
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi,
                full_event=True)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1]['provenance'] == prov
        if l[0] == 'event':
            print(l[1])
            print(s[1]['seq_num'])
            assert_allclose(l[1]['data']['i'],
                            s[1]['seq_num'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_map_stream_input(exp_db, start_uid1):
    source = Stream()

    def add5(img):
        return img + 5

    ii = {'img': ('pe1_image', source)}
    oi = [('img', {'dtype': 'array', 'source': 'testing'})]
    L = es.map(dstar(add5),
               source,
               input_info=ii,
               output_info=oi).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    prov = dict(stream_class='map', function_name=add5.__name__,
                function_module=add5.__module__,
                stream_class_module=es.map.__module__,
                input_info=ii, output_info=oi)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'start':
            assert l[1]['provenance'] == prov
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'], s[1]['data']['pe1_image'] + 5)
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
        assert l[1] != s[1]
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_double_map(exp_db, start_uid1):
    source = Stream()
    source2 = Stream()

    def add_imgs(img1, img2):
        return img1 + img2

    L = es.map(dstar(add_imgs), es.zip(source, source2),
               input_info={'img1': ('pe1_image', 0), 'img2': ('pe1_image', 1)},
               output_info=[
                   ('img',
                    {'dtype': 'array',
                     'source': 'testing'})]).sink_to_list()
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

    L = es.map(dstar(div), source,
               input_info={'img1': 'pe1_image', 'ct': 'I0'},
               output_info=[
                   ('img',
                    {'dtype': 'array',
                     'source': 'testing'})]).sink_to_list()
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


def test_map_fail(exp_db, start_uid1):
    source = Stream()

    def add5(img):
        return img + 5

    ii = {'i': 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(dstar(add5),
                source,
                input_info=ii,
                output_info=oi)
    L = dp.sink_to_list()
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

    L = es.filter(dstar(f), source,
                  input_info={'img1': 'pe1_image'}).sink_to_list()
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


def test_filter_fail(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return img1 is not None

    L = es.filter(dstar(f), source,
                  input_info={'i': 'pe1_image'}).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        assert_docs.add(l[0])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'failure'
    for n in ['start', 'descriptor', 'stop']:
        assert n in assert_docs


def test_filter_full_event(exp_db, start_uid1):
    source = Stream()

    def f(i):
        return i > 1

    L = es.filter(dstar(f), source,
                  input_info={'i': 'seq_num'},
                  full_event=True).sink_to_list()
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

    L = es.accumulate(dstar(add), source,
                      state_key='img1',
                      input_info={'img2': 'pe1_image'},
                      output_info=[('img', {
                          'dtype': 'array',
                          'source': 'testing'})]).sink_to_list()
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


def test_scan_fail(exp_db, start_uid1):
    source = Stream()

    def add(img1, img2):
        return img1 + img2

    L = es.accumulate(dstar(add), source,
                      state_key='i',
                      input_info={'img2': 'pe1_image'},
                      output_info=[('img', {
                          'dtype': 'array',
                          'source': 'testing'})]).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    state = None
    for o, l in zip(exp_db.restream(ih1, fill=True), L):
        assert_docs.add(l[0])
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'failure'
    for n in ['start', 'descriptor', 'stop']:
        assert n in assert_docs


def test_scan_start_func(exp_db, start_uid1):
    source = Stream()

    def add(img1, img2):
        return img1 + img2

    def get_array(img2):
        return img2

    L = es.accumulate(dstar(add), source,
                      start=dstar(get_array),
                      state_key='img1',
                      input_info={'img2': 'pe1_image'},
                      output_info=[('img', {
                          'dtype': 'array',
                          'source': 'testing'})]).sink_to_list()
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

    L = es.accumulate(dstar(add), source,
                      state_key='i',
                      input_info={'j': 'seq_num'},
                      output_info=[('total', {
                          'dtype': 'int',
                          'source': 'testing'})],
                      full_event=True).sink_to_list()
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

    s = es.bundle(source, source2)
    L = s.sink_to_list()

    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s = list(exp_db.restream(ih1))
    s2 = list(exp_db.restream(ih2))
    uids = set([doc['uid'] for name, doc in s] + [doc['uid'] for name, doc in
                                                  s2])
    for b in s2:
        source2.emit(b)
    for a in s:
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
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l1, l2 in L:
        assert_docs.add(l1[0])
        assert l1 != l2
        if l1[0] == 'event':
            assert l2[1]['seq_num'] == 1
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_eventify(exp_db, start_uid1):
    source = Stream()

    L = es.eventify(source, 'name',
                    output_info=[('name', {
                        'dtype': 'str',
                        'source': 'testing'})]).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)

    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        if l[0] == 'event':
            assert l[1]['data']['name'] == 'test'
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_workflow(exp_db, start_uid1):
    def subs(x1, x2):
        return x1 - x2

    hdr = exp_db[start_uid1]

    raw_data = list(hdr.stream(fill=True))
    dark_data = list(exp_db[hdr['start']['sc_dk_field_uid']].stream(fill=True))
    rds = Stream()
    dark_data_stream = Stream()

    z = es.combine_latest(rds, dark_data_stream, emit_on=rds)
    img_stream = es.map(dstar(subs),
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
