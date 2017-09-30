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
# TODO: this should be broken up into a module per node, that way we
# can test individual nodes without having to test everything

import pytest
import shed.event_streams as es
from shed.tests.utils import SinkAssertion
from shed.utils import to_event_model
from streamz.core import Stream

from ..event_streams import star


def test_clear():
    s = es.EventStream(md={'hello': 'world'})
    print(s._initial_state)
    s.md.update({'hello': 'globe'})
    print(s._initial_state)
    assert s.md == {'hello': 'globe'}
    s._clear()
    assert s.md == {'hello': 'world'}


def test_bundle():
    source = Stream()
    source2 = Stream()

    s = es.Bundle(source, source2)
    s.sink(star(SinkAssertion(False)))
    L = s.sink_to_list()

    s1 = list(to_event_model(
        [_ for _ in range(3)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    s2 = list(to_event_model(
        [_ for _ in range(3)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    uids = set([doc['uid'] for name, doc in s1] + [doc['uid'] for name, doc in
                                                   s2])
    for b in s2:
        source2.emit(b)
    for a in s1:
        source.emit(a)
    assert len(L) == len(s1) + len(s2) - 3

    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
        assert l[1]['uid'] not in uids
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_combine_latest():
    source = Stream()
    source2 = Stream()

    L = es.combine_latest(source, source2, emit_on=source).sink_to_list()
    s1 = list(to_event_model(
        [_ for _ in range(3)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    s2 = list(to_event_model(
        [_ for _ in range(2)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    for b in s2:
        source2.emit(b)
        print(b)
    for a in s1:
        source.emit(a)

    assert_docs = set()
    for name, (l1, l2) in L:
        assert_docs.add(name)
        assert l1 != l2
        if name == 'event':
            assert l2['seq_num'] == 2
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
def test_event_contents_fail():
    source = Stream()

    from operator import add

    ii = {1: 'pe1_image'}
    oi = [('image', {'dtype': 'array', 'source': 'testing'})]
    dp = es.map(add,
                source,
                5,
                input_info=ii,
                output_info=oi,
                raise_upon_error=True)
    dp.sink(star(SinkAssertion(True)))

    s = list(to_event_model(
        [_ for _ in range(2)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
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
    s2_1 = es.map(bad_function, s2, input_info={'x': 'pe1_image'},
                  raise_upon_error=True)
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


def test_string_workflow(exp_db, start_uid1):
    st = '{sample_name}/{human_timestamp}_uid={pe1_image}{ext}'
    import datetime

    def _timestampstr(timestamp):
        """ convert timestamp to strftime formate """
        timestring = datetime.datetime.fromtimestamp(
            float(timestamp)).strftime(
            '%Y%m%d-%H%M%S')
        return timestring

    class SafeDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'

    hdr = exp_db[start_uid1]

    source = Stream()

    e = es.Eventify(source)
    ht = es.map(_timestampstr, source, input_info={'timestamp': 'time'},
                full_event=True, output_info=[('human_timestamp',
                                               {'dtype': 'str'})])
    zz = es.zip(source, ht)
    zl = es.zip_latest(zz, e)
    final = es.map(lambda a, **x: a.format_map(SafeDict(**x)),
                   zl, st,
                   output_info=[('filename', {'dtype': 'str'})],
                   ext='.tiff')
    final.sink(print)
    L = final.sink_to_list()
    for nd in hdr.documents():
        source.emit(nd)

    assert_docs = set()
    for n, d in L:
        assert_docs.add(n)
        if n == 'event':
            assert '2017' in d['data']['filename']
            assert 'hi' in d['data']['filename']
            assert '.tiff' in d['data']['filename']
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_split():
    source1 = Stream()
    source2 = Stream()

    z = es.zip(source1, source2)
    uz = es.split(z, 2)
    L1 = uz.split_streams[0].sink_to_list()
    L2 = uz.split_streams[1].sink_to_list()

    s = list(to_event_model(
        [_ for _ in range(3)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))

    for s1, s2 in zip(s, s):
        source1.emit(s1)
        source2.emit(s2)

    assert_docs = set()
    for s, s1, s2 in zip(s, L1, L2):
        assert s == s1
        assert s == s2
        assert_docs.add(s1[0])
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


def test_descriptor_no_output_info(exp_db, start_uid1):
    from ..utils import to_event_model
    s1 = Stream()
    s2 = Stream()

    hdr = exp_db[start_uid1]
    a = to_event_model([1, 2, 3, 4], output_info=[('multiplyer',
                                                   {'dtype': 'int'})])

    z = es.zip(es.Eventify(s1), es.Eventify(s2))
    zz = es.map(lambda **x: x, z)

    L = zz.sink_to_list()
    for y in hdr.documents():
        s1.emit(y)
    for yy in a:
        s2.emit(yy)

    assert_docs = set()
    for l in L:
        assert_docs.add(l[0])
    for n in ['start', 'descriptor', 'event', 'stop']:
        assert n in assert_docs


@pytest.mark.xfail(raises=RuntimeError,
                   reason="Received RunStop before RunStart.")
def test_stop_before_start():
    s = list(to_event_model(
        [_ for _ in range(3)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    source = Stream()
    from operator import add
    es.map(add, source, 1, input_info={0: 'pe1_image'}, raise_upon_error=True)

    s = [s[-1]]
    for _ in range(2):
        for d in s:
            source.emit(d)


@pytest.mark.xfail(raises=RuntimeError,
                   reason="Received EventDescriptor before RunStart")
def test_event_before_start():
    s = list(to_event_model(
        [_ for _ in range(3)],
        output_info=[('pe1_image', {'dtype': 'array'})]
    ))
    source = Stream()
    from operator import add
    es.map(add, source, 1, input_info={0: 'pe1_image'}, raise_upon_error=True)

    s = [s[1]]
    for _ in range(2):
        for d in s:
            source.emit(d)
