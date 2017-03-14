##############################################################################
#
# redsky            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
from functools import partial

import numpy as np
from numpy.testing import assert_array_equal

from ..savers import NPYSaver
from ..streamer import event_map, store_dec


def test_streaming(exp_db, tmp_dir, start_uid1):
    dnsm = {'img': partial(NPYSaver, root=tmp_dir)}

    def f(img):
        return img * 2

    dec_f = store_dec(exp_db, dnsm)(
        event_map({'img': {'name': 'primary', 'data_key': 'pe1_image'}},
                  {'data_keys': {'img': {'dtype': 'array'}},
                   'name': 'primary',
                   'returns': ['img'],
                   })(f))

    input_hdr = exp_db[start_uid1]
    a = exp_db.restream(input_hdr, fill=True)
    s = False
    for (name, doc), (_, odoc) in zip(dec_f(img=a),
                                      exp_db.restream(input_hdr, fill=True)):
        if name == 'start':
            assert doc['parents'][0] == input_hdr['start']['uid']
            s = True
        if name == 'event':
            assert s is True
            assert isinstance(doc['data']['img'], np.ndarray)
            assert_array_equal(doc['data']['img'],
                               f(odoc['data']['pe1_image']))
        if name == 'stop':
            assert doc['exit_status'] == 'success'
    for ev1, ev2 in zip(exp_db.get_events(input_hdr, fill=True),
                        exp_db.get_events(exp_db[-1], fill=True)):
        assert_array_equal(f(ev1['data']['pe1_image']),
                           ev2['data']['img'])


def test_double_streaming(exp_db, tmp_dir, start_uid1):
    dnsm = {'pe1_image': partial(NPYSaver, root=tmp_dir)}

    def f(img):
        return img * 2

    dec_f = store_dec(exp_db, dnsm)(
        event_map({'img': {'name': 'primary', 'data_key': 'pe1_image'}},
                  {'data_keys': {'pe1_image': {'dtype': 'array'}},
                   'name': 'primary',
                   'returns': ['pe1_image'],
                   })(f))

    input_hdr = exp_db[start_uid1]
    a = exp_db.restream(input_hdr, fill=True)
    for (name, doc), (_, odoc) in zip(dec_f(img=dec_f(img=a)),
                                      exp_db.restream(input_hdr, fill=True)):
        if name == 'start':
            s = True
        if name == 'event':
            assert s is True
            assert isinstance(doc['data']['pe1_image'], np.ndarray)
            assert_array_equal(doc['data']['pe1_image'],
                               f(f((odoc['data']['pe1_image']))))
        if name == 'stop':
            assert doc['exit_status'] == 'success'
    for ev1, ev2 in zip(exp_db.get_events(input_hdr, fill=True),
                        exp_db.get_events(exp_db[-1], fill=True)):
        assert_array_equal(f(f(ev1['data']['pe1_image'])),
                           ev2['data']['pe1_image'])


def test_multi_stream(exp_db, tmp_dir, start_uid1):
    dnsm = {'img': partial(NPYSaver, root=tmp_dir)}

    def f(img1, img2):
        return img1 - img2

    dec_f = store_dec(exp_db, dnsm)(
        event_map({'img1': {'name': 'primary', 'data_key': 'pe1_image'},
                   'img2': {'name': 'primary', 'data_key': 'pe1_image'}},
                  {'data_keys': {'img': {'dtype': 'array'}},
                   'name': 'primary',
                   'returns': ['img'],
                   })(f))

    input_hdr = exp_db[start_uid1]
    a = exp_db.restream(input_hdr, fill=True)
    b = exp_db.restream(input_hdr, fill=True)
    s = False
    for (name, doc), (_, odoc) in zip(dec_f(img1=a, img2=b),
                                      exp_db.restream(input_hdr, fill=True)):
        if name == 'start':
            assert doc['parents'][0] == input_hdr['start']['uid']
            s = True
        if name == 'event':
            assert s is True
            assert isinstance(doc['data']['img'], np.ndarray)
            assert_array_equal(
                doc['data']['img'],
                f(odoc['data']['pe1_image'], odoc['data']['pe1_image']))
        if name == 'stop':
            assert doc['exit_status'] == 'success'
    for ev1, ev2 in zip(exp_db.get_events(input_hdr, fill=True),
                        exp_db.get_events(exp_db[-1], fill=True)):
        assert_array_equal(f(ev1['data']['pe1_image'],
                             ev1['data']['pe1_image']),
                           ev2['data']['img'])


# TODO: write more tests
"""
2. Test subsampling
3. Test stream combining
"""
