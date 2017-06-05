from ..streams import Stream
from numpy.testing import assert_allclose
import numpy as np


def test_map(exp_db, start_uid1):
    source = Stream()

    def add5(img):
        return img + 5

    L = source.dstarmap(add5,
                        input_info=[('img', 'pe1_image')],
                        output_info=[('img',
                                      {'dtype': 'array',
                                       'source': 'testing'})]).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'], s[1]['data']['pe1_image'] + 5)


def test_double_map(exp_db, start_uid1):
    source = Stream()
    source2 = Stream()

    def add_imgs(img1, img2):
        return img1 + img2

    L = source.zip(source2).dstarmap(
        add_imgs,
        input_info=[('img1', 'pe1_image'), ('img2', 'pe1_image')],
        output_info=[
            ('img',
             {'dtype': 'array',
              'source': 'testing'})]).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)
        source2.emit(a)
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'],
                            add_imgs(s[1]['data']['pe1_image'],
                                     s[1]['data']['pe1_image']))
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'


def test_filter(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return isinstance(img1, np.ndarray)

    L = source.filter(f).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'], s[1]['data']['pe1_image'])
        if l[0] == 'stop':
            print(l)
            assert l[1]['exit_status'] == 'success'


def test_combine_latest(exp_db, start_uid1, start_uid3):
    source = Stream()
    source2 = Stream()

    L = source.combine_latest(source2).sink_to_list()
    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s = exp_db.restream(ih1, fill=True)
    s2 = exp_db.restream(ih2, fill=True)
    for a in s:
        source.emit(a)
    for a in s2:
        source2.emit(a)
    for l in L:
        print(list(zip(*l))[0])
