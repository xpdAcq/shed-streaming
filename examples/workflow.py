"""Example for Max's Alginate data"""
import shed.event_streams as es
from shed.event_streams import dstar, star
from streamz.core import Stream
import numpy as np
from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D
# from functools import partial
from xpdan.tools import better_mask_img
from databroker.databroker import DataBroker as db
from bluesky.callbacks.broker import LiveImage, LiveSliderImage
from pprint import pprint
from matplotlib.colors import LogNorm
from itertools import islice


def subs(img1, img2):
    return img1 - img2


def add(img1, img2):
    return img1 + img2


def pull_array(img2):
    return img2


def generate_binner(geo, mask):
    img_shape = mask.shape
    r = geo.rArray(img_shape)
    q = geo.qArray(img_shape) / 10
    q_dq = geo.deltaQ(img_shape) / 10

    pixel_size = [getattr(geo, a) for a in ['pixel1', 'pixel2']]
    rres = np.hypot(*pixel_size)
    rbins = np.arange(np.min(r) - rres / 2., np.max(r) + rres / 2., rres / 2.)
    rbinned = BinnedStatistic1D(r.ravel(), statistic=np.max, bins=rbins, )

    qbin_sizes = rbinned(q_dq.ravel())
    qbin_sizes = np.nan_to_num(qbin_sizes)
    qbin = np.cumsum(qbin_sizes)
    return BinnedStatistic1D(q.flatten, bins=qbin, mask=mask.flatten())


def z_score_image(img, binner):
    xy = binner.xy
    for i in np.unique(xy):
        tv = (xy == i)
        img[tv] -= np.mean(img[tv])
        img[tv] /= np.std(img[tv])
    return img


def integrate(img, binner):
    return binner(img)


def polarization_correction(img, geo, polarization_factor=.99):
    return img / geo.polarization(img.shape, polarization_factor)


def div(img, count):
    return img / count


def query_dark(db, docs):
    doc = docs[0]
    return db(uid=doc['sc_dk_field_uid'])


def query_background(db, docs):
    doc = docs[0]
    return db(sample_name='kapton_film_bkgd',
              is_dark={'$exists': False})


def temporal_prox(res, docs):
    doc = docs[0]
    t = doc['time']
    dt_sq = [(t - r['time']) ** 2 for r in res]
    i = dt_sq.index(min(dt_sq))
    return next(islice(dt_sq, i, i + 1))


def load_geo(cal_params):
    from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
    ai = AzimuthalIntegrator()
    ai.setPyFAI(**cal_params)
    return ai


db.add_filter(bt_piLast='Billinge')
hdrs = db(start_time=1496855060.5545955 - 10000)

fg_uids = [h['start']['uid'] for h in hdrs if
           h['start']['sample_name'].startswith(
               'Alginate') and 'sc_dk_field_uid' in h['start'].keys()][0:1]

fg_stream = Stream(name='Foreground')
fg_dark_stream = es.QueryUnpacker(db, es.Query(db, fg_stream,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for FG Dark'))

bg_query_stream = es.Query(db, fg_stream,
                           query_function=query_background,
                           name='Query for Background')

bg_stream = es.QueryUnpacker(db, bg_query_stream)
bg_dark_stream = es.QueryUnpacker(db, es.Query(db, bg_stream,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for BG Dark'))

# Perform dark subtraction on everything
dark_sub_bg = es.map(dstar(subs),
                     es.zip(bg_stream, bg_dark_stream),
                     input_info={'img1': 'pe1_image',
                                 'img2': 'pe1_image'},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})])

# bundle the backgrounds into one stream
bg_bundle = es.BundleSingleStream(dark_sub_bg, bg_query_stream,
                                  name='Background Bundle')

# sum the backgrounds
summed_bg = es.accumulate(dstar(add), bg_bundle, start=dstar(pull_array),
                          state_key='img1',
                          input_info={'img2': 'img'},
                          output_info=[('img', {
                              'dtype': 'array',
                              'source': 'testing'})])


def event_count(x):
    return x['count'] + 1


count_bg = es.accumulate(event_count, bg_bundle, start=1,
                         state_key='count',
                         output_info=[('count', {
                             'dtype': 'int',
                             'source': 'testing'})])

ave_bg = es.map(dstar(div), es.zip(summed_bg, count_bg),
                input_info={'img': 'img', 'count': 'count'},
                output_info=[('img', {
                    'dtype': 'array',
                    'source': 'testing'})], name='Average Background')
ave_bg.sink(pprint)

dark_sub_fg = es.map(dstar(subs),
                     es.zip(fg_stream,
                            fg_dark_stream),
                     input_info={'img1': 'pe1_image',
                                 'img2': 'pe1_image'},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})],
                     name='Dark Subtracted Foreground')

# combine the fg with the summed_bg
fg_bg = es.combine_latest(dark_sub_fg, ave_bg, emit_on=dark_sub_fg)

# subtract the background images
fg_sub_bg = es.map(dstar(subs),
                   fg_bg,
                   input_info={'img1': 'img',
                               'img2': 'img'},
                   output_info=[('img', {'dtype': 'array',
                                         'source': 'testing'})],
                   name='Background Corrected Foreground')
"""
# """
# """
# make/get calibration stream
cal_md_stream = es.Eventify(fg_stream, start_key='calibration_md',
                            output_info=[('calibration_md',
                                          {'dtype': 'dict',
                                           'source': 'workflow'})],
                            md=dict(name='Calibration'))

cal_stream = es.map(load_geo, cal_md_stream,
                    input_info={'cal_params': 'calibration_md'},
                    output_info=[('geo',
                                  {'dtype': 'object', 'source': 'workflow'})])

# polarization correction
pfactor = .87
p_corrected_stream = es.map(polarization_correction,
                            es.zip(fg_sub_bg, cal_stream),
                            input_info={'img': 'img',
                                        'geo': 'geo'},
                            output_info=[('img', {'dtype': 'array',
                                                  'source': 'testing'})],
                            polarization_factor=pfactor)

# generate masks
mask_kwargs = {}
mask_stream = es.map(dstar(better_mask_img),
                     es.combine_latest(p_corrected_stream, cal_stream,
                                       emit_on=fg_sub_bg),
                     input_info={'img': 'img',
                                 'geo': 'geo'},
                     output_info=[('mask', {'dtype': 'array',
                                            'source': 'testing'})],
                     **mask_kwargs)

# generate binner stream
binner_stream = es.map(dstar(generate_binner),
                       es.combine_latest(mask_stream, cal_stream,
                                         emit_on=mask_stream),
                       input_info={'mask': 'mask',
                                   'geo': 'geo'},
                       output_info=[('binner', {'dtype': 'function',
                                                'source': 'testing'})])

# z-score the data
z_score_stream = es.map(dstar(z_score_image),
                        es.zip(p_corrected_stream, binner_stream),
                        input_info={'img': 'img',
                                    'binner': 'binner'},
                        output_info=[('z_score_img', {'dtype': 'array',
                                                      'source': 'testing'})])

# z_score_stream.sink(LiveImage('z_score_img'))

iq_stream = es.map(dstar(integrate),
                   es.zip(p_corrected_stream, binner_stream),
                   input_info={'img': 'img',
                               'binner': 'binner'},
                   output_info=[('iq', {'dtype': 'array',
                                        'source': 'testing'})])
"""
# Actually run on the background data (which is common to all)
for s, u in zip(bg_streams + bg_dark_streams, bg_uids + bg_dark_uids):
    for nd in db.restream(db[u], fill=True):
        # pprint(nd)
        s.emit(nd)
"""
"""
for fg_uid, fg_dark_uid in zip(fg_uids, fg_dark_uids):
    for u, s in zip([db[uid] for uid in [fg_uid, fg_dark_uid]],
                    [fg_stream, fg_dark_stream]):
        for nd in db.restream(u, fill=True):
            s.emit(nd)
# """
iq_stream.visualize()
