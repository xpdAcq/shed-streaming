"""Example for XPD data"""
from itertools import islice
from pprint import pprint

import numpy as np
from bluesky.callbacks.broker import LiveImage
from bluesky.callbacks.core import LiveTable
from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D
from streams.core import Stream
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import shed.event_streams as es
from shed.event_streams import dstar, star

# pull from local data, not needed at beamline
from portable_fs.sqlite.fs import FileStoreRO
from portable_mds.sqlite.mds import MDSRO
from databroker.broker import Broker
from databroker.resource_registry.handlers import AreaDetectorTiffHandler, \
    DebugHandler
import tzlocal
import os
from pprint import pprint
from xpdan.tools import better_mask_img

d = {'directory': '/home/christopher/live_demo_data',
     'timezone': tzlocal.get_localzone().zone,
     'dbpath': os.path.join('/home/christopher/live_demo_data', 'filestore')}
mds = MDSRO(d, version=1)
fs = FileStoreRO(d, version=1)
fs.register_handler('AD_TIFF', AreaDetectorTiffHandler)
db = Broker(mds=mds, fs=fs)


# def better_mask_img(geo, img, binner):
#     pass


def iq_to_pdf(stuff):
    pass


def LiveWaterfall(stuff):
    pass


def refine_structure(stuff):
    pass


def LiveStructure(stuff):
    pass


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
    return db(sample_name=doc['bkgd_sample_name'],
              is_dark={'$exists': False})


def temporal_prox(res, docs):
    doc = docs[0]
    t = doc['time']
    # print(t)
    dt_sq = [(t - r['start']['time']) ** 2 for r in res]
    i = dt_sq.index(min(dt_sq))
    min_r = next(islice(res, i, i + 1))
    # print(min_r['start']['time'])
    return min_r


def load_geo(cal_params):
    from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
    ai = AzimuthalIntegrator()
    ai.setPyFAI(**cal_params)
    return ai


def event_count(x):
    return x['count'] + 1


def SinkToDB(x):
    pass


def StubSinkToDB(x):
    pass


def live_image_factory(field='pe1_image', window_title='Raw'):
    return LiveImage(field,
                     limit_func=lambda x: (np.max(x) * .1, np.max(x) * .01),
                     # norm=LogNorm(vmin=1, vmax=1000),
                     cmap='viridis', window_title=window_title)


source = Stream(name='Foreground')
# source.sink(star(live_image_factory()))
# source.sink(star(
#     LiveTable(['temperature_setpoint', 'temperature', 'pe1_stats1_total'])))

fg_dark_stream = es.QueryUnpacker(db, es.Query(db, source,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for FG Dark'))
# fg_dark_stream.sink(star(live_image_factory()))

bg_query_stream = es.Query(db, source,
                           query_function=query_background,
                           query_decider=temporal_prox,
                           name='Query for Background')

bg_stream = es.QueryUnpacker(db, bg_query_stream)
# bg_stream.sink(star(live_image_factory(window_title='Raw Background')))
bg_dark_stream = es.QueryUnpacker(db, es.Query(db, bg_stream,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for BG Dark'))

# bg_dark_stream.sink(pprint)
# Perform dark subtraction on everything
dark_sub_bg = es.map(dstar(subs),
                     es.zip(bg_stream, bg_dark_stream),
                     input_info={'img1': ('pe1_image', 0),
                                 'img2': ('pe1_image', 1)},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})])
# dark_sub_bg.sink(pprint)
# dark_sub_bg.sink(star(LiveImage('img',
#                                 limit_func=lambda x: (
#                                 np.max(x) * .1, np.max(x) * .01),
#                                 cmap='viridis',
#                                 window_title='Dark Corrected Background')))
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

count_bg = es.accumulate(event_count, bg_bundle, start=1,
                         state_key='count',
                         output_info=[('count', {
                             'dtype': 'int',
                             'source': 'testing'})])

ave_bg = es.map(dstar(div), es.zip(summed_bg, count_bg),
                input_info={'img': ('img', 0), 'count': ('count', 1)},
                output_info=[('img', {
                    'dtype': 'array',
                    'source': 'testing'})],
                # name='Average Background'
                )
# ave_bg.sink(pprint)
# ave_bg.sink(star(LiveImage('img',
#                                 limit_func=lambda x: (
#                                 np.max(x) * .1, np.max(x) * .01),
#                                 cmap='viridis',
#                                 window_title='Average Background')))

dark_sub_fg = es.map(dstar(subs),
                     es.zip(source,
                            fg_dark_stream),
                     input_info={'img1': ('pe1_image', 0),
                                 'img2': ('pe1_image', 1)},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})],
                     # name='Dark Subtracted Foreground'
                     )

# dark_sub_fg.sink(star(LiveImage('img',
#                                 limit_func=lambda x: (
#                                 np.max(x) * .1, np.max(x) * .01),
#                                 cmap='viridis',
#                                 window_title='Dark Corrected Foreground')))
# combine the fg with the summed_bg
fg_bg = es.combine_latest(dark_sub_fg, ave_bg, emit_on=dark_sub_fg)

# subtract the background images
fg_sub_bg = es.map(dstar(subs),
                   fg_bg,
                   input_info={'img1': ('img', 0),
                               'img2': ('img', 1)},
                   output_info=[('img', {'dtype': 'array',
                                         'source': 'testing'})],
                   # name='Background Corrected Foreground'
                   )

# fg_sub_bg.sink(star(LiveImage('img',
#                                 limit_func=lambda x: (
#                                 np.max(x) * .1, np.max(x) * .01),
#                                 cmap='viridis',
#                                 window_title='Background Corrected Foreground')))
# fg_sub_bg.sink(SinkToDB)
# fg_sub_bg.sink(pprint)

# make/get calibration stream
cal_md_stream = es.Eventify(source, start_key='calibration_md',
                            output_info=[('calibration_md',
                                          {'dtype': 'dict',
                                           'source': 'workflow'})],
                            md=dict(name='Calibration'))
cal_stream = es.map(dstar(load_geo), cal_md_stream,
                    input_info={'cal_params': 'calibration_md'},
                    output_info=[('geo',
                                  {'dtype': 'object', 'source': 'workflow'})])
# cal_stream.sink(pprint)

# polarization correction
# SPLIT INTO TWO NODES
pfactor = .99
z = es.combine_latest(fg_sub_bg, cal_stream, emit_on=fg_sub_bg)
z.sink(lambda x: print(x[0][0]))
"""
p_corrected_stream = es.map(dstar(polarization_correction),
                            z,
                            input_info={'img': ('img', 0),
                                        'geo': ('geo', 1)},
                            output_info=[('img', {'dtype': 'array',
                                                  'source': 'testing'})],
                            polarization_factor=pfactor)

p_corrected_stream.sink(pprint)
"""
"""
p_corrected_stream.sink(star(LiveImage('img',
                                limit_func=lambda x: (
                                np.max(x) * .1, np.max(x) * .01),
                                cmap='viridis',
                                window_title='Polarization Corrected Foreground')))
# fg_sub_bg.sink(StubSinkToDB)

# generate masks
mask_kwargs = {'bs_width': None}
mask_stream = es.map(dstar(better_mask_img),
                     es.zip(p_corrected_stream, cal_stream,
                                       # emit_on=p_corrected_stream
                            ),
                     input_info={'img': ('img', 0),
                                 'geo': ('geo', 1)},
                     output_info=[('mask', {'dtype': 'array',
                                            'source': 'testing'})],
                     **mask_kwargs)
mask_stream.sink(pprint)
mask_stream.sink(star(LiveImage('mask',
                                # limit_func=lambda x: (
                                # np.max(x) * .1, np.max(x) * .01),
                                # cmap='viridis',
                                window_title='Mask')))
# generate binner stream
binner_stream = es.map(dstar(generate_binner),
                       es.combine_latest(mask_stream, cal_stream,
                                         emit_on=mask_stream),
                       input_info={'mask': 'mask',
                                   'geo': 'geo'},
                       output_info=[('binner', {'dtype': 'function',
                                                'source': 'testing'})])

# binner_stream.sink(StubSinkToDB)

# z-score the data
z_score_stream = es.map(dstar(z_score_image),
                        es.zip(p_corrected_stream, binner_stream),
                        input_info={'img': 'img',
                                    'binner': 'binner'},
                        output_info=[('z_score_img', {'dtype': 'array',
                                                      'source': 'testing'})])

z_score_stream.sink(LiveImage('z_score_img'))

iq_stream = es.map(dstar(integrate),
                   es.zip(p_corrected_stream, binner_stream),
                   input_info={'img': 'img',
                               'binner': 'binner'},
                   output_info=[('iq', {'dtype': 'array',
                                        'source': 'testing'})])
iq_stream.sink(star(LiveWaterfall))
# iq_stream.sink(SinkToDB)


pdf_stream = es.map(dstar(iq_to_pdf), es.zip(iq_stream, source))
pdf_stream.sink(star(LiveWaterfall))

structure = es.map(dstar(refine_structure), es.zip(pdf_stream, source))
structure.sink(LiveStructure)

# source.visualize('mystream.png',
#                  arrowsize='0.6', arrowhead='vee',
#                  center='true',
#                  margin='0.2',
#                  nodesep='0.1',
#                  ranksep='0.1')

# """
for e in db[-1].stream(fill=True):
    # plt.pause(1)
    # print(e)
    source.emit(e)

input('finished run')