"""Example for Max's Alginate data"""
import redsky.event_streams as es
from redsky.event_streams import dstar
from streams.core import Stream
import numpy as np
from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D
from functools import partial
from xpdan.tools import better_mask_img
from databroker.databroker import DataBroker as db


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


db.add_filter(bt_piLast='Billinge')
hdrs = db(start_time='2017-06-01')

# Get all the relevant headers
bg_uids = [h['start']['uid'] for h in hdrs if 'Kapton_film' in h['start']['sample_name']]
fg_uids = [h['start']['uid'] for h in hdrs if 'Alginate' in h['start']['sample_name']]

# And the darks
bg_dark_uids = [db[db[uid]['sp_dark_uid']] for uid in bg_uids]
fg_dark_uids = [db[db[uid]['sp_dark_uid']] for uid in fg_uids]

# Create streams for all the data sets
bg_streams = [Stream() for uid in bg_uids]
bg_dark_streams = [Stream() for uid in bg_dark_uids]

# Perform dark subtraction on everything
dark_sub_bg = [es.map(dstar(subs),
                      es.zip(bg_stream,
                             bg_dark_stream,
                             input_info=[('img1', 'pe1_image'),
                                         ('img2', 'pe1_image')],
                             output_info=[('img', {'dtype': 'array',
                                                   'source': 'testing'})]))
               for bg_stream,
                   bg_dark_stream in zip(bg_streams, bg_dark_streams)]

# bundle the backgrounds into one stream
bg_bundle = es.bundle(*dark_sub_bg)

# sum the backgrounds
summed_bg = es.scan(dstar(add), bg_bundle, start=dstar(pull_array),
                    state_key='img1',
                    input_info=[('img2', 'pe1_image')],
                    output_info=[('img', {
                        'dtype': 'array',
                        'source': 'testing'})])

# Actually run on the background data (which is common to all)
for s, h in zip(bg_streams + bg_dark_streams, bg_uids + bg_dark_uids):
    for nd in db.restream(h, fill=True):
        s.emit(nd)

# Create the foreground half
fg_stream = Stream()
fg_dark_stream = Stream()

dark_sub_fg = es.map(dstar(subs),
                     es.zip(fg_stream,
                            fg_dark_stream,
                            input_info=[('img1', 'pe1_image'),
                                        ('img2', 'pe1_image')],
                            output_info=[('img', {'dtype': 'array',
                                                  'source': 'testing'})]))
# combine the fg with the summed_bg
fg_bg = es.combine_latest(dark_sub_fg, summed_bg, emit_on=dark_sub_fg)

# subtract the background images
fg_sub_bg = es.map(dstar(subs),
                   fg_bg,
                   input_info=[('img1', 'img'),
                               ('img2', 'img')],
                   output_info=[('img', {'dtype': 'array',
                                         'source': 'testing'})])

# make/get calibration stream
cal_stream = Stream()

# polarization correction
pfactor = .87
ppolarization_correction = partial(polarization_correction,
                                   polarization_factor=pfactor)
p_corrected_stream = es.map(ppolarization_correction,
                            es.zip(fg_sub_bg, cal_stream),
                            input_info=[('img', 'img'),
                                        ('geo', 'geo')],
                            output_info=[('img', {'dtype': 'array',
                                                  'source': 'testing'})])

# generate masks
mask_kwargs = {}
pbetter_mask_img = partial(better_mask_img, **mask_kwargs)
mask_stream = es.map(dstar(pbetter_mask_img),
                     es.combine_latest(p_corrected_stream, cal_stream,
                                       emit_on=fg_sub_bg),
                     input_info=[('img', 'img'),
                                 ('geo', 'geo')],
                     output_info=[('mask', {'dtype': 'array',
                                            'source': 'testing'})])

# generate binner stream
binner_stream = es.map(dstar(generate_binner),
                       es.combine_latest(mask_stream, cal_stream,
                                         emit_on=mask_stream),
                       input_info=[('mask', 'mask'),
                                   ('geo', 'geo')],
                       output_info=[('binner', {'dtype': 'function',
                                                'source': 'testing'})])

# z-score the data
z_score_stream = es.map(dstar(z_score_image),
                        es.zip(p_corrected_stream, binner_stream),
                        input_info=[('img', 'img'),
                                    ('binner', 'binner')],
                        output_info=[('z_score_img', {'dtype': 'array',
                                                      'source': 'testing'})])

iq_stream = es.map(dstar(integrate),
                   es.zip(p_corrected_stream, binner_stream),
                   input_info=[('img', 'img'),
                               ('binner', 'binner')],
                   output_info=[('iq', {'dtype': 'array',
                                        'source': 'testing'})])

for fg_uid, fg_dark_uid in zip(fg_uids, fg_dark_uids):
    for h, s in zip([db[uid] for uid in [fg_uid, fg_dark_uid]],
                    [fg_stream, fg_dark_stream]):
        for nd in db.restream(h, fill=True):
            s.emit(nd)
