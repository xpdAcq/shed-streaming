"""Example for Max's Alginate data"""
import redsky.event_streams as es
from redsky.event_streams import dstar
from streams.core import Stream
import numpy as np
from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D
from functools import partial
from xpdan.tools import better_mask_img


def subs(img1, img2):
    return img1 - img2


def add(img1, img2):
    return img1 + img2


def make_empty_array(img2):
    return np.empty(img2.shape)


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

# Get all the relevent headers
bg_uids = []
fg_uids = []

# And the darks
bg_dark_uids = []
fg_dark_uids = []

# Create streams for all the data sets
bg_streams = [Stream() for uid in bg_uids]
fg_stream = Stream()

bg_dark_streams = [Stream() for uid in bg_dark_uids]
fg_dark_stream = Stream()

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

dark_sub_fg = es.map(dstar(subs),
                     es.zip(fg_stream,
                            fg_dark_stream,
                            input_info=[('img1', 'pe1_image'),
                                        ('img2', 'pe1_image')],
                            output_info=[('img', {'dtype': 'array',
                                                  'source': 'testing'})]))

# bundle the backgrounds into one stream
bg_bundle = es.bundle(*dark_sub_bg)

# sum the backgrounds
summed_bg = es.scan(dstar(add), bg_bundle, start=dstar(make_empty_array),
                    state_key='img1',
                    input_info=[('img2', 'pe1_image')],
                    output_info=[('img', {
                        'dtype': 'array',
                        'source': 'testing'})])

# combine the fg with the summed_bg
fg_bg = es.combine_latest(dark_sub_fg, dark_sub_bg, emit_on=dark_sub_fg)

# subtract the background images
fg_sub_bg = es.map(dstar(subs),
                   fg_bg,
                   input_info=[('img1', 'img'),
                               ('img2', 'img')],
                   output_info=[('img', {'dtype': 'array',
                                         'source': 'testing'})])

# make/get calibration stream
cal_stream = Stream()

# generate masks
mask_kwargs = {}
pbetter_mask_img = partial(better_mask_img, **mask_kwargs)
mask_stream = es.map(dstar(pbetter_mask_img),
                     es.combine_latest(fg_sub_bg, cal_stream,
                                       emit_on=fg_sub_bg),
                     input_info=[('img', 'img'),
                                 ('geo', 'geo')],
                     output_info=[('mask', {'dtype': 'array',
                                           'source': 'testing'})])


# generate binner stream
binner_stream = es.map(dstar(generate_binner), es.zip(mask_stream, ))
