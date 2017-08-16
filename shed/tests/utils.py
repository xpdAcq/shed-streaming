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
import tempfile
from uuid import uuid4

import numpy as np
from bluesky.examples import ReaderWithRegistry, Reader
from bluesky.plans import count


def insert_imgs(RE, reg, n, shape, save_dir=tempfile.mkdtemp(), **kwargs):
    """
    Insert images into mds and fs for testing

    Parameters
    ----------
    RE: bluesky.run_engine.RunEngine instance
    db
    n
    shape
    save_dir

    Returns
    -------

    """
    # Create detectors
    dark_det = ReaderWithRegistry('pe1_image',
                                  {'pe1_image': lambda: np.ones(shape)},
                                  reg=reg, save_path=save_dir)
    light_det = ReaderWithRegistry('pe1_image',
                                   {'pe1_image': lambda: np.ones(shape)},
                                   reg=reg, save_path=save_dir)
    light_i0 = Reader('I0', {'I0': lambda: 5})

    beamtime_uid = str(uuid4())
    base_md = dict(beamtime_uid=beamtime_uid,
                   sample_name='hi', **kwargs)

    # Insert the dark images
    dark_md = base_md.copy()
    dark_md.update(name='test-dark')

    dark_uid = RE(count([dark_det]), **dark_md)

    # Insert the light images
    light_md = base_md.copy()
    light_md.update(name='test', sc_dk_field_uid=dark_uid)
    RE(count([light_det, light_i0], num=n), **light_md)

    return save_dir
