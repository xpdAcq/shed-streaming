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
from databroker.broker import Broker
import uuid
import os
import tempfile
import time
from uuid import uuid4

import numpy as np


def build_pymongo_backed_broker(request=None):
    """Provide a function level scoped MDS instance talking to
    temporary database on localhost:27017 with v1 schema.

    """
    from metadatastore.mds import MDS
    from filestore.utils import create_test_database
    from filestore.fs import FileStore
    from filestore.handlers import NpyHandler

    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    mds_test_conf = dict(database=db_name, host='localhost',
                         port=27017, timezone='US/Eastern')
    try:
        # nasty details: to save MacOS user
        mds = MDS(mds_test_conf, 1, auth=False)
    except TypeError:
        mds = MDS(mds_test_conf, 1)

    db_name = "fs_testing_base_disposable_{}".format(str(uuid.uuid4()))
    fs_test_conf = create_test_database(host='localhost',
                                        port=27017,
                                        version=1,
                                        db_template=db_name)
    fs = FileStore(fs_test_conf, version=1)
    fs.register_handler('npy', NpyHandler)

    return Broker(mds, fs)


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
    beamtime_uid = str(uuid4())
    base_md = dict(beamtime_uid=beamtime_uid,
                   sample_name='hi', calibration_md=pyFAI_calib, **kwargs)

    # Insert the dark images
    dark_md = base_md.copy()
    dark_md.update(name='test-dark')

    dark_uid = RE(count([dark_det]), **dark_md)

    # Insert the light images
    light_md = base_md.copy()
    light_md.update(name='test', sc_dk_field_uid=dark_uid)
    RE(count([light_det], num=n), **light_md)

    return save_dir
