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
from ophyd import sim
from bluesky.plans import count
from bluesky.callbacks.core import CallbackBase


pyFAI_calib = {'calibrant_name': 'Ni24',
               'centerX': 997.79605730878336,
               'centerY': 1005.4468181991356,
               'dSpacing': [2.0345823486199999,
                            1.761935,
                            1.24592214845,
                            1.0625259782900001,
                            1.0172911743099999,
                            0.88100000000000001,
                            0.80846104616000003,
                            0.78799035527100003,
                            0.71933348779700002,
                            0.67819411620799996,
                            0.62296107422500002,
                            0.59566471873299998,
                            0.58733333333299997,
                            0.55719332372200003,
                            0.53740496185200004,
                            0.53126298914600001,
                            0.50864558715599995,
                            0.49345870161099997,
                            0.48869087287399998,
                            0.47091430825000002,
                            0.45878572229600001,
                            0.4405,
                            0.43052512191199999,
                            0.42734777131399998],
               'detector': 'Perkin detector',
               'directDist': 218.82105982728712,
               'dist': 0.21881648512877194,
               'is_pytest': True,
               'pixel1': 0.0002,
               'pixel2': 0.0002,
               'pixelX': 200.0,
               'pixelY': 200.0,
               'poni1': 0.20146140778233776,
               'poni2': 0.20092436456054058,
               'poni_file_name': '/home/timothy/xpdUser/config_base\
                                  /20170822-190241_pyFAI_calib_Ni24.poni',
               'rot1': 0.0062387227662129112,
               'rot2': -0.0017002217339242484,
               'rot3': 2.7628252550568797e-08,
               'splineFile': None,
               'tilt': 0.37048878612364949,
               'tiltPlanRotation': -164.75544250965393,
               'time': '20170822-190241',
               'wavelength': 1.832e-11}


def insert_imgs(RE, reg, n, shape, save_dir=tempfile.mkdtemp(), **kwargs):
    """
    Insert images into mds and fs for testing

    Parameters
    ----------
    RE: bluesky.run_engine.RunEngine instance
    reg: Registry instance
    n: int
        Number of images to take
    shape: tuple of ints
        The shape of the resulting images
    save_dir

    Returns
    -------

    """
    # Create detectors
    dark_det = sim.SynSignalWithRegistry(name='pe1_image',
                                         func=lambda: np.random.random(shape),
                                         reg=reg)
    light_det = sim.SynSignalWithRegistry(name='pe1_image',
                                          func=lambda: np.random.random(shape),
                                          reg=reg)
    beamtime_uid = str(uuid4())
    base_md = dict(beamtime_uid=beamtime_uid,
                   calibration_md=pyFAI_calib,
                   bt_wavelength=0.1847,
                   **kwargs)

    # Insert the dark images
    dark_md = base_md.copy()
    dark_md.update(name='test-dark', is_dark=True)

    dark_uid = RE(count([dark_det], num=1), **dark_md)

    # Insert the light images
    light_md = base_md.copy()
    light_md.update(name='test', sc_dk_field_uid=dark_uid)
    uid = RE(count([light_det], num=n), **light_md)

    return uid
