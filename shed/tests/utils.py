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
from bluesky.callbacks.core import CallbackBase


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


class SinkAssertion(CallbackBase):
    def __init__(self, fail=True, expected_docs=None):
        self.fail = fail
        self.docs = []
        if expected_docs is None:
            if fail:
                self.expected_docs = {'start', 'descriptor', 'stop'}
            else:
                self.expected_docs = {'start', 'descriptor', 'event', 'stop'}
        else:
            self.expected_docs = expected_docs

    def __call__(self, name, doc):
        """Dispatch to methods expecting particular doc types."""
        self.docs.append(name)
        return getattr(self, name)(doc)

    def stop(self, doc):
        if self.fail:
            assert doc['exit_status'] == 'failure'
            assert doc.get('reason')
        else:
            assert doc['exit_status']
            if not doc.get('reason', None):
                print(doc.get('reason', None))
            assert not doc.get('reason', None)
        assert self.expected_docs == set(self.docs)
