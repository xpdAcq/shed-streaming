"""Module for saving data in a FileStore friendly way"""
##############################################################################
#
# redsky            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright, Daniel B. Allan
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################

import numpy as np
from uuid import uuid4
import os


class NPYSaver:
    """
    Instantiating this class creates a new resource. Writing adds datums to
    that resource.
    """

    SPEC = 'npy'
    EXT = '.npy'

    def __init__(self, fs, root):
        self._root = root
        self._closed = False
        self._fs = fs
        self._fp = os.path.join(self._root, str(uuid4()))
        self._resource = self._fs.insert_resource(self.SPEC,
                                                  self._fp + self.EXT,
                                                  resource_kwargs={})
        # Open and stash a file handle (e.g., h5py.File) if applicable.

    def write(self, data):
        """
        Save data to file, generate new datum_id, insert datum, return
        datum_id.
        """
        if self._closed:
            raise RuntimeError('new_resource must be called first')
        np.save(self._fp, data)
        # save the data to self._fp
        datum_id = str(uuid4())
        self._fs.insert_datum(resource=self._resource, datum_id=datum_id,
                              datum_kwargs={})
        return datum_id

    def close(self):
        # make it impossible to accidentally write more datums to this resource
        self._closed = True
        # close file handle, if applicable

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
