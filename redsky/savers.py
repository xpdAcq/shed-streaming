import numpy as np
from uuid import uuid4
import os


def np_saver(data_name, fs_doc, folder, fs):
    """Save data in the numpy format

    Parameters
    ----------
    data_name: str
        The name of the data, also a key in ``fs_doc``
    fs_doc: dict
        The document with the data. This will be mutated with the filestore
        results
    folder: str
        A valid filepath to a folder to store all the data in
    fs: filestore.FileStore instance
        The filestore to store all the data with

    Returns
    -------
    fs_doc: dict
        The mutated document ready for insertion into metadatastore
    """
    # Save the data with the specified function in the
    # specified location, with the specified spec
    fs_uid = str(uuid4())

    # 1. Save data on disk
    save_name = os.path.join(folder, fs_uid + '.npy')

    np.save(save_name, fs_doc['data'][data_name])

    # 2. Tell FS about it
    fs_res = fs.insert_resource('npy', save_name, {})

    fs.insert_datum(fs_res, fs_uid, {})

    # 3. Replace the array in the fs_doc with the uid
    fs_doc['data'][data_name] = fs_uid
    return fs_doc


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
        self._resource = self._fs.insert_resource(self.SPEC, self._fp+self.EXT,
                                                  resource_kwargs={})
        # Open and stash a file handle (e.g., h5py.File) if applicable.

    def write(self, data):
        """Save data to file, generate new datum_id, insert datum,
        return datum_id."""
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
