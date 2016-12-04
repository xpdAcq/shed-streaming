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
