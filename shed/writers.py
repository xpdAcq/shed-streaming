from bluesky.callbacks import CallbackBase

import os
import uuid
import numpy as np


class Store(CallbackBase):
    def __init__(self, external_writers=None):
        """Decorate a generator of documents to save them to the databases.
        The input stream of (name, document) pairs passes through unchanged.
        As a side effect, documents are inserted into the databases and
        external files may be written.
        Parameters
        ----------
        external_writers : dict
            Maps data keys to a ``WriterClass``, which is responsible for
            writing data to disk and creating a record in filestore. It will be
            instantiated (possible multiple times) with the argument ``db.fs``.
            If it requires additional arguments, use ``functools.partial`` to
            produce a callable that requires only ``db.fs`` to instantiate the
            ``WriterClass``.
        """
        if external_writers is None:
            external_writers = {}  # {'name': WriterClass}
        self.external_writers = external_writers

    def __call__(self, name, doc):
        new_doc = getattr(self, name)(dict(doc))
        # The mutated fs_doc is inserted into metadatastore.
        new_doc.pop('filled', None)
        new_doc.pop('_name', None)
        return name, new_doc

    def start(self, doc):
        # Make a fresh instance of any WriterClass classes.
        self.writers = self.external_writers
        # self.writers = {data_key: cl(self.db.reg)
        #                 for data_key, cl in self.external_writers.items()}
        return doc

    def descriptor(self, doc):
        # Mutate fs_doc here to mark data as external.
        for data_name in self.external_writers.keys():
            # data doesn't have to exist
            if data_name in doc['data_keys']:
                doc['data_keys'][data_name].update(external='FILESTORE:')
        return doc

    def event(self, doc):
        # We need a selectively deeper copy since we will mutate
        # fs_doc['data'].
        doc['data'] = dict(doc['data'])
        # The writer writes data to an external file, creates a
        # datum record in the filestore database, and return that
        # datum_id. We modify fs_doc in place, replacing the data
        # values with that datum_id.
        for data_key, writer in self.writers.items():
            # data doesn't have to exist
            if data_key in doc['data']:
                fs_uid = writer.write(doc['data'][data_key])
                doc['data'][data_key] = fs_uid

        doc.update(filled={k: False for k in self.external_writers.keys()})
        return doc

    def stop(self, doc):
        for data_key, writer in list(self.writers.items()):
            # self.writer.close()
            # self.writers.pop(data_key)
            pass
        return doc


class NpyWriter:
    """
    Each call to the ``write`` method saves a file and creates a new filestore
    resource and datum record.
    """

    SPEC = 'npy'

    def __init__(self, fs, root):
        self._root = root
        self._closed = False
        self._fs = fs
        # Open and stash a file handle (e.g., h5py.File) if applicable.

    def write(self, data):
        """
        Save data to file, generate and insert new resource and datum.
        """
        if self._closed:
            raise RuntimeError('This writer has been closed.')
        fp = os.path.join(self._root, '{}.npy'.format(str(uuid.uuid4())))
        np.save(fp, data)
        resource = self._fs.insert_resource(self.SPEC, fp, resource_kwargs={})
        datum_id = str(uuid.uuid4())
        self._fs.insert_datum(resource=resource, datum_id=datum_id,
                              datum_kwargs={})
        return datum_id

    def close(self):
        self._closed = True

    def __enter__(self): return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class NpyHandler:
    def __init__(self, fp):
        self._fp = fp

    def __call__(self):
        return np.load(self._fp)
