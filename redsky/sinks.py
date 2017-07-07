"""Module for automatically storing processed data in a databroker"""


##############################################################################
#
# redsky            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2017 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################


class StoreSink(object):
    """Sink of documents to save them to the databases.

    The input stream of (name, document) pairs passes through unchanged.
    As a side effect, documents are inserted into the databases and external
    files may be written.

    Parameters
    ----------
    db: ``databroker.Broker`` instance
        The databroker to store the documents in, must have writeable
        metadatastore and writeable filestore if ``external`` is not empty.
    external_writers : dict
        Maps data keys to a ``WriterClass``, which is responsible for writing
        data to disk and creating a record in filestore. It will be
        instantiated (possible multiple times) with the argument ``db.fs``.
        If it requires additional arguments, use ``functools.partial`` to
        produce a callable that requires only ``db.fs`` to instantiate the
        ``WriterClass``.
        """

    def __init__(self, db, external_writers=None):
        self.db = db
        if external_writers is None:
            self.external_writers = {}  # {'name': WriterClass}
        else:
            self.external_writers = external_writers
        self.writers = None

    def __call__(self, nd_pair):
        name, doc = nd_pair
        name, doc, fs_doc = getattr(self, name)(doc)
        # The mutated fs_doc is inserted into metadatastore.
        fs_doc.pop('filled', None)
        fs_doc.pop('_name', None)
        self.db.mds.insert(name, fs_doc)

        # The pristine doc is yielded.
        return name, doc

    def event(self, doc):
        fs_doc = dict(doc)
        # We need a selectively deeper copy since we will mutate
        # fs_doc['data'].
        fs_doc['data'] = dict(fs_doc['data'])
        # The writer writes data to an external file, creates a
        # datum record in the filestore database, and return that
        # datum_id. We modify fs_doc in place, replacing the data
        # values with that datum_id.
        for data_key, writer in self.writers.items():
            # data doesn't have to exist
            if data_key in fs_doc['data']:
                fs_uid = writer.write(fs_doc['data'][data_key])
                fs_doc['data'][data_key] = fs_uid

        doc.update(
            filled={k: False for k in self.external_writers.keys()})
        return 'event', doc, fs_doc

    def descriptor(self, doc):
        fs_doc = dict(doc)
        # Mutate fs_doc here to mark data as external.
        for data_name in self.external_writers.keys():
            # data doesn't have to exist
            if data_name in fs_doc['data_keys']:
                fs_doc['data_keys'][data_name].update(
                    external='FILESTORE:')
        return 'descriptor', doc, fs_doc

    def start(self, doc):
        # Make a fresh instance of any WriterClass classes.
        self.writers = {data_key: cl(self.db.fs)
                        for data_key, cl in self.external_writers.items()}
        return 'start', doc, doc

    def stop(self, doc):
        for data_key, writer in list(self.writers.items()):
            writer.close()
            self.writers.pop(data_key)
        return 'stop', doc, doc
