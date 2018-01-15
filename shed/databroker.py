"""Nodes for adding data to the databroker"""
from streamz_ext import Stream


class AssetInsert(Stream):
    def __init__(self, upstream, fs, root, *,
                 stream_name=None, external_writers=None):
        super().__init__(upstream=upstream, stream_name=stream_name)
        self.root = root
        self.fs = fs
        if external_writers is None:
            external_writers = {}
        self.external_writers = external_writers
        self.writers = None

    def update(self, x, who=None):
        name, doc = x
        fs_doc = getattr(self, name)(doc)
        fs_doc.pop('filled', None)
        fs_doc.pop('_name', None)
        self._emit((name, fs_doc))

    def start(self, doc):
        # Make a fresh instance of any WriterClass classes.
        self.writers = {data_key: cl(self.fs, self.root)
                        for data_key, cl in self.external_writers.items()}
        return doc

    def descriptor(self, doc):
        fs_doc = dict(doc)
        fs_doc['data_keys'] = dict(doc['data_keys'])
        # Mutate fs_doc here to mark data as external.
        for data_name in self.external_writers.keys():
            # data doesn't have to exist
            if data_name in fs_doc['data_keys']:
                fs_doc['data_keys'][data_name] = dict(
                    doc['data_keys'][data_name])
                fs_doc['data_keys'][data_name].update(
                    external='FILESTORE:')
        return fs_doc

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

        fs_doc.update(
            filled={k: fs_doc['data'][k] for k in
                    self.external_writers.keys()})
        return fs_doc

    def stop(self, doc):
        for data_key, writer in list(self.writers.items()):
            writer.close()
            self.writers.pop(data_key)
        return doc
