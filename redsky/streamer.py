import numpy as np
from uuid import uuid4
import os

doc_name_save_func_mapping = {'start', 'insert_run_start',
                              'descriptor', 'insert_descriptor',
                              'event', 'insert_event',
                              'stop', 'insert_run_stop'}

dnsm = {'img': {'sf': np.save, 'folder': 'bla', 'spec': 'npy', 'ext': '.npy',
                'args': (), 'kwargs': {}}}


def db_store(db, data_name_save_map=None):
    if data_name_save_map is None:
        data_name_save_map = {}

    def wrap(f):
        def wrapped_f(*args, **kwargs):
            name, doc = f(*args, **kwargs)
            if name == 'start':
                db.mds.insert_run_start(**doc)
            elif name == 'descriptor':
                # Mutate the doc here to handle filestore
                for data_name in dnsm:
                    doc[data_name]['external'] = 'FILESTORE'
                    doc[data_name]['dtype'] = 'array'
                db.mds.insert_descriptor(**doc)
            elif name == 'event':
                for data_name, sub_dict in data_name_save_map.items():
                    uid = str(uuid4())
                    # Save the data with the specified function in the
                    # specified location, with the specified spec

                    # 1. Save data on disk from relevant fields
                    save_name = os.path.join(sub_dict['folder'],
                                             uid + sub_dict['ext'])
                    sub_dict['sf'](
                        doc[data_name]['data'][data_name],
                        save_name,
                        *sub_dict['args'],
                        **sub_dict['kwargs'])
                # 2. Tell FS about it
                    fs_res = db.fs.insert_resource(sub_dict['spec'],
                                                   save_name,
                                                   sub_dict['resource_kwargs'])
                    db.fs.insert_datum(fs_res, uid, sub_dict['datum_kwargs'])
                # 3. Replace the array in the doc with the uid
                    doc['data'][data_name] = uid
                db.mds.insert_event(**doc)
            elif name == 'stop':
                db.mds.insert_run_stop(**doc)

        return wrapped_f

    return wrap


def f(name_stream_pair, **kwargs):
    _, start = next(name_stream_pair)
    new_start = {}
    yield 'start', new_start
    _, descriptor = next(name_stream_pair)
    new_descriptor = {}
    yield 'descriptor', new_descriptor
    for name, ev in name_stream_pair:
        if name != 'event':
            break
        mapping = {'img': 'pe1_image'}
        mapped = {k: ev[v] for k, v in mapping.keys()}
        process(**mapped, **kwargs)
        new_event = {}
        yield 'event', new_event
    _, stop = next(name_stream_pair)
    new_stop = {}
    yield 'stop', new_stop
