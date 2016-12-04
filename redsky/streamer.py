from uuid import uuid4
import os
from copy import deepcopy as dc


def db_store(db, fs_data_name_save_map=None):
    if fs_data_name_save_map is None:
        fs_data_name_save_map = {}

    def wrap(f):
        def wrapped_f(*args, **kwargs):
            gen = f(*args, **kwargs)
            for name, doc in gen:
                fs_doc = dc(doc)

                if name == 'descriptor':
                    # Mutate the doc here to handle filestore
                    for data_name in fs_data_name_save_map.keys():
                        fs_doc['data_keys'][data_name].update(
                            external='FILESTORE:',
                            dtype='array')

                elif name == 'event':
                    # Mutate the doc here to handle filestore
                    for data_name, sub_dict in fs_data_name_save_map.items():
                        # Save the data with the specified function in the
                        # specified location, with the specified spec
                        fs_uid = str(uuid4())

                        # 1. Save data on disk
                        save_name = os.path.join(sub_dict['folder'],
                                                 fs_uid + sub_dict['ext'])

                        sub_dict['sf'](save_name, fs_doc['data'][data_name])

                        # 2. Tell FS about it
                        fs_res = db.fs.insert_resource(
                            sub_dict['spec'], save_name,
                            sub_dict['resource_kwargs'])

                        db.fs.insert_datum(fs_res, fs_uid,
                                           sub_dict['datum_kwargs'])

                        # 3. Replace the array in the fs_doc with the uid
                        fs_doc['data'][data_name] = fs_uid
                    doc.update(
                        filled={k: True for k in fs_data_name_save_map.keys()})

                # Always stash the (potentially) filestore mutated doc
                db.mds.insert(name, fs_doc)

                # Always yield the pristine doc
                yield name, doc

        return wrapped_f

    return wrap
