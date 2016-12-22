from copy import deepcopy as dc


def db_store_single_resource_single_file(db, fs_data_name_save_map=None):
    if fs_data_name_save_map is None:
        fs_data_name_save_map = {}  # {'name': (SaverClass, args, kwargs)}

    def wrap(f):
        def wrapped_f(*args, **kwargs):
            gen = f(*args, **kwargs)
            for name, doc in gen:
                fs_doc = dc(doc)

                if name == 'descriptor':
                    # Mutate the doc here to handle filestore
                    for data_name in fs_data_name_save_map.keys():
                        fs_doc['data_keys'][data_name].update(
                            external='FILESTORE:')
                    fs_doc.update(
                        filled={k: False for k in fs_data_name_save_map.keys()}
                    )

                    doc.update(
                        filled={k: True for k in fs_data_name_save_map.keys()})

                elif name == 'event':
                    # Mutate the doc here to handle filestore
                    for data_name, save_tuple in fs_data_name_save_map.items():
                        # Create instance of Saver
                        s = save_tuple[0](db.fs, *save_tuple[1],
                                          **save_tuple[2])
                        fs_uid = s.write(fs_doc['data'][data_name])
                        fs_doc['data'][data_name] = fs_uid

                # Always stash the (potentially) filestore mutated doc
                db.mds.insert(name, fs_doc)

                # Always yield the pristine doc
                yield name, doc

        return wrapped_f

    return wrap
