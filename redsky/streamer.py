"""Module for automatically storing processed data in a databroker"""
##############################################################################
#
# redsky            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
from copy import deepcopy as dc


def db_store_single_resource_single_file(db, fs_data_name_save_map=None):
    """Decorator for adding data to a databroker. This requires all the savers
    to create one resource/datum per file.

    Parameters
    ----------
    db: databroker.Broker instance
        The databroker to store the data in
    fs_data_name_save_map: dict
        The dictionary which maps data names to (Saver, saver_args,
        {saver_kwargs})

    Yields
    -------
    name, doc:
        The name of the document and the document itself
    """
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

                elif name == 'event':
                    # Mutate the doc here to handle filestore
                    for data_name, save_tuple in fs_data_name_save_map.items():
                        # Create instance of Saver
                        s = save_tuple[0](db.fs, *save_tuple[1],
                                          **save_tuple[2])
                        fs_uid = s.write(fs_doc['data'][data_name])
                        fs_doc['data'][data_name] = fs_uid
                        s.close()

                    fs_doc.update(
                        filled={k: False for k in fs_data_name_save_map.keys()}
                    )
                    doc.update(
                        filled={k: True for k in fs_data_name_save_map.keys()})

                # Always stash the (potentially) filestore mutated doc
                db.mds.insert(name, fs_doc)

                # Always yield the pristine doc
                yield name, doc

        return wrapped_f

    return wrap
