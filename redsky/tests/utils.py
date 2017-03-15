from databroker.broker import Broker
import uuid
import os
import tempfile
import time
from uuid import uuid4

import numpy as np


def build_pymongo_backed_broker(request=None):
    """Provide a function level scoped MDS instance talking to
    temporary database on localhost:27017 with v1 schema.

    """
    from metadatastore.mds import MDS
    from filestore.utils import create_test_database
    from filestore.fs import FileStore
    from filestore.handlers import NpyHandler

    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    mds_test_conf = dict(database=db_name, host='localhost',
                         port=27017, timezone='US/Eastern')
    try:
        # nasty details: to save MacOS user
        mds = MDS(mds_test_conf, 1, auth=False)
    except TypeError:
        mds = MDS(mds_test_conf, 1)

    db_name = "fs_testing_base_disposable_{}".format(str(uuid.uuid4()))
    fs_test_conf = create_test_database(host='localhost',
                                        port=27017,
                                        version=1,
                                        db_template=db_name)
    fs = FileStore(fs_test_conf, version=1)
    fs.register_handler('npy', NpyHandler)

    return Broker(mds, fs)


def insert_imgs(mds, fs, n, shape, save_dir=tempfile.mkdtemp(),
                start_uid=None):
    """
    Insert images into mds and fs for testing

    Parameters
    ----------
    mds
    fs
    n
    shape
    save_dir

    Returns
    -------

    """
    # Insert the dark images
    dark_img = np.ones(shape)
    sc_dk_field_uid = str(uuid4())
    run_start = mds.insert_run_start(uid=sc_dk_field_uid, time=time.time(),
                                     name='test-dark',
                                     dark_frame=True, )
    data_keys = {
        'pe1_image': dict(source='testing', external='FILESTORE:',
                          dtype='array')}
    data_hdr = dict(run_start=run_start,
                    data_keys=data_keys,
                    time=time.time(), uid=str(uuid4()))
    descriptor = mds.insert_descriptor(**data_hdr)
    for i, img in enumerate([dark_img]):
        fs_uid = str(uuid4())
        fn = os.path.join(save_dir, fs_uid + '.npy')
        np.save(fn, img)
        # insert into FS
        fs_res = fs.insert_resource('npy', fn, resource_kwargs={})
        fs.insert_datum(fs_res, fs_uid, datum_kwargs={})
        mds.insert_event(
            descriptor=descriptor,
            uid=str(uuid4()),
            time=time.time(),
            data={'pe1_image': fs_uid},
            timestamps={'pe1_image': time.time()},
            seq_num=i)
    mds.insert_run_stop(run_start=run_start,
                        uid=str(uuid4()),
                        time=time.time())

    imgs = [np.ones(shape)] * n
    if start_uid:
        run_start = mds.insert_run_start(uid=start_uid, time=time.time(),
                                         name='test',
                                         sc_dk_field_uid=sc_dk_field_uid)
    else:
        run_start = mds.insert_run_start(uid=str(uuid4()), time=time.time(),
                                         name='test',
                                         sc_dk_field_uid=sc_dk_field_uid)
    data_keys = {
        'pe1_image': dict(source='testing', external='FILESTORE:',
                          dtype='array')}
    data_hdr = dict(run_start=run_start,
                    data_keys=data_keys,
                    name='primary',
                    time=time.time(), uid=str(uuid4()))
    descriptor = mds.insert_descriptor(**data_hdr)
    for i, img in enumerate(imgs):
        fs_uid = str(uuid4())
        fn = os.path.join(save_dir, fs_uid + '.npy')
        np.save(fn, img)
        # insert into FS
        fs_res = fs.insert_resource('npy', fn, resource_kwargs={})
        fs.insert_datum(fs_res, fs_uid, datum_kwargs={})
        mds.insert_event(
            descriptor=descriptor,
            uid=str(uuid4()),
            time=time.time(),
            data={'pe1_image': fs_uid},
            timestamps={'pe1_image': time.time()},
            seq_num=i)
    mds.insert_run_stop(run_start=run_start,
                        uid=str(uuid4()),
                        time=time.time())
    return save_dir
