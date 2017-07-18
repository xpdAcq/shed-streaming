##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu, Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import os
import shutil

import numpy as np
import pytest

# from databroker.tests.utils import build_pymongo_backed_broker
from .utils import insert_imgs, build_pymongo_backed_broker
import tempfile
from tempfile import TemporaryDirectory
from uuid import uuid4


def clean_database(database):
    for sub_db_name in ['mds', 'fs']:
        sub_db = getattr(database, sub_db_name)
        sub_db._connection.drop_database(sub_db.config['database'])


@pytest.fixture(scope='module')
def start_uid1():
    return str(uuid4())


@pytest.fixture(scope='module')
def start_uid2():
    return str(uuid4())


@pytest.fixture(scope='module')
def start_uid3():
    return str(uuid4())


@pytest.fixture(scope='module')
def img_size():
    a = np.random.random_integers(100, 200)
    yield (a, a)


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='module')
def db(request):
    print('Making DB')
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}
    rv = param_map[request.param](request)
    yield rv
    clean_database(rv)


@pytest.fixture(scope='module')
def exp_db(db, tmp_dir, img_size, start_uid1, start_uid2, start_uid3):
    db2 = db
    mds = db2.mds
    fs = db2.fs
    insert_imgs(mds, fs, 2, img_size, tmp_dir, start_uid3)
    insert_imgs(mds, fs, 5, img_size, tmp_dir, start_uid1)
    insert_imgs(mds, fs, 5, img_size, tmp_dir, start_uid2)
    yield db2
    print("DROPPING MDS")
    mds._connection.drop_database(mds.config['database'])
    print("DROPPING FS")
    fs._connection.drop_database(fs.config['database'])


@pytest.fixture(scope='module')
def tmp_dir():
    td = TemporaryDirectory()
    yield td.name
    td.cleanup()


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='module')
def an_db(request):
    print('Making DB')
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}
    rv = param_map[request.param](request)
    yield rv
    clean_database(rv)
