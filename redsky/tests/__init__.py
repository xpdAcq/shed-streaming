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

from .utils import build_pymongo_backed_broker, insert_imgs
import tempfile


@pytest.fixture(scope='module')
def img_size():
    a = np.random.random_integers(100, 200)
    yield (a, a)


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='function')
def db(request):
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}

    return param_map[request.param](request)


@pytest.fixture(scope='function')
def exp_db(db, tmp_dir, img_size):
    db2 = db
    mds = db2.mds
    fs = db2.fs
    insert_imgs(mds, fs, 5, img_size, tmp_dir)
    yield db2
    print("DROPPING MDS")
    mds._connection.drop_database(mds.config['database'])
    print("DROPPING FS")
    fs._connection.drop_database(fs.config['database'])


@pytest.fixture(scope='module')
def tmp_dir():
    td = tempfile.mkdtemp()
    yield td
    if os.path.exists(td):
        print('removing {}'.format(td))
        shutil.rmtree(td)