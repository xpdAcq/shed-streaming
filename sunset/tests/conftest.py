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
import sys

import numpy as np
import pytest

from xpdan.simulation import build_pymongo_backed_broker
from xpdan.tests.utils import insert_imgs


@pytest.fixture(scope='module')
def img_size():
    a = np.random.random_integers(100, 200)
    yield (a, a)


@pytest.fixture(scope='module')
def mk_glbl():
    a = make_glbl(1)
    yield a
    if os.path.exists(a.base):
        print('removing {}'.format(a.base))
        shutil.rmtree(a.base)


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='module')
def db(request):
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}

    return param_map[request.param](request)


@pytest.fixture(scope='module')
def handler(exp_db):
    h = DataReduction(exp_db=exp_db)
    return h


@pytest.fixture(scope='module')
def exp_db(db, mk_glbl, img_size):
    glbl = mk_glbl
    db2 = db
    mds = db2.mds
    fs = db2.fs
    insert_imgs(mds, fs, 5, img_size, glbl.base)
    yield db2
    print("DROPPING MDS")
    mds._connection.drop_database(mds.config['database'])
    print("DROPPING FS")
    fs._connection.drop_database(fs.config['database'])
    if os.path.exists(glbl.base):
        print('removing {}'.format(glbl.base))
        shutil.rmtree(glbl.base)


@pytest.fixture(scope='module')
def an_db(db, mk_glbl, img_size):
    glbl = mk_glbl
    db2 = db
    mds = db2.mds
    fs = db2.fs
    yield db2
    print("DROPPING MDS")
    mds._connection.drop_database(mds.config['database'])
    print("DROPPING FS")
    fs._connection.drop_database(fs.config['database'])
    if os.path.exists(glbl.base):
        print('removing {}'.format(glbl.base))
        shutil.rmtree(glbl.base)


@pytest.fixture(scope='module')
def disk_mask(mk_glbl, img_size):
    mask = np.random.random_integers(0, 1, img_size).astype(bool)
    dirn = mk_glbl.base
    file_name_msk = os.path.join(dirn, 'mask_test' + '.msk')
    assert ~os.path.exists(file_name_msk)
    fit2d_save(mask, 'mask_test', dirn)
    assert os.path.exists(file_name_msk)
    file_name = os.path.join(dirn, 'mask_test' + '.npy')
    assert ~os.path.exists(file_name)
    np.save(file_name, mask)
    assert os.path.exists(file_name)
    yield (file_name_msk, file_name, mask)
