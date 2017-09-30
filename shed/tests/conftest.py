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

from .utils import insert_imgs
import tempfile
from bluesky.tests.conftest import fresh_RE, db
from bluesky.examples import ReaderWithRegistryHandler


@pytest.fixture(scope='function')
def start_uid1(exp_db):
    print(exp_db[1])
    assert 'start_uid1' in exp_db[2]['start']
    return str(exp_db[2]['start']['uid'])



@pytest.fixture(scope='module')
def img_size():
    a = np.random.random_integers(100, 200)
    yield (a, a)


# @pytest.fixture(params=[
#     # 'sqlite',
#     'mongo'], scope='module')
# def db(request):
#     param_map = {
#         # 'sqlite': build_sqlite_backed_broker,
#         'mongo': build_pymongo_backed_broker}
#
#     return param_map[request.param](request)


@pytest.fixture(scope='function')
def exp_db(db, tmp_dir, img_size, fresh_RE):
    db2 = db
    reg = db2.reg
    # reg.register_handler('npy', NpyHandler)
    reg.register_handler('RWFS_NPY', ReaderWithRegistryHandler)
    RE = fresh_RE
    RE.subscribe(db.insert)

    uid1 = insert_imgs(RE, reg, 5, img_size, tmp_dir,
                       bt_safN=0, pi_name='chris', start_uid1=True)
    uid2 = insert_imgs(RE, reg, 5, img_size, tmp_dir,
                       pi_name='tim', bt_safN=1, start_uid2=True)
    uid3 = insert_imgs(RE, reg, 2, img_size, tmp_dir,
                       pi_name='chris', bt_safN=2, start_uid3=True)
    yield db2


@pytest.fixture(scope='module')
def tmp_dir():
    td = tempfile.mkdtemp()
    yield td
    if os.path.exists(td):
        print('removing {}'.format(td))
        shutil.rmtree(td)
