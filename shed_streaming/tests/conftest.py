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
import asyncio
from bluesky.tests.conftest import NumpySeqHandler, RunEngine
import uuid
import copy


@pytest.fixture(scope="function")
def db():
    from xpdsim import build_sim_db

    _, db = build_sim_db()
    db.reg.register_handler("NPY_SEQ", NumpySeqHandler)
    db.prepare_hook = lambda name, doc: copy.deepcopy(doc)
    yield db


@pytest.fixture(scope="function")
def fresh_RE(request):
    loop = asyncio.new_event_loop()
    loop.set_debug(True)
    RE = RunEngine({}, loop=loop)
    RE.ignore_callback_exceptions = False

    def clean_event_loop():
        if RE.state != "idle":
            RE.halt()
        ev = asyncio.Event(loop=loop)
        ev.set()
        loop.run_until_complete(ev.wait())

    request.addfinalizer(clean_event_loop)
    return RE


@pytest.fixture(scope="function")
def start_uid1(exp_db):
    print(exp_db[1])
    assert "start_uid1" in exp_db[2]["start"]
    return str(exp_db[2]["start"]["uid"])


@pytest.fixture(scope="module")
def img_size():
    a = np.random.random_integers(100, 200)
    yield (a, a)


@pytest.fixture(scope="function")
def exp_db(db, tmp_dir, img_size, fresh_RE):
    db2 = db
    reg = db2.reg
    RE = fresh_RE
    RE.subscribe(db2.insert)
    bt_uid = str(uuid.uuid4)

    insert_imgs(
        RE,
        reg,
        2,
        img_size,
        tmp_dir,
        bt_safN=0,
        pi_name="chris",
        sample_name="kapton",
        sample_composition="C",
        start_uid1=True,
        bt_uid=bt_uid,
        composition_string="Au",
    )
    insert_imgs(
        RE,
        reg,
        2,
        img_size,
        tmp_dir,
        pi_name="tim",
        bt_safN=1,
        sample_name="Au",
        bkgd_sample_name="kapton",
        sample_composition="Au",
        start_uid2=True,
        bt_uid=bt_uid,
        composition_string="Au",
    )
    insert_imgs(
        RE,
        reg,
        2,
        img_size,
        tmp_dir,
        pi_name="chris",
        bt_safN=2,
        sample_name="Au",
        bkgd_sample_name="kapton",
        sample_composition="Au",
        start_uid3=True,
        bt_uid=bt_uid,
        composition_string="Au",
    )
    yield db2


@pytest.fixture(scope="module")
def tmp_dir():
    td = tempfile.mkdtemp()
    yield td
    if os.path.exists(td):
        print("removing {}".format(td))
        shutil.rmtree(td)
