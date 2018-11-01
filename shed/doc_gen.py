import time
import uuid
from collections import MutableMapping

import numpy as np

DTYPE_MAP = {np.ndarray: "array", int: "number", float: "number"}


def get_dtype(xx):
    print(type(xx))
    return DTYPE_MAP.get(type(xx), type(xx).__name__)


class CreateDocs(object):
    def __init__(self, data_keys, **kwargs):
        self.descriptor_uid = None
        self.md = kwargs
        self.data_keys = data_keys
        self.start_uid = None
        self.index_dict = dict()

    def start_doc(self, x):
        self.start_uid = str(uuid.uuid4())
        tt = time.time()
        new_start_doc = dict(
            uid=self.start_uid,
            time=tt, )
        new_start_doc.update(**self.md)
        self.index_dict = dict()
        return new_start_doc

    def descriptor(self, x):
        # XXX: handle multiple descriptors?

        # If data_keys is none then we are working with a dict
        if self.data_keys is None:
            self.data_keys = tuple([k for k in x])

        # If the incoming data is a dict extract the data as a tuple
        if isinstance(x, MutableMapping):
            x = tuple([x[k] for k in self.data_keys])
        if not isinstance(x, tuple):
            tx = tuple([x])
        # XXX: need to do something where the data is a tuple!
        elif len(self.data_keys) == 1:
            tx = tuple([x])
        else:
            tx = x
        self.descriptor_uid = str(uuid.uuid4())
        self.index_dict[self.descriptor_uid] = 1

        new_descriptor = dict(
            uid=self.descriptor_uid,
            time=time.time(),
            run_start=self.start_uid,
            name="primary",
            # TODO: source should reflect graph? (maybe with a UID)
            # TODO: submit dtype so it gets properly executed
            data_keys={
                k: {
                    "source": "analysis",
                    # XXX: how to deal with this when xx is a future?
                    "dtype": get_dtype(xx),
                    "shape": getattr(xx, "shape", []),
                }
                for k, xx in zip(self.data_keys, tx)
            },
            hints={"analyzer": {"fields": sorted(list(self.data_keys))}},
            object_keys={k: [k] for k in self.data_keys},
        )
        return new_descriptor

    def event(self, x):
        if isinstance(x, MutableMapping):
            x = tuple([x[k] for k in self.data_keys])
        if not isinstance(x, tuple) or (
                len(self.data_keys) == 1 and len(x) > 1
        ):
            tx = tuple([x])
        else:
            tx = x
        new_event = dict(
            uid=str(uuid.uuid4()),
            time=time.time(),
            timestamps={k: time.time() for k in self.data_keys},
            descriptor=self.descriptor_uid,
            filled={k: True for k in self.data_keys},
            data={k: v for k, v in zip(self.data_keys, tx)},
            seq_num=self.index_dict[self.descriptor_uid],
        )
        self.index_dict[self.descriptor_uid] += 1
        return new_event

    def stop(self, x):
        new_stop = dict(
            uid=str(uuid.uuid4()),
            time=time.time(),
            run_start=self.start_uid,
            exit_status="success",
        )
        return new_stop

    def create_doc(self, name, x):
        if name == 'start':
            _name = 'start_doc'
        else:
            _name = name
        return name, getattr(self, _name)(x)
