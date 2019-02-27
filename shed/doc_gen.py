import time
import uuid
from collections import MutableMapping

import numpy as np

from event_model import compose_run

DTYPE_MAP = {
    np.ndarray: "array",
    int: "number",
    float: "number",
    np.float: "number",
    np.float32: "number",
    np.float64: "number",
}


def get_dtype(xx):
    return DTYPE_MAP.get(type(xx), type(xx).__name__)


class CreateDocs(object):
    def __init__(self, data_keys, data_key_md=None, **kwargs):
        if data_key_md is None:
            data_key_md = {}
        if isinstance(data_keys, str):
            data_keys = (data_keys, )
        self.data_key_md = data_key_md
        self.descriptor_uid = None
        self.md = kwargs
        self.data_keys = data_keys
        self.start_uid = None

        self.desc_fac = None
        self.resc_fac = None
        self.stop_factory = None
        self.ev_fac = None
        self.evp_fac = None

    def start_doc(self, x):
        # TODO: need to match md with pipeline data
        bundle = compose_run(metadata=self.md)
        new_start_doc, self.desc_fac, self.resc_fac, self.stop_factory = bundle
        self.start_uid = new_start_doc["uid"]
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

        new_descriptor, self.ev_fac, self.evp_fac = self.desc_fac(
            name="primary",
            data_keys={
                k: {
                    "source": "analysis",
                    # XXX: how to deal with this when xx is a future?
                    "dtype": get_dtype(xx),
                    "shape": getattr(xx, "shape", []),
                    **self.data_key_md.get(k, {}),
                }
                for k, xx in zip(self.data_keys, tx)
            },
            hints={"analyzer": {"fields": sorted(list(self.data_keys))}},
            object_keys={k: [k] for k in self.data_keys},
            validate=False
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
        new_event = self.ev_fac(
            timestamps={k: time.time() for k in self.data_keys},
            filled={k: True for k in self.data_keys},
            data={k: v for k, v in zip(self.data_keys, tx)},
        )
        return new_event

    def stop(self, x):
        new_stop = self.stop_factory()
        return new_stop

    def create_doc(self, name, x):
        if name == "start":
            _name = "start_doc"
        else:
            _name = name
        return name, getattr(self, _name)(x)
