from bluesky.utils import sanitize_np
from rapidz import Stream
import os
import numpy as np
from event_model import compose_resource


@Stream.register_api()
class Store(Stream):
    def __init__(self, upstream, root, writer, resource_kwargs=None, **kwargs):
        Stream.__init__(self, upstream, **kwargs)
        if writer is None:
            writer = {}
        self.writer = writer
        self.root = root
        self.resource_kwargs = resource_kwargs
        self.init_writers = {}
        self.descriptors = {}
        self.not_issued_descriptors = set()

    def update(self, x, who=None):
        name, doc = x

        # selective copy
        doc = dict(doc)

        if name == "start":
            self.init_writers[doc["uid"]] = self.writer(
                self.root, doc, self.resource_kwargs
            )
        if name == "descriptor":
            self.descriptors[doc["uid"]] = doc
            self.not_issued_descriptors.add(doc["uid"])
            return

        elif name == "event":
            ret = []
            writer = self.init_writers[
                self.descriptors[doc["descriptor"]]["run_start"]
            ]
            for n, d in writer.write(doc):
                # If this is an event and we haven't done this descriptor yet
                if (
                    n == "event"
                    and doc["descriptor"] in self.not_issued_descriptors
                ):

                    # For each of the filled keys let us know that it is backed
                    # by FILESTORE
                    descriptor = self.descriptors[doc["descriptor"]]
                    for k, v in doc["filled"].items():
                        if not v:
                            descriptor["data_keys"][k].update(
                                external="FILESTORE:"
                            )
                    ret.append(self.emit(("descriptor", descriptor)))

                    # We're done with that descriptor now
                    self.not_issued_descriptors.remove(doc["descriptor"])
                ret.append(self.emit((n, d)))
            return ret
        elif name == "stop":
            # clean up our cache (allow multi stops if needed)
            self.init_writers.pop(doc["run_start"], None)

        return self.emit((name, doc))


class NpyWriter:
    spec = "npy"

    def __init__(self, root, start, resource_kwargs=None):
        if resource_kwargs is None:
            resource_kwargs = {}
        self.resource_kwargs = resource_kwargs
        self.root = root
        self.datum_kwargs = {}
        self.start = start

    def write(self, event):
        for k, v in event["data"].items():
            if isinstance(v, np.ndarray) and v.shape != ():
                resource_path = f'an_data/{event["uid"]}_{k}.npy'
                fpath = os.path.join(self.root, resource_path)
                os.makedirs(os.path.dirname(fpath), exist_ok=True)
                np.save(fpath, v)
                resource, compose_datum, compose_datum_page = compose_resource(
                    start=self.start,
                    spec=self.spec,
                    root=self.root,
                    resource_path=resource_path,
                    resource_kwargs=self.resource_kwargs,
                )
                yield "resource", resource
                datum = compose_datum(datum_kwargs=self.datum_kwargs)
                yield "datum", datum
                event["data"][k] = datum["datum_id"]
                event["filled"][k] = False
            # Don't write a file just for a single number!
            elif isinstance(v, np.ndarray) and v.shape == ():
                event["data"][k] = sanitize_np(v)
        yield "event", event
