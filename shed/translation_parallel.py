import time

import numpy as np
from rapidz.core import args_kwargs
from rapidz.parallel import ParallelStream

from .simple_parallel import SimpleToEventStream

ALL = "--ALL THE DOCS--"

DTYPE_MAP = {np.ndarray: "array", int: "number", float: "number"}


@args_kwargs
@ParallelStream.register_api()
class ToEventStream(SimpleToEventStream):
    """Converts data into a event stream, and passes it downstream.

    Parameters
    ----------
    upstream :
        the upstream node to receive streams from
    data_keys: tuple, optional
        Names of the data keys. If None assume incoming data is dict and use
        the keys from the dict. Defauls to None
    stream_name : str, optional
        Name for this stream node

    Notes
    --------
    The result emitted from this stream follows the document model.


    Examples
    -------------
    import uuid
    from shed.event_streams import EventStream
    from shed.translation import FromEventStream, ToEventStream

    s = EventStream()
    s2 = FromEventStream(s, 'event', ('data', 'det_image'), principle=True)
    s3 = ToEventStream(s2, ('det_image',))
    s3.sink(print)
    s.emit(('start', {'uid' : str(uuid.uuid4())}))
    s.emit(('descriptor', {'uid' : str(uuid.uuid4()),
                           'data_keys': {'det_image': {'units': 'arb'}}))
    s.emit(('event', {'uid' : str(uuid.uuid4()), 'data': {'det_image' : 1}}))
    s.emit(('stop', {'uid' : str(uuid.uuid4())}))
    prints:
    ('start',...)
    ('descriptor',...)
    ('event',...)
    ('stop',...)
    """

    def __init__(self, upstream, data_keys=None, stream_name=None, **kwargs):
        super().__init__(
            upstream=upstream,
            data_keys=data_keys,
            stream_name=stream_name,
            **kwargs
        )
        self.times = {}

    def emit(self, x, asynchronous=False):
        name, doc = x
        if name == "start":
            self.times = {time.time(): self.start_uid}
        self.times[time.time()] = self.start_uid
        super().emit(x, asynchronous=asynchronous)

    def start_doc(self, x):
        new_start_doc = super().start_doc(x)
        new_start_doc.update(graph=self.graph)
        return new_start_doc

    def stop(self, x):
        new_stop = super().stop(x)
        times = {}
        for k, node in self.translation_nodes.items():
            for t, uid in node.times.items():
                times[t] = {"node": node.uid, "uid": uid}
        new_stop.update(times=times)
        return new_stop
