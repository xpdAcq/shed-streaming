import time

import numpy as np
from rapidz.core import args_kwargs
from rapidz.parallel import ParallelStream

from .simple_parallel import SimpleToEventStream
from .translation import env_data

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
    -----
    The result emitted from this stream follows the document model.
    This is essentially a state machine. Transitions are:
    start -> stop
    start -> descriptor -> event -> stop
    Note that start -> start is not allowed, this node always issues a stop
    document so the data input times can be stored.

    Examples
    --------
    >>> import uuid
    >>> from rapidz import Stream
    >>> from shed.translation import FromEventStream, ToEventStream
    >>> source = Stream()
    >>> s2 = FromEventStream(source, 'event', ('data', 'det_image'), principle=True)
    >>> s3 = ToEventStream(s2, ('det_image',))
    >>> s3.sink(print)
    >>> from ophyd.sim import hw
    >>> hw = hw()
    >>> from bluesky.run_engine import RunEngine
    >>> RE = RunEngine()
    >>> import bluesky.plans as bp
    >>> node.sink(pprint)
    >>> RE.subscribe(lambda *x: source.emit(x))
    >>> RE(bp.scan([hw.motor1], hw.motor1, 0, 10, 11))

    prints:

    >>> ('start',...)
    >>> ('descriptor',...)
    >>> ('event',...)
    >>> ('stop',...)
    """

    def __init__(
        self,
        upstream,
        data_keys=None,
        stream_name=None,
        env_capture_functions=None,
        **kwargs
    ):
        super().__init__(
            upstream=upstream,
            data_keys=data_keys,
            stream_name=stream_name,
            **kwargs
        )
        if env_capture_functions is None:
            env_capture_functions = []
        self.env_capture_functions = env_capture_functions
        self.times = {}
        for node, attrs in self.graph.nodes.items():
            for arg in getattr(attrs["stream"], "_init_args", []):
                if getattr(arg, "__name__", "") == "<lambda>":
                    raise RuntimeError(
                        "lambda functions can not be stored "
                        "either eliminate the lambda or use "
                        "``SimpleToEventStream``"
                    )

    def emit(self, x, asynchronous=False):
        name, doc = x
        if name == "start":
            self.times = {time.time(): self.start_uid}
        self.times[time.time()] = self.start_uid
        super().emit(x, asynchronous=asynchronous)

    def start_doc(self, x):
        new_start_doc = super().start_doc(x)
        new_start_doc.update(graph=self.graph)
        new_start_doc["env"] = env_data
        if self.env_capture_functions:
            for f in self.env_capture_functions:
                new_start_doc["env"].update(f())
        return new_start_doc

    def stop(self, x):
        new_stop = super().stop(x)
        times = {}
        for k, node in self.translation_nodes.items():
            for t, uid in node.times.items():
                times[t] = {"node": node.uid, "uid": uid}
        new_stop.update(times=times)
        return new_stop
