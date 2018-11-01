import inspect
import time

import networkx as nx
import numpy as np
from streamz_ext.core import Stream
from streamz_ext.parallel import ParallelStream

from .simple_parallel import SimpleToEventStream, SimpleFromEventStream
from .simple import _hash_or_uid

from zstreamz.core import args_kwargs

ALL = "--ALL THE DOCS--"

DTYPE_MAP = {np.ndarray: "array", int: "number", float: "number"}


# @ParallelStream.register_api()
# class FromEventStream(SimpleFromEventStream):
#     """Extracts data from the event stream, and passes it downstream.
#
#     Parameters
#     ----------
#
#     doc_type : {'start', 'descriptor', 'event', 'stop'}
#         The type of document to extract data from
#     data_address : tuple
#         A tuple of successive keys walking through the document considered,
#         if the tuple is empty all the data from that document is returned as
#         a dict
#     upstream : Stream instance or None, optional
#         The upstream node to receive streams from, defaults to None
#     event_stream_name : str, optional
#         Filter by en event stream name (see :
#         http://nsls-ii.github.io/databroker/api.html?highlight=stream_name#data)
#     stream_name : str, optional
#         Name for this stream node
#     principle : bool, optional
#         If True then when this node receives a stop document then all
#         downstream ToEventStream nodes will issue a stop document.
#         Defaults to False. Note that one principle node is required for proper
#         pipeline operation.
#
#     Notes
#     -----
#     The result emitted from this stream no longer follows the document model.
#
#     This node also keeps track of when and which data came through the node.
#
#
#     Examples
#     -------------
#     import uuid
#     from shed.event_streams import EventStream
#     from shed.translation import FromEventStream
#
#     s = EventStream()
#     s2 = FromEventStream(s, 'event', ('data', 'det_image'))
#     s3 = s2.map(print)
#     s.emit(('start', {'uid' : str(uuid.uuid4())}))
#     s.emit(('descriptor', {'uid' : str(uuid.uuid4())}))
#     s.emit(('event', {'uid' : str(uuid.uuid4()), 'data': {'det_image' : 1}}))
#     s.emit(('stop', {'uid' : str(uuid.uuid4())}))
#     prints:
#     1
#     """
#
#     def __init__(
#         self,
#         doc_type,
#         data_address,
#         upstream=None,
#         event_stream_name=ALL,
#         stream_name=None,
#         principle=False,
#     ):
#         super().__init__(
#             doc_type=doc_type,
#             data_address=data_address,
#             upstream=upstream,
#             stream_name=stream_name,
#             principle=principle,
#             event_stream_name=event_stream_name,
#         )
#         self.run_start_uid = None
#         self.times = {}
#
#     def update(self, x, who=None):
#         name, doc = x
#         self.times[time.time()] = doc.get("uid", doc.get("datum_id"))
#         if name == "start":
#             self.times = {time.time(): doc["uid"]}
#             self.start_uid = doc["uid"]
#         return super().update(x, who=None)


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
