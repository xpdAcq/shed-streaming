import inspect
import time
from pprint import pprint

import networkx as nx
import numpy as np
from zstreamz.core import _deref_weakref
from streamz_ext.core import Stream, args_kwargs

from .simple import SimpleToEventStream, SimpleFromEventStream, _hash_or_uid

ALL = "--ALL THE DOCS--"

DTYPE_MAP = {np.ndarray: "array", int: "number", float: "number"}


@args_kwargs
@Stream.register_api()
class FromEventStream(SimpleFromEventStream):
    def __init__(
        self,
        doc_type,
        data_address,
        upstream=None,
        event_stream_name=ALL,
        stream_name=None,
        principle=False,
        **kwargs
    ):
        super().__init__(
            doc_type=doc_type,
            data_address=data_address,
            upstream=upstream,
            stream_name=stream_name,
            principle=principle,
            event_stream_name=event_stream_name,
            **kwargs
        )
        self.run_start_uid = None
        self.times = {}

    """Extracts data from the event stream, and passes it downstream.

    Parameters
    ----------

    doc_type : {'start', 'descriptor', 'event', 'stop'}
        The type of document to extract data from
    data_address : tuple
        A tuple of successive keys walking through the document considered,
        if the tuple is empty all the data from that document is returned as
        a dict
    upstream : Stream instance or None, optional
        The upstream node to receive streams from, defaults to None
    event_stream_name : str, optional
        Filter by en event stream name (see :
        http://nsls-ii.github.io/databroker/api.html?highlight=stream_name#data)
    stream_name : str, optional
        Name for this stream node
    principle : bool, optional
        If True then when this node receives a stop document then all
        downstream ToEventStream nodes will issue a stop document.
        Defaults to False. Note that one principle node is required for proper
        pipeline operation.

    Notes
    -----
    The result emitted from this stream no longer follows the document model.

    This node also keeps track of when and which data came through the node.


    Examples
    -------------
    import uuid
    from shed.event_streams import EventStream
    from shed.translation import FromEventStream

    s = EventStream()
    s2 = FromEventStream(s, 'event', ('data', 'det_image'))
    s3 = s2.map(print)
    s.emit(('start', {'uid' : str(uuid.uuid4())}))
    s.emit(('descriptor', {'uid' : str(uuid.uuid4())}))
    s.emit(('event', {'uid' : str(uuid.uuid4()), 'data': {'det_image' : 1}}))
    s.emit(('stop', {'uid' : str(uuid.uuid4())}))
    prints:
    1
    """

    def update(self, x, who=None):
        name, doc = x
        self.times[time.time()] = doc.get("uid", doc.get("datum_id"))
        if name == "start":
            self.times = {time.time(): doc["uid"]}
            self.start_uid = doc["uid"]
        return super().update(x, who=None)


@args_kwargs
@Stream.register_api()
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

    def create_start(self, x):
        name, new_start_doc = super().create_start(x)
        new_start_doc.update(graph=self.graph)
        return "start", new_start_doc

    def create_stop(self, x):
        new_stop = super()._create_stop(x)
        times = {}
        for k, node in self.translation_nodes.items():
            for t, uid in node.times.items():
                times[t] = {"node": node.uid, "uid": uid}
        new_stop.update(times=times)
        self.stop = ("stop", new_stop)
        if not self.futures or all(not v for v in self.futures.values()):
            self.emit(self.stop)
            self.stop = None
        return "stop", new_stop


@args_kwargs
@Stream.register_api()
class DBFriendly(Stream):
    """Make analyzed data (and graph) DB friendly"""

    def update(self, x, who=None):
        name, doc = x
        if name == "start":
            doc = dict(doc)
            graph = doc["graph"]
            for n in nx.topological_sort(graph):
                graph.node[n]["stream"] = db_friendly_node(
                    graph.node[n]["stream"]
                )
            doc["graph"] = nx.node_link_data(graph)
            # pprint(doc['graph'])
        return self.emit((name, doc))


def _is_stream(x):
    return isinstance(x, Stream)


def _deref_func(x):
    if isinstance(x, np.ufunc):
        mod = "numpy"
    else:
        mod = x.__module__
    return {"name": x.__name__, "mod": mod}


deref_dict = {_is_stream: _hash_or_uid, callable: _deref_func}


def db_friendly_node(node):
    """Extract data to make node db friendly"""
    if isinstance(node, dict):
        return node
    d = dict(node.__dict__)
    d["stream_name"] = d["name"]
    d2 = {"name": node.__class__.__name__, "mod": node.__module__}
    aa = []
    kk = {}

    for a in tuple([_deref_weakref(a) for a in node._init_args]):
        # Somethings are not storable (nodes, funcs, etc.) so we must
        # deref them so we can store them
        stored = False
        for k, v in deref_dict.items():
            # If we have a tool for storing things store it

            if k(a):
                aa.append(v(a))
                stored = True
                break
        # If none of our tools worked, store it natively
        if not stored:
            aa.append(a)

    for i, a in {
        k: _deref_weakref(v) for k, v in node._init_kwargs.items()
    }.items():
        # Somethings are not storable (nodes, funcs, etc.) so we must
        # deref them so we can store them
        stored = False
        for k, v in deref_dict.items():
            # If we have a tool for storing things store it
            if k(a):
                kk[i] = v(a)
                stored = True
                break
        # If none of our tools worked, store it natively
        if not stored:
            kk[i] = a

    d2["args"] = tuple(aa)
    d2["kwargs"] = kk
    print(d2)
    return d2
