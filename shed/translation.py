import inspect
import json
from xonsh.lib import subprocess
import time
from hashlib import sha256

import networkx as nx
import numpy as np
from event_model import sanitize_doc
from rapidz.core import Stream
from rapidz.core import _deref_weakref, args_kwargs

from .simple import SimpleToEventStream, SimpleFromEventStream, _hash_or_uid

ALL = "--ALL THE DOCS--"

DTYPE_MAP = {np.ndarray: "array", int: "number", float: "number"}


def conda_env():
    """Capture information about the conda environment this is being run in

    Returns
    -------
    dict :
        A dictionary representing the packages installed

    """
    data = subprocess.check_output(["conda", "list", "--json"])
    try:
        j_data = json.loads(data)
    except TypeError:  # pragma: noqa
        j_data = "Failed to get packages"
    return {"packages": j_data}


env_data = conda_env()


@args_kwargs
@Stream.register_api()
class FromEventStream(SimpleFromEventStream):
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

    def __init__(
        self,
        doc_type,
        data_address,
        upstream=None,
        event_stream_name=ALL,
        stream_name=None,
        principle=False,
        **kwargs,
    ):
        super().__init__(
            doc_type=doc_type,
            data_address=data_address,
            upstream=upstream,
            stream_name=stream_name,
            principle=principle,
            event_stream_name=event_stream_name,
            **kwargs,
        )
        self.run_start_uid = None
        self.times = {}

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

    def __init__(
        self,
        upstream,
        data_keys=None,
        stream_name=None,
        env_capture_functions=None,
        **kwargs,
    ):
        super().__init__(
            upstream=upstream,
            data_keys=data_keys,
            stream_name=stream_name,
            **kwargs,
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


def merkle_hash(node):
    hasher = sha256()
    dbf_node = sanitize_doc(merkle_friendly_node(node))
    hash_string = ",".join(
        str(dbf_node[k]) for k in ["name", "mod", "args", "kwargs"]
    )
    hasher.update(hash_string.encode("utf-8"))
    # Once we hit from event stream we don't need to go higher in the graph
    if isinstance(node, SimpleFromEventStream):
        return hasher.hexdigest()
    for u in node.upstreams:
        idx = u.downstreams.index(node)
        u_m_hash = merkle_hash(u)
        hasher.update(f"{idx}{u_m_hash}".encode("utf-8"))
    return hasher.hexdigest()


# TODO: move this to a callback?
@Stream.register_api()
class DBFriendly(Stream):
    """Make analyzed data (and graph) DB friendly"""

    def update(self, x, who=None):
        name, doc = x
        if name == "start":
            doc = dict(doc)
            # copy this so we do this each time and get updates
            graph = doc["graph"].copy()
            doc["graph_hash"] = merkle_hash(
                graph.nodes[doc["outbound_node"]]["stream"]
            )
            # TODO: this might not be possible if there are cycles!!!!
            for n in nx.topological_sort(graph):
                graph.node[n]["stream"] = db_friendly_node(
                    graph.node[n]["stream"]
                )
            doc["graph"] = nx.node_link_data(graph)
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

    # Assemble the args and kwargs (taking updates from runtime changes)

    args = list(node._init_args)

    # inspect the init for the number of args which are not
    # *args, remove the args which are more than that and replace with
    # node.args in case they have changed
    sig = inspect.signature(node.__init__)
    if "args" in sig.parameters and hasattr(node, "args"):
        idx = list(sig.parameters).index("args")
        args[idx:] = node.args

    kwargs = node._init_kwargs

    # If we cache the kwargs in the node add those to the init kwargs
    if hasattr(node, "kwargs"):
        kwargs.update(**node.kwargs)

    # Store the args and kwargs

    for a in tuple([_deref_weakref(a) for a in args]):
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

    for i, a in {k: _deref_weakref(v) for k, v in kwargs.items()}.items():
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
    return d2


merkle_deref_dict = {_is_stream: None, callable: _deref_func}


def merkle_friendly_node(node):
    """Extract data to make node db friendly"""
    if isinstance(node, dict):
        return node
    d = dict(node.__dict__)
    d["stream_name"] = d["name"]
    d2 = {"name": node.__class__.__name__, "mod": node.__module__}
    aa = []
    kk = {}

    # Assemble the args and kwargs (taking updates from runtime changes)

    args = list(node._init_args)

    # inspect the init for the number of args which are not
    # *args, remove the args which are more than that and replace with
    # node.args in case they have changed
    sig = inspect.signature(node.__init__)
    if "args" in sig.parameters and hasattr(node, 'args'):
        idx = list(sig.parameters).index("args")
        args[idx:] = node.args

    kwargs = node._init_kwargs

    # If we cache the kwargs in the node add those to the init kwargs
    if hasattr(node, "kwargs"):
        kwargs.update(**node.kwargs)

    # Store the args and kwargs

    for a in tuple([_deref_weakref(a) for a in args]):
        # Somethings are not storable (nodes, funcs, etc.) so we must
        # deref them so we can store them
        stored = False
        for k, v in merkle_deref_dict.items():
            # If we have a tool for storing things store it

            if k(a):
                if v is not None:
                    aa.append(v(a))
                stored = True
                break
        # If none of our tools worked, store it natively
        if not stored:
            aa.append(a)

    for i, a in {k: _deref_weakref(v) for k, v in kwargs.items()}.items():
        # Somethings are not storable (nodes, funcs, etc.) so we must
        # deref them so we can store them
        stored = False
        for k, v in merkle_deref_dict.items():
            # If we have a tool for storing things store it
            if k(a):
                if v is not None:
                    kk[i] = v(a)
                stored = True
                break
        # If none of our tools worked, store it natively
        if not stored:
            kk[i] = a

    d2["args"] = aa
    d2["kwargs"] = kk
    return d2
