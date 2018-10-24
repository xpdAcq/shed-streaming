"""Translation nodes"""
import time
import uuid
from collections import MutableMapping
from collections import deque

import networkx as nx
import numpy as np
from regolith.chained_db import ChainDB, _convert_to_dict
from streamz_ext.core import Stream, buffer, zip as szip

ALL = "--ALL THE DOCS--"

DTYPE_MAP = {np.ndarray: "array", int: "number", float: "number"}


def _hash_or_uid(node):
    return getattr(node, "uid", hash(node))


def walk_to_translation(node, graph, prior_node=None):
    """Creates a graph that is a subset of the graph from the stream.

    The walk starts at a translation ``ToEventStream`` node and ends at any
    instances of FromEventStream or ToEventStream.  Each iteration of the walk
    goes up one node, determines if that node is a ``FromEventStream`` node, if
    not walks one down to see if there are any ``ToEventStream`` nodes, if not
    it keeps walking up. The walk down allows us to replace live data with
    stored data/stubs when it comes time to get the parent uids. Graph nodes
    are hashes or uids of the node objects with ``stream=node`` in the nodes.

    Parameters
    ----------
    node : Stream instance
    graph : DiGraph instance
    prior_node : Stream instance
    """
    if node is None:
        return
    t = _hash_or_uid(node)
    graph.add_node(t, stream=node)
    if prior_node:
        tt = _hash_or_uid(prior_node)
        if graph.has_edge(t, tt):
            return
        else:
            graph.add_edge(t, tt)
            if isinstance(node, SimpleFromEventStream):
                return
            else:
                for downstream in node.downstreams:
                    ttt = _hash_or_uid(downstream)
                    if (
                        isinstance(downstream, SimpleToEventStream)
                        and ttt not in graph
                    ):
                        graph.add_node(ttt, stream=downstream)
                        graph.add_edge(t, ttt)
                        return

    for node2 in node.upstreams:
        # Stop at translation node
        if node2 is not None:
            walk_to_translation(node2, graph, node)


@Stream.register_api()
class SimpleFromEventStream(Stream):
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
    ):
        if stream_name is None:
            stream_name = doc_type + str(data_address)
        Stream.__init__(self, upstream, stream_name=stream_name)
        self.principle = principle
        self.doc_type = doc_type
        if isinstance(data_address, str):
            data_address = tuple([data_address])
        self.data_address = data_address
        self.event_stream_name = event_stream_name
        self.uid = str(uuid.uuid4())
        self.descriptor_uids = []
        self.subs = []
        self.start_uid = None

    def update(self, x, who=None):
        name, doc = x
        if name == "start":
            self.start_uid = doc["uid"]
            # Sideband start document in
            [s.create_start(x) for s in self.subs]
        if name == "descriptor" and (
            self.event_stream_name == ALL
            or self.event_stream_name == doc.get("name", "primary")
        ):
            self.descriptor_uids.append(doc["uid"])
        if name == "stop":
            # Trigger the downstream nodes to make a stop but they can emit
            # on their own time
            self.descriptor_uids = []
            [s.create_stop(x) for s in self.subs]
        inner = doc.copy()
        if name == self.doc_type and (
            (
                name == "descriptor"
                and (self.event_stream_name == doc.get("name", ALL))
            )
            or (
                name == "event" and (doc["descriptor"] in self.descriptor_uids)
            )
            or name in ["start", "stop"]
        ):

            # If we have an empty address get everything
            if self.data_address != ():
                for da in self.data_address:
                    # If it's a tuple we want multiple things at once
                    if isinstance(da, tuple):
                        inner = tuple(inner[daa] for daa in da)
                    else:
                        if da in inner:
                            inner = inner[da]
                        else:
                            return
            return self.emit(inner)


@Stream.register_api()
class SimpleToEventStream(Stream):
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
        self.start_doc = None
        if stream_name is None:
            stream_name = str(data_keys)
        Stream.__init__(self, upstream, stream_name=stream_name)
        # TODO: use first
        for up in self.upstreams:
            for n in up.downstreams.data:
                if n() is self:
                    break
            up.downstreams.data._od.move_to_end(n, last=False)
            del n
        self.index_dict = None
        self.data_keys = data_keys
        self.md = kwargs

        self.stop = None
        self.start_uid = None
        self.descriptor_uid = None
        self.uid = str(uuid.uuid4())
        self.state = None
        self.subs = []

        self.futures = {}

        self.times = {}

        # walk upstream to get all upstream nodes to the translation node
        # get start_uids from the translation node
        self.graph = nx.DiGraph()
        walk_to_translation(self, graph=self.graph)

        # XXX: what happens if we have multiple buffers in a path?
        # The outputs of one buffer would flow into the other, but we wouldn't
        # know about that since we only capture the snapshot
        # For now we just need to mandate that there is only one buffer per
        # path
        self.buffers = [
            n["stream"]
            for k, n in self.graph.node.items()
            if isinstance(n["stream"], buffer)
        ]

        self.translation_nodes = {
            k: n["stream"]
            for k, n in self.graph.node.items()
            if isinstance(
                n["stream"], (SimpleFromEventStream, SimpleToEventStream)
            )
            and n["stream"] != self
        }
        self.principle_nodes = [
            n
            for k, n in self.translation_nodes.items()
            if getattr(n, "principle", False)
            or isinstance(n, SimpleToEventStream)
        ]
        if not self.principle_nodes:
            raise RuntimeError("No Principle Nodes Detected")
        for p in self.principle_nodes:
            p.subs.append(self)

    def update(self, x, who=None):
        rl = []
        # If we have a start document ready to go, release it.
        if self.start_doc:
            # If we have a start doc, but are already started, issue emergency
            # stop doc
            if self.state == "started":
                rl.append(self.emit(self.create_stop(x)))
                [s.create_stop(x) for s in self.subs]
                self.stop = None
                self.state = "stopped"
            rl.extend(
                [
                    self.emit(self.start_doc),
                    self.emit(self.create_descriptor(x)),
                ]
            )
            [s.create_start(x) for s in self.subs]
            self.state = "started"
            self.start_doc = None
        rl.append(self.emit(self.create_event(x)))

        # If there are buffers and
        # all the futures that existed in the snapshot when stop was called
        # are done and
        # and there is a stop document (we haven't already emitted it)
        # then issue the stop doc and then reset self.stop
        if (
            self.futures
            and (
                not any(
                    fs.intersection(set(b.queue._queue))
                    for b, fs in self.futures.items()
                )
            )
            and self.stop
        ):
            rl.append(self.emit(self.stop))
            [s.create_stop(x) for s in self.subs]
            self.stop = None
            self.state = "stopped"
        return rl

    def create_start(self, x):
        if self.stop:
            self.create_stop(x)
        self.start_uid = str(uuid.uuid4())
        tt = time.time()
        new_start_doc = dict(
            uid=self.start_uid,
            time=tt,
            parent_uids=[
                v.start_uid
                for k, v in self.translation_nodes.items()
                if v.start_uid is not None
            ],
            parent_node_map={
                v.uid: v.start_uid
                for k, v in self.translation_nodes.items()
                if v.start_uid is not None
            },
        )
        new_start_doc.update(**self.md)
        self.index_dict = dict()
        self.start_doc = ("start", new_start_doc)
        return "start", new_start_doc

    def create_descriptor(self, x):
        # If data_keys is none then we are working with a dict
        if self.data_keys is None:
            self.data_keys = tuple([k for k in x])

        # If the incoming data is a dict extract the data as a tuple
        if isinstance(x, MutableMapping):
            x = tuple([x[k] for k in self.data_keys])
        if not isinstance(x, tuple):
            tx = tuple([x])
        else:
            tx = x
        self.descriptor_uid = str(uuid.uuid4())
        self.index_dict[self.descriptor_uid] = 1

        print(self.data_keys)
        new_descriptor = dict(
            uid=self.descriptor_uid,
            time=time.time(),
            run_start=self.start_uid,
            name="primary",
            # TODO: source should reflect graph? (maybe with a UID)
            data_keys={
                k: {
                    "source": "analysis",
                    "dtype": DTYPE_MAP.get(type(xx), str(type(xx))),
                    "shape": getattr(xx, "shape", []),
                }
                for k, xx in zip(self.data_keys, tx)
            },
            hints={"analyzer": {"fields": [k]} for k in self.data_keys},
            object_keys={k: [k] for k in self.data_keys},
        )
        return "descriptor", new_descriptor

    def create_event(self, x):
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
        return "event", new_event

    def _create_stop(self, x):
        # Take a snapshot of all the buffers contents
        self.futures = {b: set(b.queue._queue) for b in self.buffers}
        new_stop = dict(
            uid=str(uuid.uuid4()),
            time=time.time(),
            run_start=self.start_uid,
            exit_status="success",
        )
        self.stop = ("stop", new_stop)
        # If no buffers (running perfect back pressure) or buffers are empty
        # issue stop and clear
        # Otherwise just make the stop doc, wait for all buffers to clear
        # to issue
        return new_stop

    def create_stop(self, x):
        new_stop = self._create_stop(x)
        if not self.futures or all(not v for v in self.futures.values()):
            self.emit(self.stop)
            self.stop = None
            self.state = "stopped"
        return "stop", new_stop


@Stream.register_api()
class AlignEventStreams(szip):
    """Zips and aligns multiple streams of documents, note that the last
    upstream takes precedence where merging is not possible, this requires
    the two streams to be of equal length."""

    def __init__(self, *upstreams, stream_name=None, **kwargs):
        szip.__init__(self, *upstreams, stream_name=stream_name)
        doc_names = ["start", "descriptor", "event", "stop"]
        self.true_buffers = {
            k: {
                upstream: deque()
                for upstream in upstreams
                if isinstance(upstream, Stream)
            }
            for k in doc_names
        }
        self.true_literals = {
            k: [
                (i, val)
                for i, val in enumerate(upstreams)
                if not isinstance(val, Stream)
            ]
            for k in doc_names
        }

    def _emit(self, x):
        # flatten out the nested setup
        x = [k for l in x for k in l]
        names = x[::2]
        docs = x[1::2]
        super()._emit((names[0], _convert_to_dict(ChainDB(*docs))))

    def update(self, x, who=None):
        name, doc = x
        self.buffers = self.true_buffers[name]
        self.literals = self.true_literals[name]
        super().update((name, doc), who)
