"""Translation nodes"""
import time
import uuid
from collections import deque, Mapping

import networkx as nx
import numpy as np
from event_model import compose_descriptor, compose_run
from rapidz.core import Stream, zip as szip, move_to_first
from shed.doc_gen import CreateDocs, get_dtype
from xonsh.lib.collections import ChainDB, _convert_to_dict

ALL = "--ALL THE DOCS--"

DTYPE_MAP = {np.ndarray: "array", int: "number", float: "number"}


def _hash_or_uid(node):
    return getattr(node, "uid", hash(node))


def build_upstream_node_set(node, s=None):
    """Build a set of all the nodes in a rapidz graph

    Parameters
    ----------
    node : Stream
        The node to use as a starting point for building the set
    s : set or None
        The set to put the nodes into. If None return a new set full of nodes

    Returns
    -------
    s : set
        The set of nodes in the graph

    """
    if s is None:
        s = set()
    if node is None:
        return
    s.add(node)
    if isinstance(node, SimpleFromEventStream):
        return
    [build_upstream_node_set(n, s) for n in node.upstreams]
    return s


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
class simple_to_event_stream(Stream, CreateDocs):
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

    def __init__(
        self,
        upstream,
        data_keys=None,
        stream_name=None,
        data_key_md=None,
        **kwargs,
    ):
        if stream_name is None:
            stream_name = str(data_keys)

        Stream.__init__(self, upstream, stream_name=stream_name)
        CreateDocs.__init__(self, data_keys, data_key_md=data_key_md, **kwargs)

        move_to_first(self)

        self.incoming_start_uid = None
        self.incoming_stop_uid = None

        self.state = "stopped"
        self.subs = []

        self.uid = str(uuid.uuid4())

        # walk upstream to get all upstream nodes to the translation node
        # get start_uids from the translation node
        self.graph = nx.DiGraph()
        walk_to_translation(self, graph=self.graph)

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
            raise RuntimeError(
                f"No Principle Nodes Detected for node "
                f"{data_keys}, "
                f"{[k.data_address for k in self.translation_nodes.values()]}"
            )
        for p in self.principle_nodes:
            p.subs.append(self)

    def emit_start(self, x):
        # if we have seen this start document already do nothing, we have
        # multiple parents so we may get a start doc multiple times
        name, doc = x
        if doc["uid"] is self.incoming_start_uid:
            return
        else:
            self.incoming_start_uid = doc["uid"]
            # Prime stop document
            self.incoming_stop_uid = None
        # Emergency stop if we get a new start document and no stop has been
        # issued
        if self.state != "stopped":
            self.emit_stop(x)
        # This is a bit of jank to make certain we don't override the
        # user metadata with pipeline metadata
        old_md = dict(self.md)

        self.md.update(
            dict(
                parent_uids=list(
                    set(
                        [
                            v.start_uid
                            for k, v in self.translation_nodes.items()
                            if v.start_uid is not None
                        ]
                    )
                ),
                parent_node_map={
                    v.uid: v.start_uid
                    for k, v in self.translation_nodes.items()
                    if v.start_uid is not None
                },
                # keep track of this so we know which node we're sending
                # data from (see merkle hash in DBFriendly)
                outbound_node=self.uid,
            )
        )
        start = self.create_doc("start", x)
        self.md = old_md

        # emit starts to subs first in case we create an event from the start
        [s.emit_start(x) for s in self.subs]
        self.emit(start)
        self.state = "started"

    def emit_stop(self, x):
        name, doc = x
        if doc["uid"] is self.incoming_stop_uid:
            return
        else:
            self.incoming_stop_uid = doc["uid"]
            # Prime for next run
            self.incoming_start_uid = None
        stop = self.create_doc("stop", x)
        ret = self.emit(stop)
        [s.emit_stop(x) for s in self.subs]
        self.state = "stopped"
        return ret

    def update(self, x, who=None):
        rl = []
        # If we have a start document ready to go, release it.
        if self.state == "stopped":
            raise RuntimeError(
                "Can't emit events from a stopped state "
                "it seems that a start was not emitted"
            )
        if self.state == "started":
            rl.append(self.emit(self.create_doc("descriptor", x)))
            self.state = "described"
        rl.append(self.emit(self.create_doc("event", x)))

        return rl


_GLOBAL_SCAN_ID = 0


@Stream.register_api()
class simple_to_event_stream_new_api(Stream):
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

    def __init__(self, descriptor_dicts, stream_name=None, **kwargs):
        self.descriptor_dicts = descriptor_dicts
        self.descriptors = {}
        if stream_name is None:
            stream_name = ""
            for v in descriptor_dicts.values():
                stream_name += f"{v.get('name', 'primary')} " + ", ".join(
                    v.get("data_keys", {}).keys()
                )

        Stream.__init__(
            self,
            upstreams=[k for k in descriptor_dicts.keys()],
            stream_name=stream_name,
        )
        self.md = kwargs

        move_to_first(self)

        self.incoming_start_uid = None
        self.incoming_stop_uid = None

        self.state = "stopped"
        self.subs = []

        self.uid = str(uuid.uuid4())

        # walk upstream to get all upstream nodes to the translation node
        # get start_uids from the translation node
        self.graph = nx.DiGraph()
        walk_to_translation(self, graph=self.graph)

        self.translation_nodes = {
            k: n["stream"]
            for k, n in self.graph.node.items()
            if isinstance(
                n["stream"],
                (
                    simple_from_event_stream,
                    simple_to_event_stream,
                    simple_to_event_stream_new_api,
                ),
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
            raise RuntimeError(
                f"No Principle Nodes Detected for node "
                f"{stream_name}, "
                f"{[k.data_address for k in self.translation_nodes.values()]}"
            )
        for p in self.principle_nodes:
            p.subs.append(self)

    def emit_start(self, x):
        # if we have seen this start document already do nothing, we have
        # multiple parents so we may get a start doc multiple times
        name, doc = x
        if doc["uid"] is self.incoming_start_uid:
            return
        else:
            self.incoming_start_uid = doc["uid"]
            # Prime stop document
            self.incoming_stop_uid = None
        # Emergency stop if we get a new start document and no stop has been
        # issued
        if self.state != "stopped":
            self.emit_stop(x)
        # This is a bit of jank to make certain we don't override the
        # user metadata with pipeline metadata
        old_md = dict(self.md)

        self.md.update(
            dict(
                parent_uids=list(
                    set(
                        [
                            v.start_uid
                            for k, v in self.translation_nodes.items()
                            if v.start_uid is not None
                        ]
                    )
                ),
                parent_node_map={
                    v.uid: v.start_uid
                    for k, v in self.translation_nodes.items()
                    if v.start_uid is not None
                },
                # keep track of this so we know which node we're sending
                # data from (see merkle hash in DBFriendly)
                outbound_node=self.uid,
            )
        )
        global _GLOBAL_SCAN_ID
        _GLOBAL_SCAN_ID += 1
        self.md.update(scan_id=_GLOBAL_SCAN_ID)
        bundle = compose_run(metadata=self.md, validate=False)
        start, self.desc_fac, self.resc_fac, self.stop_factory = bundle
        self.start_uid = start["uid"]
        self.md = old_md

        # emit starts to subs first in case we create an event from the start
        [s.emit_start(x) for s in self.subs]
        self.emit(("start", start))
        self.state = "started"

    def emit_stop(self, x):
        name, doc = x
        if doc["uid"] is self.incoming_stop_uid:
            return
        else:
            self.incoming_stop_uid = doc["uid"]
            # Prime for next run
            self.incoming_start_uid = None
        stop = self.stop_factory()
        self.descriptors.clear()
        ret = self.emit(("stop", stop))
        [s.emit_stop(x) for s in self.subs]
        self.state = "stopped"
        return ret

    def update(self, x, who=None):
        rl = []
        # If we have a start document ready to go, release it.
        if self.state == "stopped":  # pragma: no coverage
            raise RuntimeError(
                "Can't emit events from a stopped state "
                "it seems that a start was not emitted"
            )

        descriptor_dict = self.descriptor_dicts[who]
        data_keys = descriptor_dict.setdefault("data_keys", {})

        # If there are no data_keys then we are taking in a dict and the
        # keys of the dict will be the keys for the stream
        if data_keys == {}:
            if not isinstance(x, Mapping):  # pragma: no coverage
                raise TypeError(
                    f"No data keys were provided so expected "
                    f"Mapping, but {type(x)} found"
                )
            data_keys = {k: {} for k in x}

        # If the incoming data is a dict extract the data as a tuple
        if isinstance(x, Mapping):
            x = tuple([x[k] for k in data_keys.keys()])
        # normalize the data to a tuple
        if not isinstance(x, tuple):
            tx = tuple([x])
        # XXX: need to do something where the data is a tuple!
        elif len(data_keys) == 1:
            tx = tuple([x])
        else:
            tx = x

        # If we haven't issued a descriptor yet make one
        if who not in self.descriptors:
            # clobber the user supplied metadata and the auto generated
            # metadata via ChainDB with resolution favoring the user's input
            descriptor, self.descriptors[who], _ = self.desc_fac(
                **_convert_to_dict(
                    ChainDB(
                        dict(
                            name="primary",
                            data_keys={
                                k: {
                                    "source": "analysis",
                                    "dtype": get_dtype(xx),
                                    "shape": getattr(xx, "shape", []),
                                    **data_keys[k].get(k, {}),
                                }
                                for k, xx in zip(data_keys, tx)
                            },
                            hints={
                                "analyzer": {"fields": sorted(list(data_keys))}
                            },
                            object_keys={k: [k] for k in data_keys},
                        ),
                        descriptor_dict,
                    )
                ),
                validate=False,
            )
            rl.append(self.emit(("descriptor", descriptor)))
            self.state = "described"
        event = self.descriptors[who](
            timestamps={k: time.time() for k in data_keys},
            data={k: v for k, v in zip(data_keys, tx)},
            validate=False,
        )
        rl.append(self.emit(("event", event)))

        return rl


@Stream.register_api()
class SimpleToEventStream(simple_to_event_stream):
    pass


@Stream.register_api()
class simple_from_event_stream(Stream):
    """Extracts data from the event stream, and passes it downstream.

        Parameters
        ----------

        doc_type : {'start', 'descriptor', 'event', 'stop'}
            The type of document to extract data from
        data_address : tuple
            A tuple of successive keys walking through the document considered,
            if the tuple is empty all the data from that document is returned
            as a dict
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
            Defaults to False. Note that one principle node is required for
            proper pipeline operation.

        Notes
        -----
        The result emitted from this stream no longer follows the document
        model.

        This node also keeps track of when and which data came through the
        node.


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
        s.emit(('event', {'uid' : str(uuid.uuid4()),
                          'data': {'det_image' : 1}}))
        s.emit(('stop', {'uid' : str(uuid.uuid4())}))
        prints:
        1
        """

    def __init__(
        self,
        upstream,
        doc_type,
        data_address,
        event_stream_name=ALL,
        stream_name=None,
        principle=False,
        **kwargs,
    ):
        if stream_name is None:
            stream_name = str(data_address)
        asynchronous = None
        if "asynchronous" in kwargs:
            asynchronous = kwargs.pop("asynchronous")
        if stream_name is None:
            stream_name = "{} {}".format(doc_type, str(data_address))
        Stream.__init__(
            self, upstream, stream_name=stream_name, asynchronous=asynchronous
        )
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
            [s.emit_start(x) for s in self.subs]
        if name == "descriptor" and (
            self.event_stream_name == ALL
            or self.event_stream_name == doc.get("name", "primary")
        ):
            self.descriptor_uids.append(doc["uid"])
        if name == "stop":
            # Trigger the downstream nodes to make a stop but they can emit
            # on their own time
            self.descriptor_uids = []
            [s.emit_stop(x) for s in self.subs]
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


class SimpleFromEventStream(simple_from_event_stream):
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
        simple_from_event_stream.__init__(
            self,
            upstream=upstream,
            doc_type=doc_type,
            data_address=data_address,
            event_stream_name=event_stream_name,
            stream_name=stream_name,
            principle=principle,
            **kwargs,
        )


@Stream.register_api()
class align_event_streams(szip):
    """Zips and aligns multiple streams of documents, note that the last
    upstream takes precedence where merging is not possible, this requires
    the two streams to be of equal length."""

    def __init__(
        self, *upstreams, event_stream_name=ALL, stream_name=None, **kwargs
    ):
        szip.__init__(self, *upstreams, stream_name=stream_name)
        doc_names = ["start", "descriptor", "event", "stop"]
        self.event_stream_name = event_stream_name
        self.descriptor_uids = []
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
        if names[0] == "start":
            docs[0].update(
                original_start_uid=docs[0]["uid"],
                original_time=docs[0]["time"],
            )
        return super()._emit((names[0], _convert_to_dict(ChainDB(*docs))))

    def update(self, x, who=None):
        name, doc = x
        if name == "start":
            # If we got a new start document for that incoming stream clear
            # all its buffers
            for buffer in self.true_buffers.values():
                buffer[who].clear()

            self.descriptor_uids = []
        if name == "descriptor":
            # if we want the descriptor continue, else bounce out
            if (
                self.event_stream_name == ALL
                or self.event_stream_name == doc.get("name", "primary")
            ):
                self.descriptor_uids.append(doc["uid"])
            else:
                return []
        # If this isn't the event we're looking for then do nothing!
        if name == "event" and doc["descriptor"] not in self.descriptor_uids:
            return []
        # don't buffer resource and datum, as those are weird and not zippable
        # maybe at some point emit those without zip
        if name in self.true_buffers:
            self.buffers = self.true_buffers[name]
            self.literals = self.true_literals[name]
            ret = super().update((name, doc), who)
            # if we have a list that means we returned something, meaning
            # the buffers were full of starts, which means we can clear the
            # rest of the buffers
            if ret == [] and name == "start":
                for name, tb in self.true_buffers.items():
                    for upstream, b in tb.items():
                        b.clear()
            return ret


@Stream.register_api()
class AlignEventStreams(align_event_streams):
    pass


@Stream.register_api()
class LastCache(Stream):
    """Cache the last event in all streams then emit them under their own
    descriptor when the stop document comes down."""

    def update(self, x, who=None):
        name, doc = x
        if name == "start":
            self.last_caches = {}
            self.start = doc
        elif name == "descriptor":
            self.last_caches[doc["uid"]] = {
                "name": doc["name"],
                "doc": None,
                "data_keys": doc["data_keys"],
            }
        elif name == "event":
            self.last_caches[doc["descriptor"]]["doc"] = doc
        elif name == "stop":
            for descriptor_uid, cache in self.last_caches.items():
                # if we don't have any docs in this stream do nothing
                if cache["doc"]:
                    descriptor, events_func, _ = compose_descriptor(
                        start=self.start,
                        streams={},
                        event_counter={},
                        name=f'final_{cache["name"]}',
                        validate=False,
                        data_keys=cache["data_keys"],
                    )
                    self.emit(("descriptor", descriptor))
                    self.emit(
                        (
                            "event",
                            events_func(
                                data=cache["doc"]["data"],
                                timestamps=cache["doc"]["timestamps"],
                            ),
                        )
                    )
        self.emit(x)
