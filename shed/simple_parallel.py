import uuid
from collections import MutableMapping

import networkx as nx
from rapidz.clients import result_maybe
from rapidz.core import move_to_first
from rapidz.parallel import ParallelStream
from shed.doc_gen import CreateDocs, get_dtype
from shed.simple import walk_to_translation, SimpleFromEventStream


@ParallelStream.register_api()
class SimpleToEventStream(ParallelStream, CreateDocs):
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
    >>> s2 = FromEventStream(source, 'event', ('data', 'det_image'),
    ...                      principle=True)
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

    # XXX: need to replace event seq_num with a conditional future.
    # f(value, current_seq_num_future)
    # If the value of the data is not null compute increment and return the
    # value otherwise don't increment and return same number.
    # Keep passing the previous future in as the current future so we
    # chain the futures.

    def __init__(self, upstream, data_keys=None, stream_name=None, **kwargs):
        if stream_name is None:
            stream_name = str(data_keys)

        ParallelStream.__init__(self, upstream, stream_name=stream_name)
        CreateDocs.__init__(self, data_keys, **kwargs)

        move_to_first(self)

        self.start_document = None

        self.state = "stopped"
        self.subs = []

        self.uid = str(uuid.uuid4())

        # walk upstream to get all upstream nodes to the translation node
        # get start_uids from the translation node
        self.graph = nx.DiGraph()
        walk_to_translation(self, graph=self.graph)

        self.translation_nodes = {
            k: n["stream"]
            for k, n in self.graph.nodes.items()
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

    def emit(self, x, asynchronous=False):
        super().emit(
            self.default_client().submit(result_maybe, x),
            asynchronous=asynchronous,
        )

    def emit_start(self, x):
        # Emergency stop
        if self.state != "stopped":
            self.emit_stop(x)
        start = self.create_doc("start", x)
        self.emit(start)
        [s.emit_start(x) for s in self.subs]
        self.state = "started"
        self.start_document = None

    def emit_stop(self, x):
        stop = self.create_doc("stop", x)
        ret = self.emit(stop)
        [s.emit_stop(x) for s in self.subs]
        self.state = "stopped"
        return ret

    def update(self, x, who=None):
        rl = []
        # If we have a start document ready to go, release it.
        if self.state == "started":
            rl.append(self.emit(self.create_doc("descriptor", x)))
            self.state = "described"
        rl.append(self.emit(self.create_doc("event", x)))

        return rl

    def start_doc(self, x):
        new_start_doc = super().start_doc(x)
        new_start_doc.update(
            dict(
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
                outbound_node=self.uid,
            )
        )
        self.start_document = ("start", new_start_doc)
        return new_start_doc

    def descriptor(self, x):
        out = super().descriptor(x)
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

        for (k, v), xx in zip(out["data_keys"].items(), tx):
            if "Future" in v["dtype"]:
                v["dtype"] = self.default_client().submit(get_dtype, xx)
        return out
