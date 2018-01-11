import time
import uuid

from databroker._core import ALL
from streamz_ext.core import Stream
import networkx as nx


def walk_to_translation(node, graph, prior_node=None):
    """Creates a graph instance that is a subset of the graph from the stream.

    The walk starts at a translation ``ToEventStream`` node and ends at any
    instances of FromEventStream or ToEventStream.  Each iteration of the walk
    goes up one node, determines if that node is a ``FromEventStream`` node, if
    not walks one down to see if there are any ``ToEventStream`` nodes, if not
    it keeps walking up. The walk down allows us to replace live data with
    stored data/stubs when it comes time to get the parent uids. Graph nodes
    are hashes of the node objects with ``stream=node`` in the nodes.

    Parameters
    ----------
    node : ToEventStream instance
    graph : networkx.DiGraph instance
    """
    if node is None:
        return
    t = hash(node)
    graph.add_node(t, stream=node)
    if prior_node:
        tt = hash(prior_node)
        if graph.has_edge(t, tt):
            return
        else:
            graph.add_edge(t, tt)
            if isinstance(node, FromEventStream):
                return
            else:
                for downstream in node.downstreams:
                    ttt = hash(downstream)
                    if isinstance(downstream,
                                  ToEventStream) and ttt not in graph:
                        graph.add_node(ttt, stream=downstream)
                        graph.add_edge(t, ttt)
                        return

    for node2 in node.upstreams:
        # Stop at translation node
        if node2 is not None:
            walk_to_translation(node2, graph, node)


@Stream.register_api()
class FromEventStream(Stream):
    """Converts an element from the event stream into a base type, and passes
    it down.

    Parameters
    ---------------
    upstream :
        the upstream node to receive streams from
    doc_type:
        The type of document ('start', 'descriptor', 'event', 'stop')
    data_address:
        A tuple of successive keys walking through the document considered
    event_stream_name : str, optional
        Filter by en event stream name (see :
        http://nsls-ii.github.io/databroker/api.html?highlight=stream_name#data)
    stream_name : str, optional
        Name for this stream node

    Notes
    --------
    The result emitted from this stream no longer follows the document model.


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

    def __init__(self, upstream, doc_type, data_address, event_stream_name=ALL,
                 stream_name=None, principle=False):
        Stream.__init__(self, upstream, stream_name=stream_name)
        self.stopped = False
        self.principle = principle
        self.doc_type = doc_type
        if isinstance(data_address, str):
            data_address = tuple([data_address])
        self.data_address = data_address
        self.event_stream_name = event_stream_name
        self.start_uid = None
        self.descriptor_uids = None
        self.run_start_uid = None
        self.subs = []

    def update(self, x, who=None):
        name, doc = x
        if name == 'start':
            self.stopped = False
            self.start_uid = doc['uid']
            self.descriptor_uids = {}
        if name == 'descriptor':
            self.descriptor_uids[doc['uid']] = doc.get('name', 'primary')
        if name == 'stop':
            self.start_uid = None
            # FIXME: I don't know what this does to backpressure
            [s.emit(s.create_stop(x)) for s in self.subs]
        inner = doc.copy()
        if (name == self.doc_type and
                ((name == 'descriptor' and
                  (self.event_stream_name is ALL or
                   self.event_stream_name == doc.get('name', 'primary'))) or
                 (name == 'event' and
                  (self.event_stream_name == ALL or
                   self.descriptor_uids[doc['descriptor']] ==
                   self.event_stream_name)) or
                 name in ['start', 'stop'])):

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
            return self._emit(inner)


@Stream.register_api()
class ToEventStream(Stream):
    """Converts an element from the base type into a event stream,
    and passes it down.

    Parameters
    ---------------
    upstream :
        the upstream node to receive streams from
    data_keys: tuple
        Names of the data keys
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

    def __init__(self, upstream, data_keys, stream_name=None, principle=False,
                 **kwargs):
        Stream.__init__(self, upstream, stream_name=stream_name)
        self.index_dict = dict()
        self.data_keys = data_keys
        self.md = kwargs
        self.principle = principle

        self.start_uid = None
        self.parent_uids = None
        self.descriptor_uid = None
        self.stopped = False

        # walk upstream to get all upstream nodes to the translation node
        # get start_uids from the translation node
        self.graph = nx.DiGraph()
        walk_to_translation(self, graph=self.graph)

        self.translation_nodes = {k: n['stream'] for k, n in
                                  self.graph.node.items()
                                  if isinstance(n['stream'],
                                                (FromEventStream,
                                                 ToEventStream)
                                                ) and n['stream'] != self}
        self.principle_nodes = [n for n in self.translation_nodes.values()
                                if n.principle is True]
        for p in self.principle_nodes:
            p.subs.append(self)

    def update(self, x, who=None):
        rl = []
        # Need a way to address translation nodes and start_uids, maybe hash
        current_start_uids = {k: v.start_uid for k, v in
                              self.translation_nodes.items()}

        # Bootstrap
        if self.parent_uids is None:
            self.parent_uids = current_start_uids
            rl.extend([self.emit(self.create_start(x)),
                       self.emit(self.create_descriptor(x))])

        # If the start uids are different then we have new data
        # Issue a stop then the start/descriptor
        elif self.parent_uids != current_start_uids and not self.stopped:
            rl.extend([self.emit(self.create_stop(x)),
                       self.emit(self.create_start(x)),
                       self.emit(self.create_descriptor(x))])

        rl.append(self.emit(self.create_event(x)))
        return rl

    def create_start(self, x):
        self.stopped = False
        self.start_uid = str(uuid.uuid4())
        new_start_doc = self.md
        new_start_doc.update(
            dict(
                uid=self.start_uid,
                time=time.time(),
                graph=list(nx.generate_edgelist(self.graph, data=True)),
                parent_uids={k: v.start_uid for k, v in
                             self.translation_nodes.items()
                             if v.start_uid is not None}))
        self.index_dict = dict()
        return 'start', new_start_doc

    def create_descriptor(self, x):
        if not isinstance(x, tuple):
            tx = tuple([x])
        else:
            tx = x
        self.descriptor_uid = str(uuid.uuid4())
        self.index_dict[self.descriptor_uid] = 0

        new_descriptor = dict(
            uid=self.descriptor_uid,
            time=time.time(),
            run_start=self.start_uid,
            name='primary',
            data_keys={k: {'source': 'analysis',
                           'dtype': str(type(xx)),
                           'shape': getattr(xx, 'shape', [])
                           } for k, xx in zip(self.data_keys, tx)})
        return 'descriptor', new_descriptor

    def create_event(self, x):
        if not isinstance(x, tuple):
            tx = tuple([x])
        else:
            tx = x
        new_event = dict(uid=str(uuid.uuid4()),
                         time=time.time(),
                         timestamps={},
                         descriptor=self.descriptor_uid,
                         filled={k[0]: True for k in self.data_keys},
                         data={k: v for k, v in zip(self.data_keys, tx)},
                         seq_num=self.index_dict[self.descriptor_uid])
        self.index_dict[self.descriptor_uid] += 1
        return 'event', new_event

    def create_stop(self, x):
        new_stop = dict(uid=str(uuid.uuid4()),
                        time=time.time(),
                        run_start=self.start_uid)
        self.start_uid = None
        self.stopped = True
        self.parent_uids = None
        return 'stop', new_stop
