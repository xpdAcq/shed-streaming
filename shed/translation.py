import uuid
import time

from streamz.core import Stream
from databroker._core import ALL
import networkx as nx


def walk_up(node, graph, prior_node=None):
    """Create graph from a single node, searching up and down the chain

    Parameters
    ----------
    node: Stream instance
    graph: networkx.DiGraph instance
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

    for nodes in node.upstreams:
        for node2 in nodes:
            if node2 is not None and not isinstance(node2, FromEventModel):
                walk_up(node2, graph, node)


class FromEventModel(Stream):
    def __init__(self, upstream, doc_type, data_address, event_stream_name=ALL,
                 stream_name=None):
        Stream.__init__(self, upstream, stream_name=stream_name)
        self.doc_type = doc_type
        if isinstance(data_address, str):
            data_address = tuple([data_address])
        self.data_address = data_address
        self.event_stream_name = event_stream_name
        self.start_uid = None
        self.descriptor_uids = None
        self.run_start_uid = None

    def update(self, x, who=None):
        name, doc = x
        if name == 'start':
            self.start_uid = doc['uid']
            self.descriptor_uids = {}
        if name == 'descriptor':
            self.descriptor_uids[doc['uid']] = doc.get('name', 'primary')
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
            for da in self.data_address:
                inner = inner[da]
            return self._emit(inner)


class ToEventModel(Stream):
    def __init__(self, upstream, data_keys, stream_name=None, **kwargs):
        Stream.__init__(self, upstream, stream_name=stream_name)
        self.index_dict = dict()
        self.data_keys = data_keys
        self.md = kwargs

        self.run_start_uid = None
        self.parent_uids = None
        self.descriptor_uid = None

        # walk upstream to get all upstream nodes to the translation node
        # get start_uids from the translation node
        self.graph = nx.DiGraph()
        walk_up(self, graph=self.graph)

        self.translation_nodes = {n: n.stream for n in g.nodes if isinstance(
            n, FromEventModel)}

    def update(self, x, who=None):
        # Need a way to address translation nodes and start_uids, maybe hash
        current_start_uids = {k: v.uid for k, v in self.translation_nodes.items()}

        # Bootstrap
        if self.parent_uids is None:
            self.parent_uids = current_start_uids
            self._emit(self.create_start(x))
            self._emit(self.create_descriptor(x))

        # If the start uids are different then we have new data
        # Issue a stop then the start/descriptor
        elif self.parent_uids != current_start_uids:
            self._emit(self.create_stop(x))
            self._emit(self.create_start(x))
            self._emit(self.create_descriptor(x))

        self._emit(self.create_event(x))

    def create_start(self, x):
        self.run_start_uid = str(uuid.uuid4())
        new_start_doc = self.md
        new_start_doc.update(dict(uid=self.run_start_uid, time=time.time(),
                                  graph=self.graph))
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
            run_start=self.run_start_uid,
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
                        run_start=self.run_start_uid,)
        return 'stop', new_stop
