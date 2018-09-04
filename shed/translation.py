import inspect
import time

import networkx as nx
import numpy as np
from streamz_ext.core import Stream

from .simple import SimpleToEventStream, SimpleFromEventStream, \
    _hash_or_uid

ALL = '--ALL THE DOCS--'

DTYPE_MAP = {np.ndarray: 'array', int: 'number', float: 'number'}


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

    def __init__(self, doc_type, data_address, upstream=None,
                 event_stream_name=ALL,
                 stream_name=None, principle=False):
        super().__init__(doc_type=doc_type, data_address=data_address,
                         upstream=upstream,
                         stream_name=stream_name,
                         principle=principle,
                         event_stream_name=event_stream_name)
        self.run_start_uid = None
        self.times = {}

    def update(self, x, who=None):
        name, doc = x
        self.times[time.time()] = doc.get('uid', doc.get('datum_id'))
        if name == 'start':
            self.times = {time.time(): doc['uid']}
            self.start_uid = doc['uid']
        return super().update(x, who=None)


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
        return 'start', new_start_doc

    def create_stop(self, x):
        new_stop = super()._create_stop(x)
        times = {}
        for k, node in self.translation_nodes.items():
            for t, uid in node.times.items():
                times[t] = {'node': node.uid, 'uid': uid}
        new_stop.update(times=times)
        self.stop = ('stop', new_stop)
        if not self.futures or all(not v for v in self.futures.values()):
            self.emit(self.stop)
            self.stop = None
        return "stop", new_stop


@Stream.register_api()
class DBFriendly(Stream):
    """Make analyzed data (and graph) DB friendly"""
    def update(self, x, who=None):
        name, doc = x
        if name == 'start':
            doc = dict(doc)
            graph = doc['graph']
            for n in nx.topological_sort(graph):
                graph.node[n]['stream'] = db_friendly_node(
                    graph.node[n]['stream'])
            doc['graph'] = nx.node_link_data(graph)
        return self.emit((name, doc))


def db_friendly_node(node):
    """Extract data to make node db friendly"""
    if isinstance(node, dict):
        return node
    d = dict(node.__dict__)
    d['stream_name'] = d['name']
    d2 = {'name': node.__class__.__name__, 'mod': node.__module__}
    for f_name in ['func', 'predicate']:
        if f_name in d:
            # carve out for numpy ufuncs which don't have modules
            if isinstance(d[f_name], np.ufunc):
                mod = 'numpy'
            else:
                mod = d[f_name].__module__
            d2[f_name] = {'name': d[f_name].__name__,
                          'mod': mod}
            d[f_name] = {'name': d[f_name].__name__,
                         'mod': mod}

    for k in ['upstreams', 'downstreams']:
        ups = []
        for up in d[k]:
            if up is not None:
                ups.append(_hash_or_uid(up))
        d2[k] = ups
        d[k] = ups
    if len(d['upstreams']) == 1:
        d2['upstream'] = d2['upstreams'][0]
        d['upstream'] = d['upstreams'][0]

    sig = inspect.signature(node.__init__)
    params = sig.parameters
    constructed_args = []
    d2['arg_keys'] = []
    for p in params:
        d2['arg_keys'].append(p)
        q = params[p]
        # Exclude self
        if str(p) != 'self':
            if q.kind == q.POSITIONAL_OR_KEYWORD and p in d:
                constructed_args.append(d[p])
            elif q.default is not q.empty:
                constructed_args.append(q.default)
            if q.kind == q.VAR_POSITIONAL:
                constructed_args.extend(d[p])
    constructed_args.extend(d.get('args', ()))
    d2['args'] = constructed_args
    d2['kwargs'] = d.get('kwargs', {})
    return d2
