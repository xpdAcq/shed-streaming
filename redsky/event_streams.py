import functools as ft
import time
import traceback
import uuid
from collections import deque

from streams.core import Stream, no_default
from tornado.locks import Condition
from builtins import zip as zzip


def star(f):
    @ft.wraps(f)
    def wraps(args):
        return f(*args)

    return wraps


def dstar(f):
    @ft.wraps(f)
    def wraps(kwargs1, **kwargs2):
        kwargs1.update(kwargs2)
        return f(**kwargs1)

    return wraps


class EventStream(Stream):
    def __init__(self, child=None, children=None,
                 output_info=None, input_info=None,
                 **kwargs):
        """
        Serve up documents and their internals as requested.
        The main way that this works is by a) ingesting documents, b) issuing
        documents, c) returning the internals of documents upon request.

        Parameters
        ----------
        input_info: list of tuples
            describs the incoming streams
        output_info: list of tuples
            describs the resulting stream
        provenance : dict, optional
            metadata about this operation

        Notes
        ------
        input_info is designed to map keys in streams to kwargs in functions.
        It is critical for the internal data from the events to be returned,
        upon `event_guts`.
        input_info = [('input_kwarg', 'data_key')]

        output_info is designed to take the output tuple and map it back into
        data_keys.
        output_info = [('data_key', {'dtype': 'array', 'source': 'testing'})]
        """
        # TODO: this needs something super maybe a Base Class
        Stream.__init__(self, child, children)
        if output_info is None:
            output_info = {}
        if input_info is None:
            input_info = {}
        self.outbound_descriptor_uid = None
        self.output_info = output_info
        self.allowed_names = ['start', 'descriptor', 'event', 'stop']
        self.i = None
        self.run_start_uid = None
        self.provenance = {}
        self.event_failed = False
        self.input_info = input_info

    def emit(self, x):
        """ Push data into the stream at this point

        This is typically done only at source Streams but can theoretically be
        done at any point
        """
        if x is not None:
            result = []
            for parent in self.parents:
                r = parent.update(x, who=self)
                if type(r) is list:
                    result.extend(r)
                else:
                    result.append(r)
            return [element for element in result if element is not None]

    def dispatch(self, nds):
        name, docs = self.curate_streams(nds)
        if name in self.allowed_names:
            return getattr(self, name)(docs)

    def update(self, x, who=None):
            return self.emit(self.dispatch(x))

    def curate_streams(self, nds):
        # If we get multiple streams make (doc, doc, doc, ...)
        if isinstance(nds[0], tuple):
            names, docs = list(zzip(*nds))
            if len(set(names)) > 1:
                raise RuntimeError('Misaligned Streams')
            name = names[0]
        # If only one stream then (doc, )
        else:
            names, docs = nds
            name = names
            docs = (docs,)
        return name, docs

    def generate_provenance(self, func=None):
        d = dict(
            stream_class=self.__class__.__name__,
            stream_class_module=self.__class__.__module__,
            # TODO: Need to support pip and other sources at some point
            # conda_list=subprocess.check_output(['conda', 'list',
            #                                     '-e']).decode()
        )
        if self.input_info:
            d.update(input_info=self.input_info)
        if self.output_info:
            d.update(output_info=self.output_info)
        if func:
            d.update(function_module=func.__module__,
                     # this line gets more complex with classes
                     function_name=func.__name__, )
        self.provenance = d

    def start(self, docs):
        """
        Issue new start document for input documents

        Parameters
        ----------
        docs: tuple of dicts or dict

        Returns
        -------

        """
        self.run_start_uid = str(uuid.uuid4())
        new_start_doc = dict(uid=self.run_start_uid,
                             time=time.time(),
                             parents=[doc['uid'] for doc in docs],
                             # parent_keys=[k for k in stream_keys],
                             provenance=self.provenance)
        return 'start', new_start_doc

    def descriptor(self, docs):
        if self.run_start_uid is None:
            raise RuntimeError("Received EventDescriptor before "
                               "RunStart.")
        # If we had to describe the output information then we need an all new
        # descriptor
        self.outbound_descriptor_uid = str(uuid.uuid4())
        new_descriptor = dict(uid=self.outbound_descriptor_uid,
                              time=time.time(),
                              run_start=self.run_start_uid)
        if self.output_info:
            new_descriptor.update(
                data_keys={k: v for k, v in self.output_info})

        # no truly new data needed
        elif all(d['data_keys'] == docs[0]['data_keys'] for d in docs):
            new_descriptor.update(data_keys=docs[0]['data_keys'])

        else:
            raise RuntimeError("Descriptor mismatch: "
                               "you have tried to combine descriptors with "
                               "different data keys")
        self.i = 0
        return 'descriptor', new_descriptor

    def event(self, docs):
        return docs

    def stop(self, docs):
        if not self.event_failed:
            if self.run_start_uid is None:
                raise RuntimeError("Received RunStop before RunStart.")
            new_stop = dict(uid=str(uuid.uuid4()),
                            time=time.time(),
                            run_start=self.run_start_uid)
            if isinstance(docs, Exception):
                self.event_failed = True
                new_stop.update(reason=repr(docs),
                                trace=traceback.format_exc(),
                                exit_status='failure')
            if not self.event_failed:
                new_stop.update(exit_status='success')
            self.outbound_descriptor_uid = None
            self.run_start_uid = None
            return 'stop', new_stop

    def event_guts(self, docs):
        """
        Provide some of the event data as a dict, which may be used as kwargs

        Parameters
        ----------
        docs

        Returns
        -------

        """
        return {input_kwarg: doc['data'][data_key] for
                (input_kwarg, data_key), doc in zzip(self.input_info, docs)}

    def issue_event(self, outputs):
        """Issue a new event

        Parameters
        ----------
        outputs: tuple, dict, or other

        Returns
        -------

        """
        if not self.event_failed:
            if self.run_start_uid is None:
                raise RuntimeError("Received Event before RunStart.")
            if isinstance(outputs, Exception):
                return self.stop(outputs)

            # Make a new event with no data
            if len(self.output_info) == 1:
                outputs = (outputs,)

            new_event = dict(uid=str(uuid.uuid4()),
                             time=time.time(),
                             timestamps={},
                             descriptor=self.outbound_descriptor_uid,
                             filled={k[0]: True for k in self.output_info},
                             seq_num=self.i)

            if self.output_info:
                new_event.update(data={output_name: output
                                       for (output_name, desc), output in
                                       zzip(self.output_info, outputs)})
            else:
                new_event.update(data=outputs['data'])
            self.i += 1
            return 'event', new_event

    def refresh_event(self, event):
        """Issue a new event

        Parameters
        ----------
        event: tuple, dict, or other

        Returns
        -------

        """
        if not self.event_failed:
            if self.run_start_uid is None:
                raise RuntimeError("Received Event before RunStart.")
            if isinstance(event, Exception):
                return self.stop(event)

            new_event = dict(event)
            new_event.update(dict(uid=str(uuid.uuid4()),
                                  time=time.time(),
                                  timestamps={},
                                  seq_num=self.i))

            self.i += 1
            return 'event', new_event


class map(EventStream):
    def __init__(self, func, child, raw=False, output_info=None,
                 input_info=None, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.raw = raw

        EventStream.__init__(self, child, output_info=output_info,
                             input_info=input_info, **kwargs)
        self.generate_provenance(func)

    def event(self, docs):
        try:
            # we need to expose the raw event data
            res = self.event_guts(docs)
            result = self.func(res, **self.kwargs)
            # Now we must massage the raw return into a new event
            result = self.issue_event(result)
        except Exception as e:
            result = self.issue_event(e)
        return result


class filter(EventStream):
    def __init__(self, predicate, child, full_event=False, **kwargs):
        self.predicate = predicate

        EventStream.__init__(self, child, **kwargs)
        self.full_event = full_event
        self.generate_provenance(predicate)

    def event(self, doc):
        if not self.full_event:
            doc = self.event_guts(doc)
        if self.predicate(doc):
            return doc


class scan(EventStream):
    def __init__(self, func, child, start=no_default, state_key=None,
                 output_info=None,
                 input_info=None):
        self.state_key = state_key
        self.func = func
        self.state = start
        EventStream.__init__(self, child, input_info=input_info,
                             output_info=output_info)
        self.generate_provenance(func)

    def event(self, doc):
        doc = self.event_guts(doc)
        # TODO: this handling of the initial state is a bit clunky
        # I need to decide if state is going to be the array or the dict
        if self.state is no_default:
            self.state = doc
        # in case we need a bit more flexibility eg lambda x: np.empty(x.shape)
        elif hasattr(self.state, '__call__'):
            self.state = self.state(doc)
        else:
            doc[self.state_key] = self.state
            result = self.func(doc)
            self.state = result
        return self.issue_event(self.state)


# class partition(EventStream):
#     def __init__(self, n, child):
#         self.n = n
#         self.buffer = []
#         EventStream.__init__(self, child)
#
#     def update(self, x, who=None):
#         self.buffer.append(x)
#         if len(self.buffer) == self.n:
#             result, self.buffer = self.buffer, []
#             return self.emit(tuple(result))
#         else:
#             return []


# class sliding_window(EventStream):
#     def __init__(self, n, child):
#         self.n = n
#         self.buffer = deque(maxlen=n)
#         EventStream.__init__(self, child)
#
#     def update(self, x, who=None):
#         self.buffer.append(x)
#         if len(self.buffer) == self.n:
#             return self.emit(tuple(self.buffer))
#         else:
#             return []


# class timed_window(EventStream):
#     def __init__(self, interval, child, loop=None):
#         self.interval = interval
#         self.buffer = []
#         self.last = gen.moment
#
#         EventStream.__init__(self, child, loop=loop)
#
#         self.loop.add_callback(self.cb)
#
#     def update(self, x, who=None):
#         self.buffer.append(x)
#         return self.last
#
#     @gen.coroutine
#     def cb(self):
#         while True:
#             L, self.buffer = self.buffer, []
#             self.last = self.emit(L)
#             yield self.last
#             yield gen.sleep(self.interval)


# class delay(EventStream):
#     def __init__(self, interval, child, loop=None):
#         self.interval = interval
#         self.queue = Queue()
#
#         EventStream.__init__(self, child, loop=loop)
#
#         self.loop.add_callback(self.cb)
#
#     @gen.coroutine
#     def cb(self):
#         while True:
#             last = time()
#             x = yield self.queue.get()
#             yield self.emit(x)
#             duration = self.interval - (time() - last)
#             if duration > 0:
#                 yield gen.sleep(duration)
#
#     def update(self, x, who=None):
#         return self.queue.put(x)


# class rate_limit(EventStream):
#     def __init__(self, interval, child):
#         self.interval = interval
#         self.next = 0
#
#         EventStream.__init__(self, child)
#
#     @gen.coroutine
#     def update(self, x, who=None):
#         now = time()
#         old_next = self.next
#         self.next = max(now, self.next) + self.interval
#         if now < old_next:
#             yield gen.sleep(old_next - now)
#         yield self.emit(x)


# class buffer(EventStream):
#     def __init__(self, n, child, loop=None):
#         self.queue = Queue(maxsize=n)
#
#         EventStream.__init__(self, child, loop=loop)
#
#         self.loop.add_callback(self.cb)
#
#     def update(self, x, who=None):
#         return self.queue.put(x)
#
#     @gen.coroutine
#     def cb(self):
#         while True:
#             x = yield self.queue.get()
#             yield self.emit(x)


class zip(EventStream):
    def __init__(self, *children, **kwargs):
        self.maxsize = kwargs.pop('maxsize', 10)
        self.buffers = [deque() for _ in children]
        self.condition = Condition()
        self.prior = ()
        EventStream.__init__(self, children=children)

    def update(self, x, who=None):
        L = self.buffers[self.children.index(who)]
        L.append(x)
        if len(L) == 1 and all(self.buffers):
            if self.prior:
                for i in range(len(self.buffers)):
                    # If the docs don't match, preempt with prior good result
                    if self.buffers[i][0][0] != self.buffers[0][0][0]:
                        self.buffers[i].appendleft(self.prior[i])
            tup = tuple(buf.popleft() for buf in self.buffers)
            self.condition.notify_all()
            self.prior = tup
            return self.emit(tup)
        elif len(L) > self.maxsize:
            return self.condition.wait()


class bundle(EventStream):
    def __init__(self, *children, **kwargs):
        self.maxsize = kwargs.pop('maxsize', 100)
        self.buffers = [deque() for _ in children]
        self.condition = Condition()
        self.prior = ()
        EventStream.__init__(self, children=children)
        self.generate_provenance()

    def update(self, x, who=None):
        L = self.buffers[self.children.index(who)]
        L.append(x)
        if len(L) == 1 and all(self.buffers):
            # if all the docs are of the same type and not an event, issue
            # new documents which are combined
            rvs = []
            while all(self.buffers):
                if all([b[0][0] == self.buffers[0][0][0] and b[0][0] != 'event'
                        for b in self.buffers]):
                    res = self.dispatch(
                        tuple([b.popleft() for b in self.buffers]))
                    rvs.append(self.emit(res))
                elif any([b[0][0] == 'event' for b in self.buffers]):
                    for b in self.buffers:
                        while b:
                            nd_pair = b[0]
                            # run the buffers down until no events are left
                            if nd_pair[0] != 'event':
                                break
                            else:
                                nd_pair = b.popleft()
                                new_nd_pair = self.refresh_event(nd_pair[1])
                                rvs.append(self.emit(new_nd_pair))

                else:
                    raise RuntimeError("There is a mismatch of docs, but none "
                                       "of them are events so we have reached "
                                       "a potential deadlock, so we raise "
                                       "this error instead")

            return rvs
        elif len(L) > self.maxsize:
            return self.condition.wait()


class combine_latest(EventStream):
    def __init__(self, *children, emit_on=None):
        self.last = [None for _ in children]
        self.special_docs_names = ['start', 'descriptor', 'stop']
        self.special_docs = {k: [None for _ in children] for k in
                             self.special_docs_names}
        self.missing = set(children)
        self.special_missing = {k: set(children) for k in
                                self.special_docs_names}
        if emit_on is not None:
            if not hasattr(emit_on, '__iter__'):
                emit_on = (emit_on,)
            self.emit_on = emit_on
        else:
            self.emit_on = children
        Stream.__init__(self, children=children)

    def update(self, x, who=None):
        name, doc = x
        if name in self.special_docs_names:
            idx = self.children.index(who)
            self.special_docs[name][idx] = x
            if self.special_missing[name] and who in \
                    self.special_missing[name]:
                self.special_missing[name].remove(who)

            self.special_docs[name][self.children.index(who)] = x
            if not self.special_missing[name] and who in self.emit_on:
                tup = tuple(self.special_docs[name])
                if tup and hasattr(tup[0], '__stream_merge__'):
                    tup = tup[0].__stream_merge__(*tup[1:])
                return self.emit(tup)
        else:
            if self.missing and who in self.missing:
                self.missing.remove(who)

            self.last[self.children.index(who)] = x
            if not self.missing and who in self.emit_on:
                tup = tuple(self.last)
                return self.emit(tup)


# class concat(EventStream):
#     def update(self, x, who=None):
#         L = []
#         for item in x:
#             y = self.emit(item)
#             if type(y) is list:
#                 L.extend(y)
#             else:
#                 L.append(y)
#         return L


# class unique(EventStream):
#     def __init__(self, child, history=None, key=identity):
#         self.seen = dict()
#         self.key = key
#         if history:
#             from zict import LRU
#             self.seen = LRU(history, self.seen)
#
#         EventStream.__init__(self, child)
#
#     def update(self, x, who=None):
#         y = self.key(x)
#         if y not in self.seen:
#             self.seen[y] = 1
#             return self.emit(x)


# class union(EventStream):
#     def update(self, x, who=None):
#         return self.emit(x)


# class collect(EventStream):
#     def __init__(self, child, cache=None):
#         if cache is None:
#             cache = deque()
#         self.cache = cache
#
#         EventStream.__init__(self, child)
#
#     def update(self, x, who=None):
#         self.cache.append(x)
#
#     def flush(self, _=None):
#         out = tuple(self.cache)
#         self.emit(out)
#         self.cache.clear()


class eventify(EventStream):
    """Generate events from data in starts"""

    def __init__(self, child, start_key, **kwargs):
        self.start_key = start_key
        self.val = None

        EventStream.__init__(self, child, **kwargs)

    def start(self, docs):
        self.val = docs[0][self.start_key]
        super().start(docs)

    def event(self, docs):
        return self.issue_event(self.val)
