"""Stream functions (actually classes) for building pipelines for the Event
Model"""
##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2017 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright (CJ-Wright)
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import functools as ft
import time
import traceback
import uuid
from collections import deque

from streams.core import Stream, no_default
from tornado.locks import Condition
from builtins import zip as zzip
from itertools import islice


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


def istar(f):
    @ft.wraps(f)
    def wraps(*args):
        return f(args)

    return wraps


class EventStream(Stream):
    """ The EventStream class handles data of the form of an infinite
    sequence of events in the Event Model.

    In the Event model, the data-stream is a series of fortunate events.
    Each event consists of a name-document pair
    where the dictionary contains the data and metadata associated with the
    event.
    More information here:
    https://nsls-ii.github.io/architecture-overview.html

    EventStreams subscribe to each other passing and transforming data between
    them. An EventStream object listens for updates from upstream, reacts to
    these updates, and then emits more data to flow downstream to all
    EventStream objects that subscribe to it. Downstream EventStream objects
    may connect at any point of an EventStream graph to get a full view of the
    data coming off of that point to do with as they will.

    Attributes
    ----------
    md : dict
        Each event stream has a Start document that contains metadata about
        the stream
    input_info : dict, optional
        Input info for an operation. Operations are callables that will apply
        operations to the data streams. Not always needed. The input_info
        provides a map between event data keys and the callable (function)
        args/kwargs

    output_info : list of tuples, optional
        Output info from the operation, not needed for all cases
        This:
            Creates a map between function returns and event data keys
            Provides information for building a descriptor for the output
            event stream. Each event stream has a descriptor which contains
            information about what is in the events in the stream.

    Examples
    --------
    >>> my_new_stream = EventStream()

    Notes
    -----
    This is intended as a base class for the stream functions (map, filter,
    etc.).

    Most implementations of the subclasses will:
    a) override update for flow control based classes
    b) override event for operator based classes
    c) override multiple document methods (see eventify) although this is rare

    It is important for the overridden document methods that the return
    follows the pattern of ``super().document_name(new_document)``. This way
    the EventStream properly issues the new document with its name and other
    needed internal flow control.

    """

    def __init__(self, child=None, children=None,
                 *, output_info=None, input_info=None, md=None, name=None,
                 raise_upon_error=True,
                 **kwargs):
        """Initialize the stream

        Parameters
        ----------
        input_info: dict, optional
            describes the incoming streams
        output_info: list of tuples, optional
            describes the resulting stream
        md: dict, optional
            Additional metadata to be added to the run start document

        Notes
        ------
        input_info is designed to map keys in streams to kwargs in functions.
        It is critical for the internal data from the events to be returned,
        upon `event_guts`.
        input_info = {'input_kwarg': ('data_key', stream_number)}
        Note that the stream number is assumed to be zero if not specified

        output_info is designed to take the output tuple and map it back into
        data_keys.
        output_info = [('data_key', {'dtype': 'array', 'source': 'testing'})]
        """
        if md is None:
            md = {}
        if name is not None:
            md.update(name=name)
        if 'name' in md.keys():
            self.name = md['name']
        else:
            self.name = None
        Stream.__init__(self, child, children, name=self.name)
        if output_info is None:
            output_info = {}
        if input_info is None:
            input_info = {}
        self.outbound_descriptor_uid = None
        self.md = md
        self.md.update(**kwargs)
        self.output_info = output_info
        self.input_info = input_info
        self.raise_upon_error = raise_upon_error

        # TODO: need multiple counters for multiple descriptors
        # This will need to be a dict with keys of descriptor names
        self.i = None
        self.run_start_uid = None
        self.provenance = {}
        self.bypass = False
        self.excep = None

        # If the stream number is not specified its zero
        for k, v in input_info.items():
            if isinstance(v, str) or len(v) < 2:
                input_info[k] = (v, 0)
            elif isinstance(v[1], Stream):
                input_info[k] = (v[0], self.children.index(v[1]))

    def emit(self, x):
        """ Push data into the stream at this point

        Events will not automatically propagate down a stream unless they are
        emitted, so for example, one could take every second event returned
        from a node function and emit it into a subsampled output stream.
        (for this specific example see ``filter``)

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
        """Dispatch to methods expecting particular doc types.

        Parameters
        ----------
        nds: tuple
            Name document pair

        Returns
        -------
        tuple:
            New name document pair
        """
        name, docs = self.curate_streams(nds)
        return getattr(self, name)(docs)

    def update(self, x, who=None):
        """Emit the new name document pair

        Parameters
        ----------
        x: tuple
            Name document pair
        who: stream instance, optional
            This is mostly used internally for emit

        Returns
        -------
        list:
            The list of issued documents, used for back pressure
        """
        return self.emit(self.dispatch(x))

    def curate_streams(self, nds):
        """Standardize name document pairs

        Parameters
        ----------
        nds: tuple, or tuple of tuples
            The name document pair(s)

        Returns
        -------
        name: str
            The name of the output doc(s)
        docs: tuple
            The document(s)

        Notes
        ------
        If we get multiple streams make (name, (doc, doc, doc, ...))
        Otherwise (name, (doc,))
        """

        if isinstance(nds[0], tuple):
            names, docs = list(zzip(*nds))
            if len(set(names)) > 1:
                raise RuntimeError('Misaligned Streams')
            name = names[0]
        else:
            names, docs = nds
            name = names
            docs = (docs,)
        return name, docs

    def generate_provenance(self, **kwargs):
        """Generate provenance information about the stream function

        Parameters
        ----------
        func: callable
            The function used inside the stream class (see map, filter, etc.)
        """
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
        # TODO: support partials?
        d.update(**kwargs)
        for k, func in d.items():
            if callable(func):
                d[k] = dict(function_module=func.__module__,
                            # this line gets more complex with classes
                            function_name=func.__name__, )
        full_event = getattr(self, 'full_event', None)
        if full_event:
            d.update(full_event=full_event)

        self.provenance = d

    def start(self, docs):
        """
        Issue new start document for input documents

        Parameters
        ----------
        docs: tuple of dicts or dict

        Returns
        -------
        name: 'start'
        doc: dict
            The document
        """
        self.run_start_uid = str(uuid.uuid4())
        new_start_doc = dict(uid=self.run_start_uid,
                             time=time.time(),
                             provenance=self.provenance, **self.md)
        if all(docs):
            new_start_doc.update(parents=[doc['uid'] for doc in docs])
        self.bypass = False
        return 'start', new_start_doc

    def descriptor(self, docs):
        """
        Issue new descriptor document for input documents

        Parameters
        ----------
        docs: tuple of dicts or dict

        Returns
        -------
        name: 'descriptor'
        doc: dict
            The document
        """
        if not self.bypass:
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
        """
        Issue event document for input documents

        Parameters
        ----------
        docs: tuple of dicts or dict

        Returns
        -------
        name: 'event'
        doc: dict
            The document
        """
        if not self.bypass:
            return 'event', docs

    def stop(self, docs):
        """
        Issue new stop document

        Parameters
        ----------
        docs: tuple of dicts or dict or Exception
            If Exception issue a stop which notes the failure

        Returns
        -------
        name: 'stop'
        doc: dict
            The document
        """
        if not self.bypass:
            if self.run_start_uid is None:
                raise RuntimeError("Received RunStop before RunStart.")
            new_stop = dict(uid=str(uuid.uuid4()),
                            time=time.time(),
                            run_start=self.run_start_uid)
            if isinstance(docs, Exception):
                self.bypass = True
                new_stop.update(reason=repr(docs),
                                trace=traceback.format_exc(),
                                exit_status='failure')
                self.excep = docs
            else:
                new_stop.update(exit_status='success')
            self.outbound_descriptor_uid = None
            self.run_start_uid = None
            return 'stop', new_stop
        elif self.raise_upon_error:
            raise self.excep

    def event_guts(self, docs, full_event=False):
        """
        Provide some of the event data as a dict, which may be used as kwargs

        Parameters
        ----------
        docs: tuple of dicts
        full_event: bool, optional
            If True expose the data from full event, else only expose data
            from the 'data' dictionary inside the event, defaults to False

        Returns
        -------

        """
        # TODO: what if I want to operate on the full event? Return full dict
        if full_event:
            return {input_kwarg: docs[position][data_key] for
                    input_kwarg, (data_key, position) in
                    self.input_info.items()}
        else:
            return {input_kwarg: docs[position]['data'][data_key] for
                    input_kwarg, (data_key, position) in
                    self.input_info.items()}

    def issue_event(self, outputs):
        """Create a new event document based off of function returns

        Parameters
        ----------
        outputs: tuple, dict, or other
            Data returned from some external functon

        Returns
        -------
        new_event: dict
            The new event

        Notes
        -----
        If the outputs of the function is an exception no event will be
        created, but a stop document will be issued.
        """
        if not self.bypass:
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
            return new_event

    def refresh_event(self, event):
        """Create a new event with the same data, but new metadata
        (uid, timestamp, etc.)

        Parameters
        ----------
        event: tuple, dict, or other
            The event document
        Returns
        -------
        new_event: dict
            A new event with the same core data
        """
        if not self.bypass:
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
            return new_event


class map(EventStream):
    """Apply a function onto every event in the stream"""

    def __init__(self, func, child, *,
                 full_event=False,
                 output_info=None, input_info=None,
                 **kwargs):
        """Initialize the node

        Parameters
        ----------
        func: callable
            The function to map on the event data
        child: EventStream instance
            The source of the data
        full_event: bool, optional
            If True expose the full event dict to the function, if False
            only expose the data from the event
        input_info: dict
            describe the incoming streams
        output_info: list of tuples
            describe the resulting stream
        """
        self.func = func
        self.kwargs = kwargs

        EventStream.__init__(self, child, output_info=output_info,
                             input_info=input_info, **kwargs)
        self.full_event = full_event
        self.generate_provenance(function=func)

    def event(self, docs):
        try:
            # we need to expose the event data
            res = self.event_guts(docs, self.full_event)
            result = self.func(res, **self.kwargs)
            # Now we must massage the raw return into a new event
            result = self.issue_event(result)
        except Exception as e:
            return super().stop(e)
        else:
            return super().event(result)


class filter(EventStream):
    """Only pass through events that satisfy the predicate"""

    def __init__(self, predicate, child, *, input_info,
                 full_event=False, **kwargs):
        """Initialize the node

        Parameters
        ----------
        predicate: callable
            The function which returns True if the event is to propagate
            further in the pipeline
        child: EventStream instance
            The source of the data
        input_info: dict
            describe the incoming streams
        full_event: bool, optional
            If True expose the full event dict to the predicate, if False
            only expose the data from the event
        """
        self.predicate = predicate

        EventStream.__init__(self, child, input_info=input_info, **kwargs)
        self.full_event = full_event
        self.generate_provenance(predicate=predicate)

    def event(self, doc):
        g = self.event_guts(doc, self.full_event)
        try:
            if self.predicate(g):
                return super().event(doc[0])
        except Exception as e:
            return super().stop(e)


class accumulate(EventStream):
    """Accumulate results with previous state

    This preforms running or cumulative reductions, applying the function
    to the previous total and the new element.  The function should take
    two arguments, the previous accumulated state and the next element and
    it should return a new accumulated state.
    """

    def __init__(self, func, child, state_key=None, *,
                 full_event=False,
                 output_info=None,
                 input_info=None, start=no_default):
        """Initialize the node

        Parameters
        ----------
        func: callable
            The function to map on the event data
        child: EventStream instance
            The source of the data
        state_key: str
            The keyword for current accumulated state in the func
        full_event: bool, optional
            If True expose the full event dict to the predicate, if False
            only expose the data from the event
        input_info: dict
            describe the incoming streams
            Note that only one key allowed since the func only takes two inputs
        output_info: list of tuples
            describe the resulting stream
        start: any or callable, optional
            Starting value for accumulation, if no_default use the event data
            dictionary, if callable run that callable on the event data, else
            use `start` as the starting data, defaults to no_default
        """
        self.state_key = state_key
        self.func = func
        self.state = start
        EventStream.__init__(self, child, input_info=input_info,
                             output_info=output_info)
        self.full_event = full_event
        self.generate_provenance(function=func)

    def event(self, doc):
        doc = self.event_guts(doc, self.full_event)

        if self.state is no_default:
            self.state = {}
            # Note that there is only one input_info key allowed for this
            # stream function so this works
            self.state = doc[next(iter(self.input_info.keys()))]
        # in case we need a bit more flexibility eg lambda x: np.empty(x.shape)
        elif hasattr(self.state, '__call__'):
            self.state = self.state(doc)
        else:
            doc[self.state_key] = self.state
            try:
                result = self.func(doc)
            except Exception as e:
                return super().stop(e)
            self.state = result
        return super().event(self.issue_event(self.state))


class zip(EventStream):
    """Combine multiple streams together into a stream of tuples"""

    def __init__(self, *children, **kwargs):
        """Initialize the Node

        Parameters
        ----------
        children: EventStream instances
            The event streams to be zipped together
        """
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
    """Combine multiple event streams into one"""

    def __init__(self, *children, **kwargs):
        """Initialize the Node

        Parameters
        ----------
        children: EventStream instances
            The event streams to be zipped together
        """
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
                first_doc_name = self.buffers[0][0][0]
                if all([b[0][0] == first_doc_name and b[0][0] != 'event'
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
                                new_nd_pair = super().event(
                                    self.refresh_event(nd_pair[1]))
                                rvs.append(self.emit(new_nd_pair))

                else:
                    raise RuntimeError("There is a mismatch of docs, but none "
                                       "of them are events so we have reached "
                                       "a potential deadlock, so we raise "
                                       "this error instead")

            return rvs
        elif len(L) > self.maxsize:
            return self.condition.wait()


union = bundle


class combine_latest(EventStream):
    """Combine multiple streams together to a stream of tuples

    This will emit a new tuple of all of the most recent elements seen from
    any stream.
    """

    def __init__(self, *children, emit_on=None):
        """Initialize the node

        Parameters
        ----------
        children: EventStream instances
            The streams to combine
        emit_on: EventStream, list of EventStreams or None
            Only emit upon update of the streams listed.
            If None, emit on update from any stream
        """
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
        EventStream.__init__(self, children=children)

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
                return self.emit(tup)
        else:
            if self.missing and who in self.missing:
                self.missing.remove(who)

            self.last[self.children.index(who)] = x
            if not self.missing and who in self.emit_on:
                tup = tuple(self.last)
                return self.emit(tup)


class eventify(EventStream):
    """Generate events from data in starts"""

    def __init__(self, child, start_key, *, output_info, **kwargs):
        """Initialize the node

        Parameters
        ----------
        child: EventStream instance
            The event stream to eventify
        start_key: str
            The run start key to use to create the events
        output_info: list of tuples, optional
            describes the resulting stream
        """
        # TODO: maybe allow start_key to be a list of relevent keys?
        self.start_key = start_key
        self.val = None

        EventStream.__init__(self, child, output_info=output_info, **kwargs)

    def start(self, docs):
        self.val = docs[0][self.start_key]
        return super().start(docs)

    def event(self, docs):
        return super().event(self.issue_event(self.val))


class query(EventStream):
    def __init__(self, db, child, query_function,
                 query_decider=None, **kwargs):
        self.db = db
        self.query_function = query_function
        self.query_decider = query_decider
        EventStream.__init__(self, child, **kwargs)
        self.generate_provenance(query_function=query_function,
                                 query_decider=query_decider)
        self.uid = None
        self.output_info = [('hdr_uid', {'dtype': 'str', 'source': 'query'})]

    def start(self, docs):
        # XXX: If we don't have a decider we return all the results
        res = self.query_function(self.db, docs)
        if self.query_decider:
            res = [self.query_decider(res, docs), ]
        self.uid = [next(iter(r.stream(fill=False)))[1]['uid'] for r in res]
        self.md.update(n_hdrs=len(self.uid))
        return super().start(docs)

    def update(self, x, who=None):
        name, docs = self.curate_streams(x)
        if name == 'start':
            el = [self.emit(self.start(docs)),
                  self.emit(self.descriptor(docs))]
            if isinstance(self.uid, list):
                for u in self.uid:
                    el.append(self.emit(super().event(self.issue_event(u))))
            el.append(self.emit(self.stop(docs)))
            return el


class query_unpacker(EventStream):
    def __init__(self, db, child, fill=True):
        self.db = db
        EventStream.__init__(self, child)
        self.fill = fill

    def update(self, x, who=None):
        name, doc = x
        if name == 'event':
            return [self.emit(nd) for nd in
                    self.db[doc['data']['hdr_uid']].stream(fill=self.fill)]
