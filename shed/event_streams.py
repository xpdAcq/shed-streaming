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
from builtins import zip as zzip
from collections import deque

from streamz.core import Stream, no_default
from tornado.locks import Condition


# TODO: if mismatch includes error stop doc be more verbose


def star(f):
    """Take tuple and unpack it into args"""

    @ft.wraps(f)
    def wraps(args):
        return f(*args)

    return wraps


def dstar(f):
    """Take dict and **kwargs and unpack both it as **kwargs"""

    @ft.wraps(f)
    def wraps(kwargs1, **kwargs2):
        kwargs1.update(kwargs2)
        return f(**kwargs1)

    return wraps


def istar(f):
    """Inverse of star, take *args and turn into tuple"""

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
        args/kwargs.
        Note that the keys for `input_info` can either be strings or integers.
        If a string, then the data associated with the value will be mapped to
        that kwarg in the funciton. If an int then the data will be placed in
        the position in the args matching the numerical value.

    output_info : list of tuples, optional
        Output info from the operation, not needed for all cases
        This:
            Creates a map between function returns and event data keys
            Provides information for building a descriptor for the output
            event stream. Each event stream has a descriptor which contains
            information about what is in the events in the stream.

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
    # these are sacred kwargs which can never be passed to operator nodes
    pop_kwargs = ['output_info', 'input_info', 'md', 'stream_name',
                  'raise_upon_error']

    def __init__(self, child=None, children=None,
                 *, output_info=None, input_info=None, md=None,
                 stream_name=None,
                 raise_upon_error=True,
                 **kwargs):
        """Initialize the stream

        Parameters
        ----------
        input_info: dict, optional
            describes the incoming streams by providing a map between function
            args/kwargs and data in each incoming stream. The form for this
            is ``{'function_arg/kwarg_position/name':
            (('keys', 'to', 'data', 'in', 'dict'), stream_number)}`` where
            the stream_number denotes which stream to pull the data from.
        output_info: list of tuples, optional
            describes the resulting stream
        md: dict, optional
            Additional metadata to be added to the run start document

        Notes
        ------
        input_info is designed to map keys in streams to kwargs in functions.
        It is critical for the internal data from the events to be returned,
        upon `event_contents`.
        input_info = {'input_kwarg': ('data_key', stream_number)}
        Note that the stream number is assumed to be zero if not specified

        output_info is designed to take the output tuple and map it back into
        data_keys.
        output_info = [('data_key', {'dtype': 'array', 'source': 'testing'})]
        """
        if md is None:
            md = {}
        if stream_name is not None:
            md.update(stream_name=stream_name)
        if 'stream_name' in md.keys():
            self.stream_name = md['stream_name']
        else:
            self.stream_name = None
        Stream.__init__(self, child, children, stream_name=self.stream_name)
        if output_info is None:
            output_info = {}
        if input_info is None:
            input_info = {}

        self.parent_uids = None
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
        # FIXME: handle (nested tuple) so that it behaves properly
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
            x = self.curate_streams(x, True)
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
        name, docs = self.curate_streams(nds, False)
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

    def curate_streams(self, nds, outbound):
        """Standardize inbound/outbound name document pairs

        Parameters
        ----------
        nds: tuple, or tuple of tuples
            The name document pair(s)
        outbound: bool
            If True curate streams outbound

        Returns
        -------
        name: str
            The name of the output doc(s)
        docs: tuple or dict
            The document(s)

        Notes
        ------
        If we get multiple streams make (name, (doc, doc, doc, ...))
        Otherwise (name, (doc,))
        """
        # if there are multiple streams
        if isinstance(nds[0], tuple):
            names, docs = list(zzip(*nds))
            if len(set(names)) > 1:
                raise RuntimeError('Misaligned Streams')
            name = names[0]
            newdocs = list()
            for doc in docs:
                # for case of ((name, ({}, {})), (name, ({}, {})))
                if isinstance(doc, tuple):
                    newdocs.extend(doc)
                else:
                    newdocs.append(doc)

            docs = tuple(newdocs)

        # if only one stream
        else:
            names, docs = nds
            name = names
            if not isinstance(docs, tuple):
                docs = (docs,)
        if not outbound:
            return name, docs
        else:
            if isinstance(docs, tuple) and len(docs) == 1:
                docs = docs[0]
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

        # Get all the callables and put them as their own dict inside the main
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
        self.parent_uids = [doc['uid'] for doc in docs if doc]
        new_start_doc = dict(uid=self.run_start_uid,
                             time=time.time(), **self.md)

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
            # If we had to describe the output information then we need an
            # all new descriptor
            self.outbound_descriptor_uid = str(uuid.uuid4())
            new_descriptor = dict(uid=self.outbound_descriptor_uid,
                                  time=time.time(),
                                  run_start=self.run_start_uid,
                                  name='primary')
            if self.output_info:
                new_descriptor.update(
                    data_keys={k: v for k, v in self.output_info})

            # no truly new data needed
            # XXX: giant hack only look at the keys because () != []
            elif all(d['data_keys'].keys() == docs[0].get('data_keys').keys()
                     for d in docs):
                new_descriptor.update(data_keys=docs[0]['data_keys'])

            else:
                rdocs = list(docs)
                rdocs.reverse()
                dk = {}
                for doc in rdocs:
                    dk.update(doc.get('data_keys'))
                new_descriptor.update(data_keys=dk)

            self.i = 1
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
                            run_start=self.run_start_uid,
                            provenance=self.provenance,
                            parents=self.parent_uids
                            )
            if isinstance(docs, Exception):
                self.bypass = True
                print(self.stream_name)
                print(self.md)
                print(traceback.format_exc())
                print(repr(docs))

                new_stop.update(reason=repr(docs),
                                trace=traceback.format_exc(),
                                exit_status='failure')
                self.excep = docs
            else:
                new_stop.update(exit_status='success')
            self.outbound_descriptor_uid = None
            self.run_start_uid = None
            return 'stop', new_stop
        elif self.raise_upon_error and self.excep:
            raise self.excep

    def event_contents(self, docs, full_event=False):
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
        args: tuple
            The arguments to be passed to a function
        kwargs: dict
            The keyword arguments to be passed to a function
        """
        # TODO: access to full document(s)
        if not self.input_info:
            kwargs = {}
            # Reverse the order of the docs so that the first doc in resolves
            # last
            rdocs = list(docs)
            rdocs.reverse()
            if full_event:
                for doc in rdocs:
                    kwargs.update(**doc)
            else:
                for doc in rdocs:
                    kwargs.update(**doc['data'])
        else:
            kwargs = {}
            # address inner dicts, not just data or everything
            for input_kwarg, (data_key, position) in self.input_info.items():
                if isinstance(data_key, tuple):
                    inner = docs[position].copy()
                    for dk in data_key:
                        inner = inner[dk]
                # for backwards compat will be removed soon
                elif full_event:
                    inner = docs[position][data_key]
                    DeprecationWarning('full_event will be removed in the '
                                       'near future please just use an empty '
                                       'data_key tuple')
                else:
                    inner = docs[position]['data'][data_key]
                kwargs[input_kwarg] = inner

        args_positions = [k for k in kwargs.keys() if isinstance(k, int)]
        args_positions.sort()

        n_args = len(args_positions)
        if args_positions and (args_positions[-1] != n_args - 1 or
                                       args_positions[0] != 0):
            errormsg = """Error, arguments supplied must be a set of integers
            ranging from 0 to number of arguments\n
            Got {} instead""".format(args_positions)
            raise ValueError(errormsg)

        args = [kwargs.pop(k) for k in args_positions]
        return args, kwargs

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

            # if output_info is not empty dict
            if self.output_info:
                new_event.update(data={output_name: output
                                       for (output_name, desc), output in
                                       zzip(self.output_info, outputs)})
            else:
                if outputs is None:
                    outputs = {}
                elif not isinstance(outputs, dict):
                    print('outputs not dict! raising  a type errror')
                    errormsg = "Error, outputs is not a dict. Can't continue\n"
                    errormsg += "This typically comes from a function\n"
                    errormsg += "whose output is not nothing or a dict.\n"
                    errormsg += "When dealing with such outputs, please \n"
                    errormsg += "use the output_info keyword argument.\n"
                    raise TypeError(errormsg)
                new_event.update(data=outputs)
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

    def _clean_kwargs(self, kwargs):
        for k in self.pop_kwargs:
            if k in kwargs:
                kwargs.pop(k)


class map(EventStream):
    """Apply a function onto every event in the stream

    Parameters
    ----------
    func: callable
        The function to map on the event data
    child: EventStream instance
        The source of the data
    *args: tuple
        args to be passed to the function,
        which are appended to the args from `input_info`
    full_event: bool, optional
        If True expose the full event dict to the function, if False
        only expose the data from the event
    input_info: dict
        describe the incoming streams
    output_info: list of tuples
        describe the resulting stream
    **kwargs: dict
        kwargs to be passed to the function

    Examples
    --------
    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> a = [1, 2, 3]  # base data
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> m = es.map(es.dstar(lambda x: x+5), source,
    ...     input_info={'x': 'det'},
    ...     output_info=[('res', {'source': 'docstring', 'dtype': 'float'})])
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc in g: z = source.emit(doc)
    >>> assert len(L) == 6
    """

    def __init__(self, func, child, *args,
                 full_event=False,
                 output_info=None, input_info=None,
                 **kwargs):
        self.func = func

        EventStream.__init__(self, child, output_info=output_info,
                             input_info=input_info, **kwargs)
        self._clean_kwargs(kwargs)
        self.func_kwargs = kwargs
        self.func_args = args
        self.full_event = full_event
        self.generate_provenance(function=func)

    def event(self, docs):
        try:
            # we need to expose the event data
            res_args, res_kwargs = self.event_contents(docs, self.full_event)
            # take the event contents and add them to the args/kwargs
            result = self.func(*res_args, *self.func_args,
                               **res_kwargs, **self.func_kwargs)
            # Now we must massage the raw return into a new event
            result = self.issue_event(result)
        except Exception as e:
            return super().stop(e)
        else:
            return super().event(result)


class filter(EventStream):
    """Only pass through events that satisfy the predicate

    Parameters
    ----------
    predicate: callable
        The function which returns True if the event is to propagate
        further in the pipeline
    child: EventStream instance
        The source of the data
    *args: tuple
        args to be passed to the function,
        which are appended to the args from `input_info`
    input_info: dict
        describe the incoming streams
    full_event: bool, optional
        If True expose the full event dict to the predicate, if False
        only expose the data from the event
    document_name: {'event', 'start', 'descriptor'}, optional
        Which document to filter on, if event only pass events which meet the
        criteria. Otherwise if ``start`` only pass streams where the criteria
        is True in the start. Otherwise if ``descriptor`` only pass streams
        where the criteria is True in the descriptor. Defaults to 'event'.
    **kwargs: dict
        kwargs to be passed to the function

    Examples
    --------
    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> a = [1, 2, 3]  # base data
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> m = es.filter(lambda x: x>1, source, input_info={'x': 'det'})
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc in g: z = source.emit(doc)
    >>> assert len(L) == 5

    Filtering full headers

    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> a = [1, 2, 3]  # base data
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> m = es.filter(lambda x: x[0]['source'] != 'to_event_model', source,
    ...     document_name='start')
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc in g: z = source.emit(doc)
    >>> assert len(L) == 0

    Filtering on descriptors

    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> a = [1, 2, 3]  # base data
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> m = es.filter(lambda x: x[0]['name'] != 'primary', source,
    ...     document_name='descriptor')
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc in g: z = source.emit(doc)
    >>> assert len(L) == 2
    """

    def __init__(self, predicate, child, *args, input_info=None,
                 full_event=False, document_name='event', **kwargs):
        """Initialize the node """
        self.predicate = predicate

        EventStream.__init__(self, child, input_info=input_info, **kwargs)
        self._clean_kwargs(kwargs)

        self.func_kwargs = kwargs
        self.func_args = args
        self.full_event = full_event
        self.document_name = document_name
        self.truth_value = None
        # Note we don't override event because with this update we don't see it
        if document_name == 'start':
            self.update = self._start_update
        elif document_name == 'descriptor':
            self.update = self._descriptor_update
            self.descriptor_truth_values = {}
        self.generate_provenance(predicate=predicate)

    def _start_update(self, x, who=None):
        # TODO: should we have something like event_contents for starts?
        name, docs = self.curate_streams(x, False)
        if name == 'start':
            self.truth_value = self.predicate(docs)
        if self.truth_value:
            self.emit((name, docs))

    def _descriptor_update(self, x, who=None):
        name, docs = self.curate_streams(x, False)
        ret = None
        if name == 'start':
            self.descriptor_truth_values = {}
            ret = super().start(docs)
        elif name == 'descriptor':
            # TODO: we need to sort out how to deal with nodes/modes which only
            # take in singular headers vs multiple
            self.descriptor_truth_values[docs[0]['uid']] = self.predicate(docs)
            if self.descriptor_truth_values[docs[0]['uid']]:
                ret = ('descriptor', docs)
        elif (name == 'event' and
              self.descriptor_truth_values[docs[0]['descriptor']]):
            ret = super().event(docs)
        elif name == 'stop':
            ret = super().stop(docs)
        if ret is not None:
            return self.emit(ret)

    def event(self, doc):
        res_args, res_kwargs = self.event_contents(doc, self.full_event)
        try:
            if self.predicate(*res_args, *self.func_args,
                              **res_kwargs, **self.func_kwargs):
                return super().event(doc[0])
        except Exception as e:
            return super().stop(e)


class accumulate(EventStream):
    """Accumulate results with previous state

    This preforms running or cumulative reductions, applying the function
    to the previous total and the new element.  The function should take
    two arguments, the previous accumulated state and the next element and
    it should return a new accumulated state.


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
    across_start: bool, optional
        If the accumulation should continue across multiple starts, defaults
        to False

    Examples
    --------
    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> a = [1, 2, 3]  # base data
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> m = es.accumulate(es.dstar(lambda a, b: a + b), source,
    ...     input_info={'b': 'det'},
    ...     output_info=[('res', {'source': 'docstring', 'dtype': 'float'})],
    ...     state_key='a')
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc in g: z = source.emit(doc)
    >>> assert len(L) == 6
    """

    def __init__(self, func, child, state_key=None,
                 full_event=False,
                 output_info=None,
                 input_info=None, start=no_default,
                 across_start=False):
        self.state_key = state_key
        self.func = func
        self.start_state = start
        self.state = start
        EventStream.__init__(self, child, input_info=input_info,
                             output_info=output_info)
        self.full_event = full_event
        if not across_start:
            self.start = self._not_across_start_start
        self.generate_provenance(function=func)

    def _not_across_start_start(self, docs):
        self.state = self.start_state
        return super().start(docs)

    def event(self, doc):
        # TODO: can accumulate support args/kwargs?
        args, data = self.event_contents(doc, self.full_event)

        if self.state is no_default:
            self.state = {}
            # Note that there is only one input_info key allowed for this
            # stream function so this works
            self.state = data[next(iter(self.input_info.keys()))]
        # in case we need a bit more flexibility eg lambda x: np.empty(x.shape)
        elif hasattr(self.state, '__call__'):
            self.state = self.state(data)
        else:
            data[self.state_key] = self.state
            try:
                result = self.func(data)
            except Exception as e:
                return super().stop(e)
            self.state = result
        return super().event(self.issue_event(self.state))


class zip(EventStream):
    """Combine multiple streams together into a stream of tuples

    Parameters
    ----------
    children: EventStream instances
        The event streams to be zipped together

    Examples
    --------
    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> from builtins import zip as zzip
    >>> a = [1, 2, 3]  # base data
    >>> b = [4, 5, 6]
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> gg = to_event_model(b, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> source2 = Stream()
    >>> m = es.zip(source, source2)
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc1, doc2 in zzip(g, gg):
    ...     z = source.emit(doc1),
    ...     zz = source2.emit(doc2)
    >>> assert len(L) == 6
    """

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


class Bundle(EventStream):
    """Combine multiple event streams into one

    Parameters
    ----------
    children: EventStream instances
        The event streams to be zipped together

    Examples
    --------
    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> from builtins import zip as zzip
    >>> a = [1, 2, 3]  # base data
    >>> b = [4, 5, 6]
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> gg = to_event_model(b, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> source2 = Stream()
    >>> m = es.Bundle(source, source2)
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc1, doc2 in zzip(g, gg):
    ...     z = source.emit(doc1)
    ...     zz = source2.emit(doc2)
    >>> assert len(L) == 9
    """

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


class BundleSingleStream(EventStream):
    """Combine multiple headers in a single stream into one

    Parameters
    ----------
    child: EventStream instances
        The event stream containing the data to be zipped together
    control_stream: {EventStream, int}
        Information to control the buffering. If int, bundle that many
        header together. If an EventStream, pull from the start document
        ``n_hdrs`` to determine the number of headers to bundle

    Examples
    --------
    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> a = [1, 2, 3]  # base data
    >>> b = [4, 5, 6]
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> gg = to_event_model(b, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> m = es.BundleSingleStream(source, 2)
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc1 in g: zz = source.emit(doc1)
    >>> for doc2 in gg: z = source.emit(doc2)
    >>> assert len(L) == 9
    """

    def __init__(self, child, control, **kwargs):
        self.maxsize = kwargs.pop('maxsize', 100)
        self.buffers = []
        self.desc_start_map = {}
        self.condition = Condition()
        self.prior = ()
        self.control = control
        self.start_count = 0
        if isinstance(control, int):
            EventStream.__init__(self, child=child)
            self.n_hdrs = control
            self.predicate = lambda x: self.start_count == self.n_hdrs
        elif callable(control):
            EventStream.__init__(self, child=child)
            self.n_hdrs = None
            self.predicate = control
        else:
            EventStream.__init__(self, children=(child, control))
            self.n_hdrs = None
            self.predicate = lambda x: False
        self.generate_provenance()
        self.emitted = {'start': False, 'descriptor': False}

    def update(self, x, who=None):
        return_values = []
        name, docs = self.curate_streams(x, False)
        print(name, self.start_count, self.n_hdrs)
        if who == self.control:
            if name == 'start':
                self.n_hdrs = x[1]['n_hdrs']
        else:
            if (name == 'start' and
                    self.emitted.get(name, False) and
                    self.predicate(docs)):
                # Reset the state
                for k in self.emitted:
                    self.emitted[k] = False
                self.start_count = 0
                # Issue a stop
                return_values.append(super().stop(docs))
            # If we have emitted that kind of document
            if self.emitted.get(name, False):
                if name == 'start':
                    self.parent_uids.extend([doc['uid'] for doc in docs])
                    self.start_count += 1
            elif name != 'stop':
                return_values.append(getattr(self, name)(docs))
                if name == 'start':
                    self.emitted[x[0]] = True
                    self.start_count += 1
                elif name == 'descriptor':
                    self.emitted[x[0]] = True
        return [self.emit(r) for r in return_values]


class combine_latest(EventStream):
    """Combine multiple streams together to a stream of tuples

    This will emit a new tuple of all of the most recent elements seen from
    any stream.

    Parameters
    ----------
    children: EventStream instances
        The streams to combine
    emit_on: EventStream, list of EventStreams or None
        Only emit upon update of the streams listed.
        If None, emit on update from any stream

    Examples
    --------
    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> a = [1, 2, 3]  # base data
    >>> b = [4, 5, 6]
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> gg = to_event_model(b, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> source2 = Stream()
    >>> m = es.combine_latest(source, source2)
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc1 in g: zz = source.emit(doc1)
    >>> for doc2 in gg: z = source2.emit(doc2)
    >>> assert len(L) == 6
    """

    special_docs_names = ['start', 'descriptor', 'stop']

    def __init__(self, *children, emit_on=None):
        self.last = [None for _ in children]
        self.special_last = {k: [None for _ in children] for k in
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
        idx = self.children.index(who)
        if name in self.special_docs_names:
            local_missing = self.special_missing[name]
            local_last = self.special_last[name]

        else:
            local_missing = self.missing
            local_last = self.last

        local_last[idx] = x
        if local_missing and who in local_missing:
            local_missing.remove(who)

        # we have a document from every one or we are on the emitting stream
        if not local_missing and who in self.emit_on:
            tup = tuple(local_last)
            return self.emit(tup)


class zip_latest(EventStream):
    """Combine multiple streams together to a stream of tuples

    This will emit a new tuple of the elements from the lossless stream paired
    with the latest elements from the other streams.

    Parameters
    ----------
    lossless : EventStream instance
        The stream who's documents will always be emitted
    children: EventStream instances
        The streams to combine

    Examples
    --------
    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> a = [1, 2, 3]  # base data
    >>> b = [4, 5, 6]
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> gg = to_event_model(b, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> source2 = Stream()
    >>> m = es.zip_latest(source, source2)
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc1 in g: zz = source.emit(doc1)
    >>> for doc2 in gg: z = source2.emit(doc2)
    >>> assert len(L) == 6
    """

    special_docs_names = ['start', 'descriptor', 'stop']

    def __init__(self, lossless, *children, **kwargs):
        children = (lossless,) + children
        self.last = [None for _ in children]
        self.special_last = {k: [None for _ in children] for k in
                             self.special_docs_names}
        self.missing = set(children)
        self.special_missing = {k: set(children) for k in
                                self.special_docs_names}
        self.lossless = lossless
        self.lossless_buffer = deque()
        # Keep track of the emitted docuement types
        self.lossless_emitted = set()
        EventStream.__init__(self, children=children, **kwargs)

    def update(self, x, who=None):
        name, doc = x
        idx = self.children.index(who)
        if name in self.special_docs_names:
            local_missing = self.special_missing[name]
            local_last = self.special_last[name]
            local_type = 'special'

        else:
            local_missing = self.missing
            local_last = self.last
            local_type = 'event'
            if who is self.lossless:
                self.lossless_buffer.append(x)

        local_last[idx] = x
        if local_missing and who in local_missing:
            local_missing.remove(who)

        if not local_missing:
            if local_type == 'special':
                self.lossless_emitted.add(name)
                local_missing.add(self.lossless)
                if name == 'stop':
                    # Clear with each stop doc
                    self.lossless_emitted.clear()
                return self.emit(tuple(local_last))
            # check start and descriptors emitted if not buffer
            if {'start', 'descriptor'} == self.lossless_emitted:
                L = []
                while self.lossless_buffer:
                    local_last[0] = self.lossless_buffer.popleft()
                    L.append(self.emit(tuple(local_last)))
                return L


class Eventify(EventStream):
    """Generate events from data in starts

    Parameters
    ----------
    child: EventStream instance
        The event stream to eventify
    start_keys: str, optional
        The run start keys to use to create the events
        If none supplied, then load all start keys into event
    output_info: list of tuples, optional
        describes the resulting stream

    Examples
    --------
    >>> from shed.utils import to_event_model
    >>> from streamz import Stream
    >>> import shed.event_streams as es
    >>> a = [1, 2, 3]  # base data
    >>> g = to_event_model(a, [('det', {'dtype': 'float'})])
    >>> source = Stream()
    >>> m = es.Eventify(source, 'uid',
    ...     output_info=[('start_uid', {'dtype': 'str'})])
    >>> l = m.sink(print)
    >>> L = m.sink_to_list()
    >>> for doc1 in g:
    ...     zz = source.emit(doc1)
    >>> assert len(L) == 4
    """

    def __init__(self, child, *keys, output_info=None,
                 document='start',
                 **kwargs):
        # TODO: maybe allow start_key to be a list of relevent keys?
        self.keys = keys
        if document == 'event':
            raise ValueError("Can't eventify event, its an event already")
        self.document = document
        self.vals = list()
        self.emit_event = False

        EventStream.__init__(self, child, output_info=output_info, **kwargs)

    def _extract_info(self, docs):
        # If there are no start keys, then use all the keys
        if not self.keys:
            self.keys = list(docs[0].keys())
        for key in self.keys:
            self.vals.append(docs[0][key])

        # If no output info provided use all the start keys
        if not self.output_info:
            self.output_info = []
            for key in self.keys:
                self.output_info.append((key, key))

        if len(self.output_info) == 1:
            self.vals = self.vals[0]
        else:
            if len(self.output_info) != len(self.vals):
                raise RuntimeError('The output_info does not match the values')

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
        name, docs = self.curate_streams(nds, False)
        if name == 'start':
            self.vals = list()
            self.emit_event = False
        if name == self.document:
            self._extract_info(docs)
        return getattr(self, name)(docs)

    def event(self, docs):
        if not self.emit_event:
            self.emit_event = True
            return super().event(self.issue_event(self.vals))


class Query(EventStream):
    """Query a databroker for data

    Parameters
    -----------
    db: databroker.Broker instance
        The databroker to be queried
    child: EventStream instance
        The stream to be subscribed to
    query_function: callable
        A function which executes a query against the databroker using the
        start document of the stream. Note that the signature must be
        ``func(Broker, docs)`` where docs is a tuple of documents and returns
        the valid headers.
    query_decider: callable, optional
        A function to decide among the query results. The signature must be
        func(list of headers, docs) and returns a single header. If not
        provided use all the headers returned. Defaults to None.
    max_n_hdrs: int, optional
        Maximum number of headers returned, if the number of headers returned
        is greater than the max then the node raises a RuntimeError

    """

    def __init__(self, db, child, query_function,
                 query_decider=None, max_n_hdrs=10, **kwargs):
        self.max_n_hdrs = max_n_hdrs
        self.db = db
        self.query_function = query_function
        self.query_decider = query_decider
        EventStream.__init__(self, child, **kwargs)
        self.generate_provenance(query_function=query_function,
                                 query_decider=query_decider)
        self.output_info = [('hdr_uid', {'dtype': 'str', 'source': 'query'})]

    def start(self, docs):
        # XXX: If we don't have a decider we return all the results
        # TODO: should this issue a stop on failure?
        self.uid = None
        res = self.query_function(self.db, docs)
        if self.query_decider:
            res = [self.query_decider(res, docs), ]
        self.uid = [next(iter(r.stream(fill=False)))[1]['uid'] for r in res]
        if len(self.uid) > self.max_n_hdrs:
            raise RuntimeError("Query returned more headers than the max "
                               "number of headers, either your query was too "
                               "broad or you need to up the max_n_hdrs.")
        self.md.update(n_hdrs=len(self.uid))
        return super().start(docs)

    def update(self, x, who=None):
        name, docs = self.curate_streams(x, False)
        if name == 'start':
            el = [self.emit(self.start(docs)),
                  self.emit(self.descriptor(docs))]
            if isinstance(self.uid, list):
                for u in self.uid:
                    el.append(self.emit(super().event(self.issue_event(u))))
            el.append(self.emit(self.stop(docs)))
            return el


class QueryUnpacker(EventStream):
    """Unpack Queries from the Query node

    This unpacks the start uids passed down from the Query node and creates
    a restream of the data for each header

    Parameters
    -----------
    db: databroker.Broker instance
        The databroker to be queried
    child: EventStream instance
        The stream to be subscribed to
    fill: bool, optional
        Whether or not to fill the documents, defaults to True

    """

    def __init__(self, db, child, fill=True, **kwargs):
        self.db = db
        EventStream.__init__(self, child, **kwargs)
        self.fill = fill

    def update(self, x, who=None):
        name, docs = self.curate_streams(x, False)
        doc = docs[0]
        if name == 'event':
            return [self.emit(nd) for nd in
                    self.db[doc['data']['hdr_uid']].documents(fill=self.fill)]


class split(EventStream):
    def __init__(self, child, n_streams):
        self.split_streams = [EventStream(
            stream_name='Split output-{}'.format(i)) for i in range(n_streams)]
        EventStream.__init__(self, child=child)

    def update(self, x, who=None):
        name, docs = self.curate_streams(x, False)
        return [s.emit((name, doc)) for s, doc in zzip(self.split_streams,
                                                       docs)]


class fill_events(EventStream):
    """Fill events without provenence"""

    def __init__(self, db, child):
        self.db = db
        EventStream.__init__(self, child=child)
        self.descs = None

    def start(self, docs):
        self.descs = None
        return 'start', docs

    def descriptor(self, docs):
        self.descs = docs
        return 'descriptor', docs

    def event(self, docs):
        d = next(self.db.fill_events(docs, self.descs))
        return 'event', d

    def stop(self, docs):
        return 'stop', docs
