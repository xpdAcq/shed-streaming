Tutorials
=========

These area tutorials designed to better understand what pySHED is, and what it
attempts to resolve.
Before beginning, we'll assume that you have the ``shed`` and
`rapidz <http://www.github.com/xpdAcq/rapidz>`_ libraries installed.

Tutorial 1 : Simple Streams
---------------------------
First, we begin with a quick review of simple streams. Let's say we
had a stream of incoming data, and needed to increment by one, and print
the result. The definition is as simple as the following::

    import rapidz.core as sc
    def myadder(x):
        return x + 1

    s = Stream()
    s2 = sc.map(myadder, s)
    s3 = sc.map(print, s2)

    # now send the data here
    s.emit(1)
    s.emit(3)

Here, the stream definition is done via ``s = Stream()``.
The data is not input into the stream until ``s.emit(var)`` is called.
The incrementing by one is done via the ``map`` method. This method
takes a function as its first argument, and the input stream as the
second argument.

If this makes sense, then we're ready to understand event streams. If it
doesn't, then it is suggested you read the documentation and exmaples
for the `rapidz <http://www.github.com/xpdAcq/rapidz>`_ library.

Tutorial 2 : A simple Event Stream
----------------------------------
An event stream is as defined by
`NSLS-II <http://nsls-ii.github.io/architecture-overview.html>`_, which is a
series of documents, beginning from a start document, descriptor, event
documents and stop documents. Please refer to the NSLS-II documentation for
more details.

SHED handles the translations between the event model, raw data, and back. 
To better understand the general
idea, let's begin with a simple event stream. 
We'll use the ``bluesky.run_engine.RunEngine`` and ``ophyd.sim.hw`` to mimic
an experiment run at a beamline.

Here is an example pipeline which multiplies the output of a detector by 5 and 
then performs a running sum of the results.
The results are then repackaged into the event model and printed via 
``pprint``. Finally we run a count scan with the ``ab_det``::
    
    from rapidz import Stream
    from shed.simple import SimpleFromEventStream, SimpleToEventStream
    from bluesky import RunEngine
    import bluesky.plans as bp
    from ophyd.sim import hw
    import operator as op
    from pprint import pprint
    
    # make some simulated devices
    hw = hw()
    RE = RunEngine()
    
    # where we'll input data
    raw_source = Stream()
    
    # extract from the events the values associated with 'data' and 'det_a'
    # (principle=True means that we listen to this node for when to issue
    # start and stop documents, all SHED pipelines must have at least one
    # principle node)
    raw_output = SimpleFromEventStream('event', ('data', 'det_a'), raw_source,
                                       principle=True)
    # multiply by 5 and performa a cumulative sum
    pipeline = raw_output.map(op.mul, 5).accumulate(op.add)

    # repackage the data in the event model under the name 'result'
    res = SimpleToEventStream(pipeline, ('result', ))

    # print out the documents as they come out
    res.sink(pprint)

    # have the RunEngine send data into the pipeline
    RE.subscribe(lambda *x: raw_source.emit(x))
    
    # Run the scan
    RE(bp.count([hw.ab_det], 5))


We can also subscribe ``BestEffortCallback`` into the pipeline for live
visualization::

    bec = BestEffortCallback()
    # starsink because we need to splay out the data as args
    res.starsink(bec)

We can also extract data from other documents, for instance from the start
document::

    from_start = SimpleFromEventStream('start', ('my_number', ) raw_source)
    from_start.sink(print)
    RE(bp.count([hw.ab_det], 5), my_number=3)


Just remember to regenerate the generator again before using it (for more
details, see python's documentation on generators). There we have it. This can
rather be lengthy, but if you think about it, this would be the simplest way to
define a series of documents. (Again, please see the `NSLS-II
<http://nsls-ii.github.io/architecture-overview.html>`_ documentation)

Now we're ready for the stream. How do we treat this? We basically need a
stream that is document conscious. Let's look at the previous example with
``myadder``. Let's say for some odd reason we needed to add 1 to the detector.
We would need to create some function as follows::

    def myadder_eventstream(namedocpair):
        name, doc = namedocpair
        if name == 'event':
            return doc['detector1'] + 1

And we could run the streams again as before. However, there are two obvious
problems with this:

1. This assumes the detector field name is known *detector1*. This could be
externally saved in a data base, but this is still awkward and not scalable

2. This returns data that does not resemble the incoming document. Ideally, we
would like to return a new set of ``(name, doc)`` pairs that resemble the event
architecture.

To resolve this, we should also read the ``descriptor_doc`` and return an
``event_doc`` by running something complicated, for example::

    from uuid import uuid4
    descriptor_buffer = dict()
    new_start_buffer = dict()

    def myadder_eventstream(namedocpair):
        name, doc = namedocpair
        if name == 'start':
            start_uid = doc['uid']
            # map old uid to a new one
            new_start_buffer[start_uid] = str(uuid4())
            # copy it and issue new uid
            newstart = start.copy()
            newstart['uid'] = str(uuid4())
            return ('start', newstart)
        if name == 'descriptor':
            # get reference to start uid
            start_uid = doc['start_uid']
            # save the descriptor for that start uid
            descriptor_buffer[start_uid] = descriptor_buffer['data_keys']
            return ('descriptor', newdescriptor)
        if name == 'event':
            # get reference to start uid
            start_uid = doc['start_uid']
            data = event['data'][descriptor_buffer[start_uid]]
            # get the first key for now, let's keep it simple here
            data_key = descriptor_buffer[start_uid].keys()[0]
            newdata = data + 1
            newevent = dict(uid=uuid4())
            newevent[data_key] = newdata
            newevent['start_uid'] = start_uid
            return ('event', newevent)
        if name == 'stop':
            # clear buffers and issue new stop
            start_uid = doc['start_uid']
            new_start_uid = new_start_buffer[start_uid]
            new_start_buffer.pop(start_uid)
            descriptor_buffer.pop(start_uid)
            stop_doc = dict(uid=str(uuid4()))
            stop_doc['start_uid'] = new_start_uid
            return ('stop', newstop)

    for namedocpair in event_stream:
        s.emit(namedocpair)
                            
You can see this is quite lengthy. Most of the boiler plate involves treating
different documents separately, and issuing new documents. This is where SHED
is useful. Rather than define this monolithic function, we let the
``event_stream`` do the work. We use it by simply running::

    from streams import Stream
    import shed.event_stream as es

    def addmydata(x):
        data = x['data']
        return data + 1
    
    s = Stream()
    # this time, we pass the stream to event_stream's
    # map method
    s2 = es.map(lambda x : x['data'] + 1, s, input_info={'data' : 'data'},
               output_info=(('data', {}),))
    s3 = es.map(print, s2, input_info={'data' : 'data'},
                output_info=(('data',{'dtype' : 'array'}),))
    
    event_streams = gen_imgs([data1, data2], name="Alex", sample="FeO")
    # generate the event streams again since generator is exhausted
    #event_streams = gen_imgs([data1, data2], name="Alex", sample="FeO")
    for namedocpair in event_streams:
        s.emit(namedocpair)

There are some extra details involving ``input_info`` and ``output_info`` that
we can ignore for now. The take home message here is that SHED allows one to
treat streams that follow the event model, without much boilerplate code.

What's Happening to the Stream?
*******************************
(Optional tutorial)

At any point in time during these tutorials, you may be wondering what's
going on with the elements in the stream. During debugging, this
connection to the raw data is especially useful. At any point, it's possible to
probe the raw output of the stream by simply using the parent ``Stream``
class. 

SHED and ``Stream`` are fully interchangeable and compatible
(so long as you know what you're doing and you adhere to SHED's name
document pair format).

Here is a typical way to intercept your output::

    from streams import Stream
    import shed.event_streams as es
    import streams.core as sc

    def addmydata(x):
        data = x['data']
        return data + 1
    
    s = Stream()
    # this time, we pass the stream to event_stream's
    # map method
    s2 = es.map(lambda x : x['data'] + 1, s, input_info={'data' : 'data'},
               output_info=(('data', {}),))
    s3 = es.map(print, s2, input_info={'data' : 'data'},
                output_info=(('data',{'dtype' : 'array'}),))
    # add these two lines
    L = sc.sink_to_list(s2)
    
    event_streams = gen_imgs([data1, data2], name="Alex", sample="FeO")
    # generate the event streams again since generator is exhausted
    #event_streams = gen_imgs([data1, data2], name="Alex", sample="FeO")
    for namedocpair in event_streams:
        s.emit(namedocpair)

Where all we've added is a ``streams.core`` import and the creation of a
list ``L`` and the map from a ``streams.core.map`` onto the stream.
You may print the outputs of this list by typing::
    for item in L:
        print(item)

Which in this case would give something like::

    ('start', {'uid': 'be352ef0-fc20-4175-b137-c7ce4111160d', 'time':
    1502716320.1736734, 'provenance': {'stream_class': 'map',
    'stream_class_module': 'shed.event_streams', 'input_info': {'data':
    ('data', 0)}, 'output_info': (('data', {}),), 'function':
    {'function_module': '__main__', 'function_name': '<lambda>'}},
    'parents': ['e82e5162-991b-4d5d-a06e-a5f34ded4960']})

    ('descriptor', {'uid': 'ae12e047-4549-46ff-9d02-5c1dc6e51359', 'time':
    1502716320.173751, 'run_start': 'be352ef0-fc20-4175-b137-c7ce4111160d',
    'data_keys': {'data': {}}})

    ('event', {'uid': 'd3728862-5bca-4bac-8f19-05da6e5fed7d', 'time':
    1502716320.174018, 'timestamps': {}, 'descriptor':
    'ae12e047-4549-46ff-9d02-5c1dc6e51359', 'filled': {'data': True},
    'seq_num': 0, 'data': {'data': array([[1, 1, 1, ..., 1, 1, 1],
           [1, 1, 1, ..., 1, 1, 1],
           [1, 1, 1, ..., 1, 1, 1],
           ..., 
           [1, 1, 1, ..., 1, 1, 1],
           [1, 1, 1, ..., 1, 1, 1],
           [1, 1, 1, ..., 1, 1, 1]])}})

    ('event', {'uid': '7aa7e146-aa2b-4369-a072-89d5b3e72b3c', 'time':
    1502716320.1746032, 'timestamps': {}, 'descriptor':
    'ae12e047-4549-46ff-9d02-5c1dc6e51359', 'filled': {'data': True},
    'seq_num': 1, 'data': {'data': array([[1, 1, 1, ..., 1, 1, 1],
           [1, 1, 1, ..., 1, 1, 1],
           [1, 1, 1, ..., 1, 1, 1],
           ..., 
           [1, 1, 1, ..., 1, 1, 1],
           [1, 1, 1, ..., 1, 1, 1],
           [1, 1, 1, ..., 1, 1, 1]])}})

    ('stop', {'uid': '12b8338c-b1c8-42b2-99dc-fbaaf2447975', 'time':
    1502716320.1751397, 'run_start': 'be352ef0-fc20-4175-b137-c7ce4111160d',
    'exit_status': 'success'})
    


Note that the output is as we expect, a series of ``(name, doc)`` pairs
where ``name`` is one of the strings ``'start'``, ``'decriptor'``,
``'event'`` or ``'stop'``. From here on, we won't dig further into the
structure of the events. But we encourage you to output intermediate
steps like this as much as you can.
