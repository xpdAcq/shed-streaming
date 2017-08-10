Tutorials
=========

These area tutorials designed to better understand what pySHED is, and what it
attempts to resolve.
Before beginning, we'll assume that you have the ``shed`` and
`streams <http://www.github.com/mrocklin/streams>`_ libraries installed.

Tutorial 1 : Simple Streams
---------------------------
First, we begin with a quick review of simple streams. Let's say we
had a stream of incoming data, and needed to increment by one, and print
the result. The definition is as simple as the following::

    import streams.core as sc
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
for the `streams <http://www.github.com/mrocklin/streams>`_ library.

Tutorial 2 : A simple Event Stream
----------------------------------
An event stream is as defined by
`NSLS-II <http://nsls-ii.github.io/architecture-overview.html>`_, which is a
series of documents, beginning from a start document, descriptor, event
documents and stop documents. Please refer to the NSLS-II documentation for
more details.

SHED's streaming method handles event streams. To better understand the general
idea, let's begin with a simple event stream. Before we begin, let's focus on
the data itself. Let's say you had a detector called *detector1* which returned
a 100 by 100 array of integer values. Let's simulate this detector with random
values for two instances, ``data1`` and ``data2``::

    import numpy as np
    data1 = np.random.random((100, 100)).astype(int)
    data2 = np.random.random((100, 100)).astype(int)

Let's say this was captured at a beamline where some extra metadata was stored
during the capture of this data, with fields ``name`` being *Alex* and
``sample`` being *FeO*. In general, this metadata can be defined arbitrarily
and is not essential to the data capture, but helps describe it in many cases.
We can define a quick generator to generate some of this data as follows::

    import time
    from uuid import uuid4
    def gen_imgs(data, **md):
        run_start = str(uuid4())
        yield 'start', dict(uid=run_start, time=time.time(), **md)
        des_uid = str(uuid4())
        yield 'descriptor', dict(run_start=run_start, data_keys={
            'data': dict(
                source='testing', dtype='array')}, time=time.time(), uid=des_uid)
        for i, datum in enumerate(data):
            yield 'event', dict(descriptor=des_uid,
                                uid=str(uuid4()),
                                time=time.time(),
                                data={'data': datum},
                                timestamps={'data': time.time()},
                                filled={'data': True},
                                seq_num=i)
        yield 'stop', dict(run_start=run_start,
                           uid=str(uuid4()),
                           time=time.time())

    event_stream = gen_imgs([data1, data2], name="Alex", sample="FeO")

The details are tedious. What is important to keep in mind is that this is
generating a list of documents as such::

    ('start', some_start_doc),
    ('descriptor', some_descriptor_doc),
    ('event', some_event_doc),
    ('event', another_event_doc),
    ('stop', some_stop_doc)

where ``some_start_doc``, etc are placeholders for the contents of each
document. To verify this, you can print the stream with::

    for namedocpair in event_stream:
        print(namedocpair)

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

Tutorial 3 : Mapping inputs and outputs
------------------------------------

Now we've learned the basics of what SHED attempts to resolve, we can move on
to more complex operations. The first obvious questions is multiple
inputs/outputs. Let's say we had the function::

