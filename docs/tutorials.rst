Tutorials
=========

These area tutorials designed to better understand what SHED is, and what it
attempts to resolve.
Before beginning, we'll assume that you have the ``shed`` and
`rapidz <http://www.github.com/xpdAcq/rapidz>`_ libraries installed.

Tutorial 1 : Simple Streams
---------------------------
First, we begin with a quick review of simple streams. Let's say we
had a stream of incoming data, and needed to increment by one, and print
the result. The definition is as simple as the following::

    from rapidz import Stream
    def myadder(x):
        return x + 1

    s = Stream()
    s2 = s.map(myadder)
    s3 = s2.sink(print)

    # now send the data here
    s.emit(1)
    s.emit(3)

Here, the stream definition is done via ``s = Stream()``.
The data is not input into the stream until ``s.emit(var)`` is called.
The incrementing by one is done via the ``map`` method. This method
takes a function as its first argument.

If this makes sense, then we're ready to understand event streams. If it
doesn't, then it is suggested you read the documentation and examples
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
``pprint``.
Finally we run a count scan with the ``ab_det`` detector::
    
    from rapidz import Stream
    from shed.simple import SimpleFromEventStream, SimpleToEventStream
    import operator as op
    from pprint import pprint

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


Now with the pipeline setup we can connect it to the ``RunEngine`` and
run our experiment::

    import bluesky.plans as bp
    from ophyd.sim import hw
    from bluesky import RunEngine

    from pprint import pprint

    # make some simulated devices
    hw = hw()
    RE = RunEngine()

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

Finally we can send the data to a databroker::

    from databroker import Broker

    # create a temporary database
    db = Broker.named('temp')

    # we use starsink here because we need to splay out the (name, document)
    # pair into the args
    res.starsink(db.insert)


