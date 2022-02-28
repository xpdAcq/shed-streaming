Tutorials
=========

These area tutorials designed to better understand what shed-streaming is, and what it
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

shed-streaming handles the translations between the event model, raw data, and back.
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
    # start and stop documents, all shed-streaming pipelines must have at least one
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

    # make some simulated devices
    hw = hw()
    RE = RunEngine()

    # have the RunEngine send data into the pipeline
    RE.subscribe(lambda *x: raw_source.emit(x))

    # Run the scan
    RE(bp.scan([hw.ab_det], hw.motor1, 0, 4, 5))


We can also subscribe ``BestEffortCallback`` into the pipeline for live
visualization::

    from bluesky.utils import install_qt_kicker()
    from bluesky.callbacks.best_effort import BestEffortCallback
    install_qt_kicker()

    bec = BestEffortCallback()
    # AlignEventStream so we inherit scan details
    # starsink because we need to splay out the data as args
    res.AlignEventStreams(raw_source).starsink(bec)

We can also extract data from other documents, for instance from the start
document::

    from_start = SimpleFromEventStream('start', ('my_number', ),
                                       upstream=raw_source)
    from_start.sink(print)
    RE(bp.count([hw.ab_det], 5), my_number=3)

Finally we can send the data to a databroker::

    from databroker import Broker

    # create a temporary database
    db = Broker.named('temp')

    # we use starsink here because we need to splay out the (name, document)
    # pair into the args
    res.starsink(db.insert)


Tutorial 3: Replay
------------------
To capture the provenance of the data processing we need to use the full
translation nodes (rather than the ``Simple`` nodes).
We can use the pipeline from above with a small modification::

    from rapidz import Stream
    from shed.translation import FromEventStream, ToEventStream
    import operator as op
    from databroker import Broker
    import bluesky.plans as bp
    from ophyd.sim import hw
    from bluesky import RunEngine
    from pprint import pprint

    # where we'll input data
    raw_source = Stream()

    # extract from the events the values associated with 'data' and 'det_a'
    # (principle=True means that we listen to this node for when to issue
    # start and stop documents, all shed-streaming pipelines must have at least one
    # principle node)
    raw_output = FromEventStream('event', ('data', 'det_a'),
                                 upstream=raw_source, principle=True)
    # multiply by 5 and performa a cumulative sum
    pipeline = raw_output.map(op.mul, 5).accumulate(op.add)

    # repackage the data in the event model under the name 'result'
    res = ToEventStream(pipeline, ('result', ))

    # print out the documents as they come out
    res.sink(pprint)

    # create a temporary database
    db = Broker.named('temp')

    # Make certain that the data is DB friendly (serialize the graph)
    # we use starsink here because we need to splay out the (name, document)
    # pair into the args
    res.DBFriendly().starsink(db.insert)

    # make some simulated devices
    hw = hw()
    RE = RunEngine()

    # Send raw data to the databroker as well
    RE.subscribe(db.insert)
    # have the RunEngine send data into the pipeline
    RE.subscribe(lambda *x: raw_source.emit(x))

    # Run the scan
    RE(bp.count([hw.ab_det], 5))

Now that we have created the pipeline, ran the experiment, and captured it into
the databroker we can then replay the analysis::

    from shed.replay import replay
    from rapidz.graph import _clean_text, readable_graph

    # get the graph and data
    graph, parents, data, vs = replay(db, db[-1])

    # make a graph with human readable names
    for k, v in graph.nodes.items():
        v.update(label=_clean_text(str(v['stream'])).strip())
    graph = readable_graph(graph)

    # create a plot of the graph so we can look at it and figure out what
    # the node names are
    # the file will be named ``mystream.png``
    graph.nodes['data det_a FromEventStream']['stream'].visualize()

    # print the results
    graph.nodes['result ToEventStream']['stream'].sink(pprint)

    # change the multiplication factor from 5 to 10
    graph.nodes['map; mul']['stream'].args = (10, )

    # rerun the analysis and print the results
    for v in vs:
        dd = data[v['uid']]
        parents[v["node"]].update(dd)

