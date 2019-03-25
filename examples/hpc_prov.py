import operator as op

from bluesky import RunEngine
from rapidz import Stream
from shed.translation import FromEventStream, ToEventStream, DBFriendly

from databroker import Broker
import bluesky.plans as bp
from ophyd.sim import hw

hw = hw()

from dask.distributed import Client
from dask_jobqueue import PBSCluster
from pprint import pprint
cluster = PBSCluster()
cluster.scale(10)  # Ask for ten workers

client = Client(cluster)

db = Broker.named('temp')

# Now this pipeline runs using HPC resources
source = Stream()
(FromEventStream('event', 'motor1', upstream=source)
 .scatter()
 .map(op.add, 1)
 .buffer(8)
 .gather()
 .ToEventStream('result').DBFriendly().starsink(db.insert))

RE = RunEngine()
RE.subscribe(lambda *x: source.emit(x))

RE(bp.count([hw.motor1], 1))

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
graph.nodes['data motor1 FromEventStream']['stream'].visualize()

# print the results
graph.nodes['result ToEventStream'].sink(pprint)

# change the multiplication factor from 5 to 10
graph.nodes['map; add'].args = (10,)

# rerun the analysis and print the results
for v in vs:
    dd = data[v['uid']]
    parents[v["node"]].update(dd)

