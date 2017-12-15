from bluesky.run_engine import RunEngine
from ophyd.sim import hw
from bluesky.plans import count
from shed.translation import FromEventStream, ToEventStream
from streamz import Stream
from bluesky.callbacks import LivePlot

lp = LivePlot('average')
# Create a graph
source = Stream()
fes = FromEventStream(source, 'event', ('data', 'noisy_det'), principle=True)
adder = fes.accumulate(lambda x, y: x + y)
counter = fes.accumulate(lambda s, x: s + 1, start=0)
averager = adder.zip(counter).map(lambda x: x[0]/x[1])
tes = ToEventStream(averager, ('average',))
tes.sink(print)
tes.sink(lambda x: lp(*x))

RE = RunEngine()
t = RE.subscribe(lambda *x: source.emit(x))
# RE.subscribe(print)
RE(count([hw().noisy_det], 100))
