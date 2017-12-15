from bluesky.run_engine import RunEngine
from ophyd.sim import hw
from bluesky.plans import count
from shed.translation import FromEventStream, ToEventStream
from streamz import Stream
from bluesky.callbacks import LivePlot
import matplotlib.pyplot as plt

# Create callbacks for plotting
fig, ax = plt.subplots()
lp = LivePlot('average', ax=ax)
lp2 = LivePlot('binned', ax=ax)
# lp3 = LivePlot('noisy_det', ax=ax)

# Create a graph
source = Stream()
# Convert from raw event model to data
fes = FromEventStream(source, 'event', ('data', 'noisy_det'), principle=True)

# Averageing graph
adder = fes.accumulate(lambda x, y: x + y)
counter = fes.accumulate(lambda s, x: s + 1, start=0)
averager = adder.zip(counter).map(lambda x: x[0]/x[1])

# Binned averaging
sw = fes.sliding_window(2).map(sum).map(lambda x: x/2)

# Convert back to Event Model
tes1 = ToEventStream(averager, ('average',))
tes2 = ToEventStream(sw, ('binned', ))

# sink to plotting
tes1.sink(lambda x: lp(*x))
tes2.sink(lambda x: lp2(*x))

# Run the scan
RE = RunEngine()
t = RE.subscribe(lambda *x: source.emit(x))
# RE.subscribe(lp3)
# RE.subscribe(print)
source.visualize(source_node=True)
RE(count([hw().noisy_det], 100))
plt.show()
