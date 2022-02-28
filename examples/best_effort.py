import matplotlib.pyplot as plt
from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.utils import install_kicker
from bluesky import RunEngine
import bluesky.plans as bp
from ophyd.sim import hw
from rapidz import Stream
from shed_streaming.simple import SimpleFromEventStream, SimpleToEventStream, AlignEventStreams
from bluesky.utils import short_uid
from bluesky.plan_stubs import checkpoint, abs_set, wait, trigger_and_read
from bluesky.preprocessors import pchain


def one_nd_step(detectors, step, pos_cache):
    """
    Inner loop of an N-dimensional step scan

    This is the default function for ``per_step`` param in ND plans.

    Parameters
    ----------
    detectors : iterable
        devices to read
    step : dict
        mapping motors to positions in this step
    pos_cache : dict
        mapping motors to their last-set positions
    """
    def move():
        yield from checkpoint()
        grp = short_uid('set')
        for motor, pos in step.items():
            if pos == pos_cache[motor]:
                # This step does not move this motor.
                continue
            yield from abs_set(motor, pos, group=grp)
            pos_cache[motor] = pos
        yield from wait(group=grp)

    motors = step.keys()
    yield from move()
    plt.pause(.001)
    yield from trigger_and_read(list(detectors) + list(motors))


install_kicker()
bec = BestEffortCallback()
bec.enable_plots()
hw = hw()
RE = RunEngine()
# build the pipeline
raw_source = Stream()
raw_output = SimpleFromEventStream('event', ('data', 'det_a'), raw_source,
                                   principle=True)
raw_output2 = SimpleFromEventStream('event', ('data', 'noisy_det'), raw_source)

pipeline = raw_output.union(raw_output2).map(lambda x: 1).accumulate(lambda x, y: x + y)

res = SimpleToEventStream(pipeline, ('result', ))

merge = AlignEventStreams(res, raw_source)
merge.starsink(bec)


RE.subscribe(lambda *x: raw_source.emit(x))
RE(
    pchain(
        bp.scan([hw.noisy_det], hw.motor, 0, 10, 10),
        bp.grid_scan(
            [hw.ab_det],
            hw.motor, 0, 10, 10,
            hw.motor2, 0, 10, 10, True,
            per_step=one_nd_step),
        bp.spiral([hw.ab_det],
                  hw.motor, hw.motor2, 0, 0, 10, 10, 1, 10,
                  per_step=one_nd_step
                  ),
        bp.grid_scan(
            [hw.direct_img],
            hw.motor, 0, 10, 10,
            hw.motor2, 0, 10, 10, True,
            per_step=one_nd_step),
    )
)

plt.show()
raw_source.visualize('best_effort.png', source_node=True)
