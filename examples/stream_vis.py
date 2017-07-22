from bluesky.callbacks.broker import LiveSliderImage
from xpdview.callbacks import LiveWaterfall
from streams.core import Stream
from redsky.event_streams import star
import numpy as np
from uuid import uuid4
import time
import matplotlib.pyplot as plt


def gen_imgs(f):
    run_start = str(uuid4())
    yield 'start', dict(uid=run_start, time=time.time(), name='test')
    des_uid = str(uuid4())
    yield 'descriptor', dict(run_start=run_start, data_keys={
        'data': dict(
            source='testing', dtype='array')}, time=time.time(), uid=des_uid)
    for i in range(10):
        yield 'event', dict(descriptor=des_uid,
                            uid=str(uuid4()),
                            time=time.time(),
                            data={'data': f()},
                            timestamps={'data': time.time()},
                            filled={'data': True},
                            seq_num=i)
    yield 'stop', dict(run_start=run_start,
                       uid=str(uuid4()),
                       time=time.time())


img_stream = Stream()
img_stream.sink(star(LiveSliderImage('data')))


def img_f():
    return np.random.random((10, 10))


for a in gen_imgs(img_f):
    print(a)
    img_stream.emit(a)
plt.show()

iq_stream = Stream()
iq_stream.sink(star(LiveWaterfall('data')))


def iq_f():
    return np.arange(10), np.exp(-1 * np.arange(10))


for b in gen_imgs(iq_f):
    iq_stream.emit(b)
plt.show()
