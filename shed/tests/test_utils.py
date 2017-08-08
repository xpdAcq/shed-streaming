from streams import Stream
from shed.event_streams import star
from ..utils import to_event_model
from bluesky.callbacks.core import CallbackBase


def test_to_event_model():
    det = [1, 2, 3]
    g = to_event_model(det, [('det', {'source': 'to_event_model',
                                      'dtype': 'float'})])

    class AssertCallback(CallbackBase):
        def start(self, doc):
            assert all([k in doc.keys() for k in ['uid', 'time',
                                                  'provenance', 'source']])

        def descriptor(self, doc):
            assert all([k in doc.keys() for k in ['uid', 'data_keys', ]])

        def event(self, doc):
            assert all([k in doc.keys() for k in ['uid', 'time',
                                                  'timestamps', 'descriptor',
                                                  'filled', 'seq_num']])

        def stop(self, doc):
            assert doc['exit_status'] == 'success'

    source = Stream()
    source.sink(star(AssertCallback()))
    for e in g:
        source.emit(e)
