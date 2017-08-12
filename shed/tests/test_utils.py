"""Test generator for yielding valid event model from an iterable"""
from streams import Stream
from shed.event_streams import star
from ..utils import to_event_model
from bluesky.callbacks.core import CallbackBase


def test_to_event_model():
    det = [1, 2, 3]
    g = to_event_model(det, [('det', {'source': 'to_event_model',
                                      'dtype': 'float'})])

    class AssertCallback(CallbackBase):
        def _check(self, doc, keys):
            assert all([k in doc.keys() for k in keys])

        def start(self, doc):
            self._check(doc, ['uid', 'time', 'source'])

        def descriptor(self, doc):
            self._check(doc, ['uid', 'data_keys', ])

        def event(self, doc):
            self._check(doc, ['uid', 'time', 'timestamps', 'descriptor',
                              'filled', 'seq_num'])

        def stop(self, doc):
            self._check(doc, ['exit_status', 'uid', 'provenance'])
            assert doc['exit_status'] == 'success'

    source = Stream()
    source.sink(star(AssertCallback()))
    source.sink(print)
    for e in g:
        source.emit(e)
