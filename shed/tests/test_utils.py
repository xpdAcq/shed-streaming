"""Test generator for yielding valid event model from an iterable"""
from streamz_ext import Stream
from ..utils import to_event_model
from bluesky.callbacks.core import CallbackBase


def test_to_event_model_md():
    det = [1, 2, 3]
    new_md = {"hello": "world"}
    new_md_copy = new_md.copy()
    g = to_event_model(det, ("det",), md=new_md)

    class AssertCallback(CallbackBase):
        def _check(self, doc, keys):
            print(list(doc.keys()), keys)
            assert all([k in doc.keys() for k in keys])

        def start(self, doc):
            self._check(doc, ["uid", "time", "hello"])
            assert doc.get("hello", False) == "world"

        def descriptor(self, doc):
            self._check(doc, ["uid", "data_keys"])

        def event(self, doc):
            self._check(
                doc, ["uid", "time", "timestamps", "descriptor", "seq_num"]
            )

        def stop(self, doc):
            self._check(doc, ["exit_status", "uid"])
            assert doc["exit_status"] == "success"

    source = Stream()
    source.starsink(AssertCallback())
    assert new_md == new_md_copy
    for e in g:
        source.emit(e)


def test_to_event_model_no_md():
    det = [1, 2, 3]
    g = to_event_model(det, ("det",))

    class AssertCallback(CallbackBase):
        def _check(self, doc, keys):
            assert all([k in doc.keys() for k in keys])

        def start(self, doc):
            self._check(doc, ["uid", "time", "source"])

        def descriptor(self, doc):
            self._check(doc, ["uid", "data_keys"])

        def event(self, doc):
            self._check(
                doc, ["uid", "time", "timestamps", "descriptor", "seq_num"]
            )

        def stop(self, doc):
            self._check(doc, ["exit_status", "uid"])
            assert doc["exit_status"] == "success"

    source = Stream()
    source.starsink(AssertCallback())
    for e in g:
        source.emit(e)
