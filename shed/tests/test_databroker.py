from streamz import Stream

from shed.databroker import AssetInsert
from shed.savers import NpyWriter
from shed.utils import to_event_model


def test_AssetInsert(db, tmp_dir):
    g = list(to_event_model(range(10), [('det', {'dtype': 'int'})]))

    source = Stream()
    l1 = source.sink_to_list()
    ai = AssetInsert(source, db.fs, tmp_dir, {'det': NpyWriter})
    l2 = ai.sink_to_list()
    ai.sink(lambda x: db.insert(*x))

    for gg in g:
        source.emit(gg)

    ret = db[-1].documents()
    ret_fill = db[-1].documents(fill=True)
    for r, ll in zip(ret, l2):
        name, doc = r
        doc.pop('filled', None)
        doc.pop('_name', None)
        assert r == ll
    for r, ll in zip(ret_fill, l1):
        assert r == ll
