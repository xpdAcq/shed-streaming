from shed.translation import FromEventModel
from shed.utils import to_event_model
from streamz import Stream


def test_translation():
    g = to_event_model(range(10), [('ct', {'units': 'arb'})])

    source = Stream()
    t = FromEventModel(source, 'event', ('data', 'ct'))
    l = t.sink_to_list()

    for gg in g:
        source.emit(gg)

    for i, ll in enumerate(l):
        assert i == ll
