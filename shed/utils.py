from .event_streams import EventStream


def to_event_model(data, output_info):
    """Take an iterable of data and put it into the event model

    Parameters
    ----------
    data: iterable


    Yields
    -------
    name: str
        Name of doc
    document: dict
        Document of data
    """
    es = EventStream(md={'source': 'to_event_model'}, output_info=output_info)
    yield es.dispatch(('start', None))
    yield es.dispatch(('descriptor', None))
    for d in data:
        e = es.issue_event(d)
        yield es.event(e)
    yield es.dispatch(('stop', None))
