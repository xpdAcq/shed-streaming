from .event_streams import EventStream


def to_event_model(data, output_info, md={}):
    """Take an iterable of data and put it into the event model

    Parameters
    ----------
    data: iterable
        The data to be inserted
    output_info: list of tuple
        The name of the data and information to put into the descriptor
    md : iterable of dicts
        an iterable of dictionaries to use as metadata for the start documents


    Yields
    -------
    name: str
        Name of doc
    document: dict
        Document of data

    Notes
    -----
    This is only for demonstration/example use, do not use for production.
    """
    es = EventStream(md={'source': 'to_event_model'}, output_info=output_info)
    # add some metadata
    es.md = md

    yield es.dispatch(('start', None))
    yield es.dispatch(('descriptor', None))
    for d in data:
        e = es.issue_event(d)
        yield es.event(e)
    yield es.dispatch(('stop', None))
