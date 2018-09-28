from shed.simple import SimpleToEventStream, SimpleFromEventStream
from zstreamz import Stream


def to_event_model(data, output_info, md=None):
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
    if md is None:
        md = {}
    else:
        md = md.copy()
    # add some metadata
    md.update({'source': 'to_event_model'})
    source = Stream()
    fes = SimpleFromEventStream('start', (),source, principle=True)
    tes = SimpleToEventStream(fes, output_info, **md)

    start = None
    for d in data:
        if not start:
            yield tes.create_start(d)
            yield tes.create_descriptor(d)
        yield tes.create_event(d)
    yield 'stop', tes._create_stop(d)
