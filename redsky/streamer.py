def f(name_stream_pair, **kwargs):
    _, start = next(name_stream_pair)
    new_start = {}
    yield 'start', new_start
    _, descriptor = next(name_stream_pair)
    new_descriptor = {}
    yield 'descriptor', new_descriptor
    for name, ev in name_stream_pair:
        if name != 'event':
            break
        mapping = {'img': 'pe1_image'}
        mapped = {k: ev[v] for k, v in mapping.keys()}
        process(**mapped, **kwargs)
        new_event = {}
        yield 'event', new_event
    _, stop = next(name_stream_pair)
    new_stop = {}
    yield 'stop', new_stop