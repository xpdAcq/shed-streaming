from numpy.testing import assert_array_equal
from ..streamer import db_store
import numpy as np
import traceback
from pprint import pprint


def multiply_by_two(img):
    return img * 2


def test_streaming(exp_db, tmp_dir):
    dnsm = {
        'img': {'sf': np.save, 'folder': tmp_dir, 'spec': 'npy',
                'ext': '.npy',
                'args': (), 'kwargs': {},
                'resource_kwargs': {}, 'datum_kwargs': {}}}

    @db_store(exp_db, dnsm)
    def sample_f(name_doc_stream_pair, **kwargs):
        process = multiply_by_two
        _, start = next(name_doc_stream_pair)
        new_start_doc = {'parents': start['uid'],
                         'function_name': process.__name__,
                         'kwargs': kwargs}  # More provenance to be defined
        yield 'start', new_start_doc
        _, descriptor = next(name_doc_stream_pair)
        new_descriptor = dict(data_keys={'img': dict(source='testing')})
        yield 'descriptor', new_descriptor
        exit_md = None
        for i, (name, ev) in enumerate(name_doc_stream_pair):
            if name == 'stop':
                break
            args_mapping = [ev['data'][k] for k in ['pe1_image']]
            kwargs_mapping = {}
            kwargs_mapped = {k: ev[v] for k, v in kwargs_mapping.items()}
            try:
                results = process(*args_mapping, **kwargs_mapped,
                                  **kwargs)
            except Exception as e:
                exit_md = dict(exit_status='failure', reason=repr(e),
                               traceback=traceback.format_exc())
            new_event = dict(descriptor=new_descriptor,
                             data={'img': results},
                             seq_num=i)
            yield 'event', new_event
        if name == 'stop':
            if exit_md is None:
                exit_md = {'exit_status': 'success'}
            new_stop = dict(** exit_md)
            yield 'stop', new_stop

    input_hdr = exp_db[-1]
    pprint(input_hdr)
    a = exp_db.restream(input_hdr, fill=True)
    for b in sample_f(a):
        pass
    pprint(exp_db[-1])
    for ev1, ev2 in zip(exp_db.get_events(input_hdr, fill=True),
                        exp_db.get_events(exp_db[-1], fill=True)):
        assert_array_equal(ev1['data']['pe1_image'] * 2,
                           ev2['data']['img'])
