from numpy.testing import assert_array_equal
import numpy as np
import traceback
from pprint import pprint
from time import time
from uuid import uuid4
from ..streamer import db_store_single_resource_single_file
from ..savers import np_saver, NPYSaver


def multiply_by_two(img):
    return img * 2


def evens(event):
    if event['seq_num'] % 2 == 0:
        return event['data']


def pass_through(event):
    return event['data']


def test_streaming(exp_db, tmp_dir, start_uid1):
    dnsm = {'img': (NPYSaver, (tmp_dir, ), {})}

    @db_store_single_resource_single_file(exp_db, dnsm)
    def sample_f(name_doc_stream_pair, **kwargs):
        process = multiply_by_two
        _, start = next(name_doc_stream_pair)
        run_start_uid = str(uuid4())
        new_start_doc = dict(uid=run_start_uid, time=time(),
                             parents=start['uid'],
                             function_name=process.__name__,
                             kwargs=kwargs)  # More provenance to be defined
        yield 'start', new_start_doc

        _, descriptor = next(name_doc_stream_pair)
        new_descriptor = dict(uid=str(uuid4()), time=time(),
                              run_start=run_start_uid,
                              data_keys={'img': dict(source='testing',
                                                     dtype='array'),
                                         })  # TODO: include shape somehow
        yield 'descriptor', new_descriptor

        exit_md = None
        for i, (name, ev) in enumerate(name_doc_stream_pair):
            if name == 'stop':
                break
            if name != 'event':
                raise Exception
            args_mapping = [ev['data'][k] for k in ['pe1_image']]
            kwargs_mapping = {}
            kwargs_mapped = {k: ev['data'][
                v] for k, v in kwargs_mapping.items()}

            try:
                results = process(*args_mapping, **kwargs_mapped,
                                  **kwargs)
            except Exception as e:
                exit_md = dict(exit_status='failure', reason=repr(e),
                               traceback=traceback.format_exc())
                break

            new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                             descriptor=new_descriptor,
                             data={'img': results},
                             seq_num=i)
            yield 'event', new_event

        if exit_md is None:
            exit_md = {'exit_status': 'success'}
        new_stop = dict(uid=str(uuid4()), time=time(),
                        run_start=run_start_uid, **exit_md)
        yield 'stop', new_stop

    input_hdr = exp_db[start_uid1]
    pprint(input_hdr)
    a = exp_db.restream(input_hdr, fill=True)
    for name, doc in sample_f(a):
        if name == 'start':
            assert doc['parents'] == input_hdr['start']['uid']
        if name == 'event':
            assert isinstance(doc['data']['img'], np.ndarray)
    pprint(exp_db[-1])
    for ev1, ev2 in zip(exp_db.get_events(input_hdr, fill=True),
                        exp_db.get_events(exp_db[-1], fill=True)):
        assert_array_equal(ev1['data']['pe1_image'] * 2,
                           ev2['data']['img'])


def test_double_streaming(exp_db, tmp_dir, start_uid1):
    dnsm = {'pe1_image': (NPYSaver, (tmp_dir,), {})}

    @db_store_single_resource_single_file(exp_db, dnsm)
    def sample_f(name_doc_stream_pair, **kwargs):
        process = multiply_by_two
        _, start = next(name_doc_stream_pair)
        run_start_uid = str(uuid4())
        new_start_doc = dict(uid=run_start_uid, time=time(),
                             parents=start['uid'],
                             function_name=process.__name__,
                             kwargs=kwargs)  # More provenance to be defined
        yield 'start', new_start_doc

        _, descriptor = next(name_doc_stream_pair)
        new_descriptor = dict(uid=str(uuid4()), time=time(),
                              run_start=run_start_uid,
                              data_keys={'pe1_image': dict(source='testing',
                                                           dtype='array'),
                                         })
        yield 'descriptor', new_descriptor

        exit_md = None
        for i, (name, ev) in enumerate(name_doc_stream_pair):
            if name == 'stop':
                break
            if name != 'event':
                raise Exception
            args_mapping = [ev['data'][k] for k in ['pe1_image']]
            kwargs_mapping = {}
            kwargs_mapped = {k: ev['data'][
                v] for k, v in kwargs_mapping.items()}

            try:
                results = process(*args_mapping, **kwargs_mapped,
                                  **kwargs)
            except Exception as e:
                exit_md = dict(exit_status='failure', reason=repr(e),
                               traceback=traceback.format_exc())
                break

            new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                             descriptor=new_descriptor,
                             data={'pe1_image': results},
                             seq_num=i)
            yield 'event', new_event

        if exit_md is None:
            exit_md = {'exit_status': 'success'}
        new_stop = dict(uid=str(uuid4()), time=time(),
                        run_start=run_start_uid, **exit_md)
        yield 'stop', new_stop

    input_hdr = exp_db[start_uid1]
    pprint(input_hdr)
    a = exp_db.restream(input_hdr, fill=True)
    for name, doc in sample_f(sample_f(a)):
        if name == 'event':
            assert isinstance(doc['data']['pe1_image'], np.ndarray)
    pprint(exp_db[-1])
    for ev1, ev2 in zip(exp_db.get_events(input_hdr, fill=True),
                        exp_db.get_events(exp_db[-1], fill=True)):
        assert_array_equal(ev1['data']['pe1_image'] * 4,
                           ev2['data']['pe1_image'])


def test_collection(exp_db, start_uid1):
    @db_store_single_resource_single_file(exp_db)
    def sample_f(name_doc_stream_pair, **kwargs):
        process = evens
        _, start = next(name_doc_stream_pair)
        if _ != 'start':
            raise Exception
        run_start_uid = str(uuid4())
        new_start_doc = dict(uid=run_start_uid, time=time(),
                             parents=start['uid'],
                             function_name=process.__name__,
                             kwargs=kwargs)  # More provenance to be defined
        yield 'start', new_start_doc

        _, descriptor = next(name_doc_stream_pair)
        if _ != 'descriptor':
            raise Exception
        new_descriptor = dict(uid=str(uuid4()), time=time(),
                              run_start=run_start_uid,
                              data_keys=descriptor['data_keys'])
        yield 'descriptor', new_descriptor

        exit_md = None
        for i, (name, ev) in enumerate(name_doc_stream_pair):
            if name == 'stop':
                break
            if name != 'event':
                raise Exception
            try:
                results = process(ev)
            except Exception as e:
                exit_md = dict(exit_status='failure', reason=repr(e),
                               traceback=traceback.format_exc())
                break
            if results is not None:
                new_event = dict(uid=str(uuid4()), time=time(), timestamps={},
                                 descriptor=new_descriptor,
                                 data=results,
                                 seq_num=i)
                yield 'event', new_event

        if exit_md is None:
            exit_md = {'exit_status': 'success'}
        new_stop = dict(uid=str(uuid4()), time=time(),
                        run_start=run_start_uid, **exit_md)
        yield 'stop', new_stop

    input_hdr = exp_db[start_uid1]
    pprint(input_hdr)
    a = exp_db.restream(input_hdr)
    for b in sample_f(a):
        pass
    pprint(exp_db[-1])
    for ev in exp_db.get_events(exp_db[-1]):
        print(ev)


def test_combine(exp_db, start_uid1, start_uid2):
    @db_store_single_resource_single_file(exp_db)
    def sample_f(*name_doc_stream_pairs, **kwargs):
        process = pass_through
        starts = []
        for name_doc_stream_pair in name_doc_stream_pairs:
            _, start = next(name_doc_stream_pair)
            if _ != 'start':
                raise Exception
            starts.append(start)
        run_start_uid = str(uuid4())
        new_start_doc = dict(uid=run_start_uid, time=time(),
                             parents=[start['uid'] for start in starts],
                             function_name=process.__name__,
                             kwargs=kwargs)  # More provenance to be defined
        yield 'start', new_start_doc

        descriptors = []
        for name_doc_stream_pair in name_doc_stream_pairs:
            _, descriptor = next(name_doc_stream_pair)
            if _ != 'descriptor':
                raise Exception
            descriptors.append(descriptor)

        # Data Bundles MUST have the same data keys
        for descriptor1 in descriptors:
            for descriptor2 in descriptors:
                if descriptor1['data_keys'] != descriptor2['data_keys']:
                    raise Exception
        new_descriptor = dict(uid=str(uuid4()), time=time(),
                              run_start=run_start_uid,
                              data_keys=descriptor['data_keys'])
        yield 'descriptor', new_descriptor

        exit_md = None
        for name_doc_stream_pair in name_doc_stream_pairs:
            for i, (name, ev) in enumerate(name_doc_stream_pair):
                if name == 'stop':
                    break
                if name != 'event':
                    raise Exception
                try:
                    results = process(ev)
                except Exception as e:
                    exit_md = dict(exit_status='failure', reason=repr(e),
                                   traceback=traceback.format_exc())
                    break
                if results is not None:
                    new_event = dict(uid=str(uuid4()), time=time(),
                                     timestamps={},
                                     descriptor=new_descriptor,
                                     data=results,
                                     seq_num=i)
                    yield 'event', new_event

        if exit_md is None:
            exit_md = {'exit_status': 'success'}
        new_stop = dict(uid=str(uuid4()), time=time(),
                        run_start=run_start_uid, **exit_md)
        yield 'stop', new_stop

    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid2]
    a1 = exp_db.restream(ih1)
    a2 = exp_db.restream(ih2)
    for b in sample_f(a1, a2):
        pass
    pprint(exp_db[-1])

    assert len(list(exp_db.get_events(exp_db[-1]))) == len(list(
        exp_db.get_events(ih1))) + len(list(exp_db.get_events(ih2)))
