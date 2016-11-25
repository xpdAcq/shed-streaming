import numpy as np
from pprint import pprint

from ..analysis_run_engine import AnalysisRunEngine, RunFunction
from numpy.testing import assert_array_equal
import pytest


def example_run_func(event_stream1, event_stream2, name):
    for e1, e2 in zip(event_stream1, event_stream2):
        yield e1['data'][name] - e2['data'][name]


def example_fail_run_func(event_stream1):
    raise NotImplementedError('Known Error')


def example_run_func2(event_stream1, name):
    for e1 in event_stream1:
        yield e1['data'][name] * 2


def example_two_yield(event_stream1, name):
    for e1 in event_stream1:
        yield np.sum(e1['data'][name]), np.mean(e1['data'][name])


subtract = RunFunction(example_run_func, ['img'],
                       [dict(source='testing',
                             external='FILESTORE:',
                             dtype='array')],
                       save_func=np.save,
                       ext='.npy',
                       spec='npy'
                       )

two_times = RunFunction(example_run_func2, ['img'],
                        [dict(source='testing',
                              external='FILESTORE:',
                              dtype='array')],
                        save_func=np.save,
                        ext='.npy',
                        spec='npy')

stats = RunFunction(example_two_yield, ['sum', 'ave'],
                    [dict(source='testing',
                          dtype='float'),
                     dict(source='testing',
                          dtype='float')
                     ],
                    save_to_filestore=False
                    )


@pytest.mark.parametrize("r_f, expect_func", [(subtract, np.subtract)])
def test_analysis_run_engine_two_hdr(exp_db, tmp_dir, r_f, expect_func):
    r_f.save_loc = tmp_dir
    are = AnalysisRunEngine(exp_db)
    run_hdrs = exp_db[-1]

    uid = are([run_hdrs] * 2, r_f, 'pe1_image', subscription=print)
    result_header = exp_db[uid]
    pprint(result_header)
    assert result_header['stop']['exit_status'] != 'failure'
    assert len(list(exp_db.get_events(result_header))) == len(list(
        exp_db.get_events(run_hdrs)))
    for res in exp_db.get_events(result_header, fill=True):
        img = res['data']['img']
        assert_array_equal(img, expect_func(img, img))


@pytest.mark.parametrize("r_f, expect_func", [(two_times, lambda x: x * 2)])
def test_analysis_run_engine_one_hdr(exp_db, tmp_dir, r_f, expect_func):
    r_f.save_loc = tmp_dir
    are = AnalysisRunEngine(exp_db)
    run_hdrs = exp_db[-1]
    uid = are(run_hdrs, r_f, 'pe1_image')
    result_header = exp_db[uid]
    pprint(result_header)
    assert result_header['stop']['exit_status'] != 'failure'
    assert len(list(exp_db.get_events(result_header))) == len(list(
        exp_db.get_events(run_hdrs)))
    for img1, img2 in zip(exp_db.get_images(result_header, 'img'),
                          exp_db.get_images(run_hdrs, 'pe1_image')):
        assert_array_equal(img1, expect_func(img2))


def test_analysis_run_engine_multi_run(exp_db):
    r_f = stats
    are = AnalysisRunEngine(exp_db)
    run_hdrs = exp_db[-1]
    uid = are(run_hdrs, r_f, 'pe1_image')
    result_header = exp_db[uid]
    pprint(result_header)
    assert result_header['stop']['exit_status'] != 'failure'
    assert len(list(exp_db.get_events(result_header))) == len(list(
        exp_db.get_events(run_hdrs)))
    for ev1, img in zip(exp_db.get_events(result_header),
                        exp_db.get_images(run_hdrs, 'pe1_image')):
        print(ev1['data'])
        ev1['data']['sum'] == np.sum(img)
        ev1['data']['ave'] == np.mean(img)


def test_analysis_run_engine_fail(exp_db, tmp_dir):
    r_f = RunFunction(example_fail_run_func, ['img'],
                      [dict(source='testing',
                            external='FILESTORE:',
                            dtype='array')], save_func=np.save, ext='.npy',
                      save_loc=tmp_dir)
    are = AnalysisRunEngine(exp_db)
    run_hdrs = exp_db[-1]
    uid = are(run_hdrs, r_f)
    result_header = exp_db[uid]
    pprint(result_header)
    assert result_header['stop']['exit_status'] == 'failure'
    assert len(list(exp_db.get_events(result_header))) != len(list(
        exp_db.get_events(run_hdrs)))
    assert result_header['stop']['reason'] == "NotImplementedError('" \
                                              "Known Error',)"
