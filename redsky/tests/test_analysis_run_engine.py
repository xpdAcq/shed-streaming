import numpy as np
from pprint import pprint

from ..analysis_run_engine import AnalysisRunEngine, RunFunction


def example_run_func(event_stream1, event_stream2, name):
    for e1, e2 in zip(event_stream1, event_stream2):
        yield e1['data'][name] - e2['data'][name]


def example_fail_run_func(event_stream1, event_stream2):
    for e1, e2 in zip(event_stream1, event_stream2):
        yield NotImplementedError('Known Error')


def test_analysis_run_engine(exp_db, tmp_dir):
    subtract = RunFunction(example_run_func, ['img'],
                           [dict(source='testing',
                                 external='FILESTORE:',
                                 dtype='array')], save_func=np.save,
                           save_loc=tmp_dir)
    are = AnalysisRunEngine(exp_db)
    run_hdrs = exp_db[-1]
    uid = are([exp_db[-1], exp_db[-1]], subtract, 'pe1_image')
    pprint(exp_db[uid])
    assert exp_db[uid]['stop']['exit_status'] != 'failure'
    assert len(list(exp_db.get_events(exp_db[uid]))) == len(list(
        exp_db.get_events(run_hdrs)))


def test_analysis_run_engine_fail(exp_db, tmp_dir):
    subtract = RunFunction(example_run_func, ['img'],
                           [dict(source='testing',
                                 external='FILESTORE:',
                                 dtype='array')], save_func=np.save,
                           save_loc=tmp_dir)
    are = AnalysisRunEngine(exp_db)
    run_hdrs = exp_db[-1]
    uid = are([exp_db[-1], exp_db[-1]], subtract)
    pprint(exp_db[uid])
    assert exp_db[uid]['stop']['exit_status'] == 'failure'
    assert len(list(exp_db.get_events(exp_db[uid]))) != len(list(
        exp_db.get_events(run_hdrs)))
