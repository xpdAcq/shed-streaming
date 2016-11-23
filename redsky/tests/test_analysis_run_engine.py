import numpy as np
from pprint import pprint

from ..analysis_run_engine import AnalysisRunEngine, RunFunction
import pytest


def example_run_func(event_stream1, event_stream2, name):
    for e1, e2 in zip(event_stream1, event_stream2):
        yield e1['data'][name] - e2['data'][name]


def example_fail_run_func(event_stream1, event_stream2):
    for e1, e2 in zip(event_stream1, event_stream2):
        yield NotImplementedError('Known Error')


def example_run_func2(event_stream1, name):
    for e1 in event_stream1:
        yield 2*e1['data'][name]


run_functions = [(example_run_func, 'pe1_image'),
                 (example_run_func2, 'pe1_image')]


@pytest.mark.parametrize("run_and_args", run_functions)
def test_analysis_run_engine(exp_db, tmp_dir, run_and_args):
    RF = RunFunction(run_and_args[0], ['img'],
                           [dict(source='testing',
                                 external='FILESTORE:',
                                 dtype='array')], save_func=np.save,
                           save_loc=tmp_dir)
    are = AnalysisRunEngine(exp_db)
    run_hdrs = exp_db[-1]
    uid = are(run_hdrs, RF, run_and_args[1])
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
