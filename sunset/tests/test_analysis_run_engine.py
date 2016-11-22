import numpy as np
from pprint import pprint

from xpdan.analysis_run_engine import AnalysisRunEngine, RunFunction


def example_run_func(event_stream1, event_stream2, name):
    for e1, e2 in zip(event_stream1, event_stream2):
        yield e1['data'][name] - e2['data'][name]


def test_analysis_run_engine(exp_db, an_db, mk_glbl):
    subtract = RunFunction(example_run_func, ['img'],
                           [dict(source='testing',
                                 external='FILESTORE:',
                                 dtype='array')], save_func=np.save,
                           save_loc=mk_glbl.base)
    are = AnalysisRunEngine(exp_db, an_db)
    uid = are([exp_db[-1], exp_db[-1]], subtract, 'pe1_image')
    pprint(an_db[uid])
    assert an_db[uid]['stop']['exit_status'] != 'failure'
