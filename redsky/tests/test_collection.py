import numpy as np
from pprint import pprint

from ..analysis_run_engine import AnalysisRunEngine
from ..collection import CollectionGen
from numpy.testing import assert_array_equal
import pytest


def evens(event_stream):
    for i, event in enumerate(event_stream):
        if i % 2 == 0:
            yield event


collect_even = CollectionGen(evens)


def upper_threshold(es1, es2, name, thresh):
    for e1, e2 in zip(es1, es2):
        if np.mean(e2['data'][name]) >= thresh:
            yield e1

collect_thresh = CollectionGen(upper_threshold, fill=[False, True])


@pytest.mark.parametrize("r_f, r_g", [(collect_even, evens)])
def test_collection_one_hdr(exp_db, r_f, r_g):
    are = AnalysisRunEngine(exp_db)
    run_hdrs = exp_db[-1]

    uid = are(run_hdrs, r_f)
    result_header = exp_db[uid]

    pprint(result_header)
    assert result_header['stop']['exit_status'] != 'failure'
    for ev1, ev2 in zip(exp_db.get_events(result_header),
                        r_g(exp_db.get_events(run_hdrs))):
        assert ev1['data'].items() == ev2['data'].items()


@pytest.mark.parametrize("r_f, r_g", [(collect_thresh, upper_threshold)])
def test_collection_two_hdr(exp_db, r_f, r_g):
    are = AnalysisRunEngine(exp_db)
    run_hdrs = [exp_db[-1]] * 2

    uid = are(run_hdrs, r_f, 'pe1_image', .5)
    result_header = exp_db[uid]

    pprint(result_header)
    assert result_header['stop']['exit_status'] != 'failure'
    for ev1, ev2 in zip(exp_db.get_events(result_header),
                        r_g(exp_db.get_events(run_hdrs),
                            exp_db.get_events(run_hdrs, fill=True),
                            'pe1_image', .5)):
        assert ev1['data'].items() == ev2['data'].items()
