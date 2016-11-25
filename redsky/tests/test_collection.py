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


@pytest.mark.parametrize("r_f, expect_func", [collect_even])
def test_analysis_run_engine_one_hdr(exp_db, r_f):
    are = AnalysisRunEngine(exp_db)
    run_hdrs = exp_db[-1]

    uid = are(run_hdrs, r_f)
    result_header = exp_db[uid]

    pprint(result_header)
    assert result_header['stop']['exit_status'] != 'failure'
    for ev1, ev2 in zip(exp_db.get_events(result_header),
                         evens(exp_db.get_events(run_hdrs))):
        assert ev1['data'].items() == ev2['data'].items()
