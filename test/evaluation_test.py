import sys
import os
sys.path.append(os.path.join('..', 'src'))
sys.path.append(os.path.join('src'))
import evaluation


def test_calculates_nwrmsle_for_estimate_and_actual_using_dataframes():
    estimate = 10
    actual = 10
    calculated_nwrmsle = evaluation.nwrmsle(estimate, actual)

    assert calculated_nwrmsle == 2
