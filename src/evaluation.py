import numpy as np
import pandas as pd

def nwrmsle(predictions, targets, weights):

    predictions = np.array([x if x > 0 else 0 for x in list(predictions)])

    targetsf = targets.astype(float)
    targetsf = np.array([x if x > 0 else 0 for x in list(targetsf)])


    weights = 1 + 0.25 * weights
    log_square_errors = (np.log(predictions + 1) - np.log(targetsf + 1)) ** 2
    return(np.sqrt(np.sum(weights * log_square_errors) / np.sum(weights)))
