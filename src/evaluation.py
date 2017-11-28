import numpy as np


def nwrmsle(predictions, targets, weights):
    predictions[predictions < 0] = np.nan
    targets[targets < 0] = np.nan
    weights = 1 + 0.25 * weights
    log_square_errors = (np.log(predictions + 1) - np.log(targets + 1)) ** 2
    return(np.sqrt(np.sum(weights * log_square_errors) / np.sum(weights)))
