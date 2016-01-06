'''
Utility analysis functions (e.g. distance between columns, overlap)
'''
import os
import json
from itertools import combinations
from collections import OrderedDict  # Get unique elements of list while preserving order
from time import time
import numpy as np
import scipy.stats as stats
import math

# from dive.data.access import get_data


###
# Get bin specifier (e.g. bin edges) given a numeric vector
###
MAX_BINS = 10
def get_bin_edges(v, procedure='freedman'):
    # v = v.tolist()
    # if procedure == 'freedman':
    IQR = np.subtract(*np.percentile(v, [75, 25]))
    bin_width = 2 * IQR * len(v)**(-1/3)
    num_bins = math.floor((max(v) - min(v)) / bin_width)
    num_bins = min(num_bins, MAX_BINS)

    # Incrementing max value by tiny amount to deal with np.digitize right edge
    # https://github.com/numpy/numpy/issues/4217
    eps = 0.0001
    old_max = max(v)
    new_max = old_max + eps
    v[v.index(old_max)] = new_max

    bin_edges = np.histogram(v, bins=num_bins)[1]
    return bin_edges
