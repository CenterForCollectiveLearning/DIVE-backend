'''
Utility analysis functions (e.g. distance between columns, overlap)
'''
from __future__ import division
import os
import json
from itertools import combinations
from collections import OrderedDict  # Get unique elements of list while preserving order
from time import time
import numpy as np
import pandas as pd
import scipy.stats as stats
import math
from decimal import Decimal
import random

from dive.data.access import get_data

import logging
logger = logging.getLogger(__name__)

def get_bin_decimals(v, max_sample=10000, default=3):
    v = v.astype(float, raise_on_error=False)
    if len(v) <= max_sample:
        sample = v
    else:
        sample = random.sample(v, max_sample)

    num_decimals = []
    for e in sample:
        num_decimals.append(str(e).find('.'))
        # num_decimals.append(Decimal.from_float(e).as_tuple().exponent * -1)

    try:
        max_decimals = max(num_decimals)
    except:
        return default
    return min(max_decimals, default)

###
# Get bin specifier (e.g. bin edges) given a numeric vector
###
DEFAULT_BINS = 10
MAX_BINS = 20
def get_bin_edges(v, procedural=True, procedure='freedman', num_bins=10):
    '''
    Given a quantitative vector, either:
    1) Automatically bin according to Freedman
    2) Procedurally bin according to some procedure
    3) Create num_bins uniform bins

    Returns the edges of each bin
    '''
    min_v = min(v)
    max_v = max(v)
    n = len(v)
    v = v.astype(float, raise_on_error=False)

    # Procedural binning
    if procedural:
        if procedure == 'freedman':
            IQR = np.subtract(*np.percentile(v, [75, 25]))
            bin_width = 2 * IQR * n**(-1/3)
            num_bins = (max_v - min_v) / bin_width
        elif procedure == 'square_root':
            num_bins = math.sqrt(n)
        elif procedure == 'doane':
            skewness = abs(stats.skew(v))
            sigma_g1 = math.sqrt((6*(n-2)) / ((n+1) * (n+3)))
            num_bins = 1 + math.log(n, 2) + math.log((1 + (skewness / sigma_g1)), 2)
        elif procedure == 'rice':
            num_bins = 2 * n**(-1/3)
        elif procedure == 'sturges':
            num_bins = math.ceil(math.log(n, 2) + 1)

        num_bins = math.floor(num_bins)
        num_bins = min(num_bins, MAX_BINS)
        if not num_bins:
            num_bins = DEFAULT_BINS

    # Incrementing max value by tiny amount to deal with np.digitize right edge
    eps = max_v * 0.00001
    fake_max = max_v + eps
    fake_min = min_v - eps
    bin_edges = np.histogram(v, range=(min_v, fake_max), bins=num_bins)[1]

    return bin_edges
