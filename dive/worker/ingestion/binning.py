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
from datetime import timedelta

from dive.base.data.access import get_data

import logging
logger = logging.getLogger(__name__)

def get_bin_decimals(v, max_sample=100, default=3):
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


def format_bin_edges_list(bin_edges_list, precision, general_type='q'):
    bin_num_to_edges = {}
    bin_num_to_formatted_edges = {}
    formatted_bin_edges_list = []

    float_formatting_string = ('%.' + str(precision) + 'f') if (precision > 0) else '%d'

    for bin_num in range(0, len(bin_edges_list) - 1):
        left_bin_edge, right_bin_edge = \
            bin_edges_list[bin_num], bin_edges_list[bin_num + 1]
        bin_num_to_edges[bin_num] = [ left_bin_edge, right_bin_edge ]

        if general_type == 'q':
            if precision > 0:
                rounded_left_bin_edge = float(float_formatting_string % left_bin_edge)
                rounded_right_bin_edge = float(float_formatting_string % right_bin_edge)
            else:
                rounded_left_bin_edge = int(float_formatting_string % left_bin_edge)
                rounded_right_bin_edge = int(float_formatting_string % right_bin_edge)
        elif general_type == 't':
            rounded_left_bin_edge = left_bin_edge
            rounded_right_bin_edge = right_bin_edge

        formatted_bin_edge = (rounded_left_bin_edge, rounded_right_bin_edge)
        formatted_bin_edges_list.append(formatted_bin_edge)

        bin_num_to_formatted_edges[bin_num] = formatted_bin_edge

    return {
        'bin_num_to_edges': bin_num_to_edges,
        'bin_num_to_formatted_edges': bin_num_to_formatted_edges,
        'formatted_bin_edges_list': formatted_bin_edges_list
    }

def get_num_bins(v, procedure='freedman', default_num_bins=10):
    v = v.astype(float, raise_on_error=False)
    n = len(v)
    min_v = min(v)
    max_v = max(v)

    # Procedural binning
    if procedure == 'freedman':
        try:
            IQR = np.subtract(*np.percentile(v, [75, 25]))
            bin_width = 2 * IQR * n**(-1/3)
            num_bins = (max_v - min_v) / bin_width
        except:
            num_bins = math.sqrt(n)
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
        return default_num_bins

    return num_bins


###
# Get bin specifier (e.g. bin edges) given a numeric vector
###
import dateutil.parser as dparser
DEFAULT_BINS = 10
MAX_BINS = 20
def get_bin_edges(v, general_type='q', procedural=True, procedure='freedman', num_bins=10, num_decimals=2):
    '''
    Given a quantitative vector, either:
    1) Automatically bin according to Freedman
    2) Procedurally bin according to some procedure
    3) Create num_bins uniform bins

    Returns the edges of each bin
    '''
    n = len(v)
    if general_type == 't':
        try:
            min_v = min(v).value
            max_v = max(v).value
        except AttributeError:
            min_v = min(v)
            max_v = max(v)

    if general_type == 'q':
        v = v.astype(float, raise_on_error=False)
        min_v = min(v)
        max_v = max(v)

    if procedural:
        num_bins = get_num_bins(v, procedure=procedure)

    edges = []
    try:
        if general_type == 'q':
            edges = np.linspace(min_v, max_v, num_bins+1)
        elif general_type == 't':
            edges = pd.to_datetime(np.linspace(min_v, max_v, num_bins+1))
    except Exception as e:
        logger.error('Error binning: %s', e, exc_info=True)
    print edges

    rounded_edges = []
    rounding_string = '{0:.' + str(num_decimals) + 'f}'
    for edge in edges:
        if general_type == 'q':
            rounded_edge = float(rounding_string.format(edge))
            if num_decimals == 0:
                rounded_edge = int(rounded_edge)
        elif general_type == 't':
            rounded_edge = edge
        rounded_edges.append(rounded_edge)

    if general_type == 'q':
        rounded_edges[-1] += 0.0001 * max_v
    elif general_type == 't':
        rounded_edges[-1] += timedelta(seconds=0.001)
    return rounded_edges
