# -*- coding: utf-8 -*-

'''
Functions for returning the data corresponding to a given visualization type and specification
'''

import math
import numpy as np
import pandas as pd
import scipy as sp
from itertools import combinations
from flask import current_app

from dive.data.in_memory_data import InMemoryData as IMD
from dive.data.access import get_data, get_conditioned_data
from dive.tasks.ingestion.type_detection import detect_time_series
from dive.tasks.ingestion.binning import get_bin_edges
from dive.tasks.visualization import GeneratingProcedure, TypeStructure, aggregation_functions

from time import time

import logging
logger = logging.getLogger(__name__)


def make_safe_string(s):
    invalid_chars = '-_.+^$ '
    for invalid_char in invalid_chars:
        s = s.replace(invalid_char, '_')
    s = 'temp_' + s
    return s


def _get_derived_field(df, label_descriptor):
    label_a, op, label_b = label.split(' ')
    return result


def get_viz_data_from_enumerated_spec(spec, project_id, conditionals, df=None, data_formats=['score']):
    '''
    Returns a dictionary containing data corresponding to spec (in automated-viz
    structure), and all necessary information to interpret data.

    There are three types of formats:
        Score: a dict of lists for scoring
        Visualize: a list of dicts (collection)
        Table: {columns: list, data: matrix}

    Args:
    spec, dataset_id, project_id, format (list of 'score', 'visualize', or 'table')
    Returns:
        data specified by spec, in specified format

    '''
    for f in data_formats:
        if f not in ['score', 'visualize', 'table']:
            raise ValueError('Passed incorrect data format', f)
    final_data = dict([(f, {}) for f in data_formats])

    gp = spec['generating_procedure']
    args = spec['args']
    dataset_id = spec['dataset_id']

    start_time = time()

    if df is None:
        df = get_data(project_id=project_id, dataset_id=dataset_id)
        df = df.dropna()
        conditioned_df = get_conditioned_data(df, conditionals)

    if gp == GeneratingProcedure.AGG.value:
        final_data = get_agg_data(df, args, data_formats)

    elif gp == GeneratingProcedure.IND_VAL.value:
        final_data = get_ind_val_data(df, args, data_formats)

    elif gp == GeneratingProcedure.BIN_AGG.value:
        final_data = get_bin_agg_data(df, args, data_formats)
    #
    # elif gp == GeneratingProcedure.MULTIGROUP_COUNT.value:
    #     final_data = get_multigroup_count_data(df, args, data_formats)

    elif gp == GeneratingProcedure.MULTIGROUP_AGG.value:
        final_data = get_multigroup_agg_data(df, args, data_formats)

    elif gp == GeneratingProcedure.VAL_AGG.value:
        final_data = get_val_agg_data(df, args, data_formats)

    elif gp == GeneratingProcedure.VAL_VAL.value:
        final_data = get_raw_comparison_data(df, args, data_formats)

    elif gp == GeneratingProcedure.VAL_COUNT.value:
        final_data = get_val_count_data(df, args, data_formats)

    elif gp == GeneratingProcedure.AGG_AGG.value:
        final_data = get_agg_agg_data(df, args, data_formats)

    logger.debug('Data for %s: %s', gp, time() - start_time)
    return final_data


def get_raw_comparison_data(df, args, data_formats):
    final_data = {}
    field_a_label = args['field_a']['name']
    field_b_label = args['field_b']['name']

    field_a_list = df[field_a_label].tolist()
    field_b_list = df[field_b_label].tolist()

    if 'score' in data_formats:
        final_data['score'] = {
            'field_a': field_a_list,
            'field_b': field_b_list
        }
    if 'visualize' in data_formats:
        data_array = []
        data_array.append([ field_a_label, field_b_label ])
        for (a, b) in zip(field_a_list, field_b_list):
            data_array.append([a, b])
        final_data['visualize'] = data_array
    if 'table' in data_formats:
        final_data['table'] = {
            'columns': df.columns.tolist(),
            'data': df.values.tolist()
        }

    return final_data


def get_multigroup_agg_data(df, args, data_formats):
    '''
    Group by two groups then aggregate on the third

    For google charts, need this form for multiple line charts:
    [
        [group_field_a, group_b_value_1, group_b_value_2]
        [2011-MAR, 100, 200]
    ]
    '''
    logger.debug('In get_multigroup_agg_data')
    agg_field = args['agg_field']['name']
    agg_fn = args['agg_fn']
    group_a_field_label = args['grouped_field_a']['name']
    group_b_field_label = args['grouped_field_b']['name']
    grouped_df = df.groupby([group_a_field_label, group_b_field_label], sort=False)
    agg_df = grouped_df.aggregate(aggregation_functions[agg_fn])[agg_field]

    results_as_data_array = []
    secondary_field_values = []

    results_grouped_by_highest_level_value = {}

    for k, v in agg_df.to_dict().iteritems():
        # Structure: {highest_level_value: (secondary_level_value, aggregated_value)}
        highest_level_value = k[0]
        secondary_level_value = k[1]

        if highest_level_value in results_grouped_by_highest_level_value:
            results_grouped_by_highest_level_value[highest_level_value][secondary_level_value] = v
        else:
            results_grouped_by_highest_level_value[highest_level_value] = { secondary_level_value: v }

        if secondary_level_value not in secondary_field_values:
            secondary_field_values.append(secondary_level_value)

    secondary_field_values = sorted(secondary_field_values)

    header_row = [ group_a_field_label ] + secondary_field_values
    results_as_data_array.append(header_row)
    for k, v in results_grouped_by_highest_level_value.iteritems():
        data_array_element = [ k ]
        for secondary_field_value in secondary_field_values:
            data_array_element.append( v[secondary_field_value] )
        results_as_data_array.append(data_array_element)

    final_data = {}
    if 'score' in data_formats:
        score_data = {
            'agg': agg_df.values
        }
        final_data['score'] = score_data
    if 'visualize' in data_formats:
        visualization_data = results_as_data_array
        final_data['visualize'] = visualization_data

    if 'table' in data_formats:
        table_data = {
            'columns': results_as_data_array[0],
            'data': results_as_data_array[1:]
        }
        final_data['table'] = table_data
    return final_data


def get_multigroup_count_data(df, args, data_formats):
    '''
    Group by one field, then by another

    For google charts, need this form for multiple (e.g. stacked) charts:
    [
        [group_a_value_1, group_b_value_1, group_b_value_2, count],
        [group_a_value_2, group_b_value_1, group_b_value_2, count]
    ]
    '''
    group_a_field_label = args['field_a']['name']
    group_b_field_label = args['field_b']['name']
    grouped_df = df.groupby([group_a_field_label, group_b_field_label], sort=False).size()

    results_as_data_array = []
    secondary_field_values = []

    results_grouped_by_highest_level_value = {}

    for k, v in grouped_df.to_dict().iteritems():
        # Structure: {(highest_level_value, secondary_level_value): value}
        highest_level_value = k[0]
        secondary_level_value = k[1]

        if highest_level_value in results_grouped_by_highest_level_value:
            results_grouped_by_highest_level_value[highest_level_value][secondary_level_value] = v
        else:
            results_grouped_by_highest_level_value[highest_level_value] = { secondary_level_value: v }

        if secondary_level_value not in secondary_field_values:
            secondary_field_values.append(secondary_level_value)

    secondary_field_values = sorted(secondary_field_values)

    header_row = [ group_a_field_label ] + secondary_field_values
    results_as_data_array.append(header_row)
    for k, v in results_grouped_by_highest_level_value.iteritems():
        data_array_element = [ k ]
        for secondary_field_value in secondary_field_values:
            data_array_element.append( v[secondary_field_value] )
        results_as_data_array.append(data_array_element)

    final_data = {}
    if 'score' in data_formats:
        score_data = {
            'agg': grouped_df.values
        }
        final_data['score'] = score_data
    if 'visualize' in data_formats:
        visualization_data = results_as_data_array
        final_data['visualize'] = visualization_data

    if 'table' in data_formats:
        table_data = {
            'columns': results_as_data_array[0],
            'data': results_as_data_array[1:]
        }
        final_data['table'] = table_data
    return final_data


def get_agg_agg_data(df, args, data_formats):
    '''
    1) Group by a categorical field
    2) Aggregate two other quantitative fields
    '''
    final_data = {}

    group_field_name = args['grouped_field']['name']
    agg_field_a_name = args['agg_field_a']['name']
    agg_field_b_name = args['agg_field_b']['name']
    agg_fn = args['agg_fn']

    grouped_df = df.groupby(group_field_name, sort=False)
    agg_df = grouped_df.aggregate(aggregation_functions[agg_fn])
    grouped_field_list = agg_df.index.tolist()
    agg_field_a_list = agg_df[agg_field_a_name].tolist()
    agg_field_b_list = agg_df[agg_field_b_name].tolist()

    data_table = []
    data_array = []
    data_table.append([ group_field_name, agg_field_a_name, agg_field_b_name ])
    data_array.append([ agg_field_a_name, agg_field_b_name ])
    for (a, b, c) in zip(grouped_field_list, agg_field_a_list, agg_field_b_list):
        data_table.append([a, b, c])
        data_array.append([b, c])

    if 'score' in data_formats:
        final_data['score'] = {
            'field_a': agg_field_a_list,
            'field_b': agg_field_b_list,
        }

    if 'visualize' in data_formats:

        final_data['visualize'] = data_array

    if 'table' in data_formats:
        final_data['table'] = {
            'columns': data_table[0],
            'data': data_table[1:]
        }
    return final_data


def get_agg_data(df, args, data_formats):
    final_data = {}
    agg_field_label = args['agg_field_a']['name']
    agg_field_data = df[agg_field_label]
    agg_fn_label = args['agg_fn']

    if agg_fn_label == 'mode':
        result = agg_field_data.value_counts().idxmax()
    else:
        agg_fn = aggregation_functions[agg_fn_label]
        result = agg_fn(agg_field_data)
    final_data['visualize'] = result
    return final_data


def get_ind_val_data(df, args, data_formats):
    final_data = {}
    field_a_label = args['field_a']['name']

    # If direct field
    if isinstance(field_a_label, basestring):
        data = df[field_a_label]
    # If derived field
    # TODO Deal with this later
    elif isinstance(field_a_label, dict):
        data = _get_derived_field(df, field_a_label)
    else:
        logger.error("Ill-formed field_a_label %s" % (field_a))

    field_a_series = df[field_a_label]

    if 'score' in data_formats:
        final_data['score'] = {
            'ind': [ i for i in range(0, len(data)) ],
            'val': field_a_series.tolist()
        }
    if 'visualize' in data_formats:
        data_array = []
        for (i, val) in enumerate(field_a_series.tolist()):
            data_array.append([
                i,
                val
            ])
        final_data['visualize'] = data_array
    if 'table' in data_formats:
        final_data['table'] = {
            'columns': df.columns.tolist(),
            'data': df.values.tolist()
        }
    return final_data


def get_bin_agg_data(df, args, data_formats):
    final_data = {}
    binning_field = args['binning_field']['name']
    binning_procedure = args['binning_procedure']
    agg_field_a = args['agg_field_a']['name']
    agg_fn = aggregation_functions[args['agg_fn']]

    unbinned_field = df[binning_field]
    try:
        bin_edges_list = list(get_bin_edges(unbinned_field, procedure=binning_procedure))
    except Exception, e:
        # Skip this spec
        return None

    bin_num_to_edges = {}  # {1: [left_edge, right_edge]}
    bin_num_to_formatted_edges = {}  # {1: [left_edge, right_edge]}
    formatted_bin_edges_list = []  # ['left_edge-right_edge']
    for bin_num in range(0, len(bin_edges_list) - 1):
        left_bin_edge, right_bin_edge = \
            bin_edges_list[bin_num], bin_edges_list[bin_num + 1]
        bin_num_to_edges[bin_num] = [ left_bin_edge, right_bin_edge ]

        rounded_left_bin_edge = '%.3f' % left_bin_edge
        rounded_right_bin_edge = '%.3f' % right_bin_edge
        formatted_bin_edge = '%s-%s' % (rounded_left_bin_edge, rounded_right_bin_edge)
        formatted_bin_edges_list.append(formatted_bin_edge)

        bin_num_to_formatted_edges[bin_num] = formatted_bin_edge

    # TODO Ensure that order is preserved here
    grouped_df = df.groupby(np.digitize(df[binning_field], bin_edges_list), sort=False) # Right edge open
    agg_df = grouped_df.aggregate(agg_fn)
    agg_values = agg_df[agg_field_a].tolist()

    if 'score' in data_formats:
        final_data['score'] = {
            'bins': bin_num_to_edges,
            'binEdges': bin_edges_list,
            'agg': agg_values
        }
    if 'visualize' in data_formats:
        data_array = [['Bin', 'Value']]
        for (formatted_bin_edges, agg_val) in zip(formatted_bin_edges_list, agg_values):
            data_array.append([
                formatted_bin_edges,
                agg_val
            ])
        final_data['visualize'] = data_array
    if 'table' in data_formats:
        table_data = []
        if args['agg_fn'] == 'count':
            columns = columns = [ 'bins of %s' % binning_field, 'count' ]
            for i, count in enumerate(agg_df.ix[:, 0].tolist()):
                new_row = [bin_num_to_formatted_edges[i], count]
                table_data.append(new_row)

        else:
            columns = [ 'bins of %s' % binning_field ] + agg_df.columns.tolist()
            for i, row in enumerate(agg_df.values.tolist()):
                new_row = [bin_num_to_formatted_edges[i]] + row
                table_data.append(new_row)

        final_data['table'] = {
            'columns': columns,
            'data': table_data
        }
    return final_data


def get_val_agg_data(df, args, data_formats):
    final_data = {}
    grouped_field_label = args['grouped_field']['name']
    agg_field_label = args['agg_field']['name']

    grouped_df = df.groupby(grouped_field_label, sort=False)
    agg_df = grouped_df.aggregate(aggregation_functions[args['agg_fn']])
    grouped_field_list = agg_df.index.tolist()
    agg_field_list = agg_df[agg_field_label].tolist()

    final_viz_data = {
        'grouped_field': agg_df.index.tolist(),
        'agg_field': agg_df[agg_field_label].tolist()
    }

    if 'score' in data_formats:
        final_data['score'] = {
            'grouped_field': grouped_field_list,
            'agg_field': agg_field_list
        }
    if 'visualize' in data_formats:
        data_array = [ [grouped_field_label, agg_field_label] ] + \
            [[g, a] for (g, a) in zip(grouped_field_list, agg_field_list)]
        final_data['visualize'] = data_array
    if 'table' in data_formats:
        final_data['table'] = {
            'columns': agg_df.columns.tolist(),
            'data': agg_df.values.tolist()
        }
    return final_data

def get_val_count_data(df, args, data_formats):
    final_data = {}
    field_a_label = args['field_a']['name']
    vc = df[field_a_label].value_counts()
    value_list = list(vc.index.values)
    counts = vc.tolist()

    if 'score' in data_formats:
        final_data['score'] = {
            'value': value_list,
            'count': counts
        }
    if 'visualize' in data_formats:
        data_array = [ [field_a_label, 'count'] ] + [[v, c] for (v, c) in zip(value_list, counts)]

        final_data['visualize'] = data_array
    if 'table' in data_formats:
        final_data['table'] = {
            'columns': [field_a_label, 'count'],
            'data': [[v, c] for (v, c) in zip(value_list, counts)]
        }
    return final_data
