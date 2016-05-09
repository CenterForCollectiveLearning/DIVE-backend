# -*- coding: utf-8 -*-

'''
Functions for returning the data corresponding to a given visualization type and specification
'''
from __future__ import division

import math
import locale
import numpy as np
import pandas as pd
import scipy as sp
from random import sample
from itertools import combinations
from flask import current_app

from dive.data.in_memory_data import InMemoryData as IMD
from dive.data.access import get_data, get_conditioned_data
from dive.tasks.ingestion.type_detection import detect_time_series
from dive.tasks.ingestion.binning import get_bin_edges, get_bin_decimals
from dive.tasks.visualization import GeneratingProcedure, TypeStructure, aggregation_functions

from time import time

import logging
logger = logging.getLogger(__name__)


def make_safe_string(s):
    # TODO Use slugify?
    invalid_chars = '-_.+^$ '
    for invalid_char in invalid_chars:
        s = s.replace(invalid_char, '_')
    s = 'temp_' + s
    return s


def _get_derived_field(df, precomputed, label_descriptor):
    label_a, op, label_b = label.split(' ')
    return result


def get_aggregated_df(groupby, aggregation_function_name):
    try:
        if aggregation_function_name == 'sum':
            agg_df = groupby.sum()
        elif aggregation_function_name == 'min':
            agg_df = groupby.min()
        elif aggregation_function_name == 'max':
            agg_df = groupby.max()
        elif aggregation_function_name == 'mean':
            agg_df = groupby.mean()
        elif aggregation_function_name == 'count':
            agg_df = groupby.count()
    except Exception as e:
        logger.error(e)
        agg_df = groupby.aggregate(aggregation_functions[aggregation_function_name])
    return agg_df


def get_viz_data_from_enumerated_spec(spec, project_id, conditionals, config, df=None, precomputed={}, data_formats=['visualize', 'table', 'score']):
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
        df = get_conditioned_data(project_id, dataset_id, df, conditionals)

    if gp == GeneratingProcedure.AGG.value:
        final_data = get_agg_data(df, precomputed, args, config, data_formats)

    elif gp == GeneratingProcedure.IND_VAL.value:
        final_data = get_ind_val_data(df, precomputed, args, config, data_formats)

    elif gp == GeneratingProcedure.BIN_AGG.value:
        final_data = get_bin_agg_data(df, precomputed, args, config, data_formats)

    elif gp == GeneratingProcedure.MULTIGROUP_COUNT.value:
        final_data = get_multigroup_count_data(df, precomputed, args, config, data_formats)

    elif gp == GeneratingProcedure.MULTIGROUP_AGG.value:
        final_data = get_multigroup_agg_data(df, precomputed, args, config, data_formats)

    elif gp == GeneratingProcedure.VAL_AGG.value:
        final_data = get_val_agg_data(df, precomputed, args, config, data_formats)

    elif gp == GeneratingProcedure.VAL_VAL.value:
        final_data = get_raw_comparison_data(df, precomputed, args, config, data_formats)

    elif gp == GeneratingProcedure.VAL_COUNT.value:
        final_data = get_val_count_data(df, precomputed, args, config, data_formats)

    elif gp == GeneratingProcedure.AGG_AGG.value:
        final_data = get_agg_agg_data(df, precomputed, args, config, data_formats)

    logger.debug('Data for %s: %s', gp, time() - start_time)
    return final_data


def get_raw_comparison_data(df, precomputed, args, config, data_formats=['visualize']):
    final_data = {}
    field_a_label = args['field_a']['name']
    field_b_label = args['field_b']['name']

    df = df.dropna(subset=[field_a_label, field_b_label])

    field_a_list = df[field_a_label].tolist()
    field_b_list = df[field_b_label].tolist()
    zipped_list = zip(field_a_list, field_b_list)
    if len(zipped_list) > 1000:
        final_list = sample(zipped_list, 1000)
    else:
        final_list = zipped_list

    if 'score' in data_formats:
        final_data['score'] = {
            'field_a': field_a_list,
            'field_b': field_b_list
        }
    if 'visualize' in data_formats:
        data_array = []
        data_array.append([ field_a_label, field_b_label ])
        for (a, b) in final_list:
            data_array.append([a, b])
        final_data['visualize'] = data_array
    if 'table' in data_formats:
        final_data['table'] = {
            'columns': df.columns.tolist(),
            'data': df.values.tolist()
        }

    return final_data


def get_multigroup_agg_data(df, precomputed, args, config, data_formats=['visualize']):
    '''
    Group by two groups then aggregate on the third

    For google charts, need this form for multiple line charts:
    [
        [group_field_a, group_b_value_1, group_b_value_2]
        [2011-MAR, 100, 200]
    ]
    '''
    agg_field = args['agg_field']['name']
    aggregation_function_name = args['agg_fn']
    group_a_field_label = args['grouped_field_a']['name']
    group_b_field_label = args['grouped_field_b']['name']

    df = df.dropna(subset=[group_a_field_label, group_b_field_label])
    groupby = df.groupby([group_a_field_label, group_b_field_label], sort=False)
    agg_df = get_aggregated_df(groupby, aggregation_function_name)[agg_field]

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
            data_array_element.append( v.get(secondary_field_value, None) )
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


def get_multigroup_count_data(df, precomputed, args, config, data_formats=['visualize']):
    '''
    Group by one field, then by another

    For google charts, need this form for multiple (e.g. stacked) charts:
    [
        [group_a_value_1, group_b_value_1, group_b_value_2, count],
        [group_a_value_2, group_b_value_1, group_b_value_2, count]
    ]
    '''
    group_a_field_name = args['field_a']['name']
    group_b_field_name = args['field_b']['name']

    df = df.dropna(subset=[group_a_field_name, group_b_field_name])

    grouped_df = df.groupby([group_a_field_name, group_b_field_name], sort=False).size()

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

    header_row = [ group_a_field_name ] + secondary_field_values
    results_as_data_array.append(header_row)
    for k, v in results_grouped_by_highest_level_value.iteritems():
        data_array_element = [ k ]
        for secondary_field_value in secondary_field_values:
            data_array_element.append( v.get(secondary_field_value) )
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


def get_agg_agg_data(df, precomputed, args, config, data_formats=['visualize']):
    '''
    1) Group by a categorical field
    2) Aggregate two other quantitative fields
    '''
    final_data = {}

    grouped_field_name = args['grouped_field']['name']
    agg_field_a_name = args['agg_field_a']['name']
    agg_field_b_name = args['agg_field_b']['name']

    df = df.dropna(subset=[agg_field_a_name, agg_field_b_name])
    aggregation_function_name = args['agg_fn']

    if 'groupby' in precomputed and grouped_field_name in precomputed['groupby']:
        grouped_df = precomputed['groupby'][grouped_field_name]
    else:
        grouped_df = df.groupby(grouped_field_name, sort=False)

    agg_df = get_aggregated_df(grouped_df, aggregation_function_name)
    grouped_field_list = agg_df.index.tolist()
    agg_field_a_list = agg_df[agg_field_a_name].tolist()
    agg_field_b_list = agg_df[agg_field_b_name].tolist()

    data_table = []
    data_array = []
    data_table.append([ grouped_field_name, agg_field_a_name, agg_field_b_name ])
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


def get_agg_data(df, precomputed, args, config, data_formats=['visualize']):
    final_data = {}
    agg_field_label = args['agg_field_a']['name']
    df = df.dropna(subset=[agg_field_label])

    agg_field_data = df[agg_field_label]
    aggregation_function_name = args['agg_fn']

    if aggregation_function_name == 'mode':
        result = agg_field_data.value_counts().idxmax()
    else:
        aggregation_function_name = aggregation_functions[aggregation_function_name_label]
        result = aggregation_function_name(agg_field_data)
    final_data['visualize'] = result
    return final_data


def get_ind_val_data(df, precomputed, args, config, data_formats=['visualize']):
    final_data = {}
    field_a_label = args['field_a']['name']

    # If direct field
    if isinstance(field_a_label, basestring):
        data = df[field_a_label]
    # If derived field
    # TODO Deal with this later
    elif isinstance(field_a_label, dict):
        data = _get_derived_field(df, precomputed, field_a_label)
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


def get_bin_agg_data(df, precomputed, args, config, data_formats=['visualize']):
    final_data = {}

    binning_field = args['binning_field']['name']
    agg_field_a = args['agg_field_a']['name']
    aggregation_function_name = args['agg_fn']

    # Handling NAs
    pre_cleaned_binning_field_values = df[binning_field]
    df = df.dropna(subset=[binning_field])
    binning_field_values = df[binning_field]
    if len(binning_field_values) == 0:
        return None

    # Configuration
    procedure = config.get('binning_procedure', 'freedman')
    binning_type = config.get('binning_type', 'procedural')
    if binning_type == 'procedural':
        procedural = True
    else:
        procedural = False
    precision = config.get('precision', None)
    num_bins = config.get('num_bins', 3)

    if not precision:
        precision = get_bin_decimals(binning_field_values)

    if precision > 0:
        float_formatting_string = '%.' + str(precision) + 'f'
    else:
        float_formatting_string = '%d'

    bin_edges_list = get_bin_edges(
        binning_field_values,
        procedural=procedural,
        procedure=procedure,
        num_bins=num_bins,
    )

    bin_num_to_edges = {}  # {1: [left_edge, right_edge]}
    bin_num_to_formatted_edges = {}  # {1: [left_edge, right_edge]}
    formatted_bin_edges_list = []  # [(left_edge, right_edge)]
    for bin_num in range(0, len(bin_edges_list) - 1):
        left_bin_edge, right_bin_edge = \
            bin_edges_list[bin_num], bin_edges_list[bin_num + 1]
        bin_num_to_edges[bin_num] = [ left_bin_edge, right_bin_edge ]

        if precision > 0:
            rounded_left_bin_edge = float(float_formatting_string % left_bin_edge)
            rounded_right_bin_edge = float(float_formatting_string % right_bin_edge)
        else:
            rounded_left_bin_edge = int(float_formatting_string % left_bin_edge)
            rounded_right_bin_edge = int(float_formatting_string % right_bin_edge)
        formatted_bin_edge = (rounded_left_bin_edge, rounded_right_bin_edge)
        formatted_bin_edges_list.append(formatted_bin_edge)

        bin_num_to_formatted_edges[bin_num] = formatted_bin_edge

    # Faster digitize? https://github.com/numpy/numpy/pull/4184
    df_bin_indices = np.digitize(binning_field_values, bin_edges_list, right=False)
    groupby = df.groupby(df_bin_indices, sort=True)
    agg_df = get_aggregated_df(groupby, aggregation_function_name)
    agg_bins_to_values = agg_df[agg_field_a].to_dict()
    agg_values = agg_bins_to_values.values()

    if 'score' in data_formats:
        final_data['score'] = {
            'bins': bin_num_to_edges,
            'bin_edges': list(bin_edges_list),
            'agg': agg_values
        }

    if 'visualize' in data_formats:
        data_array = [['Bin', 'Value', {
            'role': 'tooltip',
            'type': 'string',
            'p': { 'html': True }
        }]]

        bins = []
        for i, formatted_bin_edges in enumerate(formatted_bin_edges_list):
            bin_num = i + 1
            agg_val = agg_bins_to_values.get(bin_num, 0)
            bins.append({'v': i, 'f': str(formatted_bin_edges[0])})

            left_interval = '['
            right_interval = ')'
            if (i + 1) == len(formatted_bin_edges_list):
                right_interval = ']'

            formatted_interval = '%s%s, %s%s' % (left_interval, formatted_bin_edges[0], formatted_bin_edges[1], right_interval)

            data_array.append([
                i + 0.5,
                agg_val,
                '''
                <div style="padding: 8px 12px;">
                    <div style="white-space: nowrap;">Count in interval %s:</div>
                    <div style="font-weight: 500; font-size: 18px; padding-top: 4px;">%s</div>
                </div>
                ''' % (formatted_interval, agg_val)
            ])

        final_bin_tick = len(formatted_bin_edges_list)
        bins.append({'v': final_bin_tick, 'f': str(formatted_bin_edges_list[final_bin_tick - 1][1])})
        final_data['visualize'] = data_array
        final_data['bins'] = bins

    if 'table' in data_formats:
        table_data = []
        if aggregation_function_name == 'count':
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


def get_val_agg_data(df, precomputed, args, config, data_formats=['visualize']):
    final_data = {}
    grouped_field_name = args['grouped_field']['name']
    agg_field_name = args['agg_field']['name']

    df = df[[grouped_field_name, agg_field_name]]
    df = df.dropna(how='any', subset=[grouped_field_name, agg_field_name])

    aggregation_function_name = args['agg_fn']

    if 'groupby' in precomputed and grouped_field_name in precomputed['groupby']:
        grouped_df = precomputed['groupby'][grouped_field_name]
    else:
        grouped_df = df.groupby(grouped_field_name, sort=False)

    agg_df = get_aggregated_df(grouped_df, aggregation_function_name)
    grouped_field_list = agg_df.index.tolist()

    agg_field_list = agg_df[agg_field_name].tolist()

    if 'score' in data_formats:
        final_data['score'] = {
            'grouped_field': grouped_field_list,
            'agg_field': agg_field_list
        }
    if 'visualize' in data_formats:
        data_array = [ [grouped_field_name, agg_field_name] ] + \
            [[g, a] for (g, a) in zip(grouped_field_list, agg_field_list)]
        final_data['visualize'] = data_array
    if 'table' in data_formats:
        final_data['table'] = {
            'columns': agg_df.columns.tolist(),
            'data': agg_df.values.tolist()
        }
    return final_data


def get_val_count_data(df, precomputed, args, config, data_formats=['visualize']):
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
