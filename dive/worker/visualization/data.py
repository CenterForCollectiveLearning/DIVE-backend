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

from dive.base.data.in_memory_data import InMemoryData as IMD
from dive.base.data.access import get_data, get_conditioned_data
from dive.worker.ingestion.type_detection import detect_time_series
from dive.worker.ingestion.binning import get_bin_edges, get_bin_decimals
from dive.worker.visualization.constants import GeneratingProcedure, TypeStructure, aggregation_functions

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
        elif aggregation_function_name == 'std':
            agg_df = groupby.std()
        elif aggregation_function_name == 'sem':
            agg_df = groupby.sem()
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
        if f not in ['score', 'visualize', 'table', 'count']:
            raise ValueError('Passed incorrect data format', f)
    final_data = dict([(f, {}) for f in data_formats])

    gp = spec['generating_procedure']
    args = spec['args']
    dataset_id = spec['dataset_id']

    logger.debug('Generating Procedure: %s', gp)
    logger.debug('Arguments: %s', args)
    start_time = time()

    if df is None:
        df = get_data(project_id=project_id, dataset_id=dataset_id)
        df = get_conditioned_data(project_id, dataset_id, df, conditionals)

    generating_procedure_to_data_function = {
        GeneratingProcedure.AGG.value: get_agg_data,
        GeneratingProcedure.IND_VAL.value: get_ind_val_data,
        GeneratingProcedure.BIN_AGG.value: get_bin_agg_data,
        GeneratingProcedure.MULTIGROUP_COUNT.value: get_multigroup_count_data,
        GeneratingProcedure.MULTIGROUP_AGG.value: get_multigroup_agg_data,
        GeneratingProcedure.VAL_BOX.value: get_val_box_data,
        GeneratingProcedure.VAL_AGG.value: get_val_agg_data,
        GeneratingProcedure.VAL_VAL.value: get_raw_comparison_data,
        GeneratingProcedure.VAL_COUNT.value: get_val_count_data,
        GeneratingProcedure.AGG_AGG.value: get_agg_agg_data,
    }
    data = generating_procedure_to_data_function[gp](df,
        args,
        precomputed=precomputed,
        config=config,
        data_formats=data_formats
    )

    logger.debug('Data for %s: %s', gp, time() - start_time)
    return data


def get_raw_comparison_data(df, args, precomputed={}, config={}, data_formats=['visualize']):
    final_data = {}
    field_a_label = args['field_a']['name']
    field_b_label = args['field_b']['name']

    df = df.dropna(subset=[field_a_label, field_b_label])

    subset = config.get('subset', 100)
    is_subset = False
    if subset and (subset != 'all') and len(df.index) > subset:
        is_subset = True
        df = df.sample(subset)

    field_a_list = df[field_a_label].tolist()
    field_b_list = df[field_b_label].tolist()
    zipped_list = zip(field_a_list, field_b_list)

    if 'score' in data_formats:
        final_data['score'] = {
            'field_a': field_a_list,
            'field_b': field_b_list
        }
    if 'visualize' in data_formats:
        data_array = []
        data_array.append([ field_a_label, field_b_label ])
        for (a, b) in zipped_list:
            data_array.append([a, b])
        final_data['visualize'] = data_array
    if 'table' in data_formats:
        final_data['table'] = {
            'columns': df.columns.tolist(),
            'data': df.values.tolist()
        }
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]

    final_data['subset'] = subset if is_subset else 'all'

    return final_data


def get_multigroup_agg_data(df, args, precomputed={}, config={}, data_formats=['visualize']):
    '''
    Group by two groups then aggregate on the third

    For google charts, need this form for multiple line charts:
    [
        [group_field_a, group_b_value_1, group_b_value_2]
        [2011-MAR, 100, 200]
    ]

    Add confidence intervals if calculating mean
    '''
    agg_field = args['agg_field']['name']
    aggregation_function_name = args['agg_fn']
    group_a_field_label = args['grouped_field_a']['name']
    group_b_field_label = args['grouped_field_b']['name']

    df = df.dropna(subset=[group_a_field_label, group_b_field_label])
    groupby = df.groupby([group_a_field_label, group_b_field_label], sort=False)
    agg_df = get_aggregated_df(groupby, aggregation_function_name)[agg_field]


    mean_aggregration = (aggregation_function_name == 'mean')
    if mean_aggregration:
        sem_df = get_aggregated_df(groupby, 'sem')[agg_field]
        lower_confidence_df = (agg_df - sem_df)
        upper_confidence_df = (agg_df + sem_df)

    results_as_data_array = []
    results_as_data_array_with_interval = []
    secondary_field_values = []
    results_grouped_by_highest_level_value = {}

    agg_df_as_dict = agg_df.to_dict()

    for k, v in agg_df_as_dict.iteritems():
        # Structure: {highest_level_value: {secondary_level_value: aggregated_value}}
        highest_level_value = k[0]
        secondary_level_value = k[1]

        if highest_level_value in results_grouped_by_highest_level_value:
            results_grouped_by_highest_level_value[highest_level_value][secondary_level_value] = v
        else:
            results_grouped_by_highest_level_value[highest_level_value] = { secondary_level_value: v }

        # Header
        if secondary_level_value not in secondary_field_values:
            secondary_field_values.append(secondary_level_value)

    # secondary_field_values = sorted(secondary_field_values)

    if mean_aggregration:
        header_row = [ group_a_field_label ]
        for secondary_field_value in secondary_field_values:
            header_row.extend([
                secondary_field_value,
                {
                    'id': '%sLowerInterval' % secondary_field_value,
                    'type': 'number',
                    'role': 'interval'
                },
                {
                    'id': '%sUpperInterval' % secondary_field_value,
                    'type': 'number',
                    'role': 'interval'
                }
            ])
    else:
        header_row = [ group_a_field_label ] + secondary_field_values

    results_as_data_array.append(header_row)
    results_as_data_array_with_interval.append( [group_a_field_label] + secondary_field_values )

    for k, v in results_grouped_by_highest_level_value.iteritems():
        data_array_element = [ k ]
        data_array_element_with_interval = [ k ]
        for secondary_field_value in secondary_field_values:
            aggregation_value = v.get(secondary_field_value, None)
            data_array_element.append(aggregation_value)

            if mean_aggregration:
                sem = sem_df[k].get(secondary_field_value, None)
                if sem is None or np.isnan(sem):
                    if aggregation_value:
                        data_array_element_with_interval.append('%.3f' % aggregation_value)
                    else:
                        data_array_element_with_interval.append('')
                else:
                    if aggregation_value:
                        data_array_element_with_interval.append('%.3f ± %.3f' % (aggregation_value, sem))
                    else:
                        data_array_element_with_interval.append('')

                data_array_element.append(lower_confidence_df[k].get(secondary_field_value, None) )
                data_array_element.append(upper_confidence_df[k].get(secondary_field_value, None) )

        if mean_aggregration:
            results_as_data_array_with_interval.append(data_array_element_with_interval)
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
        if mean_aggregration:
            final_array = results_as_data_array_with_interval
        else:
            final_array = results_as_data_array

        table_data = {
            'columns': final_array[0],
            'data': final_array[1:]
        }
        final_data['table'] = table_data
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]
    return final_data


def get_multigroup_count_data(df, args, precomputed={}, config={}, data_formats=['visualize']):
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
    results_as_data_array_with_interval = []
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
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]
    return final_data


def get_agg_agg_data(df, args, precomputed={}, config={}, data_formats=['visualize']):
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
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]
    return final_data


def get_agg_data(df, args, precomputed={}, config={}, data_formats=['visualize']):
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
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]
    return final_data


def get_ind_val_data(df, args, precomputed={}, config={}, data_formats=['visualize']):
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
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]
    return final_data


def get_bin_agg_data(df, args, precomputed={}, config={}, data_formats=['visualize'], MAX_BINS=25):
    final_data = {}

    binning_field = args['binning_field']['name']
    agg_field_a = args['agg_field_a']['name']
    aggregation_function_name = args['agg_fn']

    # Handling NAs
    pre_cleaned_binning_field_values = df[binning_field]
    df = df.dropna(subset=[ binning_field ])
    binning_field_values = df[binning_field]
    if len(binning_field_values) == 0:
        return None

    # Configuration
    data_config = config
    procedure = data_config.get('binning_procedure', 'freedman')
    binning_type = data_config.get('binning_type', 'procedural')
    procedural = (binning_type == 'procedural')
    num_bins = data_config.get('num_bins')
    precision = config.get('precision', get_bin_decimals(binning_field_values))

    # Max number of bins for integers is number of unique values
    float_formatting_string = ('%.' + str(precision) + 'f') if (precision > 0) else '%d'

    print procedure, procedural, binning_type
    logger.info('%s %s %s %s', procedure, binning_type, procedural, data_config.get('num_bins'))

    if not (procedural or num_bins):
        if args['binning_field']['type'] == 'integer':
            num_bins = len(np.unique(binning_field_values))
        else:
            num_bins = 3
    num_bins = min(num_bins, MAX_BINS)

    bin_edges_list = get_bin_edges(
        binning_field_values,
        procedural=procedural,
        procedure=procedure,
        num_bins=num_bins,
        num_decimals=precision
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

    # logger.info(bin_num_to_formatted_edges)
    # logger.info(agg_df)
    # logger.info(len(agg_df.ix[:, 0].tolist()))
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
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]
    return final_data

def get_val_box_data(df, args, precomputed={}, config={}, data_formats=['visualize']):
    final_data = {}
    grouped_field_name = args['grouped_field']['name']
    boxed_field_name = args['boxed_field']['name']

    df = df[[grouped_field_name, boxed_field_name]]
    df = df.dropna(how='any', subset=[grouped_field_name, boxed_field_name])

    if 'groupby' in precomputed and grouped_field_name in precomputed['groupby']:
        grouped_df = precomputed['groupby'][grouped_field_name]
    else:
        grouped_df = df.groupby(grouped_field_name, sort=False)

    def top_whisker(group):
      Q3 = group.quantile(0.75)
      Q1 = group.quantile(0.25)
      IQR = Q3 - Q1
      return max(group[group <= Q3 + 1.5*IQR])

    def bottom_whisker(group):
      Q3 = group.quantile(0.75)
      Q1 = group.quantile(0.25)
      IQR = Q3 - Q1
      return min(group[group >= Q1 - 1.5*IQR])

    # Reduce redunancy here
    df_quantiles = grouped_df.quantile([0.25, 0.75])
    df_max = grouped_df.max()
    df_min = grouped_df.min()
    df_median = grouped_df.median()
    df_mean = grouped_df.mean()
    df_top_whisker = grouped_df.agg(top_whisker)
    df_bottom_whisker = grouped_df.agg(bottom_whisker)

    grouped_field_list = df_max.index.tolist()
    boxed_field_list = df_max[boxed_field_name].tolist()

    columns = [ 'Bottom', 'Q1', 'Median', 'Mean', 'Q3', 'Top' ]
    data_array = [[ boxed_field_name ] + columns]
    for grouped_field_value in grouped_field_list:
        Q1 = df_quantiles[boxed_field_name][grouped_field_value][0.25]
        Q3 = df_quantiles[boxed_field_name][grouped_field_value][0.75]
        bottom = df_bottom_whisker[boxed_field_name][grouped_field_value]
        top = df_top_whisker[boxed_field_name][grouped_field_value]
        maximum = df_max[boxed_field_name][grouped_field_value]
        minimum = df_min[boxed_field_name][grouped_field_value]
        median = df_median[boxed_field_name][grouped_field_value]
        mean = df_mean[boxed_field_name][grouped_field_value]

        data_element = [
            grouped_field_value,
            # minimum,
            bottom,
            Q1,
            median,
            mean,
            Q3,
            top,
            # maximum,
        ]
        data_array.append(data_element)

    if 'score' in data_formats:
        final_data['score'] = {
            'grouped_field': grouped_field_list,
            'boxed_field': boxed_field_list
        }
    if 'visualize' in data_formats:
        final_data['visualize'] = data_array

    if 'table' in data_formats:
        final_data['table'] = {
            'columns': data_array[0],
            'data': data_array[1:]
        }
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]
    return final_data



def get_val_agg_data(df, args, precomputed={}, config={}, data_formats=['visualize']):
    final_data = {}
    aggregation_function_name = args['agg_fn']
    grouped_field_name = args['grouped_field']['name']
    agg_field_name = args['agg_field']['name']

    df = df[[grouped_field_name, agg_field_name]]
    df = df.dropna(how='any', subset=[grouped_field_name, agg_field_name])

    if 'groupby' in precomputed and grouped_field_name in precomputed['groupby']:
        grouped_df = precomputed['groupby'][grouped_field_name]
    else:
        grouped_df = df.groupby(grouped_field_name, sort=False)

    agg_df = get_aggregated_df(grouped_df, aggregation_function_name)
    grouped_field_list = agg_df.index.tolist()

    mean_aggregration = (aggregation_function_name == 'mean')
    if mean_aggregration:
        sem_df = get_aggregated_df(grouped_df, 'sem')[agg_field_name]
        lower_confidence_list = (agg_df[agg_field_name] - sem_df).tolist()
        upper_confidence_list = (agg_df[agg_field_name] + sem_df).tolist()

    agg_field_list = agg_df[agg_field_name].tolist()
    if 'score' in data_formats:
        final_data['score'] = {
            'grouped_field': grouped_field_list,
            'agg_field': agg_field_list
        }
    if 'visualize' in data_formats:
        if mean_aggregration:
            data_header = [
                grouped_field_name,
                agg_field_name,
                {
                    'id': '%sLowerInterval' % agg_field_name,
                    'type': 'number',
                    'role': 'interval'
                },
                {
                    'id': '%sUpperInterval' % agg_field_name,
                    'type': 'number',
                    'role': 'interval'
                }
            ]
            data_array_no_header = [
                [g, a, a_li, a_ui] for (g, a, a_li, a_ui) in zip(grouped_field_list, agg_field_list, lower_confidence_list, upper_confidence_list)
            ]
            data_array = [ data_header ] + data_array_no_header
        else:
            data_array = [ [grouped_field_name, agg_field_name] ] + [
                [g, a] for (g, a) in zip(grouped_field_list, agg_field_list)
            ]
        final_data['visualize'] = data_array

    if 'table' in data_formats:
        table_columns = [ grouped_field_name ] + agg_df.columns.tolist()
        table_data = [ list(row) for row in zip(grouped_field_list, *agg_df.T.values.tolist()) ]  # Necessary to transpose

        final_data['table'] = {
            'columns': table_columns,
            'data': table_data
        }
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]
    return final_data


def get_val_count_data(df, args, precomputed={}, config={}, data_formats=['visualize']):
    final_data = {}
    field_a_label = args['field_a']['name']

    values = df[field_a_label].dropna()
    value_counts = values.value_counts(sort=True, dropna=True)

    subset = config.get('subset', 100)
    is_subset = False
    if subset and len(value_counts.index) > subset:
        is_subset = True
        value_counts = value_counts[:subset]
    value_list = list(value_counts.index.values)
    counts = value_counts.tolist()

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
    if 'count' in data_formats:
        final_data['count'] = df.shape[0]

    final_data['subset'] = subset if subset else 'all'

    return final_data
