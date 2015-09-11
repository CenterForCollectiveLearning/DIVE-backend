# -*- coding: utf-8 -*-

'''
Functions for returning the data corresponding to a given visualization type and specification
'''
from flask import Flask  # Don't do this
from bson.objectid import ObjectId

from . import GeneratingProcedure, TypeStructure
from data.access import get_delimiter, get_data, get_conditioned_data
from data.type_detection import detect_time_series, get_variable_type
from data.db import MongoInstance as MI
from data.in_memory_data import InMemoryData as IMD
from analysis.analysis import get_bin_edges
import viz_stats

from config import config

import numpy as np
import pandas as pd
import scipy as sp
import math
from itertools import combinations

# TODO just use regular strings?
group_fn_from_string = {
    'sum': np.sum,
    'min': np.min,
    'max': np.max,
    'mean': np.mean,
    'count': np.size
}

def makeSafeString(s):
    invalid_chars = '-_.+^$ '
    for invalid_char in invalid_chars:
        s = s.replace(invalid_char, '_')
    s = 'temp_' + s
    return s


def _get_derived_field(df, label_descriptor):
    label_a, op, label_b = label.split(' ')
    return result


def get_viz_data_from_enumerated_spec(spec, dID, pID, data_formats=['score']):
    '''
    Returns a dictionary containing data corresponding to spec (in automated-viz
    structure), and all necessary information to interpret data.

    There are three types of formats:
        Score: a dict of lists for scoring
        Visualize: a list of dicts (collection)
        Table: {columns: list, data: matrix}

    Args:
    spec, dID, pID, format (list of 'score', 'visualize', or 'table')
    Returns:
        data specified by spec, in specified format
    Raises:

    TODO Don't do so much in this function
    '''
    for f in data_formats:
        if f not in ['score', 'visualize', 'table']:
            raise ValueError('Passed incorrect data format', f)
    final_data = dict([(f, {}) for f in data_formats])

    gp = spec['generatingProcedure']
    args = spec['args']
    meta = spec['meta']

    df = get_data(pID=pID, dID=dID)

    if gp == GeneratingProcedure.IND_VAL.value:
        field_a_label = args['fieldA']['label']

        # If direct field
        if isinstance(field_a_label, basestring):
            data = df[field_a_label]
        # If derived field
        # TODO Deal with this later
        elif isinstance(field_a_label, dict):
            data = _get_derived_field(df, field_a_label)
        else:
            print "Ill-formed field_a_label %s" % (field_a)

        data = df[field_a_label]

        if 'score' in data_formats:
            final_data['score'] = {
                'ind': [ i for i in range(0, len(data)) ],
                'val': data.tolist()
            }
        if 'visualize' in data_formats:
            data = []
            for (i, val) in enumerate(data.tolist()):
                data.append({
                    'ind': i,
                    field_a_label: val
                })
            final_data['visualize'] = data
        if 'table' in data_formats:
            final_data['table'] = {
                'columns': data.columns.tolist(),
                'data': data.values.tolist()
            }

    elif gp == GeneratingProcedure.BIN_AGG.value:
        # TODO Get rid of this
        try:
            binning_field = args['binningField']['label']
            binning_procedure = args['binningProcedure']
            agg_field_a = args['aggFieldA']['label']
            agg_fn = group_fn_from_string[args['aggFn']]

            unbinned_field = df[binning_field]
            bin_edges = get_bin_edges(unbinned_field, procedure=binning_procedure)

            bin_num_to_edges = {}
            for bin_num in range(0, len(bin_edges) - 1):
                bin_num_to_edges[bin_num] = [ bin_edges[bin_num], bin_edges[bin_num + 1] ]

            grouped_df = df.groupby(np.digitize(df[binning_field], bin_edges))
            agg_df = grouped_df.aggregate(agg_fn)
            agg_values = agg_df[agg_field_a].tolist()

            if 'score' in data_formats:
                final_data['score'] = {
                    'bins': bin_num_to_edges,
                    'binEdges': bin_edges,
                    'agg': agg_values
                }
            if 'visualize' in data_formats:
                data = []
                for (bin_edges, agg_val) in zip(bin_edges, agg_values):
                    # TODO Generalize the procedure for making this string
                    data.append({
                        'bin': '%s-%s'% (bin_edges[0], bin_edges[1]),
                        agg_field_a: agg_val
                    })
                final_data['visualize'] = data
            if 'table' in data_formats:
                final_data['table'] = {
                    'columns': agg_df.columns.tolist(),
                    'data': agg_df.values.tolist()
                }
        except:
            final_data = {
            }

    # TODO Don't aggregate across numeric columns
    elif gp == GeneratingProcedure.VAL_AGG.value:
        grouped_field_label = args['groupedField']['label']
        agg_field_label = args['aggField']['label']

        grouped_df = df.groupby(grouped_field_label)
        agg_df = grouped_df.aggregate(group_fn_from_string[args['aggFn']])
        grouped_field_list = agg_df.index.tolist()
        agg_field_list = agg_df[agg_field_label].tolist()

        final_viz_data = {
            'groupedField': agg_df.index.tolist(),
            'aggField': agg_df[agg_field_label].tolist()
        }

        if 'score' in data_formats:
            final_data['score'] = {
                'groupedField': grouped_field_list,
                'aggField': agg_field_list
            }
        if 'visualize' in data_formats:
            final_data['visualize'] = \
                [{grouped_field_label: g, agg_field_label: a} for (g, a) in \
                zip(grouped_field_list, agg_field_list)]
        if 'table' in data_formats:
            final_data['table'] = {
                'columns': agg_df.columns.tolist(),
                'data': agg_df.values.tolist()
            }

    elif gp == GeneratingProcedure.VAL_VAL.value:
        fieldA_label = args['fieldA']['label']
        fieldB_label = args['fieldB']['label']

        fieldA_list = df[fieldA_label].tolist()
        fieldB_list = df[fieldB_label].tolist()

        if 'score' in data_formats:
            final_data['score'] = {
                'fieldA': fieldA_list,
                'fieldB': fieldB_list
            }
        if 'visualize' in data_formats:
            data = []
            for (a, b) in zip(fieldA_list, fieldB_list):
                data.append({
                    fieldA_label: a,
                    fieldB_label: b
                })
            final_data['visualize'] = data
        if 'table' in data_formats:
            final_data['table'] = {
                'columns': df.columns.tolist(),
                'data': df.values.tolist()
            }

    elif gp == GeneratingProcedure.VAL_COUNT.value:
        fieldA_label = args['fieldA']['label']
        vc = df[fieldA_label].value_counts()
        value_list = vc.index.tolist(),
        counts = vc.tolist()

        if 'score' in data_formats:
            final_data['score'] = {
                'val': value_list,
                'count': counts
            }
        if 'visualize' in data_formats:
            final_data['visualize'] = \
                [{fieldA_label: v, 'count': c} for (v, c) in zip(value_list, counts)]
        if 'table' in data_formats:
            final_data['table'] = {
                'columns': ['val', 'count'],
                'data': [[v, c] for (v, c) in zip(value_list, counts)]
            }

    elif gp == GeneratingProcedure.AGG_AGG.value:
        grouped_df = df.groupby(args['groupedField']['label'])
        agg_df = grouped_df.aggregate(group_fn_from_string[args['aggFn']])
        agg_field_a_list = agg_df[args['aggFieldA']['label']].tolist()
        agg_field_b_list = agg_df[args['aggFieldB']['label']].tolist()
        final_viz_data = {
            'fieldA': agg_field_a_list,
            'fieldB': agg_field_b_list
        }

        if 'score' in data_formats:
            final_data['score'] = {
                'fieldA': agg_field_a_list,
                'fieldB': agg_field_b_list,
            }
        if 'visualize' in data_formats:
            data = []
            for (a, b) in zip(agg_field_a_list, agg_field_b_list):
                data.append({
                    args['aggFieldA']['label']: a,
                    args['aggFieldB']['label']: b
                })
            final_data['visualize'] = data
        if 'table' in data_formats:
            final_data['table'] = {
                'columns': agg_df.columns.tolist(),
                'data': agg_df.values.tolist()
            }

    return final_data


### TODO Move these to some utility functions location
def dict_to_collection(d):
    result = []
    for k, v in d.iteritems():
        result.append({k: v})
    return result


def lists_to_collection(li_a, li_b):
    if len(li_a) != len(li_b):
        raise ValueError("Lists not equal size", len(li_a), len(li_b))
    else:
        result = []
        num_elements = len(li_a)
        for i in num_elements:
            result.append({li_a[i]: li_b[i]})
        return result


# df = pd.DataFrame({'AAA': [4,5,6,7], 'BBB': [10,20,30,40], 'CCC': [100,50,-30,-50]})
# spec = {'aggregate': {'field': 'AAA', 'operation': 'sum'}, 'condition': {'and': [{'field': 'AAA', 'operation': '>', 'criteria': 5}], 'or': [{'field': 'BBB', 'operation': '==', 'criteria': 10}]}, 'query': 'BBB'}
def get_viz_data_from_builder_spec(spec, conditional, pID):
    '''
    Deprecated function used to return viz data for a spec constructed by
    old builder
    '''
    ### 0) Parse and validate arguments
    # TODO Ensure well-formed spec
    dID = spec.get('dID')
    field_a = spec.get('field_a')
    operation = spec.get('operation')
    arguments = spec.get('arguments')

    if not (dID, field_a, operation):
        return "Did not pass required parameters", 400

    ### Returned data structures
    viz_result = {}
    table_result = {}

    ### 1) Access dataset
    df = get_data(pID=pID, dID=dID)

    ### 2) Apply all conditionals
    conditioned_df = get_conditioned_data(df, conditional)

    ### 3) Query based on operation
    # a) Group
    # TODO Chain with agg?
    # TODO Deal with multiple aggregations?
    if operation == 'group':
        function = arguments.get('function')
        field_b = arguments.get('field_b')
        gb = conditioned_df.groupby(field_a)

        if function == 'count':
            grouped_df = pd.DataFrame({'count': gb.size()})  # 1 col DF
        else:
            group_operation = group_fn_from_string[function]
            grouped_df = gb.aggregate(group_operation)
            grouped_df.insert(0, 'count', gb.size().tolist())  # Add Count as DF col after first aggregated field
            # grouped_df = grouped_df[[field_b]]  # Just returning all aggregated fields

        field_a_loc = conditioned_df.columns.get_loc(field_a)
        grouped_df.insert(0, field_a, grouped_df.index.tolist())  # Add grouped column to front of list

        # Table Data: Dict of matrices
        grouped_df_copy = grouped_df
        # grouped_df_copy.insert(0, field_a, grouped_df_copy.index)

        table_result = {
            'columns': grouped_df_copy.columns.tolist(),
            'data': grouped_df_copy.values.tolist(),
        }

        grouped_dict = grouped_df.to_dict()

        for k, obj in grouped_dict.iteritems():
            collection = [ { field_a: a, k: b } for a, b in obj.iteritems() ]
            viz_result[k] = collection

    # b) Vs. (raw comparison of one unique field against another)
    elif operation == 'vs':
        # TODO Get viz_data
        df.index = conditioned_df[field_a]
        df = df.drop(field_a, 1)
        final_dict = df.to_dict()
        # If taking field_b into account
        # final_dict = final_dict[field_b]

        for k, obj in final_dict.iteritems():
            viz_result[k] = [ { field_a: a, k: b } for a, b in obj.iteritems() ]

        table_result = conditioned_df.to_dict(orient='split')

    # c) Comparison
    elif operation == 'compare':
        function = arguments.get('function')
        element_x = arguments.get('element_x')
        element_y = arguments.get('element_y')
        field_b = arguments.get('field_b')
        field_c = arguments.get('field_c')

        # TODO Implement
        return

    return {
        'viz_data': viz_result,
        'table_result': table_result
    }, 200
