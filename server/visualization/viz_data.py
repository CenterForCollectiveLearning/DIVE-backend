# -*- coding: utf-8 -*-

'''
Functions for returning the data corresponding to a given visualization type and specification
'''
from flask import Flask  # Don't do this
from bson.objectid import ObjectId

from data.access import get_delimiter, get_data, detect_time_series, get_variable_type
from data.db import MongoInstance as MI
from data.in_memory_data import InMemoryData as IMD

from itertools import combinations

import viz_stats

from config import config

import numpy as np
import pandas as pd
import scipy as sp
import math


# TODO just use regular strings?
group_fn_from_string = {
    'sum': np.sum,
    'min': np.min,
    'max': np.max,
    'mean': np.mean,
    'count': np.size
}

def makeSafeString(str):
    invalid_chars = '-_.+^$ '
    for invalid_char in invalid_chars:
        str = str.replace(invalid_char, '_')
    str = 'temp_' + str
    return str

# Given a data frame and a conditional dict ({ and: [{field, operation, criteria}], or: [...]})
# Return the conditioned data frame in same dimensions as original
def getConditionedDF(df, conditional_arg):
    # Replace spaces in column names with underscore
    # cols = df.columns
    # cols = cols.map(lambda x: x.replace(' ', '_') if isinstance(x, (str, unicode)) else x)
    # df.columns = cols
    # print "DF", df.columns
    query_strings = {
        'and': '',
        'or': ''
    }
    orig_cols = df.columns.tolist()
    df.rename(columns=makeSafeString, inplace=True)
    if conditional_arg.get('and'):
        for c in conditional_arg['and']:
            field = makeSafeString(c['field'])
            operation = c['operation']
            criteria = c['criteria']
            criteria_type = get_variable_type(criteria)

            print criteria_type
            if criteria_type in ["integer", "float"]:
                query_string = '%s %s %s' % (field, operation, criteria)
            else:
                query_string = '%s %s "%s"' % (field, operation, criteria)
            query_strings['and'] = query_strings['and'] + ' & ' + query_string

    if conditional_arg.get('or'):
        for c in conditional_arg['or']:
            field = makeSafeString(c['field'])
            operation = c['operation']
            criteria = c['criteria']
            criteria_type = get_variable_type(c['criteria'])

            if criteria_type in ["integer", "float"]:
                query_string = '%s %s %s' % (field, operation, criteria)
            else:
                query_string = '%s %s "%s"' % (field, operation, criteria)
            query_strings['or'] = query_strings['or'] + ' | ' + query_string
    query_strings['and'] = query_strings['and'].strip(' & ')
    query_strings['or'] = query_strings['or'].strip(' | ')

    # Concatenate
    if not (query_strings['and'] or query_strings['or']):
        conditioned_df = df
    else:
        final_query_string = ''
        if query_strings['and'] and query_strings['or']:
            final_query_string = '%s | %s' % (query_strings['and'], query_strings['or'])
        elif query_strings['and'] and not query_strings['or']:
            final_query_string = query_strings['and']
        elif query_strings['or'] and not query_strings['and']:
            final_query_string = query_strings['or']
        print "FINAL_QUERY_STRING:", final_query_string
        conditioned_df = df.query(final_query_string)
    df.columns = orig_cols
    conditioned_df.columns = orig_cols
    return conditioned_df


# df = pd.DataFrame({'AAA': [4,5,6,7], 'BBB': [10,20,30,40], 'CCC': [100,50,-30,-50]})
# spec = {'aggregate': {'field': 'AAA', 'operation': 'sum'}, 'condition': {'and': [{'field': 'AAA', 'operation': '>', 'criteria': 5}], 'or': [{'field': 'BBB', 'operation': '==', 'criteria': 10}]}, 'query': 'BBB'}
def getVisualizationDataFromSpec(spec, conditional, pID):
    ### 0) Parse and validate arguments
    # TODO Ensure well-formed spec
    dID = spec.get('dID')
    field_a = spec.get('field_a')
    operation = spec.get('operation')
    arguments = spec.get('arguments')

    if not (dID, field_a, operation):
        return "Did not pass required parameters", 400

    ### 1) Access dataset
    df = get_data(pID=pID, dID=dID)

    ### 2) Apply all conditionals
    conditioned_df = getConditionedDF(df, conditional)

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

    # b) Vs. (raw comparison)
    elif operation == 'vs':
        viz_result = {}
        # TODO Get viz_data
        df.index = df[group_a]
        df = df.drop(group_a, 1)
        final_dict = df.to_dict()

        for k, obj in final_dict.iteritems():
            viz_result[k] = [ { field_a: a, k: b } for a, b in obj.iteritems() ]

        table_result = conditioned_df.to_dict(orient='split')

    # c) Comparison
    elif operation == 'compare':
        # TODO Implement
        return

    ### 3) Incorporate query and format result
    # Viz Data: Dict of collections
    grouped_dict = grouped_df.to_dict()
    viz_result = {}
    
    for k, obj in grouped_dict.iteritems():
        collection = [ { field_a: a, k: b } for a, b in obj.iteritems() ]
        viz_result[k] = collection

    # Table Data: Dict of matrices
    grouped_df_copy = grouped_df
    # grouped_df_copy.insert(0, field_a, grouped_df_copy.index)

    table_result = {
      'columns': grouped_df_copy.columns.tolist(),
      'data': grouped_df_copy.values.tolist(),
    }

    return { 
        'viz_data': viz_result, 
        'table_result': table_result 
    }, 200
