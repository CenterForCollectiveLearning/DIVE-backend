import pandas as pd
import numpy as np
from collections import defaultdict

from dive.base.db import db_access
from dive.base.data.access import get_data, get_conditioned_data
from dive.worker.core import task_app
from dive.worker.ingestion.utilities import get_unique
from dive.worker.ingestion.binning import get_num_bins

from celery.utils.log import get_task_logger
from dive.base.constants import GeneralDataType as GDT, aggregation_functions, Scale
from dive.worker.ingestion.binning import get_bin_edges, get_bin_decimals
from dive.worker.statistics.aggregation.helpers import *

logger = get_task_logger(__name__)


def create_one_dimensional_contingency_table(df, aggregation_variable, dep_variable, config={}):
    results_dict = {}
    formatted_results_dict = {}
    unique_indep_values = []

    aggregation_mean = False

    general_type = aggregation_variable['general_type']
    scale = aggregation_variable['scale']    
    name = aggregation_variable['name']
    bin_data = {}

    if scale == Scale.NOMINAL.value:
        unique_indep_values = get_unique(df[name], True)

    elif scale in [ Scale.ORDINAL.value, Scale.CONTINUOUS.value ]:
        values = df[name].dropna(how='any')
        (binning_edges, bin_names) = get_binning_edges_and_names(values, config.get('binningConfigX'))  # TODO Update binning function
        num_bins = len(binning_edges) -1
        bin_data = {
            'num_bins': num_bins,
            'binning_edges': binning_edges,
            'bin_names': bin_names
        }
        unique_indep_values = bin_names

    if dep_variable:
        (results_dict, aggregation_mean) = create_one_dimensional_contingency_table_with_dependent_variable(df, aggregation_variable, dep_variable, unique_indep_values, config=config, bin_data=bin_data)
    else:
        results_dict = create_one_dimensional_contingency_table_with_no_dependent_variable(df, aggregation_variable, unique_indep_values, config=config, bin_data=bin_data)

    formatted_results_dict["column_headers"] = ["VARIABLE", "AGGREGATION"]
    formatted_results_dict["row_headers"] = unique_indep_values
    formatted_results_dict["rows"] = []

    if not aggregation_mean:
        formatted_results_dict['column_total'] = 0

    for var in unique_indep_values:
        value = results_dict[var]

        if not aggregation_mean:
            formatted_results_dict['column_total'] += value

        formatted_results_dict["rows"].append({
            "field": var,
            "value": value
        })

    return formatted_results_dict


def create_one_dimensional_contingency_table_with_no_dependent_variable(df, variable, unique_indep_values, config={} ,bin_data={}):
    count_dict = defaultdict(int)

    # TODO Redo this. Should not have to iterate.
    for index in df.index:
        unique_value = parse_variable(index, variable, df, bin_data=bin_data)
        count_dict[unique_value] += 1

    return count_dict


def create_one_dimensional_contingency_table_with_dependent_variable(df, variable, dep_variable, unique_indep_values, config={}, bin_data={}):
    '''
    df : dataframe
    variable_type_aggregation:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    dep_variable :
        for cat variable: [type, numerical variable name, aggregation function name, filter function name]
        for num variable: [type, numerical variable name, aggregation function name]
    unique_indep_values : [unique values for the one variable]
    '''
    result_dict = {}
    dep_var_dict = {}
    dep_variable_general_type = dep_variable['general_type']
    dep_variable_scale = dep_variable['scale']
    dep_variable_name = dep_variable['name']

    aggregation_function_name = config.get('aggregationFunction', 'MEAN')
    aggregation_mean = (aggregation_function_name == 'MEAN')
    weight_variable_name = config.get('weightVariableName', 'UNIFORM')
    weight_dict = {}

    for index in df.index:
        unique_value = parse_variable(index, variable, df, bin_data=bin_data)
        if dep_var_dict.get(unique_value):
            dep_var_dict[unique_value].append(df.get_value(index, dep_variable_name))
            if weight_variable_name != 'UNIFORM':
                weight_dict[unique_value].append(df.get_value(index, weight_variable_name))

        else:
            dep_var_dict[unique_value] = [ df.get_value(index, dep_variable_name) ]
            weight_dict[unique_value] = None
            if weight_variable_name != 'UNIFORM':
                weight_dict[unique_value] = [df.get_value(index, weight_variable_name)]

    if dep_variable_scale in [ Scale.ORDINAL.value, Scale.CONTINUOUS.value ]:
        for unique_value in unique_indep_values:
            if dep_var_dict.get(unique_value):
                result_dict[unique_value] = parse_aggregation_function(aggregation_function_name, weight_dict[unique_value])(dep_var_dict[unique_value])
            else:
                result_dict[unique_value] = 0
    else:
        # TODO What is this???
        mapping_function_name = dep_variable[3]
        for unique_value in unique_indep_values:
            if dep_var_dict.get(unique_value):
                result_dict[unique_value] = parse_aggregation_function(aggregation_function_name, weight_dict[unique_value])(map(parse_string_mapping_function(mapping_function_name),dep_var_dict[row][col]))
            else:
                result_dict[unique_value] = 0

    return (result_dict, aggregation_mean)
