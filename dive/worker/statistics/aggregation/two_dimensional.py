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


def create_contingency_table(df, aggregation_variables, dep_variable, config={}):
    results_dict = {}
    formatted_results_dict = {}
    unique_indep_values = []
    bin_data = {}
    aggregation_mean = False

    for i, variable in enumerate(aggregation_variables):
        binningConfigKey = 'binningConfigX' if (i == 0) else 'binningConfigY'
        name = variable['name']
        general_type = variable['general_type']
        scale = variable['scale'] 

        if scale in [ Scale.NOMINAL.value, Scale.ORDINAL.value ]:
            unique_indep_values.append(get_unique(df[name], True))

        elif scale in [ Scale.CONTINUOUS.value ]:
            values = df[name].dropna(how='any')
            (binning_edges, bin_names) = get_binning_edges_and_names(values, config[binningConfigKey])
            num_bins = len(binning_edges) - 1
            unique_indep_values.append(bin_names)
            bin_data[name] = {
                'num_bins': num_bins,
                'binning_edges': binning_edges,
                'bin_names': bin_names
            }            

    if dep_variable:
        (results_dict, aggregation_mean) = create_contingency_table_with_dependent_variable(df, aggregation_variables, dep_variable, unique_indep_values, config=config, bin_data=bin_data)
    else:
        results_dict = create_contingency_table_with_no_dependent_variable(df, aggregation_variables, unique_indep_values, config=config, bin_data=bin_data)

    if not aggregation_mean:
        formatted_results_dict["column_headers"] = unique_indep_values[0] + ['Row Totals']
        column_totals = np.zeros(len(unique_indep_values[0]) + 1)
    else:
        formatted_results_dict['column_headers'] = unique_indep_values[0]

    formatted_results_dict["row_headers"] = unique_indep_values[1]
    formatted_results_dict["rows"] = []

    for row in unique_indep_values[1]:
        values = [ results_dict[row][col] for col in unique_indep_values[0] ]

        if not aggregation_mean:
            values.append(sum(values))
            column_totals += values

        formatted_results_dict["rows"].append({
            "field": row,
            "values": values
        })

    if not aggregation_mean:
        formatted_results_dict['column_totals'] = list(column_totals)
    return formatted_results_dict


def create_contingency_table_with_no_dependent_variable(df, variables, unique_indep_values, config={}, bin_data={}):
    result_dict = {}
    count_dict = defaultdict(int)

    (col_variable, row_variable) = variables
    for index in df.index:
        col = parse_variable(index, col_variable, df, bin_data=bin_data.get(col_variable['name']))
        row = parse_variable(index, row_variable, df, bin_data=bin_data.get(row_variable['name']))

        if row in count_dict:
            count_dict[row][col] += 1
        else:
            count_dict[row] = defaultdict(int)
            count_dict[row][col] = 1

    for row in unique_indep_values[1]:
        result_dict[row] = {}
        if count_dict.get(row):
            for col in unique_indep_values[0]:
                if count_dict[row].get(col):
                    result_dict[row][col] = count_dict[row][col]
                else:
                    result_dict[row][col] = 0
        else:
            for col in unique_indep_values[0]:
                result_dict[row][col] = 0

    return result_dict    


def create_contingency_table_with_dependent_variable(df, variables, dep_variable, unique_indep_values, config={}, bin_data={}):
    '''
    df : dataframe
    variable_type_aggregation:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    dep_variable :
        for cat variable: [type, numerical variable name, aggregation function name, filter function name]
        for num variable: [type, numerical variable name, aggregation function name]
    '''
    result_dict = {}
    dep_var_dict = {}

    dep_variable_general_type = dep_variable['general_type']
    dep_variable_scale = dep_variable['scale']
    dep_variable_name = dep_variable['name']

    aggregation_function_name = config.get('aggregationFunction', 'MEAN')
    aggregation_mean = (aggregation_function_name == 'MEAN')  # TODO Define in constants file
    weight_variable_name = config.get('weightVariableName', 'UNIFORM')
    weight_dict = {}

    (col_variable, row_variable) = variables

    df = df.dropna(how='any', subset=[dep_variable_name, col_variable['name'], row_variable['name']])
    
    for index in df.index:
        col = parse_variable(index, col_variable, df, bin_data=bin_data.get(col_variable['name']))
        row = parse_variable(index, row_variable, df, bin_data=bin_data.get(row_variable['name']))
        if dep_var_dict.get(row):
            if dep_var_dict[row].get(col):
                dep_var_dict[row][col].append(df.get_value(index, dep_variable_name))
                if weight_variable_name != 'UNIFORM':
                    weight_dict[row][col].append(df.get_value(index, weight_variable_name))

            else:
                dep_var_dict[row][col] = [df.get_value(index, dep_variable_name)]
                weight_dict[row][col] = None
                if weight_variable_name != 'UNIFORM':
                    weight_dict[row][col] = [df.get_value(index, weight_variable_name)]
        else:
            dep_var_dict[row] = {}
            dep_var_dict[row][col] = [df.get_value(index, dep_variable_name)]
            weight_dict[row] = {}
            weight_dict[row][col] = None
            if weight_variable_name != 'UNIFORM':
                weight_dict[row][col] = [df.get_value(index, weight_variable_name)]

    if dep_variable_scale in [ Scale.ORDINAL.value, Scale.CONTINUOUS.value ]:
        for row in unique_indep_values[1]:
            result_dict[row] = {}
            if dep_var_dict.get(row):
                for col in unique_indep_values[0]:
                    if dep_var_dict[row].get(col) != None:
                        result_dict[row][col] = parse_aggregation_function(aggregation_function_name, weight_dict[row][col])(dep_var_dict[row][col])
                    else:
                        result_dict[row][col] = 0

            else:
                for col in unique_indep_values[0]:
                    result_dict[row][col] = 0
    else:
        for row in unique_indep_values[1]:
            result_dict[row] = {}
            if dep_var_dict.get(col):
                for col in unique_indep_values[0]:
                    if dep_var_dict[row].get(col) != None:
                        result_dict[row][col] = parse_aggregation_function(aggregation_function_name, weight_dict[row][col])(map(parse_string_mapping_function(mapping_function_name), dep_var_dict[row][col]))

                    else:
                        result_dict[row][col] = 0
            else:
                for col in unique_indep_values[0]:
                    result_dict[row][col] = 0

    return (result_dict, aggregation_mean)