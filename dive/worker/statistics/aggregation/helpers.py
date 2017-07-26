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

logger = get_task_logger(__name__)

def get_binning_edges_and_names(array, config):
    procedure = config.get('binning_procedure', 'freedman')
    binning_type = config.get('binning_type', 'procedural')
    procedural = (binning_type == 'procedural')
    precision = config.get('precision', get_bin_decimals(array))
    num_bins = config.get('num_bins', 3)

    if procedural:
        num_bins = get_num_bins(array, procedure=procedure)
    bin_edges = bin_edges_list = get_bin_edges(
        array,
        num_bins,
        num_decimals=precision
    )

    names = []
    formatting_string = '%.' + str(precision) + 'f-%.' + str(precision) + 'f'
    for i in range(len(bin_edges)-1):
        names.append(formatting_string % (bin_edges[i], bin_edges[i+1]))

    return (bin_edges, names)


def find_bin(target, binningEdges, binningNames, num_bins):
    '''
    helper function to find the name of the bin the target is in
    target: the number which we are trying to find the right bin
    binningEdges: an array of floats representing the edges of the bins
    binningNames: an array of strings representing the names of hte bins
    num_bins: a number represents how many bins there are
    '''
    def searchIndex(nums, target, length, index):
        length = int(length)
        mid = length/2
        if length == 1:
            if target <= nums[0]:
                return index

            else:
                return index + 1

        elif target < nums[mid]:
            return searchIndex(nums[:mid], target, mid, index)

        else:
            return searchIndex(nums[mid:], target, length-mid, index+mid)

    #subtraction of 1 since indexing starts at 0
    return binningNames[searchIndex(binningEdges, target, num_bins, 0) - 1]


def parse_string_mapping_function(list_function):
    if list_function[0] == "FILTER":
        return (lambda x: x == list_function[1])


def parse_variable(index, variable, df, bin_data={}):
    scale = variable['scale']
    name = variable['name']

    if scale in [  Scale.ORDINAL.value, Scale.NOMINAL.value ] :
        return df.get_value(index, name)
    elif scale in [ Scale.CONTINUOUS.value ] :
        binning_edges = bin_data['binning_edges']
        bin_names = bin_data['bin_names']
        num_bins = bin_data['num_bins']
        return find_bin(df.get_value(index, name), binning_edges, bin_names, num_bins)


def find_unique_values_and_max_frequency(list):
    '''
    helper function to find the number of unique values in the list and the maximum
    frequency that an unique value has
    list: represents the list being analyzed
    '''
    seen = {}
    max = 0
    for val in list:
        if seen.get(val):
            seen[val] += 1
            if seen[val] > max:
                max = seen[val]
        else:
            seen[val] = 1
    return (len(seen), max)


def parse_aggregation_function(string_function, list_weights):
    if string_function == "SUM":
        return np.sum
    if string_function == 'MEAN':
        if not list_weights:
            return np.mean
        else:
            def weight_sum(list):
                sum = 0
                counter = 0.0
                for index in range(len(list)):
                    sum += list[index]*list_weights[index]
                    counter += list_weights[index]
                return sum/counter
            return weight_sum


def return_data_list_numerical(data_column, variable_name):
    '''
    helper function to return visualization data in the right format for numerical variables
    FOR NOW, ONLY BINS INTO 5 DIFFERENT BINS
    data_column: represents the array of data
    variable_name: represents the name of the variable that is being visualized
    '''
    count_dict = {}
    data_array = []

    (rounded_edges, names) = get_binning_edges_and_names(data_column, {})
    data_array.append([variable_name, 'count'])
    for ele in data_column:
        bin_name = find_bin(ele, rounded_edges, names, len(rounded_edges) - 1)
        if count_dict.get(bin_name):
            count_dict[bin_name] += 1
        else:
            count_dict[bin_name] = 1

    for name in names:
        count = 0
        if count_dict.get(name):
            count = count_dict[name]
        data_array.append([name, count])

    return data_array


def get_aggregation_stats_categorical(data_column, stats_dict):
    '''
    helper function to find some statistics of the data
        looks at count, max frequency, and number of unique values
    data_column: represents the array of data
    stats_dict: represents the statistical dictionary already in field_properties
    '''
    stats = []

    if stats_dict.get('count'):
        stats.append(stats_dict.get('count', len(data_column)))

    if stats_dict.get('freq'):
        stats.append(stats_dict.get('freq', find_unique_values_and_max_frequency(data_column)[1]))

    if stats_dict.get('unique'):
        stats.append(stats_dict.get('unique', find_unique_values_and_max_frequency(data_column)[0]))

    return stats


def get_aggregation_stats_numerical(data_column, stats_dict):
    '''
    helper function to find some statistics of the data
        looks at count, max, min, mean, median, and standard deviation
    data_column: represents the array of data
    stats_dict: represents the statistical dictionary already in field_properties
    '''
    stats = []
    for aggregation_function_name, aggregation_function in aggregation_functions.iteritems():
        if aggregation_function_name in stats_dict:
            stats.append(stats_dict.get(aggregation_function_name, aggregation_function(data_column)))        
    return stats


def return_data_list_categorical(data_column, variable_name):
    '''
    helper function to return visualization data in the right format for categorical variables
    data_column: represents the array of data
    variable_name: represents the name of the variable that is being visualized
    '''
    unique_elements = get_unique(data_column)

    count_dict = {}
    data_array = []

    data_array.append([variable_name, 'count'])

    for ele in data_column:
        if count_dict.get(ele):
            count_dict[ele] += 1
        else:
            count_dict[ele] = 1

    for name in unique_elements:
        data_array.append([name, count_dict[name]])

    return data_array