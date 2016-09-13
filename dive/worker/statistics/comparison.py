import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
from time import time
from itertools import chain, combinations
from operator import add, mul
import time
from math import log10, floor

from scipy.stats import ttest_ind

from dive.base.db import db_access
from dive.base.data.access import get_data
from dive.base.serialization import make_serializable
from dive.worker.ingestion.utilities import get_unique
from dive.worker.statistics.utilities import variations_equal, sets_normal

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def create_one_dimensional_contingency_table_from_spec(spec, project_id):
    comparison_variable = spec.get('comparisonVariable')
    dataset_id = spec.get("datasetId")
    dep_variable = spec.get("dependentVariable", [])

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    comparison_result = create_one_dimensional_contingency_table(df, comparison_variable, dep_variable)
    return comparison_result, 200


def create_contingency_table_from_spec(spec, project_id):
    comparison_variables = spec.get("comparisonVariables")
    dataset_id = spec.get("datasetId")
    dep_variable = spec.get("dependentVariable", [])

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    comparison_result = create_contingency_table(df, comparison_variables, dep_variable)
    return comparison_result, 200

def get_variable_summary_statistics_from_spec(spec, project_id):
    summary_statistics_result = {}
    dataset_id = spec.get("datasetId")
    field_ids = spec.get("fieldIds")
    field_properties = db_access.get_field_properties(project_id, dataset_id)

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    field_ids = set(field_ids)
    relevant_field_properties = filter(lambda field: field['id'] in field_ids, field_properties)
    summary_statistics_result = get_variable_summary_statistics(df, relevant_field_properties)
    return summary_statistics_result, 200


def run_numerical_comparison_from_spec(spec, project_id):
    comparison_result = {}

    variable_names = spec.get('variableNames', [])
    independence = spec.get('independence', True)
    dataset_id = spec.get('datasetId')
    if not (len(variable_names) >= 2 and dataset_id):
        return 'Not passed required parameters', 400

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    comparison_result['tests'] = run_valid_comparison_tests(df, variable_names, independence)
    return comparison_result, 200


def run_correlation_from_spec(spec, project_id):
    dataset_id = spec.get("datasetId")
    correlation_variables = spec.get("correlationVariables")
    correlation_result = run_correlation(df, correlation_variables)
    return correlation_result, 200

def run_correlation(df, correlation_variables):
    '''
    Runs correlations between pairs of correlation_variables
    df: the dataframe
    correlation_variables: the numerical variables to do the correlation on. A list of names.
    '''
    correlation_result = {}
    correlation_result['headers'] = correlation_variables
    correlation_result['rows'] = []

    data_columns = []
    length = len(correlation_variables)

    for correlation_variable in correlation_variables:
        data_columns.append(df[correlation_variable])

    for row in range(length):
        row_data = []
        for col in range(length):
            if row > col:
                row_data.append([None, None])
            else:
                row_data.append(stats.pearsonr(data_columns[row], data_columns[col]))
        correlation_result['rows'].append({'field': correlation_variables[row], 'data': row_data})

    return correlation_result


def get_variable_summary_statistics(df, relevant_field_properties):
    '''
    Returns the formatted dict that is sent through the endpoint.
    df: the dataframe
    relevant_field_properties: the field properties we are trying to create summary statistics for
    '''
    result_dict = {}
    result_dict['items'] = []

    categorical_headers = ['count', 'max frequency', 'unique values']
    numerical_headers = ['count', 'max', 'min', 'mean', 'median', 'standard deviation']

    for field_property in relevant_field_properties:
        stats = field_property['stats']
        name = field_property['name']
        type = field_property['general_type']
        data_column = df[name]
        if  type == 'c':
            result_dict['items'].append({'type': type, 'field':name, 'stats': get_summary_stats_categorical(data_column, stats), 'viz_data': return_data_list_categorical(data_column, name)})
        elif field_property['general_type'] == 'q':
            result_dict['items'].append({'type': type, 'field':name, 'stats': get_summary_stats_numerical(data_column, stats), 'viz_data': return_data_list_numerical(data_column, name)})

    result_dict['categoricalHeaders'] = categorical_headers
    result_dict['numericalHeaders'] = numerical_headers
    return result_dict


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


def return_data_list_numerical(data_column, variable_name):
    '''
    helper function to return visualization data in the right format for numerical variables
    FOR NOW, ONLY BINS INTO 5 DIFFERENT BINS
    data_column: represents the array of data
    variable_name: represents the name of the variable that is being visualized
    '''
    count_dict = {}
    data_array = []

    (names, roundedEdges) = find_binning_edges_equal_spaced(data_column, 5)
    data_array.append([variable_name, 'count'])
    for ele in data_column:
        bin_name = find_bin(ele, roundedEdges, names, 5)
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


def get_summary_stats_categorical(data_column, stats_dict):
    '''
    helper function to find some statistics of the data
        looks at count, max frequency, and number of unique values
    data_column: represents the array of data
    stats_dict: represents the statistical dictionary already in field_properties
    '''
    stats = []

    if stats_dict.get('count'):
        stats.append(stats_dict['count'])
    else:
        stats.append(len(data_column))

    if stats_dict.get('freq'):
        stats.append(stats_dict['freq'])
    else:
        stats.append(find_unique_values_and_max_frequency(data_column)[1])

    if stats_dict.get('unique'):
        stats.append(stats_dict['unique'])
    else:
        stats.append(find_unique_values_and_max_frequency(data_column)[0])
    return stats


def get_summary_stats_numerical(data_column, stats_dict):
    '''
    helper function to find some statistics of the data
        looks at count, max, min, mean, median, and standard deviation
    data_column: represents the array of data
    stats_dict: represents the statistical dictionary already in field_properties
    '''
    stats = []

    if stats_dict.get('count'):
        stats.append(stats_dict['count'])
    else:
        stats.append(len(data_column))

    if stats_dict.get('max'):
        stats.append(stats_dict['max'])
    else:
        stats.append(max(data_column))

    if stats_dict.get('min'):
        stats.append(stats_dict['min'])
    else:
        stats.append(min(data_column))

    if stats_dict.get('mean'):
        stats.append(stats_dict['mean'])
    else:
        stats.append(np.mean(data_column))

    if stats_dict.get('median'):
        stats.append(stats_dict['median'])
    else:
        stats.append(np.median(data_column))

    if stats_dict.get('std'):
        stats.append(stats_dict['std'])
    else:
        stats.append(np.std(data_column))

    return stats


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

def parse_string_mapping_function(list_function):
    if list_function[0] == "FILTER":
        return (lambda x: x == list_function[1])


def parse_variable(num, index, variable_type_summary, df):
    '''
    helper function to return the appropriate independent variable value from the dataframe
    num: 0 represents parsing the column, 1 represents parsing the row
    index: represents the index of the dataframe we are extracting the value from
    variable_type_summary:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    df : dataframe
    '''
    type_variable = variable_type_summary[num][0]
    passed_variable = variable_type_summary[num][1]

    if type_variable == 'cat':
        return df.get_value(index, passed_variable)
    elif type_variable == 'num':
        binning_edges = variable_type_summary[num][2]
        binning_names = variable_type_summary[num][3]
        return find_bin(df.get_value(index, passed_variable[0]), binning_edges, binning_names, passed_variable[1])


def create_one_dimensional_contingency_table_with_dependent_variable(df, variable_type_summary, dep_variable, unique_indep_values):
    '''
    df : dataframe
    variable_type_summary:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    dep_variable :
        for cat variable: [type, numerical variable name, aggregation function name, filter function name]
        for num variable: [type, numerical variable name, aggregation function name]
    unique_indep_values : [unique values for the one variable]
    '''
    result_dict = {}
    dep_var_dict = {}
    type_string = dep_variable[0]
    dep_variable_name = dep_variable[1]
    aggregation_function_name = dep_variable[2][0]
    aggregationMean = aggregation_function_name == 'MEAN'
    weight_variable_name = dep_variable[2][1]
    weight_dict = {}

    for index in df.index:
        var = parse_variable(0, index, variable_type_summary, df)
        if dep_var_dict.get(var):
            dep_var_dict[var].append(df.get_value(index, dep_variable_name))
            if weight_variable_name != 'UNIFORM':
                weight_dict[var].append(df.get_value(index, weight_variable_name))

        else:
            dep_var_dict[var] = [df.get_value(index, dep_variable_name)]
            weight_dict[var] = None
            if weight_variable_name != 'UNIFORM':
                weight_dict[var] = [df.get_value(index, weight_variable_name)]

    if type_string == 'q':
        for var in unique_indep_values:
            if dep_var_dict.get(var):
                result_dict[var] = parse_aggregation_function(aggregation_function_name, weight_dict[var])(dep_var_dict[var])

            else:
                result_dict[var] = 0
    else:
        mapping_function_name = dep_variable[3]
        for var in unique_indep_values:
            if dep_var_dict.get(var):
                result_dict[var] = parse_aggregation_function(aggregation_function_name, weight_dict[var])(map(parse_string_mapping_function(mapping_function_name),dep_var_dict[row][col]))
            else:
                result_dict[var] = 0

    return (result_dict, aggregationMean)


def create_one_dimensional_contingency_table_with_no_dependent_variable(df, variable_type_summary, unique_indep_values):
    '''
    df : dataframe
    variable_type_summary:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    unique_indep_values : [unique values for the one variable]
    '''
    result_dict = {}
    count_dict = {}

    for index in df.index:
        var = parse_variable(0, index, variable_type_summary, df)
        if count_dict.get(var):
            count_dict[var]+= 1
        else:
            count_dict[var] = 1

    for var in unique_indep_values:
        if count_dict.get(var):
            result_dict[var] = count_dict[var]
        else:
            result_dict[var] = 0

    return result_dict


def create_one_dimensional_contingency_table(df, comparison_variable, dep_variable):
    '''
    comparison_variable: represents the variable used to create the contingency table.
    Is either an independent_variable or categorical_variable
        independent_variable : represents an independent numerical variable. It is of form [numerical variable name, number of bins]
        categorical_variable: represents an independent categorical variable name. It is a string
    dep_variable :
        for cat variable: [type, numerical variable name, aggregation function name, filter function name]
        for num variable: [type, numerical variable name, aggregation function name]

    supported mapping functions:
        (FILTER, target) -> returns 1 if value == target, 0 otherwise
    supported aggregation functions:
        SUM, MEAN
    '''
    #a list of lists
    results_dict = {}
    formatted_results_dict = {}
    unique_indep_values = []
    variable_type_summary = []

    aggregationMean = False

    if comparison_variable[0] == 'c':
        unique_indep_values = get_unique(df[comparison_variable[1]], True)
        variable_type_summary.append(('cat', comparison_variable[1]))
    elif comparison_variable[0] == 'q':
        (names, binningEdges) = find_binning_edges_equal_spaced(df[comparison_variable[1]], comparison_variable[2])
        unique_indep_values = names
        variable_type_summary.append(('num', [comparison_variable[1], comparison_variable[2]], binningEdges, names))

    if dep_variable:
        (results_dict, aggregationMean) = create_one_dimensional_contingency_table_with_dependent_variable(df, variable_type_summary, dep_variable, unique_indep_values)

    else:
        results_dict = create_one_dimensional_contingency_table_with_no_dependent_variable(df, variable_type_summary, unique_indep_values)


    formatted_results_dict["column_headers"] = ["VARIABLE", "AGGREGATION"]
    formatted_results_dict["row_headers"] = unique_indep_values
    formatted_results_dict["rows"] = []

    if not aggregationMean:
        formatted_results_dict['column_total'] = 0

    for var in unique_indep_values:
        value = results_dict[var]

        if not aggregationMean:
            formatted_results_dict['column_total'] += value

        formatted_results_dict["rows"].append({
            "field": var,
            "value": value
        })

    return formatted_results_dict


def create_contingency_table_with_dependent_variable(df, variable_type_summary, dep_variable, unique_indep_values):
    '''
    df : dataframe
    variable_type_summary:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    dep_variable :
        for cat variable: [type, numerical variable name, aggregation function name, filter function name]
        for num variable: [type, numerical variable name, aggregation function name]
    '''
    result_dict = {}
    dep_var_dict = {}
    dep_variable_type = dep_variable[0]
    dep_variable_name = dep_variable[1]
    aggregation_function_name = dep_variable[2][0]
    aggregationMean = aggregation_function_name == 'MEAN'
    weight_variable_name = dep_variable[2][1]
    weight_dict = {}

    for index in df.index:
        col = parse_variable(0, index, variable_type_summary, df)
        row = parse_variable(1, index, variable_type_summary, df)
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

    if dep_variable_type == 'q':
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
                        result_dict[row][col] = parse_aggregation_function(aggregation_function_name, weight_dict[row][col])(map(parse_string_mapping_function(mapping_function_name),dep_var_dict[row][col]))

                    else:
                        result_dict[row][col] = 0
            else:
                for col in unique_indep_values[0]:
                    result_dict[row][col] = 0

    return (result_dict, aggregationMean)


def create_contingency_table_with_no_dependent_variable(df, variable_type_summary, unique_indep_values):
    '''
    df : dataframe
    variable_type_summary:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    unique_indep_values : [[unique values for columns], [unique values for rows]]
    '''
    result_dict = {}
    count_dict = {}

    for index in df.index:
        col = parse_variable(0, index, variable_type_summary, df)
        row = parse_variable(1, index, variable_type_summary, df)
        logger.info('%s %s %s', index, col, row)
        if count_dict.get(row):
            if count_dict[row].get(col):
                count_dict[row][col]+= 1

            else:
                count_dict[row][col] = 1

        else:
            count_dict[row] = {}
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


def create_contingency_table(df, comparison_variables, dep_variable):
    '''
    comparison_variables: represents the variables used to create the contingency table.
    Is a list of independent_variable and categorical_variable
        independent_variable : represents an independent numerical variable. It is of form [numerical variable name, number of bins]
        categorical_variable: represents an independent categorical variable name. It is a string
    dep_variable :
        for cat variable: [type, numerical variable name, aggregation function name, filter function name]
        for num variable: [type, numerical variable name, aggregation function name]

    supported mapping functions:
        (FILTER, target) -> returns 1 if value == target, 0 otherwise
    supported aggregation functions:
        SUM, MEAN
    '''
    #a list of lists
    results_dict = {}
    formatted_results_dict = {}
    unique_indep_values = []
    variable_type_summary = []

    aggregationMean = False

    for var in comparison_variables:
        if var[0] == 'c':
            unique_indep_values.append(get_unique(df[var[1]], True))
            variable_type_summary.append(('cat', var[1]))
        elif var[0] == 'q':
            (names, binningEdges) = find_binning_edges_equal_spaced(df[var[1]], var[2])
            unique_indep_values.append(names)
            variable_type_summary.append(('num', [var[1], var[2]], binningEdges, names))

    if dep_variable:
        (results_dict, aggregationMean) = create_contingency_table_with_dependent_variable(df, variable_type_summary, dep_variable, unique_indep_values)
    else:
        results_dict = create_contingency_table_with_no_dependent_variable(df, variable_type_summary, unique_indep_values)

    if not aggregationMean:
        formatted_results_dict["column_headers"] = unique_indep_values[0] + ['Row Totals']
    else:
        formatted_results_dict['column_headers'] = unique_indep_values[0]
    formatted_results_dict["row_headers"] = unique_indep_values[1]
    formatted_results_dict["rows"] = []
    if not aggregationMean:
        formatted_results_dict['column_totals'] = np.zeros(len(unique_indep_values[0]) + 1)

    for row in unique_indep_values[1]:
        values = [ results_dict[row][col] for col in unique_indep_values[0] ]

        if not aggregationMean:
            values.append(sum(values))
            formatted_results_dict['column_totals'] += values

        formatted_results_dict["rows"].append({
            "field": row,
            "values": values
        })

    return formatted_results_dict


def find_binning_edges_equal_spaced(array, num_bins):
    '''
    helper function to get the formatted names and edges of the bins.
    The bins will be equally spaced, and rounded to 1 decimal place. The right edge is open.
    array: represents the array being binning_edges
    num_bins: represents how many bins we want to bin the data
    '''
    theMin = min(array)
    theMax = max(array)

    edges = np.linspace(theMin, theMax, num_bins+1)

    roundedEdges = []
    for i in range(len(edges)-1):
        roundedEdges.append( float('%.1f' % edges[i]))
    roundedEdges.append(float('%.1f' % edges[-1])+0.1)

    names = []
    for i in range(len(edges)-1):
        names.append('%s-%s' % (str(roundedEdges[i]), str(roundedEdges[i+1])))

    return (names, roundedEdges)


def find_bin(target, binningEdges, binningNames, num_bins):
    '''
    helper function to find the name of the bin the target is in
    target: the number which we are trying to find the right bin
    binningEdges: an array of floats representing the edges of the bins
    binningNames: an array of strings representing the names of hte bins
    num_bins: a number represents how many bins there are
    '''
    def searchIndex(nums, target, length, index):
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
    return binningNames[searchIndex(binningEdges, target, num_bins, 0)-1]


# args must be a list of lists
def run_valid_comparison_tests(df, variable_names, independence):
    '''
    Run non-regression tests
    Performs comparisons between different data sets
    Requires more than one data set to be sent
    '''
    args = []
    for name in variable_names:
        args.append(df[name])

    results = []
    normal = sets_normal(.25, *args)
    numDataSets = len(args)
    variations_equal = variations_equal(.25, *args)

    # Assuming independence
    valid_tests = get_valid_tests(variations_equal, True, normal, numDataSets)
    results = [ {
        'test': test,
        'values': valid_tests[test](*args)
    } for test in valid_tests]

    return results

def run_comparison_from_spec(spec, project_id):
    # 1) Parse and validate arguments
    indep = spec.get('indep', [])
    dep = spec.get('dep', [])
    dataset_id = spec.get('dataset_id')
    test = spec.get('test', 'ttest')
    if not (dataset_id and dep):
        return 'Not passed required parameters', 400

    fields = db_access.get_field_properties(project_id, dataset_id)

    # 2) Access dataset
    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    # 3) Run test based on parameters and arguments
    comparison_result = run_comparison(df, fields, indep, dep, test)
    return {
        'data': comparison_result
    }, 200

def run_comparison(df, fields, indep, dep, test):
    indep_data = {}
    if indep:
        for indep_field_name in indep:
            indep_data[indep_field_name] = df[indep_field_name]

    else:
        for field in fields:
            field_name = field['name']
            if (field_name is not dep_field_name) and (field['general_type'] == 'q'):
                indep_data[field_name] = df[field_name]

    dep_data = {}
    for dep_field_name in dep:
        dep_data[dep_field_name] = df[dep_field_name]

    if test is 'ttest':
        return ttest(df, fields, indep, dep)

def ttest(df, fields, indep, dep):
    # Ensure single field
    dep_field_name = dep[0]
    indep_field_name = indep[0]
    unique_indep_values = get_unique(df[indep_field_name])

    subsets = {}
    for v in unique_indep_values:
        subsets[v] = np.array(df[df[indep_field_name] == v][dep_field_name])

    result = {}
    for (x, y) in combinations(unique_indep_values, 2):
        (statistic, pvalue) = ttest_ind(subsets[x], subsets[y])
        result[str([x, y])] = {
            'statistic': statistic,
            'pvalue': pvalue
        }

    return result



##################
#Functions to determine which tests could be run
##################
def get_valid_tests(equal_var, independent, normal, num_samples):
    '''
    Get valid tests given number of samples and statistical characterization of
    samples:

    Equal variance
    Indepenence
    Normality
    '''
    if num_samples == 1:
        valid_tests = {
            'chisquare': stats.chisquare,
            'power_divergence': stats.power_divergence,
            'kstest': stats.kstest
        }
        if normal:
            valid_tests['input']['one_sample_ttest'] = stats.ttest_1samp

    elif num_samples == 2:
        if independent:
            valid_tests = {
                'mannwhitneyu': stats.mannwhitneyu,
                'kruskal': stats.kruskal,
                'ks_2samp': stats.ks_2samp
            }
            if normal:
                valid_tests['two_sample_ttest'] = stats.ttest_ind
                if equal_var:
                    valid_tests['f_oneway'] = stats.f_oneway
        else:
            valid_tests = {
                'two_sample_ks': stats.ks_2samp,
                'wilcoxon': stats.wilcoxon
            }
            if normal:
                valid_tests['two_sample_related_ttest'] = stats.ttest_rel

    elif num_samples >= 3:
        if independent:
            valid_tests = {
                'kruskal': stats.kruskal
            }
            if normal and equal_var:
                valid_tests['f_oneway'] = stats.f_oneway

        else:
            valid_tests['friedmanchisquare'] = stats.friedmanchisquare

    return valid_tests
