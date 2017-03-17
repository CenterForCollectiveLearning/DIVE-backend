import pandas as pd
import numpy as np

from dive.base.db import db_access
from dive.base.data.access import get_data, get_conditioned_data
from dive.worker.core import task_app
from dive.worker.ingestion.utilities import get_unique
from dive.worker.ingestion.binning import get_num_bins

from celery.utils.log import get_task_logger
from dive.base.constants import GeneralDataType as GDT
from dive.worker.ingestion.binning import get_bin_edges, get_bin_decimals

logger = get_task_logger(__name__)


def create_one_dimensional_contingency_table_from_spec(spec, project_id, conditionals=[]):
    aggregation_variable = spec.get('aggregationVariable')
    dataset_id = spec.get("datasetId")
    dep_variable = spec.get("dependentVariable", [])

    fields = []
    if dep_variable:
        fields.append(dep_variable[1])
    fields.append(aggregation_variable[1])

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = get_conditioned_data(project_id, dataset_id, df, conditionals)

    df_subset = df[ fields ]
    df_ready = df_subset.dropna(how='any')  # Remove unclean

    aggregation_result = create_one_dimensional_contingency_table(df_ready, aggregation_variable, dep_variable)
    return aggregation_result, 200


def create_contingency_table_from_spec(spec, project_id, conditionals=[]):
    aggregation_variables = spec.get("aggregationVariables")
    aggregation_variables_names = [ av[1] for av in aggregation_variables ]
    dataset_id = spec.get("datasetId")
    dep_variable = spec.get("dependentVariable", [])

    fields = []
    if dep_variable:
        fields.append(dep_variable[1])
    fields = fields + aggregation_variables_names

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = get_conditioned_data(project_id, dataset_id, df, conditionals)
    df_subset = df[ fields ]
    df_ready = df_subset.dropna(how='any')  # Remove unclean

    aggregation_result = create_contingency_table(df_ready, aggregation_variables, dep_variable)
    return aggregation_result, 200

def run_aggregation_from_spec(spec, project_id, conditionals=[]):
    aggregation_statistics_result = {}
    dataset_id = spec.get("datasetId")
    field_ids = spec.get("fieldIds")

    field_properties = db_access.get_field_properties(project_id, dataset_id)
    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = get_conditioned_data(project_id, dataset_id, df, conditionals)
    df = df.dropna()  # Remove unclean

    field_ids = set(field_ids)
    relevant_field_properties = filter(lambda field: field['id'] in field_ids, field_properties)
    aggregation_statistics_result = get_variable_aggregation_statistics(df, relevant_field_properties)
    return aggregation_statistics_result, 200


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


def get_aggregation_stats_numerical(data_column, stats_dict):
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


def parse_variable(num, index, variable_type_aggregation, df):
    '''
    helper function to return the appropriate independent variable value from the dataframe
    num: 0 represents parsing the column, 1 represents parsing the row
    index: represents the index of the dataframe we are extracting the value from
    variable_type_aggregation:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    df : dataframe
    '''
    type_variable = variable_type_aggregation[num][0]
    passed_variable = variable_type_aggregation[num][1]

    if type_variable == 'cat':
        return df.get_value(index, passed_variable)
    elif type_variable == 'num':
        binning_edges = variable_type_aggregation[num][2]
        binning_names = variable_type_aggregation[num][3]
        return find_bin(df.get_value(index, passed_variable[0]), binning_edges, binning_names, passed_variable[1])


def create_one_dimensional_contingency_table_with_dependent_variable(df, variable_type_aggregation, dep_variable, unique_indep_values):
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
    type_string = dep_variable[0]
    dep_variable_name = dep_variable[1]
    aggregation_function_name = dep_variable[2][0]
    aggregationMean = aggregation_function_name == 'MEAN'
    weight_variable_name = dep_variable[2][1]
    weight_dict = {}

    for index in df.index:
        var = parse_variable(0, index, variable_type_aggregation, df)
        if dep_var_dict.get(var):
            dep_var_dict[var].append(df.get_value(index, dep_variable_name))
            if weight_variable_name != 'UNIFORM':
                weight_dict[var].append(df.get_value(index, weight_variable_name))

        else:
            dep_var_dict[var] = [df.get_value(index, dep_variable_name)]
            weight_dict[var] = None
            if weight_variable_name != 'UNIFORM':
                weight_dict[var] = [df.get_value(index, weight_variable_name)]

    if type_string in [ GDT.Q.value, GDT.T.value ]:
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


def create_one_dimensional_contingency_table_with_no_dependent_variable(df, variable_type_aggregation, unique_indep_values):
    '''
    df : dataframe
    variable_type_aggregation:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    unique_indep_values : [unique values for the one variable]
    '''
    result_dict = {}
    count_dict = {}

    for index in df.index:
        var = parse_variable(0, index, variable_type_aggregation, df)
        if count_dict.get(var):
            count_dict[var] += 1
        else:
            count_dict[var] = 1

    for var in unique_indep_values:
        if count_dict.get(var):
            result_dict[var] = count_dict[var]
        else:
            result_dict[var] = 0

    return result_dict


def create_one_dimensional_contingency_table(df, aggregation_variable, dep_variable):
    '''
    aggregation_variable: represents the variable used to create the contingency table.
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
    variable_type_aggregation = []

    aggregationMean = False

    if len(aggregation_variable) == 2:
        aggregation_variable_type, name = aggregation_variable
    else:
        aggregation_variable_type, name, aggregation_config = aggregation_variable


    if aggregation_variable_type == GDT.C.value:
        unique_indep_values = get_unique(df[name], True)
        variable_type_aggregation.append(('cat', name))

    elif aggregation_variable_type in [ GDT.Q.value, GDT.T.value ]:

        values = df[name].dropna(how='any')
        (binning_edges, names) = get_binning_edges_and_names(values, aggregation_config)
        num_bins = len(binning_edges) -1
        unique_indep_values = names
        variable_type_aggregation.append(('num', [name, num_bins], binning_edges, names))

    if dep_variable:
        (results_dict, aggregationMean) = create_one_dimensional_contingency_table_with_dependent_variable(df, variable_type_aggregation, dep_variable, unique_indep_values)
    else:
        results_dict = create_one_dimensional_contingency_table_with_no_dependent_variable(df, variable_type_aggregation, unique_indep_values)


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


def create_contingency_table_with_dependent_variable(df, variable_type_aggregation, dep_variable, unique_indep_values):
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
    dep_variable_type = dep_variable[0]
    dep_variable_name = dep_variable[1]
    aggregation_function_name = dep_variable[2][0]
    aggregationMean = aggregation_function_name == 'MEAN'
    weight_variable_name = dep_variable[2][1]
    weight_dict = {}

    for index in df.index:
        col = parse_variable(0, index, variable_type_aggregation, df)
        row = parse_variable(1, index, variable_type_aggregation, df)
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

    if dep_variable_type in [ GDT.Q.value, GDT.T.value ]:
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


def create_contingency_table_with_no_dependent_variable(df, variable_type_aggregation, unique_indep_values):
    '''
    df : dataframe
    variable_type_aggregation:
       for cat variables: ['cat', field]
       for num variables: ['num', [field, num_bins], binning_edges, binning_names]
    unique_indep_values : [[unique values for columns], [unique values for rows]]
    '''
    result_dict = {}
    count_dict = {}

    for index in df.index:
        col = parse_variable(0, index, variable_type_aggregation, df)
        row = parse_variable(1, index, variable_type_aggregation, df)
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


def create_contingency_table(df, aggregation_variables, dep_variable):
    '''
    aggregation_variables: represents the variables used to create the contingency table.
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
    variable_type_aggregation = []

    aggregationMean = False

    for (var_type, name, config) in aggregation_variables:
        print var_type, (var_type in [ GDT.Q.value, GDT.T.value ])
        if var_type == GDT.C.value:
            unique_indep_values.append(get_unique(df[name], True))
            variable_type_aggregation.append(('cat', name))
        elif var_type in [ GDT.Q.value, GDT.T.value ]:
            values = df[name].dropna(how='any')
            (binning_edges, names) = get_binning_edges_and_names(values, config)

            num_bins = len(binning_edges) - 1
            unique_indep_values.append(names)
            variable_type_aggregation.append(('num', [name, num_bins], binning_edges, names))

    if dep_variable:
        (results_dict, aggregationMean) = create_contingency_table_with_dependent_variable(df, variable_type_aggregation, dep_variable, unique_indep_values)
    else:
        results_dict = create_contingency_table_with_no_dependent_variable(df, variable_type_aggregation, unique_indep_values)

    if not aggregationMean:
        formatted_results_dict["column_headers"] = unique_indep_values[0] + ['Row Totals']
    else:
        formatted_results_dict['column_headers'] = unique_indep_values[0]
    formatted_results_dict["row_headers"] = unique_indep_values[1]
    formatted_results_dict["rows"] = []
    if not aggregationMean:
        column_totals = np.zeros(len(unique_indep_values[0]) + 1)

    for row in unique_indep_values[1]:
        values = [ results_dict[row][col] for col in unique_indep_values[0] ]

        if not aggregationMean:
            values.append(sum(values))
            column_totals += values

        formatted_results_dict["rows"].append({
            "field": row,
            "values": values
        })

    if not aggregationMean:
        formatted_results_dict['column_totals'] = list(column_totals)
    return formatted_results_dict


def get_binning_edges_and_names(array, config):
    procedure = config.get('binning_procedure', 'freedman')
    binning_type = config.get('binning_type', 'procedural')
    if binning_type == 'procedural':
        procedural = True
    else:
        procedural = False

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


def save_aggregation(spec, result, project_id, conditionals={}):
    existing_aggregation_doc = db_access.get_aggregation_from_spec(project_id, spec, conditionals=conditionals)
    if existing_aggregation_doc:
        db_access.delete_aggregation(project_id, existing_aggregation_doc['id'], conditionals=conditionals)
    inserted_aggregation = db_access.insert_aggregation(project_id, spec, result, conditionals=conditionals)
    return inserted_aggregation
