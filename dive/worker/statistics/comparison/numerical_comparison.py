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
from dive.base.data.access import get_data, get_conditioned_data
from dive.worker.ingestion.utilities import get_unique
from dive.worker.statistics.utilities import are_variations_equal, sets_normal

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


# args must be a list of lists
def run_valid_numerical_comparison_tests(df, variable_names, independence=True):
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
    variations_equal = are_variations_equal(.25, *args)

    # Assuming independence
    valid_tests = get_valid_tests(variations_equal, True, normal, numDataSets)
    results = [ {
        'test': test,
        'values': format_results(valid_tests[test](*args))
    } for test in valid_tests]

    return results


def format_results(test_result):
    return {
        'statistic': test_result.statistic,
        'pvalue': test_result.pvalue
    }

# def run_numerical_comparison_from_spec(spec, project_id, conditionals={}):
#     comparison_result = {}
#
#     variable_names = spec.get('variableNames', [])
#     independence = spec.get('independence', True)
#     dataset_id = spec.get('datasetId')
#     if not (len(variable_names) >= 2 and dataset_id):
#         return 'Not passed required parameters', 400
#
#     df = get_data(project_id=project_id, dataset_id=dataset_id)
#     df = get_conditioned_data(project_id, dataset_id, df, conditionals)
#     df = df.dropna()  # Remove unclean
#
#     comparison_result['tests'] =
#     return comparison_result, 200


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
