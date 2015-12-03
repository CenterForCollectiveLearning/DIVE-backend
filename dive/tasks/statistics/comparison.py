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

from dive.db import db_access
from dive.data.access import get_data
from dive.tasks.ingestion.utilities import get_unique

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

def run_numerical_comparison_from_spec(spec, project_id):
    variable_names = spec.get('variable_names', [])
    independence = spec.get('independence', True)
    dataset_id = spec.get('dataset_id')
    if not (len(variable_names) >= 2 and dataset_id):
        return 'Not passed required parameters', 400

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    comparison_result = run_valid_comparison_tests(df, variable_names, independence)
    return {
        'data': comparison_result
    }, 200

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

    results = {}
    normal = sets_normal(.25,*args)
    numDataSets = len(args)
    equalVar = variations_equal(.25,*args)

    ################we are assuming independence right now
    valid_tests = get_valid_tests(equalVar, True, normal, numDataSets)
    for test in valid_tests:
        results[test] = valid_tests[test](*args)
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
        print "else"
        for field in fields:
            field_name = field['name']
            if (field_name is not dep_field_name) and (field['general_type'] == 'q'):
                indep_data[field_name] = df[field_name]
    dep_data = {}
    for dep_field_name in dep:
        dep_data[dep_field_name] = df[dep_field_name]

    if test is 'ttest':
        return ttest(df, fields, indep, dep)
    return 'here!'

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


    # averageValuesInCategory = len(df[indep_field_name])/len(unique_indep_values)
    #
    # if !(dep):
    #     results = run_valid_comparison_tests(count.values())
    #
    # else:
    #     dep_field_name = dep[0]
    #     for v in unique_indep_values:
    #         subsets[v] = np.array(df[df[indep_field_name] == v][dep_field_name])

##################
#Functions to determine which tests could be run
##################

#return a boolean, if p-value less than threshold, returns false
def variations_equal(THRESHOLD, *args):
    return stats.levene(*args)[1]>THRESHOLD

#if normalP is less than threshold, not considered normal
def sets_normal(THRESHOLD, *args):
    normal = True;
    for arg in args:
        if len(arg) < 8:
            return False
        if stats.normaltest(arg)[1] < THRESHOLD:
            normal = False;

    return normal




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
            # 'kstest': stats.kstest
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

def run_comparison_oneCategorical(df, indep, dep):
    # Runs if indep consists of just one categorical variable name

    #if there is only a categorical variable, we run tests on count
    indep_field_name = indep[0]
    unique_indep_values = ["male", "female"]
    results = {}
    categoryInformation = {}


    if not dep:
        for v in unique_indep_values:
            categoryInformation[v] = 0
        for value in df[indep_field_name]:
            categoryInformation[value] = categoryInformation[value] + 1
        return run_valid_comparison_tests([categoryInformation.values()])
    elif dep:
        dep_field_name = dep[0]
        categoryInformation["count"] = {}
        categoryInformation["values"] = {}
        for v in unique_indep_values:
            categoryInformation["count"][v] = 0
            categoryInformation["values"][v] = []
        for index in range(len(df[indep_field_name])):
            indepField = df.get_value(index, indep_field_name)
            categoryInformation["count"][indepField] = categoryInformation["count"][indepField] + 1
            categoryInformation["values"][indepField].append(df.get_value(index, dep_field_name))
        results["count"] = run_valid_comparison_tests([categoryInformation["count"].values()])
        for v in unique_indep_values:
            results[v] = stats.ttest_1samp(categoryInformation['values'][v], float(df[dep_field_name].sum())/len(unique_indep_values))
        return results

    return categoryInformation

# dict = {"gender": ["male", "female", "female", "male"], "weight": [300, 90, 80, 95]}
# df = pd.DataFrame(data = dict)
# print run_comparison_oneCategorical(df, ["gender"], ["weight"])
