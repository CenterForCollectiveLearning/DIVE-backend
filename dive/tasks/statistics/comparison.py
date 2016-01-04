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

#let's just do count by two categorical variables right now
def create_contingency_table_from_spec(spec, project_id):
    ind_cat_variables = spec.get("ind_cat_variables", [])
    ind_num_variables = spec.get("ind_num_variables", [])
    dataset_id = spec.get("dataset_id")
    dep_num_variable = spec.get("dep_num_variable")
    dep_cat_variable = spec.get("dep_cat_variable")

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    comparison_result = create_contingency_table_categorical(df, ind_cat_variables, ind_num_variables, dep_num_variable, dep_cat_variable)
    return {
        'data': comparison_result
    }, 200

def parse_aggregation_function(string_function):
    if string_function == "MEAN":
        return np.mean
    if string_function == "SUM":
        return np.sum

def parse_string_mapping_function(list_function):
    if list_function[0] == "FILTER":
        return (lambda x: x == list_function[1])

def create_contingency_table_categorical(df, ind_cat_variables, ind_num_variables, dep_num_variable, dep_cat_variable):
    #a list of lists
    contingencyDict = {}
    unique_dep_values = []
    typeVariables = []

    for var in ind_cat_variables:
        unique_dep_values.append(get_unique(df[var]))
        typeVariables.append(('cat', var))
    for var in ind_num_variables:
        (names, binningEdges) = find_binning_edges_equal_spaced(df[var[0]], var[1])
        unique_dep_values.append(names)
        typeVariables.append(('num', var, binningEdges, names))

    def parse_type_variable(num, index):
        if typeVariables[num][0] == 'cat':
            return df.get_value(index, typeVariables[num][1])
        elif typeVariables[num][0] == 'num':
            return find_bin(df.get_value(index, typeVariables[num][1][0]), typeVariables[num][2], typeVariables[num][3], typeVariables[num][1][1])

    if dep_num_variable:
        numVarDict = {}
        for index in range(len(df)):
            indepField1 = parse_type_variable(0, index)
            indepField2 = parse_type_variable(1, index)
            if numVarDict.get((indepField1, indepField2)):
                numVarDict[(indepField1, indepField2)].append(df.get_value(index, dep_num_variable[0]))
            else:
                numVarDict[(indepField1, indepField2)] = [df.get_value(index, dep_num_variable[0])]

        for var1 in unique_dep_values[0]:
            for var2 in unique_dep_values[1]:
                contingencyDict[(var1, var2)] = parse_aggregation_function(dep_num_variable[1])(numVarDict.get((var1, var2)))

        return contingencyDict

    elif dep_cat_variable:
        catVarDict = {}
        for index in range(len(df)):
            indepField1 = parse_type_variable(0, index)
            indepField2 = parse_type_variable(1, index)
            if catVarDict.get((indepField1, indepField2)):
                catVarDict[(indepField1, indepField2)].append(df.get_value(index, dep_cat_variable[0]))
            else:
                catVarDict[(indepField1, indepField2)] = [df.get_value(index, dep_cat_variable[0])]

        for var1 in unique_dep_values[0]:
            for var2 in unique_dep_values[1]:
                contingencyDict[(var1, var2)] = parse_aggregation_function(dep_cat_variable[2])(map(parse_string_mapping_function(dep_cat_variable[1]),catVarDict.get((var1, var2))))

        return contingencyDict
    else:
        countDict = {}
        for index in range(len(df)):
            indepField1 = parse_type_variable(0, index)
            indepField2 = parse_type_variable(1, index)
            if countDict.get((indepField1, indepField2)):
                countDict[(indepField1, indepField2)] += 1
            else:
                countDict[(indepField1, indepField2)] = 1

        for var1 in unique_dep_values[0]:
            for var2 in unique_dep_values[1]:
                contingencyDict[(var1, var2)] = countDict.get((var1, var2))

    return contingencyDict
#binning functions
##################
##binning is hard
##we want to round to three floats
##right edge is open
def find_binning_edges_equal_spaced(array, num_bins):
    theMin = min(array)
    theMax = max(array)

    edges = np.linspace(theMin, theMax, num_bins+1)

    roundedEdges = []
    for i in range(len(edges)-1):
        roundedEdges.append( float('%.3f' % edges[i]))
    roundedEdges.append(float('%.3f' % edges[-1])+0.001)

    names = []
    for i in range(len(edges)-1):
        names.append('%s-%s' % (str(roundedEdges[i]), str(roundedEdges[i+1])))

    return (names, roundedEdges)

def find_bin(target, binningEdges, binningNames, num_bins):
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
    return binningNames[searchIndex(binningEdges, target, num_bins, 0)-1]


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
