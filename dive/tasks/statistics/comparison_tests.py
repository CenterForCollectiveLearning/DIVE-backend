import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import ttest_ind
import statsmodels.api as sm
from statsmodels.formula.api import ols

from dive.db import db_access
from dive.data.access import get_data
from dive.tasks.ingestion.utilities import get_unique
from dive.tasks.statistics.utilities import get_design_matrices, variations_equal

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def run_anova_from_spec(spec, project_id):
    '''
    For now, spec will be form:
        datasetId
        independentVariables - list names, must be categorical
        dependentVariables - list names, must be numerical
        numBins - number of bins for the independent quantitative variables (if they exist)
    '''
    anova_result = {}

    dependent_variables = spec.get('dependentVariables', [])
    independent_variables = spec.get('independentVariables', [])
    dataset_id = spec.get('datasetId')

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    anova_result = run_anova(df, independent_variables, dependent_variables)
    return anova_result, 200


def run_anova(df, independent_variables, dependent_variables):
    '''
    Returns either a dictionary with the anova stats are an empty list (if the anova test
    is not valid)
    df : dataframe
    independent_variables : list of independent_variable's, where each independent_variable is of form [type, name, num_bins (0 means will be treated as continuous)]
    depedendent_variables : list of dependent_variable's, where each dependent_variable is of form [type, name]
    '''
    num_independent_variables = len(independent_variables)
    num_dependent_variables = len(dependent_variables)

    transformed_data = add_binned_columns_to_df(df, independent_variables, dependent_variables)
    if num_dependent_variables == 1:
        return anova(transformed_data, independent_variables, dependent_variables[0])

    return []


def add_binned_columns_to_df(df, independent_variables, dependent_variables):
    '''
    Adds the binned names as a column to the data
    The key for the binned data is of format _bins_(name of variable)
    df : dataframe
    independent_variables : list of independent_variable's, where each independent_variable is of form [type, name, num_bins (0 means will be treated as continuous)]
    depedendent_variables : list of dependent_variable's, where each dependent_variable is of form [type, name]
    '''
    transformed_data = {}
    for independent_variable in independent_variables:
        transformed_data[independent_variable[1]] = df[independent_variable[1]]
        num_bins = independent_variable[2]
        if num_bins > 0:
            bin_list = []
            data_column = df[independent_variable[1]]
            names, rounded_edges = find_binning_edges_equal_spaced(data_column, independent_variable[2])

            for entry in data_column:
                bin_list.append(find_bin(entry, rounded_edges, names, num_bins))

            transformed_data['_bins_%s' %independent_variable[1]] = bin_list


    for dependent_variable in dependent_variables:
        transformed_data[dependent_variable] = df[dependent_variable]

    return pd.DataFrame.from_dict(transformed_data)


def get_formatted_name(variable):
    '''
    Returns the formatted name of the variable
    variable: of form [type, name, num_bins (0 means will be treated as continuous)]
    '''
    if variable[0] == 'q':
        if variable[2]:
            return '_bins_%s' % variable[1]
        else:
            return variable[1]
    else:
        return 'C(%s)' % variable[1]


def anova(transformed_data, independent_variables, dependent_variable):
    '''
    Returns the formatted dictionary with the anova results
    transformed_data: a df with the added binned columns
    independent_variables : list of independent_variable's, where each independent_variable is of form [type, name, num_bins (0 means will be treated as continuous)]
    depedendent_variables : list of dependent_variable's, where each dependent_variable is of form [type, name]
    '''

    if len(independent_variables) >= 2:
        interaction_term = '%s:%s' % (independent_variables[0][1], independent_variables[1][1])
        has_interaction_term = True

    independent_variable_names = [ iv[1] for iv in independent_variables ]
    y, X = get_design_matrices(transformed_data, dependent_variable, independent_variable_names, interactions=[ independent_variable_names ])
    data_linear_model = sm.OLS(y, X).fit()
    anova_table = sm.stats.anova_lm(data_linear_model).transpose()

    column_headers = ['df', 'sum_sq', 'mean_sq', 'F', 'PR(>F)']

    results = {}
    results['column_headers'] = ['Degrees of Freedom', 'Sum Squares', 'Mean Squares', 'F', 'Probability > F']
    results['stats'] = []

    stats_main = []
    stats_residual = []
    stats_compare = []

    # Stat fields per term
    for independent_variable_name in independent_variable_names:
        stats_variable = [ anova_table[independent_variable_name][header] for header in column_headers]
        stats_main.append(stats_variable)

    # Residual fields
    for header in column_headers:
        stats_residual.append(anova_table['Residual'][header])
        if has_interaction_term:
            stats_compare.append(anova_table[interaction_term][header])

    for index in range(len(independent_variables)):
        results['stats'].append({'field': independent_variables[index][1], 'stats': stats_main[index]})

    if has_interaction_term:
        results['stats'].append({'field': interaction_term, 'stats': stats_compare})

    results['stats'].append({'field': 'Residual', 'stats': stats_residual})
    return results


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


def run_numerical_comparison_from_spec(spec, project_id):
    variable_names = spec.get('variableNames', [])
    independence = spec.get('independence', True)
    dataset_id = spec.get('datasetId')
    if not (len(variable_names) >= 2 and dataset_id):
        return 'Not passed required parameters', 400

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    comparison_result = run_valid_comparison_tests(df, variable_names, independence)
    return comparison_result, 200


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
    # normal = sets_normal(.05,*args)
    normal = True
    numDataSets = len(args)
    equalVar = variations_equal(.25,*args)

    ################we are assuming independence right now
    valid_tests = get_valid_tests(equalVar, True, normal, numDataSets)
    for test in valid_tests:
        results.append({'test':test, 'values':valid_tests[test](*args)})

    return results


##################
#Functions to determine which tests could be run
##################

#return a boolean, if p-value less than threshold, returns false
def variations_equal(THRESHOLD, *args):
    return stats.levene(*args)[1]>THRESHOLD

#if normalP is less than threshold, not considered normal
def sets_normal(THRESHOLD, *args):
    for arg in args:
        if len(arg) < 8:
            return False
        if stats.normaltest(arg)[1] < THRESHOLD:
            return False;

    return True

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
