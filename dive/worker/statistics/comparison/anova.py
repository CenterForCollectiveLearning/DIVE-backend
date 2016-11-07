import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import ttest_ind
import statsmodels.api as sm
from statsmodels.formula.api import ols

from dive.base.db import db_access
from dive.base.data.access import get_data, get_conditioned_data
from dive.worker.ingestion.utilities import get_unique
from dive.worker.statistics.utilities import get_design_matrices, are_variations_equal

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def run_anova_from_spec(spec, project_id, conditionals={}):
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
    dependent_variables_names = dependent_variables
    independent_variables_names = [ iv[1] for iv in independent_variables ]
    dataset_id = spec.get('datasetId')

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = get_conditioned_data(project_id, dataset_id, df, conditionals)

    df_subset = df[ dependent_variables_names + independent_variables_names ]
    df_ready = df_subset.dropna(how='all')  # Remove unclean

    anova_result = run_anova(df_ready, independent_variables, dependent_variables)
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

    has_interaction_term = False
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
