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



def run_anova(df, independent_variables_names, dependent_variables_names):
    '''
    Returns either a dictionary with the anova stats are an empty list (if the anova test
    is not valid)
    df : dataframe
    independent_variables : list of independent_variable's, where each independent_variable is of form [type, name, num_bins (0 means will be treated as continuous)]
    depedendent_variables : list of dependent_variable's, where each dependent_variable is of form [type, name]
    '''
    num_independent_variables = len(independent_variables_names)
    num_dependent_variables = len(dependent_variables_names)

    transformed_data = add_binned_columns_to_df(df, independent_variables_names, dependent_variables_names)
    if num_dependent_variables == 1:
        first_dependent_variable = dependent_variables_names[0]
        return anova(transformed_data, independent_variables_names, first_dependent_variable)

    return []


def add_binned_columns_to_df(df, independent_variables_names, dependent_variables_names):
    '''
    Adds the binned names as a column to the data
    The key for the binned data is of format _bins_(name of variable)
    df : dataframe
    independent_variables : list of independent_variable's, where each independent_variable is of form [type, name, num_bins (0 means will be treated as continuous)]
    depedendent_variables : list of dependent_variable's, where each dependent_variable is of form [type, name]
    '''
    transformed_data = {}
    for independent_variable_name in independent_variables_names:
        transformed_data[independent_variable_name] = df[independent_variable_name]

        # TODO Get number of bins programmatically
        # num_bins = independent_variable_name
        num_bins = 0
        if num_bins > 0:
            bin_list = []
            data_column = df[independent_variable_name]
            names, rounded_edges = find_binning_edges_equal_spaced(data_column, num_bins)

            for entry in data_column:
                bin_list.append(find_bin(entry, rounded_edges, names, num_bins))

            transformed_data['_bins_{}'.format(independent_variable_name)] = bin_list


    for dependent_variable_name in dependent_variables_names:
        transformed_data[dependent_variable_name] = df[dependent_variable_name]

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


def anova(transformed_data, independent_variables_names, dependent_variable_name):
    '''
    Returns the formatted dictionary with the anova results
    transformed_data: a df with the added binned columns
    independent_variables : list of independent_variable's, where each independent_variable is of form [type, name, num_bins (0 means will be treated as continuous)]
    depedendent_variables : list of dependent_variable's, where each dependent_variable is of form [type, name]
    '''

    has_interaction_term = False
    if len(independent_variables_names ) >= 2:
        interaction_term = '%s:%s' % (independent_variables_names[0], independent_variables_names[1])
        has_interaction_term = True

    y, X = get_design_matrices(transformed_data, dependent_variable_name, independent_variables_names, interactions=[ independent_variables_names ])
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
    for independent_variable_name in independent_variables_names:
        stats_variable = [ anova_table[independent_variable_name][header] for header in column_headers]
        stats_main.append(stats_variable)

    # Residual fields
    for header in column_headers:
        stats_residual.append(anova_table['Residual'][header])
        if has_interaction_term:
            stats_compare.append(anova_table[interaction_term][header])

    for index in range(len(independent_variables_names)):
        results['stats'].append({'field': independent_variables_names[index], 'stats': stats_main[index]})

    if has_interaction_term:
        results['stats'].append({'field': interaction_term, 'stats': stats_compare})

    results['stats'].append({'field': 'Residual', 'stats': stats_residual})
    return results
