import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
from time import time
from itertools import chain, combinations
from operator import add, mul
from math import log10, floor

from dive.tasks.statistics.utilities import sets_normal
from dive.db import db_access
from dive.data.access import get_data

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def run_regression_from_spec(spec, project_id):
    # 1) Parse and validate arguments
    model = spec.get('model', 'lr')
    independent_variables = spec.get('independentVariables', [])
    dependent_variable = spec.get('dependentVariable')
    estimator = spec.get('estimator', 'ols')
    degree = spec.get('degree', 1)
    weights = spec.get('weights', None)
    functions = spec.get('functions', [])
    dataset_id = spec.get('datasetId')
    all_fields = db_access.get_field_properties(project_id, dataset_id)

    if not (dataset_id and dependent_variable):
        return "Not passed required parameters", 400

    # 2) Access dataset
    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    all_independent_variable_data = {}
    if independent_variables:
        for independent_variable_name in independent_variables:
            all_independent_variable_data[independent_variable_name] = df[independent_variable_name]
    else:
        for field in all_fields:
            field_name = field['name']
            if (field_name != dependent_variable) and (field['general_type'] == 'q'):
                all_independent_variable_data[field_name] = df[field_name]
    dependent_variable_data = df[dependent_variable]

    # 3) Run test based on parameters and arguments
    # TODO Reduce the number of arguments
    regression_result = run_cascading_regression(df, all_independent_variable_data, dependent_variable_data, model=model, degree=degree, functions=functions, estimator=estimator, weights=weights)
    return regression_result, 200

    {
        'variables': [],
        'regressionsByColumn': [
            {
                'fields': [],
                'rSquared': "",
                'regressions': [
                    {
                        'variable': "",
                        'coefficient': {'x1': "", 'const': ""},
                        'standardError': {'x1': "", 'const': ""},
                        'pValue': {'x1': "", 'const': ""}
                    }
                ]
            }
        ]
    }

def run_cascading_regression(df, all_indep_data, dep_data,  model='lr', degree=1, functions=[], estimator='ols', weights=None):
    # Format data structures


    indep_fields = all_indep_data.keys()
    regression_results = {
        'regressionsByColumn': [],
        'variables': indep_fields
    }

    for num_indep in range(1, len(indep_fields) + 1):
        considered_indep_fields = combinations(indep_fields, num_indep)

        for considered_indep_tuple in considered_indep_fields:
            regression_result = {}

            if len(considered_indep_tuple) == 0:
                continue

            # TODO Distinguish between regression types in here
            for considered_indep in considered_indep_tuple:
                indep_data_vector = np.array(all_indep_data[considered_indep])

            indep_data_matrix = []

            # Field transformation if polynomial regression
            if model == 'lr':
                indep_data_matrix.append(indep_data_vector)
            elif model == 'pr':
                if degree == 1:
                    indep_data_matrix.append(indep_data_vector)
                else:
                    for deg in range(1, degree + 1):
                        indep_data_matrix.append(indep_data_vector**deg)
            elif model == 'gr':
                for func in funcArray:
                    indep_data_matrix.append(func(indep_data_vector))

            # Run regression
            model_result = multivariate_linear_regression(dep_data, indep_data_matrix, estimator, weights)

            # Format results
            considered_indep_fields_list = list(considered_indep_tuple)
            if len(considered_indep_fields_list) == 1:
                considered_indep_fields_list = considered_indep_fields_list[0]
            considered_indep_fields_string = str(considered_indep_fields_list)

            regression_result = {
                'fields': considered_indep_fields_string,
                'rSquared': model_result.rsquared,
                'fTest': model_result.fvalue,
                'stats': test_regression_fit(model_result.resid, dep_data),
                'regressions': [{
                    'variable': "",
                    'coefficient': model_result.params,
                    'standardError': model_result.bse,
                    'pValue': model_result.pvalues
                }]
            }
            regression_results['regressionsByColumn'].append(regression_result)

    return regression_results

# Multivariate linear regression function
def multivariate_linear_regression(y, x, estimator, weights=None):
    ones = np.ones(len(x[0]))
    X = sm.add_constant(np.column_stack((x[0], ones)))
    for ele in x[1:]:
        X = sm.add_constant(np.column_stack((ele, X)))

    if estimator == 'ols':
        return sm.OLS(y, X).fit()

    elif estimator == 'wls':
        return sm.WLS(y, X, weights).fit()

    elif estimator == 'gls':
        return sm.GLS(y, X).fit()

    return None


def test_regression_fit(residuals, actual_y):
    '''
    Run regression tests
    Tests how well the regression line predicts the data
    '''
    predicted_y = np.array(residuals) + np.array(actual_y)

    # Non-parametric tests (chi-square and KS)
    chisquare = stats.chisquare(predicted_y, actual_y)
    kstest = stats.ks_2samp(predicted_y, actual_y)
    results = {
        'chi_square': {
            'test_statistic': chisquare[0],
            'p_value': chisquare[1]
        },
        'ks_test': {
            'test_statistic': kstest[0],
            'p_value': kstest[1]
        }
    }

    if len(set(residuals)) > 1:
        wilcoxon = stats.wilcoxon(residuals)
        results['wilcoxon'] = {
            'testStatistic': wilcoxon[0],
            'pValue': wilcoxon[1]
        }

    if sets_normal(0.2, residuals, actual_y):
        t_test_result = stats.ttest_1samp(residuals, 0)
        results['t_test'] = {
            'test_statistic':t_test_result[0],
            'p_value':t_test_result[1]
        }

    return results
