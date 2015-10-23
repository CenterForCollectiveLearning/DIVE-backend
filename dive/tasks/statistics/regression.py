import patsy
import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.discrete import discrete_model

from collections import Counter
from time import time
from itertools import chain, combinations
from operator import add, mul
from math import log10, floor

from dive.tasks.statistics.utilities import sets_normal
from dive.db import db_access
from dive.data.access import get_data

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def get_full_field_documents_from_names(names):
    fields = []
    for name in names:
        matched_field = next((f for f in all_fields if f['name'] == name), None)
        if matched_field:
            fields.append(matched_field)
    return fields


def run_regression_from_spec(spec, project_id):
    # 1) Parse and validate arguments
    model = spec.get('model', 'lr')
    independent_variables_names = spec.get('independentVariables', [])
    dependent_variable_name = spec.get('dependentVariable', [])
    estimator = spec.get('estimator', 'ols')
    degree = spec.get('degree', 1)
    weights = spec.get('weights', None)
    functions = spec.get('functions', [])
    dataset_id = spec.get('datasetId')

    if not (dataset_id and dependent_variable_name):
        return "Not passed required parameters", 400


    # Map variables to field documents
    all_fields = db_access.get_field_properties(project_id, dataset_id)
    dependent_variable = next((f for f in all_fields if f['name'] == dependent_variable_name), None)

    independent_variables = []
    if independent_variables_names:
        independent_variables = get_full_field_documents_from_names(independent_variables_names)
    else:
        for field in all_fields:
            if (field['name'] != dependent_variable_name) and (not field['is_unique']):
                independent_variables.append(field)

    # Determine regression model based on number of type of variables
    variable_types = Counter({
        'independent': { 'q': 0, 'c': 0 },
        'dependent': { 'q': 0, 'c': 0}
    })

    for independent_variable in independent_variables:
        variable_type = independent_variable['general_type']
        variable_types['independent'][variable_type] += 1

    # 2) Access dataset
    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()

    # 3) Run test based on parameters and arguments
    regression_result = run_cascading_regression(df, independent_variables, dependent_variable, model=model, degree=degree, functions=functions, estimator=estimator, weights=weights)
    return regression_result, 200

    # {
    #     'variables': [],
    #     'regressionsByColumn': [
    #         {
    #             'fields': [],
    #             'rSquared': "",
    #             'regressions': [
    #                 {
    #                     'variable': "",
    #                     'coefficient': {'x1': "", 'const': ""},
    #                     'standardError': {'x1': "", 'const': ""},
    #                     'pValue': {'x1': "", 'const': ""}
    #                 }
    #             ]
    #         }
    #     ]
    # }

def run_cascading_regression(df, independent_variables, dependent_variable, model='lr', degree=1, functions=[], estimator='ols', weights=None):
    # Format data structures
    indep_fields = [ iv['name'] for iv in independent_variables ]
    regression_results = {
        'regressionsByColumn': [],
        'variables': indep_fields
    }

    for num_indep in range(1, len(independent_variables) + 1):
        considered_independent_variables = combinations(independent_variables, num_indep)

        for considered_independent_variables in considered_independent_variables:
            regression_result = {}

            if len(considered_independent_variables) == 0:
                continue

            # Run regression
            regression_result = multivariate_linear_regression(df, considered_independent_variables, dependent_variable, estimator, weights)

            # Test regression
            if regression_result.get('resid'):
                dep_data = df[dependent_variable['name']]
                regression_stats = test_regression_fit(regression_result['resid'], dep_data)

            # Format results
            considered_independent_variables_names = [ civ['name'] for civ in considered_independent_variables ]

            regression_result['fields'] = considered_independent_variables_names
            regression_results['columnParams'] = regression_result
            regression_results['regressionsByColumn'].append(regression_result)

    return regression_results


def _parse_confidence_intervals(model_result):
    conf_int = model_result.conf_int()
    parsed_conf_int = {}
    for field, d in conf_int.iteritems():
        parsed_conf_int[field] = [d[0], d[1]]
    logger.info(parsed_conf_int)
    return parsed_conf_int


def create_regression_formula(independent_variables, dependent_variable):
    formula = '%s ~ ' % (dependent_variable['name'])
    terms = []
    for independent_variable in independent_variables:
        if independent_variable['general_type'] == 'q':
            term = independent_variable['name']
        else:
            term = 'C(%s)' % (independent_variable['name'])
        terms.append(term)
    concatenated_terms = ' + '.join(terms)
    formula = formula + concatenated_terms
    formula = formula.encode('ascii')

    logger.info("Regression formula %s:", formula)
    return formula


def replace_nan_in_numpy(m):
    return np.where(np.isnan(m), None, m)


def multivariate_linear_regression(df, independent_variables, dependent_variable, estimator, weights=None):
    formula = create_regression_formula(independent_variables, dependent_variable)
    y, X = patsy.dmatrices(formula, df, return_type='dataframe')

    if dependent_variable['general_type'] == 'q':
        model_result = sm.OLS(y, X).fit()
        parsed_result = {
            'rSquared': model_result.rsquared,
            'rSquaredAdj': model_result.rsquared_adj,
            'fTest': model_result.fvalue,
            'stats': regression_stats,
            'conf_int': _parse_confidence_intervals(model_result),
            'params': model_result.params,
            't_values': model_result.tvalues,
            'p_values': model_result.pvalues,
            'aic': model_result.aic,
            'bic': model_result.bic,
            'ste': model_result.bse
        }

    elif dependent_variable['general_type'] == 'c':
        model_result = discrete_model.MNLogit(y, X).fit(maxiter=100, disp=False)
        parsed_result = {
            'aic': model_result.aic,
            'bic': model_result.bic,
            'p_values': replace_nan_in_numpy(model_result.pvalues[0]),
            't_values': replace_nan_in_numpy(model_result.tvalues[0]),
            'params': replace_nan_in_numpy(model_result.params[0]),
            'ste':replace_nan_in_numpy(model_result.bse[0]),
        }

    return parsed_result


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
            'test_statistic': t_test_result[0],
            'p_value': t_test_result[1]
        }

    return results
