import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
import statsmodels.formula.api as smf
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
    indep = spec.get('indep', [])
    dep_field_name = spec.get('dep')
    estimator = spec.get('estimator', 'ols')
    degree = spec.get('degree', 1)
    weights = spec.get('weights', None)
    functions = spec.get('functions', [])
    dataset_id = spec.get('dataset_id')
    fields = db_access.get_field_properties(project_id, dataset_id)

    if not (dataset_id and dep_field_name):
        return "Not passed required parameters", 400

    # 2) Access dataset
    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    all_indep_data = {}
    if indep:
        for indep_field_name in indep:
            all_indep_data[indep_field_name] = df[indep_field_name]
    else:
        for field in fields:
            field_name = field['name']
            if (field_name != dep_field_name) and (field['general_type'] == 'q'):
                all_indep_data[field_name] = df[field_name]
    dep_data = df[dep_field_name]

    # 3) Run test based on parameters and arguments
    # TODO Reduce the number of arguments
    regression_result = run_cascading_regression(df, fields, all_indep_data, dep_data, dep_field_name, model=model, degree=degree, functions=functions, estimator=estimator, weights=weights)
    regression_data = get_regression_data(df, fields, all_indep_data, dep_data, dep_field_name, regression_result)
    return {
        'result': regression_result,
        # 'data': regression_data
    }, 200


def get_regression_data(df, fields, all_indep_data, dep_data, dep_field_name, regression_result):
    '''
    Show plot of dependent field against all others
    '''
    specs = []

    for indep_field_name, indep_data in all_indep_data.iteritems():
        spec = {
            'viz_type': ['scatterplot'],
            'args': {}
        }
        regression_data_array = [[indep_field_name, dep_field_name]]
        regression_data_array.append(zip(dep_data, indep_data))
        spec['data'] = regression_data_array
        spec['args']['x'] = indep_field_name
        spec['args']['y'] = dep_field_name
        specs.append(spec)

    return specs


def run_cascading_regression(df, fields, all_indep_data, dep_data, dep_field_name, model='lr', degree=1, functions=[], estimator='ols', weights=None):
    # Format data structures


    indep_fields = all_indep_data.keys()
    regression_results = {
        'results': [],
        'indep_fields': indep_fields,
        'list': [],
        'size_list': []
    }

    regression_results['indep_fields'] = indep_fields

    for num_indep in range(1, len(indep_fields) + 1):
        considered_indep_fields = combinations(indep_fields, num_indep)

        for considered_indep_tuple in considered_indep_fields:
            regression_result = {}

            if len(considered_indep_tuple) == 0:
                continue

            indep_data_matrix = []
            for considered_indep in considered_indep_tuple:
                indep_data_vector = np.array(all_indep_data[considered_indep])

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
            regression_results['list'].append(considered_indep_fields_list)
            regression_results['size_list'].append(num_indep)

            conf_int = model_result.conf_int().transpose().to_dict()
            parsed_conf_int = {}
            for field, d in conf_int.iteritems():
                parsed_conf_int[field] = [d[0], d[1]]

            regression_result = {
                'fields': considered_indep_fields_list,
                'conf_int': parsed_conf_int,
                'params': model_result.params,
                't_values': model_result.tvalues,
                'p_values': model_result.pvalues,
                'r_squared': model_result.rsquared,
                'r_squared_adj': model_result.rsquared_adj,
                'aic': model_result.aic,
                'bic': model_result.bic,
                'f_test': model_result.fvalue,
                'std': model_result.bse,
                'stats': test_regression_fit(model_result.resid, dep_data)
            }
            regression_results['results'].append(regression_result)

    regression_results['list'] = regression_results['list'][:-1]
    regression_results['size_list'] = regression_results['size_list'][:-1]
    regression_results['results'] = regression_results['results'][:-1]
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
