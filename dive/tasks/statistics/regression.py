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
    dependent_variable_name = spec.get('dependentVariable')
    estimator = spec.get('estimator', 'ols')
    degree = spec.get('degree', 1)
    weights = spec.get('weights', None)
    functions = spec.get('functions', [])
    dataset_id = spec.get('datasetId')

    all_fields = db_access.get_field_properties(project_id, dataset_id)

    # Map variables to field documents
    dependent_variable = next((f for f in all_fields if f['name'] == dependent_variable_name), None)

    independent_variables = []
    if independent_variables_names:
        independent_variables = get_full_field_documents_from_names(independent_variables_names)
    else:
        for field in all_fields:
            if (field['name'] != dependent_variable_name) and (not field['is_unique']):
                independent_variables.append(field)

    if not (dataset_id and dependent_variable):
        return "Not passed required parameters", 400

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
        'fields': indep_fields
    }

    for num_indep in range(1, len(independent_variables) + 1):
        considered_independent_variables = combinations(independent_variables, num_indep)

        for considered_independent_variables in considered_independent_variables:
            regression_result = {}

            if len(considered_independent_variables) == 0:
                continue

            # Run regression
            model_result = multivariate_linear_regression(df, considered_independent_variables, dependent_variable, estimator, weights)

            # Test regression
            dep_data = df[dependent_variable['name']]
            regression_stats = test_regression_fit(model_result.resid, dep_data)

            # Format results
            fields = [ civ['name'] for civ in considered_independent_variables ]

            confidence_intervals = model_result.conf_int().transpose().to_dict()
            parsed_confidence_intervals = {}
            for field, d in confidence_intervals.iteritems():
                parsed_confidence_intervals[field] = [d[0], d[1]]

            properties = [
                {
                    'type': 'coefficient',
                    'data': model_result.params.to_dict()
                },
                {
                    'type': 'standardError',
                    'data': model_result.bse.to_dict()
                },
                {
                    'type': 'pValue',
                    'data': model_result.pvalues.to_dict()
                },
                {
                    'type': 'tValue',
                    'data': model_result.tvalues.to_dict()
                },
                {
                    'type': 'confidenceIntervals',
                    'data': parsed_confidence_intervals
                },
            ]

            constants = {
                'coefficient': model_result.params.to_dict().get('Intercept'),
                'standardError': model_result.bse.to_dict().get('Intercept'),
                'pValue': model_result.pvalues.to_dict().get('Intercept'),
                'tValue': model_result.tvalues.to_dict().get('Intercept'),
                'confidenceIntervals': parsed_confidence_intervals.get('Intercept')
            }

            regression_result = {
                'regressedFields': fields,
                'regression': {
                    'constants': constants,
                    'propertiesByField': formatPropertiesByField(fields, properties)
                },
                'columnProperties': {
                    'rSquared': model_result.rsquared,
                    'rSquaredAdj': model_result.rsquared_adj,
                    'fTest': model_result.fvalue,
                    'stats': test_regression_fit(model_result.resid, dep_data),
                    'aic': model_result.aic,
                    'bic': model_result.bic,
                    'std': model_result.bse
                }
            }
            regression_results['regressionsByColumn'].append(regression_result)

    return regression_results

def formatPropertiesByField(fields, properties):
    propertiesByField = []

    for (i, field) in enumerate(fields):
        formattedProperty = {
            'field': field
        }

        for _property in properties:
            formattedProperty[_property.get('type')] = _property.get('data').get(field)

        propertiesByField.append(formattedProperty)

    return propertiesByField

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


def multivariate_linear_regression(df, independent_variables, dependent_variable, estimator, weights=None):
    formula = create_regression_formula(independent_variables, dependent_variable)

    return smf.ols(formula=formula, data=df).fit()


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
