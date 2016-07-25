'''
'''
import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.discrete import discrete_model
from sklearn.linear_model import LogisticRegression

from collections import OrderedDict
from time import time
from itertools import chain, combinations
from operator import add, mul
from math import log10, floor
from patsy import dmatrices

from dive.db import db_access
from dive.data.access import get_data
from dive.task_core import celery, task_app
from dive.tasks.statistics.regression import ModelSelectionType as MST
from dive.tasks.statistics.regression.model_recommendation import recommend_selection_type, construct_models
from dive.tasks.statistics.utilities import sets_normal, difference_of_two_lists
from dive.tasks.statistics.regression.helpers import get_full_field_documents_from_field_names, get_field_names_from_considered_independent_variables
from dive.resources.serialization import replace_unserializable_numpy

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

def run_regression_from_spec(spec, project_id):
    '''
    Wrapper function for five discrete steps:
    1) Parse arguments (in this function)
    2) Loading data from DB for fields and dataframe
    3) Construct / recommend models given those fields
    4) Run regressions described by those models
    5) Format results
    '''
    model = spec.get('model', 'lr')
    regression_type = spec.get('regressionType')
    independent_variable_names = spec.get('independentVariables', [])
    dependent_variable_name = spec.get('dependentVariable', [])
    interaction_term_ids = spec.get('interactionTerms', [])
    estimator = spec.get('estimator', 'ols')
    degree = spec.get('degree', 1)  # need to find quantitative, categorical
    weights = spec.get('weights', None)
    functions = spec.get('functions', [])
    dataset_id = spec.get('datasetId')

    if not (dataset_id and dependent_variable_name):
        return 'Not passed required parameters', 400

    print 'independent variables', independent_variable_names

    dependent_variable, independent_variables, interaction_terms, df = \
        load_data(dependent_variable_name, independent_variable_names, interaction_term_ids, dataset_id, project_id)

    selection_type = recommend_selection_type(independent_variable_names)

    considered_independent_variables_per_model, patsy_models = \
        construct_models(df, dependent_variable, independent_variables, interaction_terms, selection_type)

    raw_results = run_models(df, patsy_models, dependent_variable, regression_type)

    formatted_results = format_results(raw_results, dependent_variable, independent_variable_names, considered_independent_variables_per_model, interaction_terms)

    return formatted_results, 200


def load_data(dependent_variable_name, independent_variable_names, interaction_term_ids, dataset_id, project_id):
    '''
    Load DF and full field documents
    '''
    # Map variables to field documents

    with task_app.app_context():
        all_fields = db_access.get_field_properties(project_id, dataset_id)
        interaction_terms = db_access.get_interaction_term_properties(interaction_term_ids)
    dependent_variable = next((f for f in all_fields if f['name'] == dependent_variable_name), None)

    independent_variables = []
    if independent_variable_names:
        independent_variables = get_full_field_documents_from_field_names(all_fields, independent_variable_names)
    else:
        for field in all_fields:
            if (not (field['general_type'] == 'c' and field['is_unique']) \
                and field['name'] != dependent_variable_name):
                independent_variables.append(field)    

    # 2) Access dataset
    with task_app.app_context():
        df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna(axis=0, how='any')

    return dependent_variable, independent_variables, interaction_terms, df


def run_models(df, patsy_models, dependent_variable, regression_type, degree=1, functions=[], estimator='ols', weights=None):
    model_results = []

    map_function_to_type = {
        'linear': run_linear_regression,
        'logistic': run_logistic_regression,
        'polynomial': run_polynomial_regression
    }

    # Iterate over and run each models
    for patsy_model in patsy_models:
        regression_result = {}

        # Run regression
        model_result = map_function_to_type[regression_type](df, patsy_model, dependent_variable, estimator, weights)
        model_results.append(model_result)
    return model_results


def parse_confidence_intervals(model_result):
    conf_int = model_result.conf_int().transpose().to_dict()

    parsed_conf_int = {}
    for field, d in conf_int.iteritems():
        parsed_conf_int[field] = [ d[0], d[1] ]
    return parsed_conf_int


def run_linear_regression(df, patsy_model, dependent_variable, estimator, weights):
    y, X = dmatrices(patsy_model, df, return_type='dataframe')

    model_result = sm.OLS(y, X).fit()

    p_values = model_result.pvalues.to_dict()
    t_values = model_result.tvalues.to_dict()
    params = model_result.params.to_dict()
    ste = model_result.bse.to_dict()
    conf_ints = parse_confidence_intervals(model_result)

    constants = {
        'p_value': p_values.get('Intercept'),
        't_value': t_values.get('Intercept'),
        'coefficient': params.get('Intercept'),
        'standard_error': ste.get('Intercept'),
        'conf_int': conf_ints.get('Intercept')
    }

    regression_field_properties = {
        'p_value': p_values,
        't_value': t_values,
        'coefficient': params,
        'standard_error': ste,
        'conf_int': conf_ints
    }

    total_regression_properties = {
        'aic': model_result.aic,
        'bic': model_result.bic,
        'dof': model_result.nobs,
        'r_squared': model_result.rsquared,
        'r_squared_adj': model_result.rsquared_adj,
        'f_test': model_result.fvalue,
        # 'resid': model_result.resid.tolist()
    }

    regression_results = restructure_field_properties_dict(constants, regression_field_properties, total_regression_properties)

    return regression_results


def run_logistic_regression(df, patsy_model, dependent_variable, estimator, weights):
    y, X = dmatrices(patsy_model, df, return_type='dataframe')

    model_result = discrete_model.MNLogit(y, X).fit(maxiter=100, disp=False, method="nm")

    p_values = model_result.pvalues[0].to_dict()
    t_values = model_result.tvalues[0].to_dict()
    params = model_result.params[0].to_dict()
    ste = model_result.bse[0].to_dict()

    constants = {
        'p_value': p_values.get('Intercept'),
        't_value': t_values.get('Intercept'),
        'coefficient': params.get('Intercept'),
        'standard_error': ste.get('Intercept')
    }

    regression_field_properties = {
        'p_value': p_values,
        't_value': t_values,
        'coefficient': params,
        'standard_error': ste
    }

    total_regression_properties = {
        'aic': model_result.aic,
        'bic': model_result.bic,
        'r_squared': model_result.prsquared,
        'r_squared_adj': model_result.prsquared,
        'llf': model_result.llf,
        'llnull': model_result.llnull,
        'llr_pvalue': model_result.llr_pvalue
        # 'f_test': model_result.f_test
    }

    regression_results = restructure_field_properties_dict(constants, regression_field_properties, total_regression_properties)

    return regression_results


def run_polynomial_regression():
    return


def restructure_field_properties_dict(constants, regression_field_properties, total_regression_properties):
    # Restructure field properties dict from
    # { property: { field: value }} -> [ field: field, properties: { property: value } ]

    categorical_field_values = {}
    properties_by_field_dict = {}

    for prop_type, field_names_and_values in regression_field_properties.iteritems():
        for field_name, value in field_names_and_values.iteritems():
            if field_name in properties_by_field_dict:
                properties_by_field_dict[field_name][prop_type] = value
            else:
                properties_by_field_dict[field_name] = { prop_type: value }

    properties_by_field_collection = []
    for field_name, properties in properties_by_field_dict.iteritems():
        new_doc = {
            'field': field_name
        }
        base_field, value_field = _get_fields_categorical_variable(field_name)
        new_doc['base_field'] = base_field
        new_doc['value_field'] = value_field
        new_doc.update(properties)

        # Update list mapping categorical fields to values
        if value_field:
            if base_field not in categorical_field_values:
                categorical_field_values[base_field] = [ value_field ]
            else:
                categorical_field_values[base_field].append(value_field)

        properties_by_field_collection.append(new_doc)

    return {
        'constants': constants,
        'categorical_field_values': categorical_field_values,
        'properties_by_field': properties_by_field_collection,
        'total_regression_properties': total_regression_properties
    }


def _get_fields_categorical_variable(s):
    '''
    Parse base and value fields out of statsmodels categorical encoding
    e.g.
        1) 'department[T.Engineering]' -> [ department, Engineering ]
        2) 'department[T.Engineering]:gender[T.Male]' -> [ department:gender, Engineering:Male ]
    '''
    base_field = s
    value_field = None

    logger.info(s)
    bracket_count = s.count('[')
    colon_count = s.count(':')
    if bracket_count:
        if bracket_count == 1:
            if colon_count == 0:
                base_field = s.split('[')[0]
                value_field = s.split('[T.')[1].strip(']')
            elif colon_count == 1:
                q_first = (s.split(':')[0].count('[') == 0)
                if q_first:
                    q_field, full_c_field = s.split(':')
                    base_field = '%s:%s' % (q_field, full_c_field.split('[')[0])
                else:
                    full_c_field, q_field = s.split(':')
                    base_field = '%s:%s' % (full_c_field.split('[')[0], q_field)
                value_field = s.split('[T.')[1].strip(']')
                logger.info('%s: %s', base_field, value_field)

        elif bracket_count == 2:
            first_term, second_term = s.split(':')
            base_field = '%s:%s' % (first_term.split('[')[0], second_term.split('[')[0])
            value_field = '%s:%s' % (first_term.split('[T.')[1].strip(']'), second_term.split('[T.')[1].strip(']'))

    return base_field, value_field


def format_results(model_results, dependent_variable, independent_variable_names, considered_independent_variables_per_model, interaction_terms):
    # Initialize returned data structures
    # independent_variable_names = [ iv['name'] for iv in independent_variables ]
    regression_results = {
        'regressions_by_column': [],
        'independent_variable_ids': []
    }

    independent_variable_names, independent_variable_ids = get_field_names_from_considered_independent_variables(considered_independent_variables_per_model)
    
    print 'ind var', independent_variable_ids

    regression_fields_dict = OrderedDict([(ivn, None) for ivn in independent_variable_names ])

    for model_result, considered_independent_variables in zip(model_results, considered_independent_variables_per_model):
        # Move categorical field values to higher level
        for field_name, field_values in model_result['categorical_field_values'].iteritems():
            regression_fields_dict[field_name] = field_values

        # Test regression
        regression_stats = None
        if model_result['total_regression_properties'].get('resid'):
            dep_data = df[dependent_variable['name']]
            regression_stats = test_regression_fit(model_result['total_regression_properties']['resid'], dep_data)

        # Format results
        field_names = []
        for civ in considered_independent_variables:
            if type(civ) is list:
                field = [ var['name'] for var in civ ]
                field_names.append(':'.join(field))
            else:
                field_names.append(civ['name'])

        regression_result = {
            'regressed_fields': field_names,
            'regression': {
                'constants': model_result['constants'],
                'properties_by_field': model_result['properties_by_field']
            },
            'column_properties': model_result['total_regression_properties']
        }
        regression_results['regressions_by_column'].append(regression_result)
        if regression_stats:
            regression_result['regression']['stats'] = regression_stats

    regression_results['num_columns'] = len(model_results)

    # Convert regression fields dict into collection
    regression_fields_collection = []
    for field, values in regression_fields_dict.iteritems():
        regression_fields_collection.append({
            'name': field,
            'values': values
        })

    for term in interaction_terms:
        formatted_interaction_term_string = '%s:%s' % (term[0]['name'], term[1]['name'])
        if formatted_interaction_term_string not in regression_fields_dict:
            regression_fields_collection.append({
                'name': formatted_interaction_term_string,
                'values': None
            })

    regression_results['fields'] = regression_fields_collection
    regression_results['independent_variable_ids'] = independent_variable_ids

    print 'returning regression results', regression_results

    return regression_results


def save_regression(spec, result, project_id):
    with task_app.app_context():
        existing_regression_doc = db_access.get_regression_from_spec(project_id, spec)
        if existing_regression_doc:
            db_access.delete_regression(project_id, existing_regression_doc['id'])
        result = replace_unserializable_numpy(result)
        inserted_regression = db_access.insert_regression(project_id, spec, result)
        return inserted_regression
