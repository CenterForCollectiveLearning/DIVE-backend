'''
Module containing functions accepting a data frame and returning a single recommended model

Currently: LASSO and Greedy Forward R2
'''
import numpy as np
from patsy import dmatrices, ModelDesc, Term, LookupFactor, EvalFactor
import statsmodels.api as sm
from sklearn import linear_model

from dive.base.data.access import get_data
from dive.base.db import db_access

from dive.worker.statistics.regression import ModelRecommendationType as MRT, ModelCompletionType as MCT
from dive.worker.statistics.regression.pipelines import run_models, format_results, construct_models

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def get_initial_regression_model_recommendation(project_id, dataset_id, dependent_variable_id=None, recommendation_type=MRT.LASSO, table_layout=MCT.LEAVE_ONE_OUT):
    df = get_data(project_id=project_id, dataset_id=dataset_id)
    field_properties = db_access.get_field_properties(project_id, dataset_id)
    quantitative_field_properties = [ fp for fp in field_properties if fp['general_type'] == 'q']

    dependent_variable = next((f for f in field_properties if f['id'] == dependent_variable_id), None) \
        if dependent_variable_id \
        else np.random.choice(quantitative_field_properties, size=1)[0]

    independent_variables = [ fp for fp in field_properties \
        if (fp['general_type'] == 'q' and fp['name'] != dependent_variable['name'] and not fp['is_unique'])]

    recommendationTypeToFunction = {
        MRT.FORWARD_R2: forward_r2,
        MRT.LASSO: lasso,
    }

    result = recommendationTypeToFunction[recommendation_type](df, dependent_variable, independent_variables)

    return {
        'recommendation': True,
        'table_layout': table_layout,
        'recommendation_type': recommendation_type,
        'dependent_variable_id': dependent_variable['id'],
        'independent_variables_ids': [ x['id'] for x in result ],
    }


def forward_r2(df, dependent_variable, independent_variables, interaction_terms=[], model_limit=5):
    '''
    Return forward selection model based on r-squared. Returns full (last) model
    For now: linear model

    TODO Vary marginal threshold based on all other contributions
    '''
    regression_variable_combinations = []
    regression_type = 'linear'

    MARGINAL_THRESHOLD_PERCENTAGE = 0.1  # Need x * r2 of last model to include variable

    last_r2 = 0.0
    last_variable_set = []
    remaining_variables = independent_variables

    for number_considered_variables in range(0, len(independent_variables)):
        r2s = []
        for variable in remaining_variables:
            considered_variables = last_variable_set + [ variable ]

            considered_independent_variables_per_model, patsy_models = \
                construct_models(df, dependent_variable, considered_variables, interaction_terms, table_layout=MCT.ALL_VARIABLES.value)

            raw_results = run_models(df, patsy_models, dependent_variable, regression_type)
            formatted_results = format_results(raw_results, dependent_variable, independent_variables, considered_independent_variables_per_model, interaction_terms)

            r_squared_adj = formatted_results['regressions_by_column'][0]['column_properties']['r_squared']
            r2s.append(r_squared_adj)

        max_r2 = max(r2s)
        marginal_r2 = max_r2 - last_r2
        max_variable = remaining_variables[r2s.index(max_r2)]

        if marginal_r2 < (last_r2 * MARGINAL_THRESHOLD_PERCENTAGE):
            break

        last_r2 = max_r2
        last_variable_set.append(max_variable)
        remaining_variables.remove(max_variable)
        regression_variable_combinations.append(last_variable_set[:])  # Neccessary to make copy on each iteration

        if len(regression_variable_combinations) > model_limit:
            break

    largest_variable_set = regression_variable_combinations[-1]
    return largest_variable_set


def lasso(df, dependent_variable, independent_variables, interaction_terms=[], model_limit=5):
    considered_independent_variables_per_model, patsy_models = \
        construct_models(df, dependent_variable, independent_variables, interaction_terms, table_layout=MCT.ALL_VARIABLES.value)
    full_patsy_model = patsy_models[0]

    y, X = dmatrices(full_patsy_model, df, return_type='dataframe')

    clf = linear_model.Lasso(alpha = 0.1)
    clf.fit(X, y)
    fit_coef = clf.coef_
    column_means = np.apply_along_axis(np.mean, 1, X)

    selected_variables = [ independent_variable for (i, independent_variable) in enumerate(independent_variables) if ( abs(fit_coef[i]) >= column_means[i] ) ]

    return selected_variables
