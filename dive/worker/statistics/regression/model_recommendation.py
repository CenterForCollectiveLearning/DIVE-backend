'''
Module containing functions accepting a data frame and returning a single recommended model

Currently: LASSO and Greedy Forward R2
'''
import numpy as np
from patsy import dmatrices, ModelDesc, Term, LookupFactor, EvalFactor
import statsmodels.api as sm
from sklearn import linear_model
from sklearn.svm import SVR
from sklearn.feature_selection import RFE

from dive.base.data.access import get_data
from dive.base.db import db_access

from dive.worker.statistics.regression import ModelRecommendationType as MRT, ModelCompletionType as MCT
from dive.worker.statistics.regression.pipelines import run_models, format_results, construct_models

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def get_initial_regression_model_recommendation(project_id, dataset_id, dependent_variable_id=None, recommendation_type=MRT.LASSO.value, table_layout=MCT.LEAVE_ONE_OUT.value):
    df = get_data(project_id=project_id, dataset_id=dataset_id)
    field_properties = db_access.get_field_properties(project_id, dataset_id)
    quantitative_field_properties = [ fp for fp in field_properties if fp['general_type'] == 'q']

    dependent_variable = next((f for f in field_properties if f['id'] == dependent_variable_id), None) \
        if dependent_variable_id \
        else np.random.choice(quantitative_field_properties, size=1)[0]

    independent_variables = [ fp for fp in field_properties \
        if (not (fp['general_type'] == 'c' and fp['is_unique']) and fp['name'] != dependent_variable['name'])]

    recommendationTypeToFunction = {
        MRT.FORWARD_R2.value: forward_r2,
        MRT.LASSO.value: lasso,
        MRT.RFE.value: recursive_feature_elimination,
        MRT.FORWARD_F.value: f_regression
    }

    result = recommendationTypeToFunction[recommendation_type](df, dependent_variable, independent_variables)

    return {
        'recommended': True,
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
    selected_variables = []
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
        selected_variables = last_variable_set[:]  # Neccessary to make copy on each iteration

        if len(selected_variables) >= model_limit:
            break

    return selected_variables


def recursive_feature_elimination(df, dependent_variable, independent_variables, interaction_terms=[], model_limit=5):
    print [ x['name'] for x in independent_variables ]
    considered_independent_variables_per_model, patsy_models = \
        construct_models(df, dependent_variable, independent_variables, interaction_terms, table_layout=MCT.ALL_VARIABLES.value)
    y, X = dmatrices(patsy_models[0], df, return_type='dataframe')

    estimator = SVR(kernel='linear')
    selector = RFE(estimator, 5, step=1)
    selector = selector.fit(X, y)
    print selector.support_
    print selector.ranking_
    return

def f_regression(df, dependent_variable, independent_variables, interaction_terms=[], model_limit=5):
    return


def lasso(df, dependent_variable, independent_variables, interaction_terms=[], model_limit=5):
    print [ x['name'] for x in independent_variables ]
    considered_independent_variables_per_model, patsy_models = \
        construct_models(df, dependent_variable, independent_variables, interaction_terms, table_layout=MCT.ALL_VARIABLES.value)
    print patsy_models
    y, X = dmatrices(patsy_models[0], df, return_type='dataframe')

    clf = linear_model.Lasso(
        alpha = 1.0,
        normalize=True
    )
    clf.fit(X, y)
    fit_coef = clf.coef_
    column_means = np.apply_along_axis(np.mean, 1, X)

    selected_variables = [ independent_variable for (i, independent_variable) in enumerate(independent_variables) if ( abs(fit_coef[i]) >= column_means[i] ) ]

    return selected_variables
