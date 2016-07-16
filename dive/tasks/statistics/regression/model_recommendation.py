# from dive.tasks.statistics.utilities import create_patsy_model
import numpy as np
from patsy import dmatrices, ModelDesc, Term, LookupFactor, EvalFactor
import statsmodels.api as sm
from sklearn import linear_model

from dive.tasks.statistics.regression import ModelSelectionType as MST

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

def construct_models(df, dependent_variable, independent_variables, interaction_terms=None, selection_type=MST.ALL_BUT_ONE.value):
    '''
    Given dependent and independent variables, return list of patsy model.

    Classify into different systems:
        1) Whether data is involved
        2) Whether the final regressions are actually run in the process

    regression_variable_combinations = [ [x], [x, y], [y, z] ]
    models = [ ModelDesc(lhs=y, rhs=[x]), ... ]
    '''
    model_selection_name_to_function = {
        MST.ALL_BUT_ONE.value: all_but_one,
        MST.LASSO.value: lasso,
        MST.FORWARD_R2.value: forward_r2
    }

    model_selection_function = model_selection_name_to_function[selection_type]
    regression_variable_combinations = model_selection_function(df, dependent_variable, independent_variables, interaction_terms)

    patsy_models = convert_regression_variable_combinations_to_patsy_models(dependent_variable, regression_variable_combinations)

    return ( regression_variable_combinations, patsy_models )

def convert_regression_variable_combinations_to_patsy_models(dependent_variable, regression_variable_combinations):
    patsy_models = []
    for regression_variable_combination in regression_variable_combinations:
        model = create_patsy_model(dependent_variable, regression_variable_combination)
        patsy_models.append(model)

    return patsy_models

def create_patsy_model(dependent_variable, independent_variables):
    '''
    Construct and return patsy formula (object representation)

    TODO: Take both names and field documents
    '''
    lhs = [ Term([LookupFactor(dependent_variable['name'])]) ]
    rhs = [ Term([]) ]

    for iv in independent_variables:
        if type(iv) is list:
            desc = [ Term([LookupFactor(term['name']) for term in iv]) ]
            rhs += desc
        else:
            rhs += [ Term([LookupFactor(iv['name'])]) ]

    return ModelDesc(lhs, rhs)

def all_but_one(df, dependent_variable, independent_variables, interaction_terms, model_limit=8):
    '''
    Return one model with all variables, and N-1 models with one variable left out
    '''
    # Create list of independent variables, one per regression
    regression_variable_combinations = []
    if len(independent_variables) == 2:
        for i, considered_field in enumerate(independent_variables):
            regression_variable_combinations.append([ considered_field ])
    if len(independent_variables) > 2:
        for i, considered_field in enumerate(independent_variables):
            all_fields_except_considered_field = independent_variables[:i] + independent_variables[i+1:]
            regression_variable_combinations.append(all_fields_except_considered_field)
    regression_variable_combinations.append(independent_variables)

    combinations_with_interactions = []
    if interaction_terms:
        for rvc in regression_variable_combinations:
            for interaction_term in interaction_terms:
                if rvc_contains_all_interaction_variables(interaction_term, rvc):
                    new_combination = rvc[:]
                    new_combination.append(interaction_term)
                    combinations_with_interactions.append(new_combination)

    regression_variable_combinations = regression_variable_combinations + combinations_with_interactions
    
    return regression_variable_combinations


def forward_r2(df, dependent_variable, independent_variables, model_limit=8):
    '''
    Return forward selection model based on r-squared.

    For now: linear model
    '''
    regression_variable_combinations = []

    MARGINAL_THRESHOLD = 0.1

    last_r2 = 0.0
    last_variable_set = []
    remaining_variables = independent_variables

    for number_considered_variables in range(0, len(independent_variables)):
        r2s = []
        for variable in remaining_variables:
            considered_variables = last_variable_set + [ variable ]

            patsy_model = create_patsy_model(dependent_variable, considered_variables)
            y, X = dmatrices(patsy_model, df, return_type='dataframe')
            model_result = sm.OLS(y, X).fit()
            r_squared_adj = model_result.rsquared_adj
            r2s.append(r_squared_adj)

        max_r2 = max(r2s)
        marginal_r2 = max_r2 - last_r2
        max_variable = remaining_variables[r2s.index(max_r2)]

        if marginal_r2 < MARGINAL_THRESHOLD:
            break

        last_r2 = max_r2
        last_variable_set.append(max_variable)
        remaining_variables.remove(max_variable)

        regression_variable_combinations.append(last_variable_set[:])  # Neccessary to make copy on each iteration

    return regression_variable_combinations

def lasso(df, dependent_variable, independent_variables, model_limit=8):
    '''
    Return one model with all variables, and N-1 models with one variable left out
    '''
    # Create list of independent variables, one per regression
    regression_variable_combinations = []

    full_patsy_model = create_patsy_model(dependent_variable, independent_variables)
    y, X = dmatrices(full_patsy_model, df, return_type='dataframe')

    clf.fit(X, y)
    fit_coef = clf.coef_
    column_means = np.apply_along_axis(np.mean, 1, X)

    clf = linear_model.Lasso(alpha = 0.1)

    regression_variable_combination = []
    for i, independent_variable in enumerate(independent_variables):
        if abs(fit_coef[i]) >= column_means[i]:
            regression_variable_combination.append(independent_variable)
        print independent_variable['name'], fit_coef[i], (abs(fit_coef[i]) < column_means[i])
    regression_variable_combinations.append(regression_variable_combination)

    return regression_variable_combinations

def rvc_contains_all_interaction_variables(interaction_term, regression_variable_combination):
    matches = 0

    for variable in regression_variable_combination:
        for term in interaction_term:
            if variable['name'] == term['name']:
                matches += 1

    return matches == len(interaction_term)
