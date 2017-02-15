def one_at_a_time_and_all_but_one(df, dependent_variable, independent_variables, interaction_terms):
    regression_variable_combinations = []
    for independent_variable in independent_variables:
        regression_variable_combinations.append([ independent_variable ])

    all_but_one_regression_variable_combinations = all_but_one(df, dependent_variable, independent_variables, interaction_terms)
    regression_variable_combinations.extend(all_but_one_regression_variable_combinations)
    return regression_variable_combinations


def all_but_one(df, dependent_variable, independent_variables, interaction_terms):
    '''
    Return one model with all variables, and N-1 models with one variable left out

    Technically not a model
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


def all_variables(df, dependent_variable, independent_variables, interaction_terms):
    '''
    Returns model including all independent_variables
    '''
    regression_variable_combinations = [ independent_variables + interaction_terms ]
    return regression_variable_combinations
