from collections import Counter, OrderedDict

def get_variable_type_counts(dependent_variables, independent_variables):
    '''
    Return count of C, T, Q variables
    '''
    variable_types = Counter({
        'independent': { 'q': 0, 'c': 0, 't': 0 },
        'dependent': { 'q': 0, 'c': 0, 't': 0 }
    })

    for dependent_variable in dependent_variables:
        dependent_variable_type = dependent_variable['general_type']
        variable_types['dependent'][dependent_variable_type] += 1

    for independent_variable in independent_variables:
        independent_variable_type = independent_variable['general_type']
        variable_types['independent'][independent_variable_type] += 1

    return variable_types


def get_full_field_documents_from_field_names(all_fields, names):
    fields = []
    for name in names:
        matched_field = next((f for f in all_fields if f['name'] == name), None)
        if matched_field:
            fields.append(matched_field)
    return fields


def rvc_contains_all_interaction_variables(interaction_term, regression_variable_combination):
    matches = 0

    for variable in regression_variable_combination:
        for term in interaction_term:
            if variable['name'] == term['name']:
                matches += 1

    return matches == len(interaction_term)

def get_field_names_from_considered_independent_variables(independent_variables):
    variable_names = []
    variable_ids = []
    for combinations in independent_variables:
        for variable in combinations:
            variable_names.append(variable['name'])
            variable_ids.append(variable['id'])

    #get rid of duplicates
    return list(OrderedDict.fromkeys(variable_names)), list(OrderedDict.fromkeys(variable_ids))

def create_variable_combinations(independent_variables):
    regression_variable_combinations = []

    if len(independent_variables) == 2:
        for i, considered_field in enumerate(independent_variables):
            regression_variable_combinations.append([ considered_field ])
    if len(independent_variables) > 2:
        for i, considered_field in enumerate(independent_variables):
            all_fields_except_considered_field = independent_variables[:i] + independent_variables[i+1:]
            regression_variable_combinations.append(all_fields_except_considered_field)
    regression_variable_combinations.append(independent_variables)

    return regression_variable_combinations
