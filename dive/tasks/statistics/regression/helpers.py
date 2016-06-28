from collections import Counter

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
