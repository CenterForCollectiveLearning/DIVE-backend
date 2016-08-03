from scipy import stats
from patsy import dmatrices, ModelDesc, Term, LookupFactor, EvalFactor

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def get_design_matrices(df, dependent_variable, independent_variables, interactions=[]):
    patsy_model = create_patsy_model(dependent_variable, independent_variables, interactions=interactions)
    y, X = dmatrices(patsy_model, df, return_type='dataframe')
    return (y, X)


def create_patsy_model(dependent_variable, independent_variables, interactions=[]):
    '''
    Construct and return patsy formula (object representation)
    '''

    # 1) Handling passing in [{'name': X}] vs [X]
    lhs_var = dependent_variable
    rhs_vars = independent_variables
    if 'name' in dependent_variable:
        lhs_var = dependent_variable['name']

    if 'name' in independent_variables[0]:
        new_rhs_vars = []
        for iv in independent_variables:
            if type(iv) is list:
                new_rhs_vars.append([x['name'] for x in iv])
            else:
                if 'name' in iv:
                    new_rhs_vars.append(iv['name'])
                else:
                    new_rhs_vars.append(iv)
        rhs_vars = new_rhs_vars

    if interactions:
        first_interaction = interactions[0]
        if 'name' in first_interaction:
            new_interactions = []
            for interaction in interactions:
                new_interactions.append([term['name'] for term in interaction])
            rhs_interactions = new_interactions
        else:
            rhs_interactions = interactions

    # 2) Constructing model
    lhs = [ Term([LookupFactor(lhs_var)]) ]

    rhs = [ Term([]) ]
    for rhs_var in rhs_vars:
        if type(rhs_var) is list:
            rhs += [ Term([ LookupFactor(term) for term in rhs_var ]) ]
        else:
            rhs += [ Term([LookupFactor(rhs_var)]) ]

    if interactions:
        rhs += [ Term([ LookupFactor(term) for term in interaction ]) for interaction in rhs_interactions ]

    model = ModelDesc(lhs, rhs)
    logger.info(model)
    return model


def are_variations_equal(THRESHOLD, *args):
    '''
    Return a boolean, if p-value less than threshold, returns false
    '''
    return stats.levene(*args)[1] > THRESHOLD


def sets_normal(THRESHOLD, *args):
    '''
    If normalP is less than threshold, not considered normal
    '''
    normal = True;
    for arg in args:
        if len(arg) < 8:
            return False
        if stats.normaltest(arg)[1] < THRESHOLD:
            normal = False;

    return normal


def difference_of_two_lists(l1, l2):
    return [ x for x in l2 if x not in set(l1) ]


def make_safe_string(s):
    invalid_chars = '-_.+^$ '
    if not s.startswith('temp_name_'):
        for invalid_char in invalid_chars:
            s = s.replace(invalid_char, '_')
        s = 'temp_name_' + s
    return s
