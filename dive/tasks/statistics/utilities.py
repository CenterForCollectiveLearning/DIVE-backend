from scipy import stats


def variations_equal(THRESHOLD, *args):
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
