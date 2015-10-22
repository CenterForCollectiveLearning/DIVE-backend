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
