#return a boolean, if p-value less than threshold, returns false
def variationsEqual(THRESHOLD, *args):
    return stats.levene(*args)[1] > THRESHOLD

#if normalP is less than threshold, not considered normal
def setsNormal(THRESHOLD, *args):
    normal = True;
    for arg in args:
        if stats.normaltest(arg)[1] < THRESHOLD:
            normal = False;

    return normal
