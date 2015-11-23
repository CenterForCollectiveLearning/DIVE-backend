def list_elements_from_indices(li, indices):
    if not (type(indices) is list):
        indices = [ indices ]
    return [ li[i] for i in indices ]


def difference_of_lists(li1, li2):
    # Returns difference of two shallow lists in order
    s1 = set(li1)
    diff1 = [x for x in li2 if x not in s1]
    s2 = set(li2)
    diff2 = [x for x in li1 if x not in s2]
    if len(diff1) > len(diff2):
        return diff1
    elif len(diff1) == len(diff2):
        return diff1
    else:
        return diff2
