import numpy as np

# Return unique elements from list while maintaining order in O(N)
# http://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-python-whilst-preserving-order
def get_unique(li, preserve_order=False):
    if preserve_order:
        seen = set()
        seen_add = seen.add
        return [x for x in li if not (x in seen or seen_add(x))]
    else:
        return list(np.unique(li))
