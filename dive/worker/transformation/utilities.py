import os

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


def get_transformed_file_name(directory, prefix, fallback_title, original_dataset_title, dataset_type):
    MAX_PATH_CHARS = 255
    title = original_dataset_title
    name = title + dataset_type
    path = os.path.join(directory, name)

    if path > MAX_PATH_CHARS:
        title = '%s %s' % (prefix, fallback_title)
        name = title + dataset_type
        path = os.path.join(directory, name)

    if os.path.exists(path):
        file_number = 0
        while os.path.exists(path):
            title = '%s %s_%s' % (prefix, original_dataset_title, file_number)
            name = '%s%s' % (title, dataset_type)
            path = os.path.join(directory, name)
            file_number = file_number + 1
    return title, name, path
