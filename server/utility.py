'''
Home to our lovely utility functions
'''

import re
import pandas as pd
from collections import OrderedDict  # Get unique elements of list while preserving order


# Utility function to detect extension and return delimiter
def get_delimiter(path):
    f = open(path)
    filename = path.rsplit('/')[-1]
    extension = filename.rsplit('.', 1)[1]
    if extension == 'csv':
        delim = ','
    elif extension == 'tsv':
        delim = '\t'
    return delim


# function to filter uploaded files
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


# Return list of dataset and column unique IDs
# Currently datasets and attributes are just numbers (UUIDs are overkill)
def get_canonical_form(path):
    # TODO Abstract this file reading
    f = open(path)
    filename = path.rsplit('/')[-1]
    extension = filename.rsplit('.', 1)[1]
    delim = get_delimiter(path)
    l = f.readline().split(delim)
    cols = [i for i in range(0,len(l))]

    return cols


INT_REGEX = "^-?[0-9]+$"
FLOAT_REGEX = "[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?"
# Utility function to get the type of a variable
# TODO: Parse dates
def get_variable_type(v):
    if re.match(INT_REGEX, v): r = "int"
    elif re.match(FLOAT_REGEX, v): r = "float"
    else: r = "str"
    return r


# Detect if a list is comprised of unique elements
def detect_unique_list(l):
    return (len(set(l)) == len(l))


# TODO Strip new lines and quotes
def read_file(path, delim):
    df = pd.read_table(path, sep=delim)
    columns = []

    # TODO Is this the right thing to do?
    for col in df:
        columns.append(df[col])

    header = list(df.columns.values)
    return header, columns
    # lens = []
    # for line in lines:
    #     lens.append(len(line.split(delim)))
    # print min(lens)
    # row_matrix = [line.split(delim) for line in lines]
    # column_matrix = zip(*row_matrix)
    # return column_matrix


# Find the distance between two lists
# Currently naively uses intersection over union of unique lists
def get_distance(l1, l2):
    s1, s2 = set(l1), set(l2)
    d = float(len(s1.intersection(s2))) / len(s1.union(s2))
    # d = float(len(s1 ^ s2)) / len(s1 | s2)

    return d


# Find if a relationship is one-to-one or one-to-many
# Currently naively compares length of lists
def get_hierarchy(l1, l2):
    if len(l1) > len(l2):
        res = "N1"
    elif len(l1) == len(l2):
        res = "11"
    else:
        res = "1N"
    return res


# Return unique elements from list while maintaining order in O(N)
# http://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-python-whilst-preserving-order
def get_unique(li):
    return list(OrderedDict.fromkeys(li))


# Utility function to get a list of column types in a dataset given a file path
# TODO Check if header
# TODO Use some scheme of parsing such that they aren't all strings
def get_column_types(path):
    f = open(path)
    header = f.readline()
    sample_line = f.readline()
    extension = path.rsplit('.', 1)[1]
    delim = get_delimiter(path)

    types = [get_variable_type(v) for v in sample_line.split(delim)]
    return types


# function to get sample from data file
def get_sample_data(path):
    f = open(path)
    filename = path.rsplit('/')[-1]
    extension = filename.rsplit('.', 1)[1]
    header = f.readline()
    rows = 0
    cols = 0

    sample = {}
    for i in range(5):
        line = f.readline()
        if not line:
            break
        else:
            delim = get_delimiter(path)
            sample[i] = [item.strip() for item in line.split(delim)]
            cols = max(cols, len(sample[i]))

    with open(path) as f:
        for rows, l in enumerate(f):
            pass
    rows += 1

    # Parse header
    header = header.split(delim)

    return sample, rows, cols, extension, header