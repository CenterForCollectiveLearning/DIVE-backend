'''
Utility analysis functions (e.g. distance between columns, overlap)
'''
import os
import json
from data import *
from itertools import combinations
from collections import OrderedDict  # Get unique elements of list while preserving order
from db import MongoInstance as MI


# Detect if a list is comprised of unique elements
def detect_unique_list(l):
    THRESHOLD = 0.90
    if (len(set(l)) / float(len(l))) >= THRESHOLD:
        return True
    return False


# Find the distance between two lists
# Currently naively uses intersection over union of unique lists
def get_distance(l1, l2):
    s1, s2 = set(l1), set(l2)
    d = float(len(s1.intersection(s2))) / len(s1.union(s2))
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


types_dict = {}
headers_dict = {}
raw_columns_dict = {}
is_unique_dict = {}
uniqued_columns_dict = {}
stats_dict = {}
# Compute properties of all passed datasets
# Arguments: pID + list of dIDs
# Returns a mapping from dIDs to properties
def compute_properties(pID, datasets):
    stats_dict = {}
    types_dict = {}
    headers_dict = {}
    is_unique_dict = {}
    for dataset in datasets:
        dID = dataset['dID']
        path = dataset['path']
        # sheet = dataset['sheet']
        header, columns = read_file(path)
        print type(columns)
        data = {}
        for i in range(len(header)) :
            field = header[i]
            # print field
            # print data.keys()
            # print columns[i]
            data[field] = columns[i]
        df = pd.DataFrame(data)

        # delim = get_delimiter(path)

        # Statistical properties
        # df = pd.read_table(path, sep=delim)
        df_stats = df.describe()
        df_stats_dict = json.loads(df_stats.to_json())
        stats_dict[dID] = df_stats_dict
    
        # Replace nan
        # entropy 
        # gini
    
        # List of booleans -- is a column composed of unique elements?
        is_unique = [ detect_unique_list(col) for col in columns ]
        types = get_column_types(path)

        # Save properties into collection
        dataset_properties = {
            'types': types,
            'uniques': is_unique,
            'headers': header,
            'stats': df_stats_dict
        }
        types_dict[dID] = dataset_properties['types']
        headers_dict[dID] = dataset_properties['headers']
        is_unique_dict[dID] = dataset_properties['uniques']
        stats_dict[dID] = dataset_properties['stats']
        tID = MI.upsertProperty(dID, pID, dataset_properties)

    return stats_dict, types_dict, headers_dict, is_unique_dict


# Argument: pID + list of dIDs
def compute_ontologies(pID, datasets):
    dIDs = [d['dID'] for d in datasets]

    # Get data (TODO: abstract this)
    raw_columns_dict = {}
    uniqued_columns_dict = {}
    for d in datasets:
        dID = d['dID']
        path = d['path']
        # sheet = d['sheet']
        header, columns = read_file(path)
        raw_columns_dict[dID] = [list(col) for col in columns]
        uniqued_columns_dict[dID] = [get_unique(col) for col in columns]

    overlaps = {}
    hierarchies = {}
    # TODO Create a tighter loop to avoid double computes
    # TODO Make agnostic to ordering of pair
    for dID_a, dID_b in combinations(dIDs, 2):
        print dID_a, dID_b
        raw_cols_a = raw_columns_dict[dID_a]
        raw_cols_b = raw_columns_dict[dID_b]
        uniqued_cols_a = uniqued_columns_dict[dID_a]
        uniqued_cols_b = uniqued_columns_dict[dID_b]
        overlaps['%s\t%s' % (dID_a, dID_b)] = {}
        hierarchies['%s\t%s' % (dID_a, dID_b)] = {}

        for index_a, col_a in enumerate(raw_cols_a):
            for index_b, col_b in enumerate(raw_cols_b):
                h = get_hierarchy(col_a, col_b)
                d = get_distance(col_a, col_b)
                if d:
                    overlaps['%s\t%s' % (dID_a, dID_b)]['%s\t%s' % (index_a, index_b)] = d
                    hierarchies['%s\t%s' % (dID_a, dID_b)]['%s\t%s' % (index_a, index_b)] = h
                    # TODO How do you store this?
                    ontology = {
                        'source_dID': dID_a,
                        'target_dID': dID_b,
                        'source_index': index_a,
                        'target_index': index_b,
                        'distance': d,
                        'hierarchy': h
                    }
                    oID = MI.upsertOntology(pID, ontology)
    return overlaps, hierarchies