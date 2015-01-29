'''
Utility analysis functions (e.g. distance between columns, overlap)
'''
import os
import json
from data import *
from itertools import combinations
from collections import OrderedDict  # Get unique elements of list while preserving order
from db import MongoInstance as MI
from time import time
import numpy as np


# Detect if a list is comprised of unique elements
def detect_unique_list(l):
    # TODO Vary threshold by number of elements (be smarter about it)
    THRESHOLD = 0.95

    # Comparing length of uniqued elements with original list
    if (len(np.unique(l)) / float(len(l))) >= THRESHOLD:
        return True
    return False


# Return unique elements from list while maintaining order in O(N)
# http://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-python-whilst-preserving-order
def get_unique(li):
    return list(np.unique(li))


# Either pull computed properties from the DB, or compute from scratch
def get_properties(pID, datasets):
    dIDs = [d['dID'] for d in datasets]
    properties = MI.getProperty({'dID': {'$in': dIDs}}, pID)

    if properties:
        print "\tUsing computed properties"
        types_dict = {}
        headers_dict = {}
        is_unique_dict = {}
        stats_dict = {}
        for p in properties:
            dID = p['dID']
            types_dict[dID] = p['types']
            headers_dict[dID] = p['headers']
            is_unique_dict[dID] = p['uniques']
            stats_dict[dID] = p['stats']
        return stats_dict, types_dict, headers_dict, is_unique_dict
    else:
        print "\tComputing properties"
        return compute_properties(pID, datasets)


# Either pull computed ontologies from the DB, or compute from scratch
def get_ontologies(pID, datasets):
    dIDs = [d['dID'] for d in datasets]
    ontologies = MI.getOntology({'$or': [{'source_dID': {'$in': dIDs}}, {'target_dID': {'$in': dIDs}}]}, pID)

    if ontologies:
        print "\tUsing computed ontologies"
        overlaps = {}
        hierarchies = {}
        for source_dID, target_dID in combinations(dIDs, 2):
            overlaps['%s\t%s' % (source_dID, target_dID)] = {}
            hierarchies['%s\t%s' % (source_dID, target_dID)] = {}

        for o in ontologies:
            source_dID, target_dID, source_index, target_index, d, h = \
                o['source_dID'], o['target_dID'], o['source_index'], o['target_index'], o['distance'], o['hierarchy']
            overlaps['%s\t%s' % (source_dID, target_dID)]['%s\t%s' % (source_index, target_index)] = d
            hierarchies['%s\t%s' % (source_dID, target_dID)]['%s\t%s' % (source_index, target_index)] = h
        return overlaps, hierarchies
    else:
        print "\tComputing ontologies"
        return compute_ontologies(pID, datasets)


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
        header, df = read_file(path)

        # Statistical properties
        print "\tDescribing datasets"
        df_stats = df.describe()
        df_stats_dict = json.loads(df_stats.to_json())
        stats_dict[dID] = df_stats_dict
    
        # Replace nan
        # entropy 
        # gini
    
        print "\tDetecting uniques"
        start_time = time()
        # List of booleans -- is a column composed of unique elements?
        is_unique = [ detect_unique_list(df[col]) for col in df ]
        print "\t\t", time() - start_time, "seconds"
        print "\tGetting types"
        types = get_column_types(df)

        # Save properties into collection
        dataset_properties = {
            'types': types,
            'uniques': is_unique,
            'headers': list(header),
            'stats': df_stats_dict
        }

        types_dict[dID] = dataset_properties['types']
        headers_dict[dID] = dataset_properties['headers']
        is_unique_dict[dID] = dataset_properties['uniques']
        stats_dict[dID] = dataset_properties['stats']
        tID = MI.upsertProperty(dID, pID, dataset_properties)

    return stats_dict, types_dict, headers_dict, is_unique_dict


# Find the distance between two sets
# Currently naively uses Jaccard distance between two sets
def get_distance(list_a, list_b):
    set_a, set_b = set(list_a), set(list_b)
    return float(len(set_a.intersection(set_b))) / len(set_a.union(set_b))


def get_hierarchy(l1, l2):
    if (l1 > l2):
        res = "N1"
    elif (l1 == l2):
        res = "11"
    else:
        res = "1N"
    return res


# Argument: pID + list of dIDs
def compute_ontologies(pID, datasets):
    dIDs = [d['dID'] for d in datasets]

    print "\tPopulating dictionaries"
    # Get data (TODO: abstract this)
    lengths_dict = {}
    raw_columns_dict = {}
    uniqued_dict = {}
    for d in datasets:
        dID = d['dID']
        path = d['path']
        print "\t\tReading file"
        header, df = read_file(path)

        print "\t\tGetting raw cols"
        raw_columns_dict[dID] = [list(df[col]) for col in df]

        print "\t\tGetting unique cols"
        uniqued_dict[dID] = [get_unique(df[col]) for col in df]

        print "\t\tGetting col lengths"
        lengths_dict[dID] = [len(df[col]) for col in df]

    print "\tIterating through columns"
    overlaps = {}
    hierarchies = {}
    for dID_a, dID_b in combinations(dIDs, 2):
        # print dID_a, dID_b
        raw_cols_a = raw_columns_dict[dID_a]
        raw_cols_b = raw_columns_dict[dID_b]
        overlaps['%s\t%s' % (dID_a, dID_b)] = {}
        hierarchies['%s\t%s' % (dID_a, dID_b)] = {}

        for index_a, col_a in enumerate(raw_cols_a):
            for index_b, col_b in enumerate(raw_cols_b):
                # print '\t', index_a, index_b

                unique_a, unique_b = uniqued_dict[dID_a][index_a], uniqued_dict[dID_b][index_b]
                d = get_distance(unique_a, unique_b)

                if d:
                    length_a, length_b = lengths_dict[dID_a][index_a], lengths_dict[dID_b][index_b]
                    h = get_hierarchy(length_a, length_b)

                    overlaps['%s\t%s' % (dID_a, dID_b)]['%s\t%s' % (index_a, index_b)] = d
                    hierarchies['%s\t%s' % (dID_a, dID_b)]['%s\t%s' % (index_a, index_b)] = h

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