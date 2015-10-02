'''
Utility analysis functions (e.g. distance between columns, overlap)
'''
import os
import json
from itertools import combinations
from collections import OrderedDict  # Get unique elements of list while preserving order
from time import time
import numpy as np
import scipy.stats as stats
import math

from dive.data.access import get_data
from dive.tasks.ingestion.type_detection import get_field_types

# Return unique elements from list while maintaining order in O(N)
# http://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-python-whilst-preserving-order
def get_unique(li):
    return list(np.unique(li))

###
# Get bin specifier (e.g. bin edges) given a numeric vector
###
MAX_BINS = 20
def get_bin_edges(v, procedure='freedman'):
    v = v.tolist()
    # if procedure == 'freedman':
    IQR = np.subtract(*np.percentile(v, [75, 25]))
    bin_width = 2 * IQR * len(v)**(-1/3)
    num_bins = math.floor((max(v) - min(v)) / bin_width)
    num_bins = min(num_bins, MAX_BINS)

    # Incrementing max value by tiny amount to deal with np.digitize right edge
    # https://github.com/numpy/numpy/issues/4217
    eps = 0.0001
    old_max = max(v)
    new_max = old_max + eps
    v[v.index(old_max)] = new_max

    bin_edges = np.histogram(v, bins=num_bins)[1]
    return bin_edges


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


def compute_ontologies(project_id, datasets) :
    new_dataset_ids = [d['dataset_id'] for d in datasets]
    all_datasets = db_access.get_datasets(project_id)
    all_dataset_ids = [d['dataset_id'] for d in all_datasets]
    print "NEW: ", new_dataset_ids
    print "ALL: ", all_dataset_ids

    lengths_dict = {}
    raw_columns_dict = {}
    uniqued_dict = {}
    for d in all_datasets:
        dataset_id = d['dataset_id']

        print "\t\tReading file"
        df = get_data(project_id=project_id, dataset_id=dataset_id)
        header = df.columns.values

        print "\t\tGetting raw cols"
        raw_columns_dict[dataset_id] = [list(df[col]) for col in df]

        print "\t\tGetting unique cols"
        uniqued_dict[dataset_id] = [get_unique(df[col]) for col in df]

        print "\t\tGetting col lengths"
        lengths_dict[dataset_id] = [len(df[col]) for col in df]

    print "\tIterating through columns"

    overlaps = {}
    hierarchies = {}
    for dataset_id_a, dataset_id_b in combinations(all_dataset_ids, 2):

        if (dataset_id_a not in new_dataset_ids) and (dataset_id_b not in new_dataset_ids) :
            continue

        raw_cols_a = raw_columns_dict[dataset_id_a]
        raw_cols_b = raw_columns_dict[dataset_id_b]
        overlaps['%s\t%s' % (dataset_id_a, dataset_id_b)] = {}
        hierarchies['%s\t%s' % (dataset_id_a, dataset_id_b)] = {}

        for index_a, col_a in enumerate(raw_cols_a):
            for index_b, col_b in enumerate(raw_cols_b):
                unique_a, unique_b = uniqued_dict[dataset_id_a][index_a], uniqued_dict[dataset_id_b][index_b]
                d = get_distance(unique_a, unique_b)

                if d:
                    length_a, length_b = lengths_dict[dataset_id_a][index_a], lengths_dict[dataset_id_b][index_b]
                    h = get_hierarchy(length_a, length_b)

                    overlaps['%s\t%s' % (dataset_id_a, dataset_id_b)]['%s\t%s' % (index_a, index_b)] = d
                    hierarchies['%s\t%s' % (dataset_id_a, dataset_id_b)]['%s\t%s' % (index_a, index_b)] = h

                    if d > 0.25 :
                        ontology = {
                            'source_dataset_id': dataset_id_a,
                            'target_dataset_id': dataset_id_b,
                            'source_index': index_a,
                            'target_index': index_b,
                            'distance': d,
                            'hierarchy': h
                        }
                        oID = MI.upsertOntology(project_id, ontology)
    return overlaps, hierarchies


def get_ontologies(project_id, datasets):
    overlaps = {}
    hierarchies = {}

    ontologies = MI.getOntology({}, project_id)
    for ontology in ontologies:
        dataset_id_a = ontology['source_dataset_id']
        dataset_id_b = ontology['target_dataset_id']

        key = '%s\t%s' % (dataset_id_a, dataset_id_b)
        if key not in overlaps :
            overlaps[key] = {}
        if key not in hierarchies :
            hierarchies[key] = {}

        index_a = ontology['source_index']
        index_b = ontology['target_index']
        d = ontology['distance']
        h = ontology['hierarchy']
        overlaps['%s\t%s' % (dataset_id_a, dataset_id_b)]['%s\t%s' % (index_a, index_b)] = d
        hierarchies['%s\t%s' % (dataset_id_a, dataset_id_b)]['%s\t%s' % (index_a, index_b)] = h

    return overlaps, hierarchies
