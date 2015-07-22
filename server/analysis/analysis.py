'''
Utility analysis functions (e.g. distance between columns, overlap)
'''
import os
import json
from itertools import combinations
from collections import OrderedDict  # Get unique elements of list while preserving order
from data.db import MongoInstance as MI
from data.access import get_data, upload_file, get_column_types, get_delimiter, is_numeric
from time import time
import numpy as np
import scipy.stats as stats


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


# Compute properties of all passed datasets
# Currently only getting properties by column
# Arguments: pID + dataset documents
# Returns a mapping from dIDs to properties
def compute_properties(pID, dataset_docs):
    properties_by_dID = {}

    for dataset in dataset_docs:
        property_dict = {
            'types': [],
            'label': [],
            'values': [],
            'unique': [],
            'normality': {},
            'stats': {},
            'misc': {}
        }
        dID = dataset['dID']
        df = get_data(pID=pID, dID=dID)
        df = df.fillna('')

        labels = df.columns.values
        property_dict['label'] = labels.tolist()

        print "Calculating properties for dID", dID
        # Statistical properties
        # Only conduct on certain types?
        print "\tDescribing datasets"
        df_stats = df.describe()
        df_stats_dict = json.loads(df_stats.to_json())
        df_stats_list = []
        for l in labels:
            if l in df_stats_dict:
                df_stats_list.append(df_stats_dict[l])
            else:
                df_stats_list.append({})
        property_dict['stats'] = df_stats_dict

        ### Getting column types
        print "\tGetting types"
        types = get_column_types(df)
        property_dict['types'] = types
    
        ### Determining normality
        print "\tDetermining normality"
        start_time = time()
        normality = []
        for i, col in enumerate(df):
            type = types[i]
            print "TYPE:", type
            if type in ["int", "float"]:
                try:
                    ## Coerce data vector to float
                    d = df[col].astype(np.float)
                    normality_result = stats.normaltest(d)
                except ValueError:
                    normality_result = None                    
            else:
                normality_result = None
            normality.append(normality_result)

        property_dict['normality'] = normality
        print "\t\t", time() - start_time, "seconds"
    
        ### Detecting if a column is unique
        print "\tDetecting uniques"
        start_time = time()
        # List of booleans -- is a column composed of unique elements?
        unique = [ detect_unique_list(df[col]) for col in df ]
        property_dict['unique'] = unique
        print "\t\t", time() - start_time, "seconds"

        ### Unique values for columns
        print "\tGetting unique values"
        start_time = time()
        unique_values = []
        raw_uniqued_values = [ get_unique(df[col]) for col in df ]
        for i, col in enumerate(raw_uniqued_values):
            type = types[i]
            if type in ["integer", "float"]:
                unique_values.append([])
            else:
                unique_values.append(col)
        property_dict['values'] = unique_values
        print "\t\t", time() - start_time, "seconds"

        # Save properties into collection
        tID = MI.upsertProperty(dID, pID, property_dict)

        properties_by_dID[dID] = property_dict
    return properties_by_dID

# Get properties given dataset_docs
# Either retrieve from DB or, if not available, calculate
# TODO Accept list of dIDs
def get_properties(pID, datasets) :

    # Try to retrieve from DB, then format into dict keyed by dID
    find_doc = {"$or" : map(lambda x: {'dID' : x['dID']}, datasets)}
    properties_list = MI.getProperty(find_doc, pID)
    if len(properties_list):
        properties_by_dID = {}
        for p in properties_list:
            dID = p['dID']
            del p['dID']
            del p['tID']
            properties_by_dID[dID] = p

    # If not in DB, compute
    else:
        properties_by_dID = compute_properties(pID, datasets)

    return properties_by_dID


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


def compute_ontologies(pID, datasets) :
    new_dIDs = [d['dID'] for d in datasets]
    all_datasets = MI.getData({}, pID)
    all_dIDs = [d['dID'] for d in all_datasets]
    print "NEW: ", new_dIDs
    print "ALL: ", all_dIDs

    lengths_dict = {}
    raw_columns_dict = {}
    uniqued_dict = {}
    for d in all_datasets:
        dID = d['dID']

        print "\t\tReading file"
        df = get_data(pID=pID, dID=dID)
        header = df.columns.values

        print "\t\tGetting raw cols"
        raw_columns_dict[dID] = [list(df[col]) for col in df]

        print "\t\tGetting unique cols"
        uniqued_dict[dID] = [get_unique(df[col]) for col in df]

        print "\t\tGetting col lengths"
        lengths_dict[dID] = [len(df[col]) for col in df]

    print "\tIterating through columns"
    
    overlaps = {}
    hierarchies = {}
    for dID_a, dID_b in combinations(all_dIDs, 2):

        if (dID_a not in new_dIDs) and (dID_b not in new_dIDs) :
            continue
        
        raw_cols_a = raw_columns_dict[dID_a]
        raw_cols_b = raw_columns_dict[dID_b]
        overlaps['%s\t%s' % (dID_a, dID_b)] = {}
        hierarchies['%s\t%s' % (dID_a, dID_b)] = {}

        for index_a, col_a in enumerate(raw_cols_a):
            for index_b, col_b in enumerate(raw_cols_b):
                unique_a, unique_b = uniqued_dict[dID_a][index_a], uniqued_dict[dID_b][index_b]
                d = get_distance(unique_a, unique_b)

                if d:
                    length_a, length_b = lengths_dict[dID_a][index_a], lengths_dict[dID_b][index_b]
                    h = get_hierarchy(length_a, length_b)

                    overlaps['%s\t%s' % (dID_a, dID_b)]['%s\t%s' % (index_a, index_b)] = d
                    hierarchies['%s\t%s' % (dID_a, dID_b)]['%s\t%s' % (index_a, index_b)] = h

                    if d > 0.25 :
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


def get_ontologies(pID, datasets) :
    overlaps = {}
    hierarchies = {}

    ontologies = MI.getOntology({}, pID)
    for ontology in ontologies:
        dID_a = ontology['source_dID']
        dID_b = ontology['target_dID']

        key = '%s\t%s' % (dID_a, dID_b)
        if key not in overlaps :
            overlaps[key] = {}
        if key not in hierarchies :
            hierarchies[key] = {}

        index_a = ontology['source_index']
        index_b = ontology['target_index']
        d = ontology['distance']
        h = ontology['hierarchy']
        overlaps['%s\t%s' % (dID_a, dID_b)]['%s\t%s' % (index_a, index_b)] = d
        hierarchies['%s\t%s' % (dID_a, dID_b)]['%s\t%s' % (index_a, index_b)] = h

    return overlaps, hierarchies
