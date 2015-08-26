import json
import numpy as np

from time import time

from data.db import MongoInstance as MI
from data.access import get_data, get_column_types
from analysis.analysis import get_unique
from scipy import stats as sc_stats

# Retrieve proeprties given dataset_docs
# TODO Accept list of dIDs
def get_properties(pID, datasets, get_values = False) :
    aggregatedProperties = []
    _property_labels = []

    _find_doc = {"$or" : map(lambda x: {'dID' : x['dID']}, datasets)}
    _all_properties = MI.getProperty(_find_doc, pID)

    if len(_all_properties):
        _properties_by_dID = {}

        for p in _all_properties:

            dID = p['dID']
            del p['dID']
            if not _properties_by_dID.get(dID):
                _properties_by_dID[dID] = []

            _properties_by_dID[dID].append(p)

    # If not in DB, compute
    else:
        _properties_by_dID = compute_properties(pID, datasets)

    for _dID, _properties_data in _properties_by_dID.iteritems():
        for _property in _properties_data:
            if _property['label'] in _property_labels:
                _j = _property_labels.index(_property['label'])
                aggregatedProperties[_j]['dIDs'].append(_dID)

            else:
                _property_labels.append(_property['label'])
                _property['dIDs'] = [_dID]

                if not get_values:
                    del _property['values']


                if _property.get('_id'):
                    _property['propertyID'] = str(_property['_id'])
                    del _property['_id']

                aggregatedProperties.append(_property)

    return aggregatedProperties

# Retrieve entities given datasets
def get_entities(pID, datasets):
    _properties = get_properties(pID, datasets, get_values = True)
    _all_entities = filter(lambda x: x['type'] not in ['float', 'integer'], _properties)

    parent_entities = filter(lambda x: not x['is_child'], _all_entities)

    for i, _entity in enumerate(parent_entities):
        if _entity['child']:
            _entity['child'] = populate_child_entities(_entity['child'], [], _all_entities)

    return parent_entities

def populate_child_entities(entity_name, child_entities, all_entities):
    _entity = filter(lambda x: x['label'] == entity_name, all_entities)
    if _entity:
        _entity = _entity[0]
        if _entity['child']:
            child_entities = populate_child_entities(_entity['child'], child_entities, all_entities)

    if child_entities:
        return [_entity] + child_entities

    return None

# Retrieve entities given datasets
def get_attributes(pID, datasets):
    attributes = []
    _properties = get_properties(pID, datasets)
    attributes = filter(lambda x: x['type'] in ['float', 'integer'], _properties)

    return attributes

# Compute properties of all passed datasets
# Currently only getting properties by column
# Arguments: pID + dataset documents
# Returns a mapping from dIDs to properties
def compute_properties(pID, dataset_docs):
    properties_by_dID = {}

    for dataset in dataset_docs:
        properties = []

        dID = dataset['dID']
        df = get_data(pID=pID, dID=dID)
        df = df.fillna('')

        _labels = df.columns.values
        properties = [ None ] * len(_labels.tolist())

        for i, label in enumerate(_labels.tolist()):
            properties[i] = {}
            properties[i]['label'] = label

        print "Calculating properties for dID", dID
        # Statistical properties
        # Only conduct on certain types?
        print "\tDescribing datasets"
        df_stats = df.describe()
        df_stats_dict = json.loads(df_stats.to_json())
        df_stats_list = []
        for l in _labels:
            if l in df_stats_dict:
                df_stats_list.append(df_stats_dict[l])
            else:
                df_stats_list.append({})
        for i, stats in enumerate(df_stats_list):
            properties[i]['stats'] = stats

        ### Getting column types
        print "\tGetting types"
        _types = get_column_types(df)
        for i, _type in enumerate(_types):
            properties[i]['type'] = _type

        ### Determining normality
        print "\tDetermining normality"
        start_time = time()
        for i, col in enumerate(df):
            _type = _types[i]
            if _type in ["integer", "float"]:
                try:
                    ## Coerce data vector to float
                    d = df[col].astype(np.float)
                    normality_result = sc_stats.normaltest(d)
                except ValueError:
                    normality_result = None
            else:
                normality_result = None

            properties[i]['normality'] = normality_result

        print "\t\t", time() - start_time, "seconds"

        ### Detecting if a column is unique
        print "\tDetecting uniques"
        start_time = time()
        # List of booleans -- is a column composed of unique elements?
        for i, col in enumerate(df):
            properties[i]['unique'] = detect_unique_list(df[col])

        print "\t\t", time() - start_time, "seconds"

        ### Unique values for columns
        print "\tGetting unique values"
        start_time = time()
        unique_values = []
        raw_uniqued_values = [ get_unique(df[col]) for col in df ]
        for i, col in enumerate(raw_uniqued_values):
            _type = _types[i]
            if _type in ["integer", "float"]:
                properties[i]['values'] = []
            else:
                properties[i]['values'] = col

        print "\t\t", time() - start_time, "seconds"

        ### Detect parents
        print "\tGetting entity hierarchies"
        start_time = time()
        MAX_ROW_THRESHOLD = 100

        for i, col in enumerate(df):
            if i < (len(df.columns) - 1):
                if not properties[i]['unique'] and properties[i]['type'] not in ['float', 'integer'] and properties[i+1]['type'] not in ['float', 'integer']:
                    _all_next_col_values = []

                    if len(properties[i]['values']) > 1:
                        for j, value in enumerate(properties[i]['values']):
                            # TODO: be much smarter about sampling columns rather than just taking the first X rows
                            if j > MAX_ROW_THRESHOLD:
                                break

                            sub_df = df.loc[df[properties[i]['label']] == value]
                            _next_col_values = sub_df[properties[i+1]['label']]

                            _all_next_col_values.extend(set(_next_col_values))

                        _all_next_col_values = [x for x in _all_next_col_values if x != "#"]

                        if len(_all_next_col_values) == len(set(_all_next_col_values)):
                            properties[i]['child'] = properties[i+1]['label']
                            properties[i+1]['is_child'] = True

            if not properties[i].get('child'):
                properties[i]['child'] = None

            if not properties[i].get('is_child'):
                properties[i]['is_child'] = False

        print "\t\t", time() - start_time, "seconds"

        # Save properties into collection
        for _property in properties:
            _property['dID'] = dID
            if MI.getProperty({'dID': dID, 'label': _property['label']}, pID):
                tID = MI.upsertProperty(dID, pID, _property)
            else:
                tID = MI.setProperty(pID, _property)

        properties_by_dID[dID] = properties
    return properties_by_dID


# Detect if a list is comprised of unique elements
def detect_unique_list(l):
    # TODO Vary threshold by number of elements (be smarter about it)
    THRESHOLD = 0.95

    # Comparing length of uniqued elements with original list
    if (len(np.unique(l)) / float(len(l))) >= THRESHOLD:
        return True
    return False
