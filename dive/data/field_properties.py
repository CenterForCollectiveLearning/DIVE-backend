'''
Dataset field properties
'''

import json
import numpy as np
from scipy import stats as sc_stats
from time import time

from dive.db.db import MongoInstance as MI
from dive.db import db_access
from dive.data.access import get_data
from dive.data.type_detection import get_column_types
from dive.data.analysis import get_unique, get_bin_edges


# Retrieve proeprties given dataset_docs
# TODO Accept list of dataset_ids
def get_field_properties(project_id, dataset_ids, get_values=False, flatten=True) :
    '''
    Returns field properties given a list of dataset_ids. Basically a wrapper /
    formatter around compute_field_properties.

    Args:
    project_id
    dataset_ids
    get_values - return unique values of fields along with properties
    flatten - return as list of all properties vs a dict keyed by dataset_id
    '''
    properties_by_dataset_id = {}

    for dataset_id in dataset_ids:
        field_properties = db_access.get_field_properties(project_id, dataset_id)
        if not field_properties:
            field_properties = compute_field_properties(project_id, dataset_id)

        for field_property in field_properties:
            continue
        _properties_by_dataset_id[dataset_id] = _dataset_properties

    aggregatedProperties = []
    _property_labels = []

    for _dataset_id, _properties_data in _properties_by_dataset_id.iteritems():
        for _property in _properties_data:
            if _property['label'] in _property_labels:
                _j = _property_labels.index(_property['label'])
                aggregatedProperties[_j]['dataset_ids'].append(_dataset_id)

            else:
                _property_labels.append(_property['label'])
                _property['dataset_ids'] = [_dataset_id]

                if not get_values:
                    del _property['values']

                _property['property_id']
                del _property('id')

                aggregatedProperties.append(_property)

    return aggregatedProperties

# Retrieve entities given datasets
def get_entities(project_id, datasets):
    _properties = get_field_properties(project_id, datasets, get_values = True)
    _all_entities = filter(lambda x: x['type'] not in ['float', 'integer'], _properties)

    parent_entities = filter(lambda x: not x['is_child'], _all_entities)

    for i, _entity in enumerate(parent_entities):
        if _entity['child']:
            _entity['child'] = populate_child_entities(_entity['child'], [], _all_entities)

    return parent_entities

def populate_child_entities(entity_name, child_entities, all_entities):
    _entity = filter(lambda x: x['label'] == entity_name, all_entities)[0]
    if _entity['child']:
        child_entities = populate_child_entities(_entity['child'], child_entities, all_entities)

    return [_entity] + child_entities

# Retrieve entities given datasets
def get_attributes(project_id, datasets):
    attributes = []
    _properties = get_field_properties(project_id, datasets)
    attributes = filter(lambda x: x['type'] in ['float', 'integer'], _properties)

    return attributes

# TODO Reduce iterations over data elements

def compute_field_properties(project_id, dataset_ids):
    '''
    Compute properties of all passed datasets
    Currently only getting properties by column

    Arguments: project_id + dataset documents
    Returns a mapping from dataset_ids to properties
    '''
    properties_by_dataset_id = {}

    for dataset in dataset_docs:
        properties = []

        dataset_id = dataset['dataset_id']
        df = get_data(project_id=project_id, dataset_id=dataset_id)
        df = df.fillna('')

        _labels = df.columns.values
        properties = [ None ] * len(_labels.tolist())

        for i, label in enumerate(_labels.tolist()):
            properties[i] = {}
            properties[i]['label'] = label

        logger.info("Calculating properties for dataset_id %s", dataset_id)
        # Statistical properties
        # Only conduct on certain types?
        logger.info("\tDescribing datasets")
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
        start_time = time()
        _types = get_column_types(df)
        for i, _type in enumerate(_types):
            properties[i]['type'] = _type
        type_time = time() - start_time
        logger.info("Field type detection took %s seconds", type_time)

        ### Determining normality
        start_time = time()
        for i, col in enumerate(df):
            _type = _types[i]
            if _type in ["int", "float"]:
                try:
                    ## Coerce data vector to float
                    d = df[col].astype(np.float)
                    normality_result = sc_stats.normaltest(d)
                except ValueError:
                    normality_result = None
            else:
                normality_result = None
            properties[i]['normality'] = normality_result
        normality_time = time() - start_time
        logger.info("Normality analysis took %s seconds", normality_time)


        ### Detecting if a column is unique
        start_time = time()
        # List of booleans -- is a column composed of unique elements?
        for i, col in enumerate(df):
            properties[i]['unique'] = detect_unique_list(df[col])
        is_unique_time = time() - start_time
        logger.info("Unique detection took %s seconds", is_unique_time)

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
                if not properties[i]['unique'] and properties[i]['type'] not in ['float', 'int'] and properties[i+1]['type'] not in ['float', 'int']:
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
            _property['dataset_id'] = dataset_id
            if MI.getFieldProperty({'dataset_id': dataset_id, 'label': _property['label']}, project_id):
                print "saving field property", project_id
                tID = MI.upsertFieldProperty(_property, dataset_id, project_id)
            else:
                print "saving gield property,", project_id
                tID = MI.setFieldProperty(_property, project_id)

        properties_by_dataset_id[dataset_id] = properties
    return properties_by_dataset_id


# Detect if a list is comprised of unique elements
def detect_unique_list(l):
    # TODO Vary threshold by number of elements (be smarter about it)
    THRESHOLD = 0.95

    # Comparing length of uniqued elements with original list
    if (len(np.unique(l)) / float(len(l))) >= THRESHOLD:
        return True
    return False
