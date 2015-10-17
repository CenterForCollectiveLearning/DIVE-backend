'''
Dataset field properties
'''

import json
import numpy as np
import pandas as pd
from time import time
from scipy import stats as sc_stats
from flask import current_app
from itertools import permutations

from dive.db import db_access
from dive.task_core import celery, task_app
from dive.data.access import get_data
from dive.tasks.ingestion import DataType, numeric_types, quantitative_types, categorical_types
from dive.tasks.ingestion.type_detection import calculate_field_type
from dive.tasks.ingestion.utilities import get_unique

from celery import states
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


quantitative_stats_functions = {
    'min': np.min,
    'max': np.max,
    'average': np.average,
    'median': np.median,
    'std': np.std,
    'var': np.var
}


def calculate_field_stats(field_type, field_values, logging=False):
    if logging: start_time = time()
    percentiles = [(i * .5) / 10 for i in range(1, 20)]

    df = pd.DataFrame(field_values)
    stats = df.describe(percentiles=percentiles).to_dict().values()[0]

    return stats


@celery.task(bind=True, task_name='field_properties')
def compute_field_properties(self, dataset_id, project_id, track_started=True):
    '''
    Compute field properties of a specific dataset
    Currently only getting properties by column

    Arguments: project_id + dataset ids
    Returns a mapping from dataset_ids to properties

    '''
    self.update_state(state=states.PENDING, meta={'status': 'Computing dataset properties'})

    logger.info("Computing field properties for dataset_id %s", dataset_id)

    with task_app.app_context():
        df = get_data(project_id=project_id, dataset_id=dataset_id)

    num_fields = len(df.columns)
    all_field_properties = [ {} for i in range(num_fields) ]

    # Single-field types
    for (i, field_name) in enumerate(df):

        field_values = df[field_name]

        # Field types
        field_type, field_type_scores = calculate_field_type(field_name, field_values)

        # Map to general field types
        general_type = 'q' if (field_type in quantitative_types) else 'c'

        # Uniqueness
        is_unique = detect_unique_list(field_values)

        # Unique values
        if not (field_type in quantitative_types or is_unique):
            unique_values = get_unique(field_values)
        else:
            unique_values = None

        # Stats
        stats = calculate_field_stats(field_type, field_values)

        # ID
        is_id = True if ((field_type is DataType.INTEGER.value) and is_unique) else False

        # Normality
        normality = None
        if general_type is 'q':
            try:
                d = field_values.astype(np.float)
                normality_test_result = sc_stats.normaltest(d)
                statistic = normality_test_result.statistic
                pvalue = normality_test_result.pvalue
                if pvalue < 0.05:
                    normality = True
                else:
                    normality = False
            except ValueError:
                normality = None

        all_field_properties[i].update({
            'index': i,
            'name': field_name,
            'type': field_type,
            'general_type': general_type,
            'type_scores': field_type_scores,
            'is_id': is_id,
            'stats': stats,
            'normality': normality,
            'is_unique': is_unique,
            'unique_values': unique_values,
            'child': None,
            'is_child': False
        })

    # Detect hierarchical relationships
    # Hierarchical relationships
    # Given the unique values of current field, are the corresponding values
    # in another field a complete set of t?
    MAX_UNIQUE_VALUES_THRESHOLD = 100
    for field_a, field_b in permutations(all_field_properties, 2):
        if field_a['is_unique'] or (field_a['general_type'] is 'q') or (field_b['general_type'] is 'q'):
            continue

        field_b_unique_corresponding_values = []
        for unique_value_index, unique_value_a in enumerate(field_a['unique_values']):
            if unique_value_index > MAX_UNIQUE_VALUES_THRESHOLD:
                continue
            sub_df = df.loc[df[field_a['name']] == unique_value_a]
            field_b_unique_corresponding_values.extend(set(sub_df[field_b['name']]))

        if detect_unique_list(field_b_unique_corresponding_values):
            all_field_properties[all_field_properties.index(field_a)]['child'] = field_b['name']
            all_field_properties[all_field_properties.index(field_b)]['is_child'] = True

    self.update_state(state=states.SUCCESS)
    return all_field_properties


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
    _entity = filter(lambda x: x['name'] == entity_name, all_entities)[0]
    if _entity['child']:
        child_entities = populate_child_entities(_entity['child'], child_entities, all_entities)

    return [_entity] + child_entities


# Retrieve entities given datasets
def get_attributes(project_id, datasets):
    attributes = []
    _properties = get_field_properties(project_id, datasets)
    attributes = filter(lambda x: x['type'] in ['float', 'integer'], _properties)
    return attributes


# Detect if a list is comprised of unique elements
def detect_unique_list(l):
    # TODO Vary threshold by number of elements (be smarter about it)
    THRESHOLD = 0.95

    # Comparing length of uniqued elements with original list
    if (len(np.unique(l)) / float(len(l))) >= THRESHOLD:
        return True
    return False


@celery.task(bind=True, ignore_result=True)
def save_field_properties(self, all_properties, dataset_id, project_id):
    ''' Upsert all field properties corresponding to a dataset '''
    self.update_state(state=states.PENDING, meta={'status': 'Saving field properties'})
    field_properties_with_id = []
    for field_properties in all_properties:
        name = field_properties['name']

        with task_app.app_context():
            existing_field_properties = db_access.get_field_properties(project_id, dataset_id, name=name)

        if existing_field_properties:
            logger.debug("Updating field property of dataset %s with name %s", dataset_id, name)
            with task_app.app_context():
                field_properties = db_access.update_field_properties(project_id, dataset_id, **field_properties)
        else:
            logger.debug("Inserting field property of dataset %s with name %s", dataset_id, name)
            with task_app.app_context():
                field_properties = db_access.insert_field_properties(project_id, dataset_id, **field_properties)
        field_properties_with_id.append(field_properties)
    self.update_state(state=states.SUCCESS, meta={'status': 'Saved field properties'})
