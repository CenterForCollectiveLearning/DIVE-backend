from __future__ import unicode_literals
'''
Dataset field properties
'''

import json
import numpy as np
import pandas as pd
from time import time
from random import sample, randint
from scipy import stats as sc_stats
from flask import current_app
from itertools import permutations

from dive.base.db import db_access
from dive.base.data.access import get_data, coerce_types
from dive.base.data.in_memory_data import InMemoryData as IMD
from dive.worker.core import celery, task_app
from dive.base.constants import GeneratingProcedure as GP, TypeStructure as TS, \
    VizType as VT, TermType, aggregation_functions, GeneralDataType as GDT, DataType as DT, Scale, specific_type_to_general_type, specific_type_to_scale
from dive.worker.ingestion.type_detection import calculate_field_type
from dive.worker.ingestion.id_detection import detect_id
from dive.worker.ingestion.utilities import get_unique
from dive.worker.visualization.data import get_bin_agg_data, get_val_count_data

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

total_palette = [
    '#000000',
    '#404040',
    '#858585',
    '#59443D',
    '#8E5E34',
    '#993A4C',
    '#C1392B',
    '#F3595C',
    '#D85499',
    '#C5A3CE',
    '#9354D8',
    '#8544AD',
    '#6A5674',
    '#B46100',
    '#E67E22',
    '#FFA600',
    '#F1C40F',
    '#9AC2E6',
    '#56748A',
    '#5457D8',
    '#54D893',
    '#99D854',
    '#00BF00',
    '#608300',
    '#055D2A',
]


def sample_with_maximum_distance(li, num_samples, random_start=True):
    num_elements = len(li)
    skip_length = int(num_elements / float(num_samples))

    start_index = 0
    if random_start:
        start_index = randint(0, num_elements)

    samples = []
    sample_indices_range = range(start_index, start_index + (skip_length * num_samples), skip_length)
    samples = [ li[(sample_index % num_elements)] for sample_index in sample_indices_range ]
    return samples


def calculate_field_stats(field_type, general_type, field_values, logging=False):
    if logging: start_time = time()
    percentiles = [(i * .5) / 10 for i in range(1, 20)]

    df = pd.DataFrame(field_values)
    stats = df.describe(percentiles=percentiles).to_dict().values()[0]
    stats['total_count'] = df.shape[0]
    return stats



def detect_contiguous_integers(field_values):
    sorted_unique_list = sorted(np.unique(field_values))

    for i in range(len(sorted_unique_list) - 1):
        diff = abs(sorted_unique_list[i + 1] - sorted_unique_list[i])
        if diff > 1:
            return False
    return True


def compute_single_field_property_nontype(field_name, field_values, field_type, general_type):
    field_values_no_na = field_values.dropna(how='any')
    all_null = (len(field_values_no_na) == 0)
    num_na = len(field_values) - len(field_values_no_na)
    is_unique = detect_unique_list(field_values)

    unique_values = [ e for e in get_unique(field_values) if not pd.isnull(e) ] if (general_type == 'c' and not is_unique) else None
    is_id = detect_id(field_name, field_type, is_unique)

    stats, contiguous, scale, viz_data, normality = [ None ]*5

    if not all_null:
        stats = calculate_field_stats(field_type, general_type, field_values)
        contiguous = get_contiguity(field_name, field_values, field_values_no_na, field_type, general_type)
        scale = get_scale(field_name, field_values, field_type, general_type, contiguous)
        viz_data = get_field_distribution_viz_data(field_name, field_values, field_type, general_type, scale, is_id, contiguous)
        normality = get_normality(field_name, field_values, field_type, general_type, scale)

    return {
        'scale': scale,
        'contiguous': contiguous,
        'viz_data': viz_data,
        'is_id': is_id,
        'stats': stats,
        'num_na': num_na,
        'normality': normality,
        'is_unique': is_unique,
        'unique_values': unique_values,
        'child': None,
        'is_child': False,
        'manual': {}
    }

def get_scale(field_name, field_values, field_type, general_type, contiguous):
    scale = specific_type_to_scale[field_type]
    if contiguous:
        scale = Scale.ORDINAL.value
    return scale


def get_contiguity(field_name, field_values, field_values_no_na, field_type, general_type, MAX_CONTIGUOUS_FIELDS=30):
    contiguous = False

    if field_type == DT.INTEGER.value:
        value_range = max(field_values_no_na) - min(field_values_no_na) + 1
        if (value_range <= MAX_CONTIGUOUS_FIELDS):
            contiguous = detect_contiguous_integers(field_values_no_na)
    return contiguous


def get_field_distribution_viz_data(field_name, field_values, field_type, general_type, scale, is_id, contiguous):
    viz_data = None
    if is_id: return viz_data

    df = pd.DataFrame.from_dict({ field_name: field_values })
    field_document = { 'name': field_name, 'type': field_type, 'scale': scale, 'general_type': general_type }
    if scale in [ Scale.CONTINUOUS.value ]:
        if general_type == GDT.T.value:
            spec = {
                'field_a': field_document,
                'agg_fn': 'count',
                'viz_types': [ VT.LINE.value ]
            }
            viz_data_function = get_val_count_data
        else:
            spec = {
                'binning_field': field_document,
                'agg_field_a': field_document,
                'agg_fn': 'count',
                'viz_types': [ VT.HIST.value ]
            }
            viz_data_function = get_bin_agg_data

    elif scale in [ Scale.ORDINAL.value, Scale.NOMINAL.value ]:
        spec = {
            'field_a': field_document,
            'viz_types': [ VT.BAR.value ]
        }
        viz_data_function = get_val_count_data
    try:
        viz_data = viz_data_function(df, spec)
    except Exception as e:
        logger.error('Error getting viz data: %s', e, exc_info=True)
        return None

    return { 'spec': spec, 'data': viz_data }


def get_normality(field_name, field_values, field_type, general_type, scale):
    normality = None
    if general_type is 'q':
        try:
            d = field_values.astype(np.float)
            normality_test_result = sc_stats.normaltest(d)
            if normality_test_result:
                statistic = normality_test_result.statistic
                pvalue = normality_test_result.pvalue
                if pvalue < 0.05:
                    normality = True
                else:
                    normality = False
        except ValueError:
            normality = None
    return normality


def compute_single_field_property_type(field_name, field_values, field_position=None, num_fields=None, field_type=None, general_type=None, type_scores={}):
    field_properties = {}

    field_type, type_scores = calculate_field_type(field_name, field_values, field_position, num_fields)
    general_type = specific_type_to_general_type[field_type]

    if field_name == 'year':
        print field_name
        print field_type
        print type_scores
    return {
        'type': field_type,
        'general_type': general_type,
        'type_scores': type_scores
    }


def compute_all_field_properties(dataset_id, project_id, compute_hierarchical_relationships=False, track_started=True):
    '''
    Compute field properties of a specific dataset
    Currently only getting properties by column

    Arguments: project_id + dataset ids
    Returns a mapping from dataset_ids to properties
    '''

    logger.debug("Computing field properties for dataset_id %s", dataset_id)

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    num_fields = len(df.columns)
    field_properties = [ {} for i in range(num_fields) ]

    palette = total_palette + [ '#007BD7' for i in range(0, num_fields - len(total_palette)) ]
    if num_fields <= len(total_palette):
        palette = sample_with_maximum_distance(total_palette, num_fields, random_start=True)

    # 1) Detect field types
    for (i, field_name) in enumerate(df):
        logger.info('[%s | %s] Detecting type for field %s', project_id, dataset_id, field_name)
        field_values = df[field_name]
        d = field_property_type_object = compute_single_field_property_type(field_name, field_values, field_position=i, num_fields=num_fields)
        field_properties[i].update({
            'index': i,
            'name': field_name,
        })
        field_properties[i].update(d)

    # Necessary to coerce here?
    coerced_df = coerce_types(df, field_properties)
    IMD.insertData(dataset_id, coerced_df)

    # 2) Rest
    for (i, field_name) in enumerate(coerced_df):
        field_values = coerced_df[field_name]
        d = field_properties_nontype_object = compute_single_field_property_nontype(
            field_name,
            field_values,
            field_properties[i]['type'],
            field_properties[i]['general_type'],
        )
        field_properties[i].update({
            'color': palette[i],
            'child': None,
            'is_child': False,
            'manual': {}
        })
        field_properties[i].update(d)

    # logger.debug("Detecting hierarchical relationships")
    # Detect hierarchical relationships
    # Hierarchical relationships
    # Given the unique values of current field, are the corresponding values
    # in another field a complete set of t?
    # if compute_hierarchical_relationships:
    #     MAX_UNIQUE_VALUES_THRESHOLD = 100
    #     for field_a, field_b in permutations(field_properties, 2):
    #         logger.debug('%s - %s', field_a['name'], field_b['name'])
    #         if field_a['is_unique'] or (field_a['general_type'] is 'q') or (field_b['general_type'] is 'q'):
    #             continue
    #
    #         field_b_unique_corresponding_values = []
    #         for unique_value_index, unique_value_a in enumerate(field_a['unique_values']):
    #             if unique_value_index > MAX_UNIQUE_VALUES_THRESHOLD:
    #                 continue
    #             sub_df = df.loc[df[field_a['name']] == unique_value_a]
    #             field_b_unique_corresponding_values.extend(set(sub_df[field_b['name']]))
    #
    #         if detect_unique_list(field_b_unique_corresponding_values):
    #             field_properties[field_properties.index(field_a)]['child'] = field_b['name']
    #             field_properties[field_properties.index(field_b)]['is_child'] = True
    #
    # logger.debug("Done computing field properties")

    return {
        'desc': 'Done computing field properties for %s fields' % len(field_properties),
        'result': field_properties
    }


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


def save_field_properties(all_properties_result, dataset_id, project_id):
    ''' Upsert all field properties corresponding to a dataset '''
    logger.debug('In save_field_properties for dataset_id %s and project_id %s', dataset_id, project_id)

    all_properties = all_properties_result['result']
    field_properties_with_id = []
    for field_properties in all_properties:
        name = field_properties['name']

        existing_field_properties = db_access.get_field_properties(project_id, dataset_id, name=name)

        if existing_field_properties:
            field_properties = db_access.update_field_properties(project_id, dataset_id, **field_properties)
        else:
            field_properties = db_access.insert_field_properties(project_id, dataset_id, **field_properties)
        field_properties_with_id.append(field_properties)
    return {
        'desc': 'Saved %s field properties' % len(field_properties_with_id),
        'result': {
            'id': dataset_id
        }
    }
