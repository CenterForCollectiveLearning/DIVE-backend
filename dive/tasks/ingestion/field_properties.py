'''
Dataset field properties
'''

import json
import numpy as np
import pandas as pd
from time import time
from random import sample
from scipy import stats as sc_stats
from flask import current_app
from itertools import permutations

from dive.db import db_access
from dive.task_core import celery, task_app
from dive.data.access import get_data, coerce_types
from dive.data.in_memory_data import InMemoryData as IMD
from dive.tasks.ingestion import DataType, specific_to_general_type
from dive.tasks.ingestion.type_detection import calculate_field_type
from dive.tasks.ingestion.id_detection import detect_id
from dive.tasks.ingestion.utilities import get_unique
from dive.tasks.visualization.data import get_bin_agg_data, get_val_count_data

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

total_palette = ["#000000", "#FFFF00", "#1CE6FF", "#FF34FF", "#FF4A46", "#008941", "#006FA6", "#A30059", "#FFDBE5", "#7A4900","#0000A6","#63FFAC","#B79762","#004D43","#8FB0FF","#997D87","#5A0007","#809693","#FEFFE6","#1B4400","#4FC601","#3B5DFF","#4A3B53","#FF2F80","#61615A","#BA0900","#6B7900","#00C2A0","#FFAA92","#FF90C9","#B903AA","#D16100","#DDEFFF","#000035","#7B4F4B","#A1C299","#300018","#0AA6D8","#013349","#00846F","#372101","#FFB500","#C2FFED","#A079BF","#CC0744","#C0B9B2","#C2FF99","#001E09","#00489C","#6F0062","#0CBD66","#EEC3FF","#456D75","#B77B68","#7A87A1","#788D66","#885578","#FAD09F","#FF8A9A","#D157A0","#BEC459","#456648","#0086ED","#886F4C","#34362D","#B4A8BD","#00A6AA","#452C2C","#636375","#A3C8C9","#FF913F","#938A81","#575329","#00FECF","#B05B6F","#8CD0FF","#3B9700","#04F757","#C8A1A1","#1E6E00","#7900D7","#A77500","#6367A9","#A05837","#6B002C","#772600","#D790FF","#9B9700","#549E79","#FFF69F","#201625","#72418F","#BC23FF","#99ADC0","#3A2465","#922329","#5B4534","#FDE8DC","#404E55","#0089A3","#CB7E98","#A4E804","#324E72","#6A3A4C","#83AB58","#001C1E","#D1F7CE","#004B28","#C8D0F6","#A3A489","#806C66","#222800","#BF5650","#E83000","#66796D","#DA007C","#FF1A59","#8ADBB4","#1E0200","#5B4E51","#C895C5","#320033","#FF6832","#66E1D3","#CFCDAC","#D0AC94","#7ED379","#012C58","#7A7BFF","#D68E01","#353339","#78AFA1","#FEB2C6","#75797C","#837393","#943A4D","#B5F4FF","#D2DCD5","#9556BD","#6A714A","#001325","#02525F","#0AA3F7","#E98176","#DBD5DD","#5EBCD1","#3D4F44","#7E6405","#02684E","#962B75","#8D8546","#9695C5","#E773CE","#D86A78","#3E89BE","#CA834E","#518A87","#5B113C","#55813B","#E704C4","#00005F","#A97399","#4B8160","#59738A","#FF5DA7","#F7C9BF","#643127","#513A01","#6B94AA","#51A058","#A45B02","#1D1702","#E20027","#E7AB63","#4C6001","#9C6966","#64547B","#97979E","#006A66","#391406","#F4D749","#0045D2","#006C31","#DDB6D0","#7C6571","#9FB2A4","#00D891","#15A08A","#BC65E9","#FFFFFE","#C6DC99","#203B3C","#671190","#6B3A64","#F5E1FF","#FFA0F2","#CCAA35","#374527","#8BB400","#797868","#C6005A","#3B000A","#C86240","#29607C","#402334","#7D5A44","#CCB87C","#B88183","#AA5199","#B5D6C3","#A38469","#9F94F0","#A74571","#B894A6","#71BB8C","#00B433","#789EC9","#6D80BA","#953F00","#5EFF03","#E4FFFC","#1BE177","#BCB1E5","#76912F","#003109","#0060CD","#D20096","#895563","#29201D","#5B3213","#A76F42","#89412E","#1A3A2A","#494B5A","#A88C85","#F4ABAA","#A3F3AB","#00C6C8","#EA8B66","#958A9F","#BDC9D2","#9FA064","#BE4700","#658188","#83A485","#453C23","#47675D","#3A3F00","#061203","#DFFB71","#868E7E","#98D058","#6C8F7D","#D7BFC2","#3C3E6E","#D83D66","#2F5D9B","#6C5E46","#D25B88","#5B656C","#00B57F","#545C46","#866097","#365D25","#252F99","#00CCFF","#674E60","#FC009C","#92896B"]
total_palettes = ['#F3595C', '#78C466', '#579AD6', '#FCA853', '#9F65AD', '#D07054', '#D97DB5']

def calculate_field_stats(field_type, field_values, logging=False):
    if logging: start_time = time()
    percentiles = [(i * .5) / 10 for i in range(1, 20)]

    df = pd.DataFrame(field_values)
    stats = df.describe(percentiles=percentiles).to_dict().values()[0]

    return stats


def compute_field_properties(dataset_id, project_id, compute_hierarchical_relationships=False, track_started=True):
    '''
    Compute field properties of a specific dataset
    Currently only getting properties by column

    Arguments: project_id + dataset ids
    Returns a mapping from dataset_ids to properties
    '''

    logger.debug("Computing field properties for dataset_id %s", dataset_id)

    with task_app.app_context():
        df = get_data(project_id=project_id, dataset_id=dataset_id)

    num_fields = len(df.columns)
    field_properties = [ {} for i in range(num_fields) ]
    palette = sample(total_palette, num_fields)

    # 1) Detect field types
    for (i, field_name) in enumerate(df):
        field_values = df[field_name]
        logger.debug('Computing field properties for field %s', field_name)
        field_type, field_type_scores = calculate_field_type(field_name, field_values, i, num_fields)
        general_type = specific_to_general_type[field_type]

        print field_name, field_type, general_type
        field_properties[i].update({
            'index': i,
            'name': field_name,
            'type': field_type,
            'general_type': general_type,
            'type_scores': field_type_scores,
        })

    coerced_df = coerce_types(df, field_properties)
    IMD.insertData(dataset_id, coerced_df)

    # 2) Rest
    for (i, field_name) in enumerate(df):
        logger.debug('Computing field properties for field %s', field_name)

        field_values = df[field_name]
        field_type = field_properties[i]['type']
        general_type = field_properties[i]['general_type']

        # Uniqueness
        is_unique = detect_unique_list(field_values)

        # Unique values for categorical fields
        if general_type is 'c':
            unique_values = [ e for e in get_unique(field_values) if not pd.isnull(e) ]
        else:
            unique_values = None

        stats = calculate_field_stats(field_type, field_values)
        is_id = detect_id(field_name, field_type, is_unique)

        # Binning
        viz_data = None
        if general_type in ['q', 't']:
            binning_spec = {
                'binning_field': { 'name': field_name },
                'agg_field_a': { 'name': field_name },
                'agg_fn': 'count',
            }
            try:
                viz_data = get_bin_agg_data(df, {}, binning_spec, {})
            except:
                pass
        elif general_type is 'c':
            val_count_spec = {
                'field_a': { 'name': field_name },
            }
            try:
                viz_data = get_val_count_data(df, {}, val_count_spec, {})
            except:
                pass

        # Normality
        # Skip for now
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

        field_properties[i].update({
            'color': palette[i],
            'viz_data': viz_data,
            'is_id': is_id,
            'stats': stats,
            'normality': normality,
            'is_unique': is_unique,
            'unique_values': unique_values,
            'child': None,
            'is_child': False,
            'manual': {}
        })

    logger.debug("Detecting hierarchical relationships")
    # Detect hierarchical relationships
    # Hierarchical relationships
    # Given the unique values of current field, are the corresponding values
    # in another field a complete set of t?
    if compute_hierarchical_relationships:
        MAX_UNIQUE_VALUES_THRESHOLD = 100
        for field_a, field_b in permutations(field_properties, 2):
            logger.debug('%s - %s', field_a['name'], field_b['name'])
            if field_a['is_unique'] or (field_a['general_type'] is 'q') or (field_b['general_type'] is 'q'):
                continue

            field_b_unique_corresponding_values = []
            for unique_value_index, unique_value_a in enumerate(field_a['unique_values']):
                if unique_value_index > MAX_UNIQUE_VALUES_THRESHOLD:
                    continue
                sub_df = df.loc[df[field_a['name']] == unique_value_a]
                field_b_unique_corresponding_values.extend(set(sub_df[field_b['name']]))

            if detect_unique_list(field_b_unique_corresponding_values):
                field_properties[field_properties.index(field_a)]['child'] = field_b['name']
                field_properties[field_properties.index(field_b)]['is_child'] = True

    logger.debug("Done computing field properties")

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
    return {
        'desc': 'Saved %s field properties' % len(field_properties_with_id),
        'result': {
            'dataset_id': dataset_id
        }
    }
