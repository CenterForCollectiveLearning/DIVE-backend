import math
import copy
from pprint import pprint
from time import time
from scipy import stats as sc_stats
from flask import current_app

from dive.db import db_access
from dive.data.access import get_data, get_conditioned_data
from dive.task_core import celery, task_app
from dive.tasks.utilities import timeit
from dive.tasks.ingestion import specific_to_general_type
from dive.tasks.visualization import GeneratingProcedure as GP, TypeStructure as TS, TermType as TT
from dive.tasks.visualization.data import get_viz_data_from_enumerated_spec
from dive.tasks.visualization.score_specs import score_spec

from celery import states
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def get_full_fields_for_conditionals(conditionals, dataset_id, project_id):
    conditionals_with_full_docs = {'and': [], 'or': []}
    with task_app.app_context():
        field_properties = db_access.get_field_properties(project_id, dataset_id)

    for clause, conditional_list in conditionals.iteritems():
        for conditional in conditional_list:
            new_conditional = {
                'operation': conditional['operation'],
                'criteria': conditional['criteria']
            }
            matched_field_doc = next((f for f in field_properties if f['id'] == conditional['field_id']), None)
            new_conditional['field'] = {
                'general_type': matched_field_doc['general_type'],
                'name': matched_field_doc['name']
            }
            conditionals_with_full_docs[clause].append(new_conditional)

    return conditionals_with_full_docs

def attach_data_to_viz_specs(enumerated_viz_specs, dataset_id, project_id, conditionals, config):
    '''
    Get data corresponding to each viz spec (before filtering and scoring)
    '''
    viz_specs_with_data = []

    start_time = time()
    # Get dataframe
    if project_id and dataset_id:
        with task_app.app_context():
            df = get_data(project_id=project_id, dataset_id=dataset_id, profile=True)
            df = df.dropna()
            conditioned_df = get_conditioned_data(project_id, dataset_id, df, conditionals)

    precomputed = {
        'groupby': {}
    }
    for i, spec in enumerate(enumerated_viz_specs):
        viz_spec_with_data = spec
        # TODO Move this into another function
        if spec['args'].get('grouped_field'):
            grouped_field = spec['args']['grouped_field']['name']
            grouped_df = conditioned_df.groupby(grouped_field)
            precomputed['groupby'][grouped_field] = grouped_df

        # try:
        data = get_viz_data_from_enumerated_spec(spec, project_id, conditionals, config,
            df=conditioned_df,
            precomputed=precomputed,
            data_formats=['score', 'visualize']
        )

        # except Exception as e:
        #     logger.error("Error getting viz data %s", e, exc_info=True)
        #     continue

        if not data:
            logger.info('No data for spec with generating procedure %s', spec['generating_procedure'])
            continue
        viz_spec_with_data['data'] = data
        viz_specs_with_data.append(viz_spec_with_data)

    logger.info('Attaching data to %s specs took %.3fs', len(viz_specs_with_data), time() - start_time)
    return viz_specs_with_data


def filter_viz_specs(viz_specs_with_data):
    '''
    Filtering enumerated viz specs based on interpretability, usability, and renderability
    '''
    filtered_viz_specs = []

    for s in viz_specs_with_data:
        # Don't show aggregations with only one element
        if not s['data']['visualize']:
            continue
        if s['generating_procedure'] != GP.MULTIGROUP_COUNT.value:
            if (len(s['data']['visualize']) <= 2):
                continue
        filtered_viz_specs.append(s)
    return filtered_viz_specs


def score_viz_specs(filtered_viz_specs, dataset_id, project_id, selected_fields, sort_key='relevance'):
    ''' Scoring viz specs based on effectiveness, expressiveness, and statistical properties '''
    scored_viz_specs = []
    for i, spec in enumerate(filtered_viz_specs):
        scored_spec = spec

        score_doc = score_spec(spec, selected_fields)
        if not score_doc:
            continue
        scored_spec['scores'] = score_doc

        del scored_spec['data']['score']

        scored_viz_specs.append(spec)

    sorted_viz_specs = sorted(scored_viz_specs, key=lambda k: next(s for s in k['scores'] if s['type'] == 'relevance')['score'], reverse=True)

    return sorted_viz_specs


def save_viz_specs(specs, dataset_id, project_id, selected_fields, conditionals, config):
    with task_app.app_context():
        # Delete existing specs with same parameters
        existing_specs = db_access.get_specs(
            project_id, dataset_id, selected_fields=selected_fields, conditionals=conditionals, config=config)
        if existing_specs:
            for spec in existing_specs:
                db_access.delete_spec(project_id, spec['id'])
        inserted_specs = db_access.insert_specs(project_id, specs, selected_fields, conditionals, config)
    return inserted_specs
