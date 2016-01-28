import math
import copy
from pprint import pprint
from time import time
from scipy import stats as sc_stats
from flask import current_app

from dive.db import db_access
from dive.data.access import get_data, get_conditioned_data
from dive.task_core import celery, task_app
from dive.tasks.ingestion import specific_to_general_type
from dive.tasks.visualization import GeneratingProcedure, TypeStructure, TermType
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


@celery.task(bind=True)
def attach_data_to_viz_specs(self, enumerated_viz_specs_result, dataset_id, project_id, conditionals):
    '''
    Get data corresponding to each viz spec (before filtering and scoring)
    '''
    self.update_state(state=states.PENDING, meta={'desc': 'Attaching data to visualization specs'})
    enumerated_viz_specs = enumerated_viz_specs_result['result']
    viz_specs_with_data = []

    # Get dataframe
    with task_app.app_context():
        df = get_data(project_id=project_id, dataset_id=dataset_id)
        df = df.dropna()
        conditioned_df = get_conditioned_data(project_id, dataset_id, df, conditionals)

    for i, spec in enumerate(enumerated_viz_specs):
        if ((i + 1) % 100) == 0:
            logger.info('Attached data to %s out of %s specs', (i + 1), len(enumerated_viz_specs))

        viz_spec_with_data = spec
        # TODO Optimize data reads
        with task_app.app_context():
            try:
                data = get_viz_data_from_enumerated_spec(spec, project_id, conditionals, df=conditioned_df, data_formats=['score', 'visualize'])
            except Exception as e:
                logger.error("Error getting viz data %s", e, exc_info=True)
                continue
        if not data:
            continue
        viz_spec_with_data['data'] = data
        viz_specs_with_data.append(viz_spec_with_data)

    return {
        'desc': 'Attached data to %s visualization specs' % len(viz_specs_with_data),
        'result': viz_specs_with_data
    }


@celery.task(bind=True)
def filter_viz_specs(self, viz_specs_with_data_result, project_id):
    '''
    Filtering enumerated viz specs based on interpretability, usability, and renderability
    '''
    self.update_state(state=states.PENDING, meta={'desc': 'Filtering visualization specs'})
    viz_specs_with_data = viz_specs_with_data_result['result']
    filtered_viz_specs = []

    for s in viz_specs_with_data:
        # Don't show aggregations with only one element
        if (len(s['data']['visualize']) <= 2):
            continue
        filtered_viz_specs.append(s)
    return {
        'desc': 'Filtered %s visualization specs' % len(filtered_viz_specs),
        'result': filtered_viz_specs
    }


@celery.task(bind=True)
def score_viz_specs(self, filtered_viz_specs_result, dataset_id, project_id, selected_fields, sort_key='relevance'):
    ''' Scoring viz specs based on effectiveness, expressiveness, and statistical properties '''
    self.update_state(state=states.PENDING, meta={'desc': 'Scoring visualization specs'})

    filtered_viz_specs = filtered_viz_specs_result['result']
    scored_viz_specs = []
    for i, spec in enumerate(filtered_viz_specs):
        if ((i + 1) % 100) == 0:
            logger.info('Scored %s out of %s specs', i, len(filtered_viz_specs))
        scored_spec = spec

        score_doc = score_spec(spec, selected_fields)
        if not score_doc:
            continue
        scored_spec['scores'] = score_doc

        del scored_spec['data']['score']

        scored_viz_specs.append(spec)

    sorted_viz_specs = sorted(scored_viz_specs, key=lambda k: next(s for s in k['scores'] if s['type'] == 'relevance')['score'], reverse=True)

    return {
        'desc': 'Scored %s visualizations' % len(sorted_viz_specs),
        'result': sorted_viz_specs
    }


@celery.task(bind=True)
def format_viz_specs(self, scored_viz_specs_result, project_id):
    ''' Get viz specs into a format usable by front end '''
    self.update_state(state=states.PENDING, meta={'desc': 'Formatting visualization specs'})

    formatted_viz_specs = scored_viz_specs_result['result']
    return {
        'desc': 'Formatted %s visualizations' % len(formatted_viz_specs),
        'result': formatted_viz_specs
    }


@celery.task(bind=True)
def save_viz_specs(self, specs_result, dataset_id, project_id, selected_fields, conditionals):
    self.update_state(state=states.PENDING, meta={'desc': 'Saving visualization specs'})

    specs = specs_result['result']
    with task_app.app_context():
        # Delete existing specs with same parameters
        existing_specs = db_access.get_specs(project_id, dataset_id, selected_fields=selected_fields, conditionals=conditionals)
        if existing_specs:
            for spec in existing_specs:
                db_access.delete_spec(project_id, spec['id'])
        inserted_specs = db_access.insert_specs(project_id, specs, selected_fields, conditionals)
    return {
        'desc': 'Saved %s visualizations' % len(inserted_specs),
        'result': inserted_specs
    }
