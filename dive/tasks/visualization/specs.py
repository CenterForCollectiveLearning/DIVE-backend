import math
import copy
from pprint import pprint
from time import time
from scipy import stats as sc_stats
from flask import current_app

from dive.db import db_access
from dive.task_core import celery, task_app
from dive.tasks.visualization import GeneratingProcedure, TypeStructure, TermType, specific_to_general_type
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
def filter_viz_specs(self, enumerated_viz_specs, project_id):
    ''' Filtering enumerated viz specs based on interpretability and renderability '''
    self.update_state(state=states.PENDING)
    filtered_viz_specs = enumerated_viz_specs
    # self.update_state(state=states.SUCCESS, meta={'status': 'Filtered viz specs'})
    return filtered_viz_specs


@celery.task(bind=True)
def score_viz_specs(self, filtered_viz_specs, dataset_id, project_id, selected_fields, conditionals, sort_key='relevance'):
    ''' Scoring viz specs based on effectiveness, expressiveness, and statistical properties '''
    self.update_state(state=states.PENDING)

    full_conditionals = get_full_fields_for_conditionals(conditionals, dataset_id, project_id)

    scored_viz_specs = []
    for i, spec in enumerate(filtered_viz_specs):
        if ((i + 1) % 100) == 0:
            logger.info('Scored %s out of %s specs', (i + 1), len(filtered_viz_specs))
        scored_spec = spec

        # TODO Optimize data reads
        with task_app.app_context():
            try:
                data = get_viz_data_from_enumerated_spec(spec, project_id, full_conditionals, data_formats=['score', 'visualize'])
            except Exception as e:
                logger.error("Error getting viz data %s", e, exc_info=True)
                continue
        if not data:
            continue
        scored_spec['data'] = data

        score_doc = score_spec(spec, selected_fields)
        if not score_doc:
            continue
        scored_spec['scores'] = score_doc

        del scored_spec['data']['score']

        scored_viz_specs.append(spec)

    sorted_viz_specs = sorted(scored_viz_specs, key=lambda k: next(s for s in k['scores'] if s['type'] == 'relevance')['score'], reverse=True)

    # self.update_state(state=states.SUCCESS, meta={'status': 'Scored viz specs'})
    return sorted_viz_specs


@celery.task(bind=True)
def format_viz_specs(self, scored_viz_specs, project_id):
    ''' Get viz specs into a format usable by front end '''
    self.update_state(state=states.PENDING)
    formatted_viz_specs = scored_viz_specs

    # self.update_state(state=states.SUCCESS, meta={'status': 'Formatted viz specs'})
    return formatted_viz_specs


@celery.task(bind=True)
def save_viz_specs(self, specs, dataset_id, project_id, selected_fields, conditionals):
    self.update_state(state=states.PENDING)

    with task_app.app_context():
        # Delete existing specs with same parameters
        existing_specs = db_access.get_specs(project_id, dataset_id, selected_fields=selected_fields, conditionals=conditionals)
        if existing_specs:
            for spec in existing_specs:
                db_access.delete_spec(project_id, spec['id'])
        inserted_specs = db_access.insert_specs(project_id, specs, selected_fields, conditionals)
    return inserted_specs
    # self.update_state(state=states.SUCCESS, meta={'status': 'Saved viz specs'})
