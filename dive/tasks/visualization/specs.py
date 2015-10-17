import math
import copy
from pprint import pprint
from time import time
from scipy import stats as sc_stats
from flask import current_app

from dive.db import db_access
from dive.task_core import celery, task_app
from dive.tasks.visualization import GeneratingProcedure, TypeStructure, TermType, specific_to_general_type
from dive.tasks.visualization.marginal_spec_functions import A, B, C, D, E, F, G, H
from dive.tasks.visualization.data import get_viz_data_from_enumerated_spec
from dive.tasks.visualization.type_mapping import get_viz_types_from_spec
from dive.tasks.visualization.scoring import score_spec

from celery import states
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def enumerate_viz_specs_no_args(field_properties):
    ''' Return field-wise summary statistics if no arguments '''
    specs = []
    for field in field_properties:
        # Skip unique fields
        if field['is_id']: continue
        if field['is_unique']: continue

        general_type = field['general_type']
        if general_type == 'c':
            C_specs = C(field)
            specs.extend(C_specs)
        elif general_type == 'q':
            A_specs = A(field)
            specs.extend(A_specs)
        else:
            raise ValueError('Not valid general_type', general_type)
    return specs


@celery.task()
def enumerate_viz_specs(project_id, dataset_id, selected_fields):
    '''
    TODO Move key filtering to the db query
    TODO Incorporate 0D and 1D data returns
    TODO Use IDs instead of names for fields
    '''
    logger.info('%s %s %s', project_id, dataset_id, selected_fields)
    specs = []

    # Get field properties
    with task_app.app_context():
        field_properties = db_access.get_field_properties(project_id, dataset_id)
        new_field_properties = []
        for field in field_properties:
            desired_keys = ['is_id', 'is_unique', 'general_type', 'type', 'name', 'id']
            new_field = { k: field[k] for k in desired_keys }
            new_field_properties.append(new_field)
        field_properties = new_field_properties

    if selected_fields:
        selected_field_docs = []
        c_fields = []
        c_fields_not_selected = []
        q_fields = []
        q_fields_not_selected = []

        n_c = 0
        n_q = 0
        n_c_total = 0
        n_q_total = 0

        for field in field_properties:
            is_selected_field = next((selected_field for selected_field in selected_fields if selected_field['id'] == field['id']), None)
            if is_selected_field:
                selected_field_docs.append(field)

            general_type = field['general_type']
            if general_type == 'c':
                if is_selected_field:
                    n_c = n_c + 1
                    c_fields.append(field)
                else:
                    c_fields_not_selected.append(field)
                n_c_total = n_c_total + 1

            elif general_type == 'q':
                if is_selected_field:
                    n_q = n_q + 1
                    q_fields.append(field)
                else:
                    q_fields_not_selected.append(field)
                n_q_total = n_q_total + 1

        logger.info('n_c = %s | n_q = %s', n_c, n_q)
        logger.info('c_fields %s', [f['name'] for f in c_fields])
        logger.info('q_fields %s', [f['name'] for f in q_fields])

        # Single-field properties
        no_args_specs = enumerate_viz_specs_no_args(selected_field_docs)
        specs.extend(no_args_specs)

        # Expanded visualization cases (e.g. including fields not provided in arguments)
        for c_field_1 in c_fields:
            # Pairs of C fields
            for c_field_2 in c_fields_not_selected:
                F_specs = F([c_field_1, c_field_2])
                specs.extend(F_specs)
            # C + Q field
            for q_field in q_fields_not_selected:
                D_specs = D(c_field_1, q_field)
                specs.extend(D_specs)
        for q_field_1 in q_fields:
            # Pairs of Q fields
            for q_field_2 in q_fields_not_selected:
                B_specs = B([q_field_1, q_field_2])
                specs.extend(B_specs)
            for c_field in c_fields_not_selected:
                D_specs = D(c_field, q_field_1)
                specs.extend(D_specs)

        # Marginal and cascading visualization cases (excluding single-field cases)
        # Essentially treating selection like a mini dataset
        if (n_c == 0):
            if (n_q > 1):
                B_specs = B(q_fields)
                specs.extend(B_specs)
        elif (n_c == 1):
            if (n_q == 1):
                D_specs = D(c_fields[0], q_fields[0])
                specs.extend(D_specs)
            elif (n_q > 1):
                for q_field in q_fields:
                    D_specs = D(c_fields[0], q_fields[0])
                    specs.extend(D_specs)
                E_specs = E(c_fields[0], q_fields)
                specs.extend(E_specs)
        elif (n_c > 1):
            if (n_q == 0):
                F_specs = F(c_fields)
                specs.extend(F_specs)
            elif (n_q == 1):
                for c_field in c_fields:
                    D_specs = D(c_fields, q_fields[0])
                    specs.extend(D_specs)
                G_specs = G(c_fields, q_fields[0])
                specs.extend(G_specs)
            elif (n_q > 1):
                H_specs = H(c_fields, q_fields)
                specs.extend(H_specs)

    else:
        no_args_specs = enumerate_viz_specs_no_args(field_properties)
        specs.extend(no_args_specs)

    # Assign viz_types and dataset_id
    for spec in specs:
        viz_types = get_viz_types_from_spec(spec)
        spec['viz_types'] = viz_types
        spec['dataset_id'] = dataset_id

    logger.info("Number of specs: %s", len(specs))

    return specs


@celery.task(bind=True)
def filter_viz_specs(self, enumerated_viz_specs, project_id):
    ''' Filtering enumerated viz specs based on interpretability and renderability '''
    self.update_state(state=states.PENDING)
    filtered_viz_specs = enumerated_viz_specs
    # self.update_state(state=states.SUCCESS, meta={'status': 'Filtered viz specs'})
    return filtered_viz_specs


@celery.task(bind=True)
def score_viz_specs(self, filtered_viz_specs, project_id):
    ''' Scoring viz specs based on effectiveness, expressiveness, and statistical properties '''
    self.update_state(state=states.PENDING)
    scored_viz_specs = []
    for i, spec in enumerate(filtered_viz_specs):
        if ((i + 1) % 100) == 0:
            logger.info('Scored %s out of %s specs', (i + 1), len(filtered_viz_specs))
        scored_spec = spec

        # TODO Opt=imize data reads
        with task_app.app_context():
            try:
                data = get_viz_data_from_enumerated_spec(spec, project_id, data_formats=['score', 'visualize'])
            except Exception as e:
                logger.error("Error getting viz data %s", e, exc_info=True)
                continue
        if not data:
            continue
        scored_spec['data'] = data

        score_doc = score_spec(spec)
        if not score_doc:
            continue
        scored_spec['score'] = score_doc

        del scored_spec['data']['score']

        scored_viz_specs.append(spec)

    # self.update_state(state=states.SUCCESS, meta={'status': 'Scored viz specs'})
    return scored_viz_specs


@celery.task(bind=True)
def format_viz_specs(self, scored_viz_specs, project_id):
    ''' Get viz specs into a format usable by front end '''
    self.update_state(state=states.PENDING)
    field_keys = ['fieldA', 'fieldB', 'binningField', 'aggFieldA', 'aggFieldB']

    formatted_viz_specs = []
    for s in scored_viz_specs:
        fields = {
            'categorical': [],  # TODO Propagate this
            'quantitative': []
        }
        args = s['args']

        # Extract all fields
        for field_key in field_keys:
            if field_key in args:
                field = args[field_key]
                field_general_type = specific_to_general_type[field['type']]
                if field_general_type is 'q': general_type_key = 'quantitative'
                else: general_type_key = 'categorical'

                fields[general_type_key].append({
                    'name': field['name'],
                    'id': field['id'],
                    'fieldType': field['type']
                })

        s['fields'] = fields

        formatted_viz_specs.append(s)

    # self.update_state(state=states.SUCCESS, meta={'status': 'Formatted viz specs'})
    return formatted_viz_specs


@celery.task(bind=True)
def save_viz_specs(self, specs, dataset_id, project_id):
    logger.info("Saving viz specs")
    self.update_state(state=states.PENDING)
    with task_app.app_context():
        # TODO Delete existing specs
        existing_specs = db_access.get_specs(project_id, dataset_id)
        if existing_specs:
            for spec in existing_specs:
                db_access.delete_spec(project_id, spec['id'])
        inserted_specs = db_access.insert_specs(project_id, specs)
    return inserted_specs
    # self.update_state(state=states.SUCCESS, meta={'status': 'Saved viz specs'})
