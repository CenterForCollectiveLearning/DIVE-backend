import math
import copy
from pprint import pprint
from time import time
from scipy import stats as sc_stats
from flask import current_app

from dive.db import db_access
from dive.task_core import celery, task_app
from dive.tasks.visualization.marginal_spec_functions import A, B, C, D, E, F, G, H
from dive.tasks.visualization.data import get_viz_data_from_enumerated_spec
from dive.tasks.visualization.type_mapping import get_viz_types_from_spec
from dive.tasks.visualization.scoring import score_spec

from celery import states
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


specific_to_general_type = {
    'float': 'q',
    'integer': 'q',
    'string': 'c',
    'continent': 'c',
    'countryName': 'c',
    'countryCode2': 'c',
    'countryCode3': 'c',
    'datetime': 'q',
}


@celery.task(bind=True)
def enumerate_viz_specs(self, dataset_id, project_id):
    ''' Enumerated viz specs given data, properties, and ontologies '''
    self.update_state(state='PROGRESS')
    with task_app.app_context():
        field_properties = db_access.get_field_properties(project_id, dataset_id)

    specs = []

    c_fields = []
    q_fields = []
    for f in field_properties:
        general_type = specific_to_general_type[f['type']]
        if general_type is 'q':
            q_fields.append(f)
        elif general_type is 'c':
            c_fields.append(f)

    c_count = len(c_fields)
    q_count = len(q_fields)


    # Cases A - B
    # Q > 0, C = 0
    if q_count and not c_count:
        # Case A) Q = 1, C = 0
        if q_count == 1:
            logger.info("Case A")
            q_field = q_fields[0]
            A_specs = A(q_field)
            specs.extend(A_specs)
        elif q_count >= 1:
            logger.info("Case B")
            for q_field in q_fields:
                A_specs = A(q_field)
                specs.extend(A_specs)
            B_specs = B(q_fields)
            specs.extend(B_specs)

    # Cases C - E
    # C = 1
    elif c_count == 1:
        # Case C) C = 1, Q = 0
        if q_count == 0:
            logger.info("Case C")
            c_field = c_fields[0]
            C_specs = C(c_field)
            specs.extend(C_specs)

        # Case D) C = 1, Q = 1
        elif q_count == 1:
            logger.info("Case D")

            # One case of A
            q_field = q_fields[0]
            A_specs = A(q_field)
            specs.extend(A_specs)

            # One case of C
            c_field = c_fields[0]
            C_specs = C(c_field)
            specs.extend(C_specs)

            # One case of D
            c_field, q_field = c_fields[0], q_fields[0]
            D_specs = D(c_field, q_field)
            specs.extend(D_specs)

        # Case E) C = 1, Q >= 1
        elif q_count > 1:
            logger.info("Case E")

            for q_field in q_fields:
                # N_Q cases of A
                A_specs = A(q_field)
                specs.extend(A_specs)

                # N_Q cases of D
                D_specs = D(c_fields[0], q_field)
                specs.extend(D_specs)

            # N_C cases of C
            for c_field in c_fields:
                C_specs = C(c_field)
                specs.extend(C_specs)

            # One case of B
            B_specs = B(q_fields)
            specs.extend(B_specs)

            # One case of E
            E_specs = E(c_field, q_fields)
            specs.extend(E_specs)


    # Cases F - H
    # C >= 1
    elif c_count >= 1:
        # Case F) C >= 1, Q = 0
        if q_count == 0:
            logger.info("Case F")

            # N_C cases of C
            for c_field in c_fields:
                C_specs = C(c_field)
                specs.extend(C_specs)

            # One case of F
            F_specs = F(c_fields)
            specs.extend(F_specs)

        # Case G) C >= 1, Q = 1
        elif q_count == 1:
            logger.info("Case G")
            q_field = q_fields[0]

            # N_C cases of D
            for c_field in c_fields:
                D_specs = D(c_field, q_field)
                specs.extend(D_specs)

            # One case of F
            F_specs = F(c_fields)
            specs.extend(F_specs)

            # One case of G
            G_specs = G(c_fields, q_field)
            specs.extend(G_specs)

        # Case H) C >= 1, Q > 1
        elif q_count > 1:
            logger.info("Case H")

            # N_C cases of C
            # N_C cases of E
            for c_field in c_fields:
                C_specs = C(c_field)
                specs.extend(C_specs)

                E_specs = E(c_field, q_fields)
                specs.extend(E_specs)

                # N_C * N_Q cases of D
                for q_field in q_fields:
                    D_specs = D(c_field, q_field)
                    specs.extend(D_specs)

            # N_Q cases of A
            # N_Q cases of G
            for q_field in q_fields:
                A_specs = A(q_field)
                specs.extend(A_specs)

                G_specs = G(c_fields, q_field)
                specs.extend(G_specs)

            # One case of B
            B_specs = B(q_fields)
            specs.extend(B_specs)

            # One case of F
            F_specs = F(c_fields)
            specs.extend(F_specs)

    all_specs_with_types = []

    desired_viz_types = ["hist", "scatter", "bar", "line", "pie"]
    # Assign viz types to specs (not 1-1)
    for spec in specs:
        spec['dataset_id'] = dataset_id
        viz_types = get_viz_types_from_spec(spec)
        for viz_type in viz_types:

            # Necessary to deep copy?
            spec_with_viz_type = spec
            if viz_type in desired_viz_types:
                spec_with_viz_type['viz_type'] = viz_type
                all_specs_with_types.append(spec_with_viz_type)
            else:
                continue

    logger.info("Number of specs: %s", len(all_specs_with_types))

    self.update_state(state=states.SUCCESS, meta={'status': 'Enumerated viz specs'})
    return all_specs_with_types


@celery.task(bind=True)
def filter_viz_specs(self, enumerated_viz_specs, project_id):
    ''' Filtering enumerated viz specs based on interpretability and renderability '''
    self.update_state(state='PROGRESS')
    filtered_viz_specs = enumerated_viz_specs
    self.update_state(state=states.SUCCESS, meta={'status': 'Filtered viz specs'})
    return filtered_viz_specs


@celery.task(bind=True)
def score_viz_specs(self, filtered_viz_specs, project_id):
    ''' Scoring viz specs based on effectiveness, expressiveness, and statistical properties '''
    self.update_state(state='PROGRESS')
    scored_viz_specs = []
    for i, spec in enumerate(filtered_viz_specs):
        if ((i + 1) % 100) == 0:
            logger.info('Scored %s out of %s specs', (i + 1), len(filtered_viz_specs))
        scored_spec = spec

        # TODO Optimize data reads
        with task_app.app_context():
            try:
                data = get_viz_data_from_enumerated_spec(spec, project_id, data_formats=['score', 'visualize'])
            except Exception as e:
                logger.error(e)
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

    self.update_state(state=states.SUCCESS, meta={'status': 'Scored viz specs'})
    return scored_viz_specs


@celery.task(bind=True)
def format_viz_specs(self, scored_viz_specs, project_id):
    ''' Get viz specs into a format usable by front end '''
    self.update_state(state='PROGRESS')
    field_keys = ['fieldA', 'fieldB', 'binningField', 'aggFieldA', 'aggFieldB']

    formatted_viz_specs = []
    for s in scored_viz_specs:
        properties = {
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

                properties[general_type_key].append({
                    'name': field['name'],
                    'id': field['id'],
                    'fieldType': field['type']
                })

        s['properties'] = properties

        formatted_viz_specs.append(s)

    self.update_state(state=states.SUCCESS, meta={'status': 'Formatted viz specs'})
    return formatted_viz_specs


@celery.task(bind=True, ignore_result=True)
def save_viz_specs(self, specs, dataset_id, project_id):
    logger.info("Saving viz specs")
    self.update_state(state='PROGRESS')
    with task_app.app_context():
        # TODO Delete existing specs
        existing_specs = db_access.get_specs(project_id, dataset_id)
        if existing_specs:
            for spec in existing_specs:
                db_access.delete_spec(project_id, spec['id'])
        db_access.insert_specs(project_id, specs)
    self.update_state(state=states.SUCCESS, meta={'status': 'Saved viz specs'})
