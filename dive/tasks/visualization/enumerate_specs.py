from dive.db import db_access
from dive.task_core import celery, task_app
from dive.tasks.visualization import GeneratingProcedure, TypeStructure, TermType, specific_to_general_type
from dive.tasks.visualization.marginal_spec_functions import A, B, C, D, E, F, G, H
from dive.tasks.visualization.data import get_viz_data_from_enumerated_spec
from dive.tasks.visualization.type_mapping import get_viz_types_from_spec
from dive.tasks.visualization.score_specs import score_spec

import logging
logger = logging.getLogger(__name__)


@celery.task()
def enumerate_viz_specs(project_id, dataset_id, selected_fields):
    '''
    TODO Move key filtering to the db query
    TODO Incorporate 0D and 1D data returns
    '''
    specs = []

    # Get field properties
    with task_app.app_context():
        desired_keys = ['is_id', 'is_unique', 'general_type', 'type', 'name', 'id']
        raw_field_properties = db_access.get_field_properties(project_id, dataset_id)
        field_properties = [{ k: field[k] for k in desired_keys } for field in raw_field_properties]

    logger.info('Number of fields: %s', len(field_properties))

    if selected_fields:
        selected_field_docs, c_fields, c_fields_not_selected, q_fields, q_fields_not_selected = \
            get_selected_fields(field_properties, selected_fields)

        baseline_specs = get_baseline_viz_specs(selected_field_docs)
        specs.extend(baseline_specs)

        cascading_viz_specs = get_cascading_viz_specs(c_fields, q_fields, c_fields_not_selected, q_fields_not_selected)
        specs.extend(cascading_viz_specs)

        expanded_viz_specs = get_expanded_viz_specs(c_fields, q_fields, c_fields_not_selected, q_fields_not_selected)
        specs.extend(expanded_viz_specs)
    else:
        baseline_specs = get_baseline_viz_specs(field_properties)
        specs.extend(baseline_specs)

    # Assign viz_types and dataset_id
    for spec in specs:
        # viz_types = get_viz_types_from_spec(spec)
        # spec['viz_types'] = viz_types
        spec['dataset_id'] = dataset_id

    logger.info("Number of specs: %s", len(specs))

    return specs


def get_baseline_viz_specs(field_properties):
    '''
    Single-field summary visualizations
    '''
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
    logger.info('Got %s baseline specs', len(specs))
    return specs


def get_selected_fields(field_properties, selected_fields):
    '''
    Get selected fields, selected Q and C fields, and non-selected Q and C fields
    '''
    selected_field_docs = []
    c_fields = []
    c_fields_not_selected = []
    q_fields = []
    q_fields_not_selected = []


    for field in field_properties:
        is_selected_field = next((selected_field for selected_field in selected_fields if selected_field['field_id'] == field['id']), None)
        if is_selected_field:
            selected_field_docs.append(field)

        general_type = field['general_type']
        if general_type == 'c':
            if is_selected_field:
                c_fields.append(field)
            else:
                c_fields_not_selected.append(field)

        elif general_type == 'q':
            if is_selected_field:
                q_fields.append(field)
            else:
                q_fields_not_selected.append(field)

    logger.info('n_c = %s | n_q = %s', len(c_fields), len(q_fields))
    logger.info('c_fields %s', [f['name'] for f in c_fields])
    logger.info('q_fields %s', [f['name'] for f in q_fields])
    return selected_field_docs, c_fields, c_fields_not_selected, q_fields, q_fields_not_selected


def get_cascading_viz_specs(c_fields, q_fields, c_fields_not_selected, q_fields_not_selected):
    '''
    Marginal and cascading visualization cases (excluding single-field cases)
    Essentially treating selection like a mini dataset
    '''
    specs = []
    n_c = len(c_fields)
    n_q = len(q_fields)

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
                D_specs = D(c_fields[0], q_fields[0])
                specs.extend(D_specs)
            G_specs = G(c_fields, q_fields[0])
            specs.extend(G_specs)
        elif (n_q > 1):
            H_specs = H(c_fields, q_fields)
            specs.extend(H_specs)
    logger.info('Got %s cascading specs', len(specs))
    return specs


def get_expanded_viz_specs(c_fields, q_fields, c_fields_not_selected, q_fields_not_selected):
    '''
    Expanded visualization cases (e.g. including fields not provided in arguments)
    '''
    specs = []
    n_c = len(c_fields)
    n_q = len(q_fields)

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
    logger.info('Got %s expanded specs', len(specs))
    return specs
