from dive.db import db_access
from dive.task_core import celery, task_app
from dive.tasks.ingestion import specific_to_general_type
from dive.tasks.visualization import GeneratingProcedure, TypeStructure, TermType
from dive.tasks.visualization.marginal_spec_functions import *
from dive.tasks.visualization.data import get_viz_data_from_enumerated_spec
from dive.tasks.visualization.type_mapping import get_viz_types_from_spec
from dive.tasks.visualization.score_specs import score_spec

from celery import states

import logging
logger = logging.getLogger(__name__)



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
        selected_field_docs, c_fields, c_fields_not_selected, q_fields, q_fields_not_selected, t_fields, t_fields_not_selected = \
            get_selected_fields(field_properties, selected_fields)

        baseline_specs = get_baseline_viz_specs(selected_field_docs)
        specs.extend(baseline_specs)

        cascading_viz_specs = get_cascading_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected)
        specs.extend(cascading_viz_specs)

        expanded_viz_specs = get_expanded_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected)
        specs.extend(expanded_viz_specs)
    else:
        baseline_specs = get_baseline_viz_specs(field_properties)
        specs.extend(baseline_specs)

    # Assign viz_types and dataset_id
    for spec in specs:
        # viz_types = get_viz_types_from_spec(spec)
        # spec['viz_types'] = viz_types
        spec['dataset_id'] = dataset_id

    return specs


def get_selected_fields(field_properties, selected_fields):
    '''
    Get selected fields, selected Q, C, and T fields, and non-selected Q, C, and T fields
    '''
    selected_field_docs = []
    c_fields = []
    c_fields_not_selected = []
    q_fields = []
    q_fields_not_selected = []
    t_fields = []
    t_fields_not_selected = []

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

        elif general_type == 't':
            if is_selected_field:
                t_fields.append(field)
            else:
                t_fields_not_selected.append(field)

    logger.info('n_c = %s, n_q = %s, n_t = %s', len(c_fields), len(q_fields), len(t_fields))
    logger.info('c_fields: %s', [f['name'] for f in c_fields])
    logger.info('q_fields: %s', [f['name'] for f in q_fields])
    logger.info('t_fields: %s', [f['name'] for f in t_fields])
    return selected_field_docs, c_fields, c_fields_not_selected, q_fields, q_fields_not_selected, t_fields, t_fields_not_selected


def get_baseline_viz_specs(field_properties):
    '''
    Single-field summary visualizations
    '''
    specs = []
    for field in field_properties:
        # Skip unique and ID fields

        general_type = field['general_type']
        if general_type == 'c':
            if field['is_unique'] or field['is_id']: continue
            single_c_specs = single_c(field)
            specs.extend(single_c_specs)
        elif general_type == 'q':
            single_q_specs = single_q(field)
            specs.extend(single_q_specs)
        elif general_type == 't':
            single_t_specs = single_t(field)
            specs.extend(single_t_specs)
        else:
            raise ValueError('Not valid general_type', general_type)
    logger.info('Got %s baseline specs', len(specs))
    return specs


def get_cascading_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected):
    '''
    Marginal and cascading visualization cases (excluding single-field cases)
    Essentially treating selection like a mini dataset
    '''
    specs = []
    n_c = len(c_fields)
    n_q = len(q_fields)
    n_t = len(t_fields)

    # TODO Implement the cascading aspect, not only the marginal cases
    # TODO Can we encode requirements into the spec functions themselves?

    # Single field specs, single type
    if (n_c == 1) and (n_t == 0) and (n_q == 0):
        specs.extend(single_c(c_fields[0]))
    if (n_c == 0) and (n_t == 1) and (n_q == 0):
        specs.extend(single_t(t_fields[0]))
    if (n_c == 0) and (n_t == 0) and (n_q == 1):
        specs.extend(single_q(q_fields[0]))

    # Single field specs, multi type
    if (n_c == 1) and (n_t == 1) and (n_q == 0):
        specs.extend(single_ct(c_fields[0], t_fields[0]))
    if (n_c == 1) and (n_t == 0) and (n_q == 1):
        specs.extend(single_cq(c_fields[0], q_fields[0]))
    if (n_c == 0) and (n_t == 1) and (n_q == 1):
        specs.extend(single_tq(t_fields[0], q_fields[0]))
    if (n_c == 1) and (n_t == 1) and (n_q == 1):
        specs.extend(single_ctq(c_fields[0], t_fields[0], q_fields[0]))

    # Multi field specs, single type
    if (n_c > 1) and (n_t == 0) and (n_q == 0):
        specs.extend(multi_c(c_fields))
    if (n_c == 0) and (n_t > 1) and (n_q == 0):
        specs.extend(multi_t(t_fields))
    if (n_c == 0) and (n_t == 0) and (n_q > 1):
        specs.extend(multi_q(q_fields))

    # Mixed field specs, multi type
    if (n_c == 1) and (n_t > 1) and (n_q == 0):
        specs.extend(single_c_multi_t(c_fields[0], t_fields))
    if (n_c == 1) and (n_t == 0) and (n_q > 1):
        specs.extend(single_c_multi_q(c_fields[0], q_fields))
    if (n_c > 1) and (n_t == 0) and (n_q == 1):
        specs.extend(single_q_multi_c(c_fields, q_fields[0]))
    if (n_c == 0) and (n_t > 1) and (n_q == 1):
        specs.extend(single_q_multi_t(t_fields, q_fields[0]))
    if (n_c > 1) and (n_t == 1) and (n_q == 0):
        specs.extend(single_t_multi_c(t_fields[0], c_fields))
    if (n_c == 0) and (n_t == 1) and (n_q > 1):
        specs.extend(single_t_multi_q(t_fields[0], q_fields))

    # Multi field specs, multi type
    if (n_c > 1) and (n_t > 1) and (n_q == 0):
        specs.extend(multi_ct(c_fields, t_fields))
    if (n_c > 1) and (n_t == 0) and (n_q > 1):
        specs.extend(multi_cq(c_fields, q_fields))
    if (n_c == 0) and (n_t > 0) and (n_q > 1):
        specs.extend(multi_tq(t_fields, q_fields))
    if (n_c > 1) and (n_t > 1) and (n_q > 1):
        specs.extend(multi_ctq(c_fields, t_fields, q_fields))

    logger.debug('Got %s cascading specs', len(specs))
    return specs


def get_expanded_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected):
    '''
    Expanded visualization cases (e.g. including fields not provided in arguments)
    '''
    specs = []
    n_c = len(c_fields)
    n_q = len(q_fields)
    n_t = len(t_fields)

    for c_field_1 in c_fields:
        # Pairs of C fields
        for c_field_2 in c_fields_not_selected:
            multi_c_specs = multi_c([c_field_1, c_field_2])
            specs.extend(multi_c_specs)
        # C + Q field
        for q_field in q_fields_not_selected:
            single_cq_specs = single_cq(c_field_1, q_field)
            specs.extend(single_cq_specs)
    for q_field_1 in q_fields:
        # Pairs of Q fields
        for q_field_2 in q_fields_not_selected:
            multi_q_specs = multi_q([q_field_1, q_field_2])
            specs.extend(multi_q_specs)
        for c_field in c_fields_not_selected:
            single_cq_specs = single_cq(c_field, q_field_1)
            specs.extend(single_cq_specs)
    logger.debug('Got %s expanded specs', len(specs))
    return specs
