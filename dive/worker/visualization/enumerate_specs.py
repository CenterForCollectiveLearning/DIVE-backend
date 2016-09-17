import numpy as np
from itertools import combinations

from dive.base.db import db_access
from dive.worker.core import celery, task_app
from dive.worker.ingestion.constants import specific_to_general_type
from dive.worker.visualization import GeneratingProcedure, TypeStructure, TermType
from dive.worker.visualization.marginal_spec_functions import *
from dive.worker.visualization.data import get_viz_data_from_enumerated_spec
from dive.worker.visualization.type_mapping import get_viz_types_from_spec
from dive.worker.visualization.score_specs import score_spec

from celery import states

import logging
logger = logging.getLogger(__name__)

def get_list_of_unique_dicts(li):
    return list(np.unique(np.array(li)))

def enumerate_viz_specs(project_id, dataset_id, selected_fields, recommendation_types=[], spec_limit=None):
    '''
    TODO Move key filtering to the db query
    TODO Incorporate 0D and 1D data returns
    '''
    specs = []
    num_selected_fields = len(selected_fields)

    logger.info('Recommendation Types %s', recommendation_types)

    # Get field properties
    with task_app.app_context():
        desired_keys = ['is_id', 'is_unique', 'general_type', 'type', 'name', 'id']
        raw_field_properties = db_access.get_field_properties(project_id, dataset_id, is_id=False)
        field_properties = [{ k: field[k] for k in desired_keys } for field in raw_field_properties]

    if selected_fields:
        selected_field_docs, c_fields, c_fields_not_selected, q_fields, q_fields_not_selected, t_fields, t_fields_not_selected = \
            get_selected_fields(field_properties, selected_fields)

        if 'baseline' in recommendation_types:
            baseline_viz_specs = get_baseline_viz_specs(selected_field_docs)
            specs.extend([dict(s, recommendation_type='baseline') for s in baseline_viz_specs ])

        if 'subset' in recommendation_types:
            subset_viz_specs = get_subset_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected)
            specs.extend([dict(s, recommendation_type='subset') for s in subset_viz_specs ])

        if 'exact' in recommendation_types:
            exact_viz_specs = get_exact_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected)
            specs.extend([dict(s, recommendation_type='exact') for s in exact_viz_specs ])

        if 'expanded' in recommendation_types:
            expanded_viz_specs = get_expanded_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected)
            specs.extend([dict(s, recommendation_type='expanded') for s in expanded_viz_specs ])

    else:
        if 'exact' in recommendation_types:
            baseline_viz_specs = get_baseline_viz_specs(field_properties)
            specs.extend([dict(s, recommendation_type='exact') for s in baseline_viz_specs ])

    # Limit Number of specs
    if spec_limit:
        specs = specs[:spec_limit]

    # Deduplicate
    specs = get_list_of_unique_dicts(specs)

    # Assign viz_types and dataset_id
    for spec in specs:
        spec['dataset_id'] = dataset_id

    logger.info('Number of unique specs: %s', len(specs))

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

    return selected_field_docs, c_fields, c_fields_not_selected, q_fields, q_fields_not_selected, t_fields, t_fields_not_selected


def get_baseline_viz_specs(field_properties):
    '''
    Single-field visualizations
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
            if field['is_id']: continue
            single_q_specs = single_q(field)
            specs.extend(single_q_specs)
        elif general_type == 't':
            single_t_specs = single_t(field)
            specs.extend(single_t_specs)
        else:
            raise ValueError('Not valid general_type', general_type)
    return specs


def get_subset_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected):
    '''
    Multi-field visualizations using subset of selected fields
    Not including exact match cases
    '''
    specs = []
    n_c = len(c_fields)
    n_q = len(q_fields)
    n_t = len(t_fields)

    if (n_c + n_q + n_t) <= 2:
        return specs

    if n_c >= 2:
        for c_field_1, c_field_2 in combinations(c_fields, 2):
            multi_c_specs = multi_c([c_field_1, c_field_2])
            specs.extend(multi_c_specs)
        if (n_c >= 3) and n_q:
            for q_field in q_fields:
                single_q_multi_c_specs = single_q_multi_c(c_fields, q_field)
                specs.extend(single_q_multi_c_specs)

    if n_q >= 2:
        for q_field_1, q_field_2 in combinations(q_fields, 2):
            multi_q_specs = multi_q([q_field_1, q_field_2])
            specs.extend(multi_q_specs)
        if (n_q >= 3) and n_c:
            for c_field in c_fields:
                single_c_multi_q_specs = single_c_multi_q(c_field, q_fields)
                specs.extend(single_c_multi_q_specs)

    if (n_c and n_q) and ((n_c != 1) or (n_q != 1)):
        for c_field in c_fields:
            for q_field in q_fields:
                single_cq_specs = single_cq(c_field, q_field)
                specs.extend(single_cq_specs)

    if (n_t and n_q) and ((n_t != 1) or (n_q != 1)):
        for t_field in t_fields:
            for q_field in q_fields:
                single_tq_specs = single_tq(t_field, q_field)
                specs.extend(single_tq_specs)

    return specs

def get_exact_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected):
    '''
    Exact visualization cases (excluding single-field cases)
    TODO Make sure only one case is fulfilled in each instance
    '''
    specs = []
    n_c = len(c_fields)
    n_q = len(q_fields)
    n_t = len(t_fields)

    # Multi field specs, multi type
    if (n_c > 1) and (n_t > 1) and (n_q == 0):
        specs.extend(multi_ct(c_fields, t_fields))
    if (n_c > 1) and (n_t == 0) and (n_q > 1):
        specs.extend(multi_cq(c_fields, q_fields))
    if (n_c == 0) and (n_t > 0) and (n_q > 1):
        specs.extend(multi_tq(t_fields, q_fields))
    if (n_c > 1) and (n_t > 1) and (n_q > 1):
        specs.extend(multi_ctq(c_fields, t_fields, q_fields))

    if specs: return specs

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

    if specs: return specs

    # Multi field specs, single type
    if (n_c > 1) and (n_t == 0) and (n_q == 0):
        specs.extend(multi_c(c_fields))
    if (n_c == 0) and (n_t > 1) and (n_q == 0):
        specs.extend(multi_t(t_fields))
    if (n_c == 0) and (n_t == 0) and (n_q > 1):
        specs.extend(multi_q(q_fields))

    if specs: return specs

    # Single field specs, multi type
    if (n_c == 1) and (n_t == 1) and (n_q == 0):
        specs.extend(single_ct(c_fields[0], t_fields[0]))
    if (n_c == 1) and (n_t == 0) and (n_q == 1):
        specs.extend(single_cq(c_fields[0], q_fields[0]))
    if (n_c == 0) and (n_t == 1) and (n_q == 1):
        specs.extend(single_tq(t_fields[0], q_fields[0]))
    if (n_c == 1) and (n_t == 1) and (n_q == 1):
        specs.extend(single_ctq(c_fields[0], t_fields[0], q_fields[0]))

    if specs: return specs

    # Single field specs, single type
    if (n_c == 1) and (n_t == 0) and (n_q == 0):
        specs.extend(single_c(c_fields[0]))
    if (n_c == 0) and (n_t == 1) and (n_q == 0):
        specs.extend(single_t(t_fields[0]))
    if (n_c == 0) and (n_t == 0) and (n_q == 1):
        specs.extend(single_q(q_fields[0]))

    return specs


def get_expanded_viz_specs(c_fields, q_fields, t_fields, c_fields_not_selected, q_fields_not_selected, t_fields_not_selected):
    '''
    Expanded visualization cases (e.g. including non-selected fields)
    '''
    specs = []
    n_c = len(c_fields)
    n_q = len(q_fields)
    n_t = len(t_fields)

    # One selected C
    for c_field_1 in c_fields:
        # Pairs of C fields
        for c_field_2 in c_fields_not_selected:
            multi_c_specs = multi_c([c_field_1, c_field_2])
            specs.extend(multi_c_specs)
        # C + Q field
        for q_field in q_fields_not_selected:
            single_cq_specs = single_cq(c_field_1, q_field)
            specs.extend(single_cq_specs)
        for t_field in t_fields_not_selected:
            single_ct_specs = single_ct(t_field, c_field_1)
            specs.extend(single_ct_specs)

    # One expanded Q
    for q_field_1 in q_fields:
        # Pairs of Q fields
        for q_field_2 in q_fields_not_selected:
            multi_q_specs = multi_q([q_field_1, q_field_2])
            specs.extend(multi_q_specs)
        for c_field in c_fields_not_selected:
            single_cq_specs = single_cq(c_field, q_field_1)
            specs.extend(single_cq_specs)
        for t_field in t_fields_not_selected:
            single_tq_specs = single_tq(t_field, q_field_1)
            specs.extend(single_tq_specs)


    return specs
