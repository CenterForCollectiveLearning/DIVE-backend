import math
import copy
from pprint import pprint
from time import time
from scipy import stats as sc_stats
from flask import current_app

from dive.db import db_access
from dive.tasks import celery
from dive.data.field_properties import get_field_properties
from dive.visualization.marginal_spec_functions import A, B, C, D, E, F, G, H
from dive.visualization.data import get_viz_data_from_enumerated_spec
from dive.visualization.type_mapping import get_viz_types_from_spec
from dive.visualization.scoring import score_spec

import logging
logger = logging.getLogger(__name__)

@celery.task
def compute_viz_specs(project_id, dataset_id=None):
    '''
    Wrapper function used to
        1. Enumerate
        2. Filter
        3. Score
        4. Format
    visualization specifications.

    Accepts:
        project_id, dataset_id (optionally)
    Returns:
        List of scored specs for dataset_id (all datasets if dataset_id not specified)
    '''
    dataset_find_doc = {}

    datasets = db_access.get_datasets(project_id)
    logger.info("Datasets %s", datasets)

    dataset_ids = [d['id'] for d in datasets]
    field_properties_by_dataset_id = get_field_properties(project_id, dataset_ids)

    # TODO Store ontologies
    ontologies = None  # db_access.get_ontology(project_id)

    enumerated_viz_specs = enumerate_viz_specs(datasets, field_properties_by_dataset_id, ontologies, project_id)
    filtered_viz_specs = filter_viz_specs(enumerated_viz_specs, project_id)
    scored_viz_specs = score_viz_specs(filtered_viz_specs, project_id)
    formatted_viz_specs = format_viz_specs(scored_viz_specs)

    # Saving specs, using return from insert to get id
    final_viz_specs = {}
    for dataset_id, specs in formatted_viz_specs.iteritems():
        final_viz_specs[dataset_id] = db_access.insert_specs(project_id, dataset_id, specs)

    if dataset_id:
        logger.info("Returning just specs, no dataset_id mapping")
        return final_viz_specs[dataset_id]
    return final_viz_specs

def get_viz_specs(project_id, dataset_id=None):
    ''' Get viz specs if exists and compute if doesn't exist '''

    ### TODO Fix bug with getting tons of specs when recomputing

    specs_find_doc = {}
    if dataset_id: specs_find_doc['dataset_id'] = dataset_id

    existing_specs = db_access.get_specs(project_id, dataset_id)
    logger.info("Number of existing specs: %s", len(existing_specs))
    if existing_specs and not current_app.config['RECOMPUTE_VIZ_SPECS']:
        if dataset_id:
            return existing_specs
        else:
            result = {}
            for s in existing_specs:
                dataset_id = s['dataset_id']
                if dataset_id not in result: result[dataset_id] = [s]
                else: result[dataset_id].append(s)
            return result
    else:
        logger.info("Computing viz specs")
        return compute_viz_specs(project_id, dataset_id)


specific_to_general_type = {
    'float': 'q',
    'integer': 'q',
    'string': 'c',
    'continent': 'c',
    'countryName': 'c',
    'datetime': 'q'
}


# TODO Move the case classifying into dataset ingestion (doesn't need to be here!)
# 1) Enumerated viz specs given data, properties, and ontologies
def enumerate_viz_specs(datasets, field_properties_by_dataset_id, ontologies, project_id):
    dataset_ids = [ d['id'] for d in datasets ]
    specs_by_dataset_id = dict([(dataset_id, []) for dataset_id in dataset_ids])

    types_by_dataset_id = {}
    fields_by_dataset_id = {}

    # Iterate through datasets (no cross-dataset visualizations for now)
    for dataset_id, dataset_field_properties in field_properties_by_dataset_id.iteritems():
        specs = []
        c_fields = []
        q_fields = []
        for f in dataset_field_properties:
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
                print "Case A"
                q_field = q_fields[0]
                A_specs = A(q_field)
                specs.extend(A_specs)
            elif q_count >= 1:
                print "Case B"
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
                print "Case C"
                c_field = c_fields[0]
                C_specs = C(c_field)
                specs.extend(C_specs)

            # Case D) C = 1, Q = 1
            elif q_count == 1:
                print "Case D"

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
                print "Case E"

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
                print "Case F"

                # N_C cases of C
                for c_field in c_fields:
                    C_specs = C(c_field)
                    specs.extend(C_specs)

                # One case of F
                F_specs = F(c_fields)
                specs.extend(F_specs)

            # Case G) C >= 1, Q = 1
            elif q_count == 1:
                print "Case G"
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
                print "Case H"

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
            viz_types = get_viz_types_from_spec(spec)
            print viz_types
            for viz_type in viz_types:

                # Necessary to deep copy?
                spec_with_viz_type = copy.deepcopy(spec)
                if viz_type in desired_viz_types:
                    spec_with_viz_type['vizType'] = viz_type
                    all_specs_with_types.append(spec_with_viz_type)
                else:
                    continue

        specs_by_dataset_id[dataset_id] = all_specs_with_types

        print "\tN_c:", c_count
        print "\tN_q:", q_count
        print "\tNumber of specs:", len(specs_by_dataset_id[dataset_id])
        # pprint(specs_by_dataset_id)
    return specs_by_dataset_id


# 2) Filtering enumerated viz specs based on interpretability and renderability
def filter_viz_specs(enumerated_viz_specs, project_id):
    filtered_viz_specs = enumerated_viz_specs
    return filtered_viz_specs


# 3) Scoring viz specs based on effectiveness, expressiveness, and statistical properties
def score_viz_specs(filtered_viz_specs, project_id):
    scored_viz_specs_by_dataset_id = {}
    for dataset_id, specs in filtered_viz_specs.iteritems():
        scored_viz_specs = []
        for spec in specs:
            scored_spec = spec

            # TODO Optimize data reads
            data = get_viz_data_from_enumerated_spec(spec, dataset_id, project_id, data_formats=['score', 'visualize'])
            if not data:
                continue
            scored_spec['data'] = data

            score_doc = score_spec(spec)
            if not score_doc:
                continue
            scored_spec['score'] = score_doc

            del scored_spec['data']['score']

            scored_viz_specs.append(spec)

        scored_viz_specs_by_dataset_id[dataset_id] = scored_viz_specs

    return scored_viz_specs_by_dataset_id


def format_viz_specs(scored_viz_specs):
    ''' Get viz specs into a format usable by front end '''
    field_keys = ['fieldA', 'fieldB', 'binningField', 'aggFieldA', 'aggFieldB']

    formatted_viz_specs_by_dataset_id = {}
    for dataset_id, specs in scored_viz_specs.iteritems():
        formatted_viz_specs = []
        for s in specs:
            s['dataset_id'] = dataset_id
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

        formatted_viz_specs_by_dataset_id[dataset_id] = formatted_viz_specs

    return formatted_viz_specs_by_dataset_id
