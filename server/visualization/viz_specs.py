from data.db import MongoInstance as MI
from marginal_spec_functions import A, B, C, D, E, F, G, H
from viz_stats import *
from scipy import stats as sc_stats
from viz_data import get_viz_data_from_enumerated_spec
from viz_type_mapping import get_viz_types_from_spec
from scoring import score_spec

from pprint import pprint
from time import time
import math
import uuid


def compute_viz_specs(pID, dID=None):
    '''
    Wrapper function used to
        1. Enumerate
        2. Filter
        3. Score
        4. Format
    visualization specifications.

    Accepts:
        pID, dID (optionally)
    Returns:
        List of scored specs for dID (all datasets if dID not specified)
    '''
    dataset_find_doc = {}
    if dID: dataset_find_doc = {'_id': ObjectId(dID)}
    datasets = MI.getData(dataset_find_doc, pID)
    field_properties = MI.getFieldProperty(None, pID)
    ontologies = MI.getOntology(None, pID)

    enumerated_viz_specs = enumerate_viz_specs(datasets, field_properties, ontologies, pID)
    filtered_viz_specs = filter_viz_specs(enumerated_viz_specs, pID)
    scored_viz_specs = score_viz_specs(filtered_viz_specs, pID)
    formatted_viz_specs = format_viz_specs(scored_viz_specs)

    # Saving specs
    saved_viz_specs = []
    for dID_key, specs in formatted_viz_specs.iteritems():
        for spec in specs:
            spec['dID'] = dID_key
        saved_viz_specs.extend(specs)

    if saved_viz_specs:
        sIDs = MI.setSpecs(saved_viz_specs, pID)
        for s in saved_viz_specs:
            s['id'] = str(s['_id'])
            del s['_id']

    if dID:
        print "Returning just specs, no dID mapping", dID
        return formatted_viz_specs[dID]
    return formatted_viz_specs


def get_viz_specs(pID, dID=None):
    ''' Get viz specs if exists and compute if doesn't exist '''
    RECOMPUTE = True

    specs_find_doc = {}
    if dID: specs_find_doc['dID'] = dID

    existing_specs = MI.getSpecs(specs_find_doc, pID)
    if existing_specs and not RECOMPUTE:
        if dID:
            return existing_specs
        else:
            result = {}
            for s in existing_specs:
                dID = s['dID']
                if dID not in result: result[dID] = [s]
                else: result[dID].append(s)
            return result
    else:
        return compute_viz_specs(pID, dID)


specific_to_general_type = {
    'float': 'q',
    'integer': 'q',
    'string': 'c',
    'continent': 'c',
    'countryName': 'c',
    'datetime': 'q'
}

def specs_to_viz_types(specs):
    result = []
    return result


class Specification(object):
    '''
    A visualization specification with the following properties: structure, args, and description.
    '''
    def __init__(self, structure, arguments, description={}):
        self.structure = structure
        self.arguments = arguments
        self.description = description


# TODO Move the case classifying into dataset ingestion (doesn't need to be here!)
# 1) Enumerated viz specs given data, properties, and ontologies
def enumerate_viz_specs(datasets, properties, ontologies, pID):
    dIDs = [ d['dID'] for d in datasets ]
    specs_by_dID = dict([(dID, []) for dID in dIDs])

    types_by_dID = {}
    fields_by_dID = {}

    for p in properties:
        dID = p['dID']
        relevant_fields = {
            'id': str(p['propertyID']),
            'label': p['label'],
            'unique': p['unique'],
            'type': p['type'],
            'normality': p['normality'],
            'values': p['values']
        }
        if dID in fields_by_dID:
            # TODO Necessary to preserve the order of fields?
            fields_by_dID[dID].append(relevant_fields)
        else:
            fields_by_dID[dID] = [ relevant_fields ]

    # Iterate through datasets (no cross-dataset visualizations for now)
    for dID in fields_by_dID.keys():
        specs = []
        c_fields = []
        q_fields = []
        fields = fields_by_dID[dID]
        for f in fields:
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

        # Assign viz types to specs (not 1-1)
        for spec in specs:
            viz_types = get_viz_types_from_spec(spec)
            for viz_type in viz_types:
                spec_with_viz_type = spec
                spec_with_viz_type['vizType'] = viz_type
                all_specs_with_types.append(spec_with_viz_type)

        specs_by_dID[dID] = all_specs_with_types
        specs_by_dID[dID] = specs

        print "\tN_c:", c_count
        print "\tN_q:", q_count
        print "\tNumber of specs:", len(specs_by_dID[dID])
        # pprint(specs_by_dID)
    return specs_by_dID


# 2) Filtering enumerated viz specs based on interpretability and renderability
def filter_viz_specs(enumerated_viz_specs, pID):
    filtered_viz_specs = enumerated_viz_specs
    return filtered_viz_specs


# 3) Scoring viz specs based on effectiveness, expressiveness, and statistical properties
def score_viz_specs(filtered_viz_specs, pID):
    scored_viz_specs_by_dID = {}
    for dID, specs in filtered_viz_specs.iteritems():
        scored_viz_specs = []
        for spec in specs:
            scored_spec = spec
            # TODO Optimize data reads
            # TODO Don't attach data to spec
            data = get_viz_data_from_enumerated_spec(spec, dID, pID, data_formats=['score', 'visualize'])
            if not data:
                continue
            scored_spec['data'] = data


            score_doc = score_spec(spec)
            if not score_doc:
                continue
            scored_spec['score'] = score_doc

            del scored_spec['data']['score']

            scored_viz_specs.append(spec)
        scored_viz_specs_by_dID[dID] = scored_viz_specs

    return scored_viz_specs_by_dID


def format_viz_specs(scored_viz_specs):
    ''' Get viz specs into a format usable by front end '''
    field_keys = ['fieldA', 'fieldB', 'binningField', 'aggFieldA', 'aggFieldB']

    formatted_viz_specs_by_dID = {}
    for dID, specs in scored_viz_specs.iteritems():
        formatted_viz_specs = []
        for s in specs:
            s['dID'] = dID
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
                        'label': field['label'],
                        'id': field['id'],
                        'fieldType': field_key
                    })

            s['properties'] = properties


            formatted_viz_specs.append(s)

        formatted_viz_specs_by_dID[dID] = formatted_viz_specs

    return formatted_viz_specs_by_dID
