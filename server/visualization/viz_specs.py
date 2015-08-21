from data.access import is_numeric
from itertools import combinations
from data.db import MongoInstance as MI
from viz_stats import *

from time import time

# Wrapper function
def get_viz_specs(pID):
    d = MI.getData(None, pID)
    p = MI.getProperty(None, pID)
    o = MI.getOntology(None, pID)

    existing_specs = MI.getSpecs(pID, {})
    enumerated_viz_specs = enumerate_viz_specs(d, p, o)
    filtered_viz_specs = filter_viz_specs(enumerated_viz_specs)
    scored_viz_specs = score_viz_specs(filtered_viz_specs)
    return scored_viz_specs

specific_to_general_type = {
    'float': 'q',
    'integer': 'q',
    'string': 'c'
}

# TODO How to document defaults?
aggregation_functions = {
    'sum': np.sum,
    'min': np.min,
    'max': np.max,
    'mean': np.mean,
    'count': np.size
}

# 1) Enumerated viz specs given data, properties, and ontologies
def enumerate_viz_specs(datasets, properties, ontologies):
    enumerated_viz_specs = []
    field_types = [ specific_to_general_type[p] for p in properties]

    q_count = field_types.count('q')
    c_count = field_types.count('c')

    # Cases A - B
    # Q > 0, C = 0
    if q_count and not c_count:
        # Case A) Q = 1, C = 0
        if q_count == 1:
            return
        # Case B) Q > 1, C = 0
        elif q_count >= 1:
            return

    # Cases C - E
    # C = 1
    if c_count == 1:
        # Case C) C = 1, Q = 0
        if q_count == 0:
            return
        # Case D) C = 1, Q = 1
        elif q_count == 1:
            return
        # Case E) C = 1, Q >= 1
        elif q_count > 1:
            return

    # Cases F - H
    # C >= 1
    if c_count >= 1:
        # Case F) C >= 1, Q = 0
        if q_count == 0:
            return
        # Case G) C >= 1, Q = 1
        elif q_count == 1:
            return
        # Case H) C >= 1, Q > 1
        elif q_count > 1:
            return

    return enumerated_viz_specs
# 2) Filtering enumerated viz specs based on interpretability and renderability
def filtered_viz_specs(enumerated_viz_specs):
    filtered_viz_specs = []
    return filtered_viz_specs

# 3) Scoring viz specs based on effectiveness, expressiveness, and statistical properties
def scored_viz_specs(filtered_viz_specs):
    scored_viz_specs = []
    return scored_viz_specs
