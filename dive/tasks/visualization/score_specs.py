from dive.tasks.visualization import GeneratingProcedure, TypeStructure

import logging
logger = logging.getLogger(__name__)

# Expressiveness
def get_expressiveness(spec):
    result = {}
    return result

# Effectiveness
def get_effectiveness(spec):
    result = {}
    return result


def gini(list_of_values):
  sorted_list = sorted(list_of_values)
  height, area = 0, 0
  for value in sorted_list:
    height += value
    area += height - value / 2.
  fair_area = height * len(list_of_values) / 2
  return (fair_area - area) / fair_area


import numpy as np
from scipy.stats import entropy, normaltest, mode
def _mode(v):
    m = mode(v)
    return [ m[0][0], m[1][0] ]


# Uniformity: Chi-Sq test or KL test against uniform distribution?
univariate_tests = {
    # 'gini': gini,
    # 'entropy': entropy,  # Shannon entropy, base e
    # # 'normality': normaltest,  # Requires at least n >= 8
    # 'median': np.median,
    # 'average': np.average,
    # 'std': np.std,
    # 'variance': np.var,
    # 'maximum': np.max,
    # 'minimum': np.min,
    # 'mode': _mode,
    'size': len,
}


from scipy.stats import pearsonr, linregress
# Two quantitative variables
bivariate_tests = {
    'correlation': pearsonr,
    'linearRegression': linregress
}

# Statistical
def get_statistical_properties(data, gp, ts):
    stats = {}
    # Single quantitative field:
    if ts in [TypeStructure.C_Q.value, TypeStructure.B_Q.value, TypeStructure.Q_Q.value]:
        v = None
        if gp == GeneratingProcedure.VAL_AGG.value:
            v = data.get('aggField')
        if gp == GeneratingProcedure.IND_VAL.value:
            v = data.get('val')
        if gp == GeneratingProcedure.BIN_AGG.value:
            v = data.get('agg')
        if gp == GeneratingProcedure.MULTIGROUP_COUNT.value:
            v = data.get('agg')

        if v:
            for test_name, test_fn in univariate_tests.iteritems():
                try:
                    stats[test_name] = test_fn(v)
                except:
                    logger.error('Failed scoring for test %s, gp %s, ts %s', test_name, gp, ts)
                    continue
    if ts in [TypeStructure.Q_Q]:
        v = None
        if gp == GeneratingProcedure.AGG_AGG.value:
            v1 = data.get('fieldA')
            v2 = data.get('fieldB')
        if gp == GeneratingProcedure.VAL_VAL.value:
            v1 = data.get('fieldA')
            v2 = data.get('fieldB')
        if v:
            for test_name, test_fn in bivariate_tests.iteritems():
                stats[test_name] = test_fn(v1, v2)

    return stats


def get_relevance_score(spec, visualization_fields, selected_fields):
    ''' Increase by number specified fields that are included '''
    score = 0
    for field in selected_fields:
        if field['name'] in visualization_fields:
            score = score + 10
    return score


def score_spec(spec, selected_fields):
    score_doc = {
        'stats': {},
        'relevance': 0,
    }
    data = spec['data']['score']
    visualization_fields = spec['fields']
    gp = spec['generating_procedure']
    ts = spec['type_structure']

    if selected_fields:
        score_doc['relevance'] = get_relevance_score(spec, visualization_fields, selected_fields)

    score_doc['stats'] = get_statistical_properties(data, gp, ts)

    return score_doc
