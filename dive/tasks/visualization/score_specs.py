import numpy as np
from scipy.stats import entropy, normaltest, mode, pearsonr, linregress
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


def _mode(v):
    m = mode(v)
    return [ m[0][0], m[1][0] ]


def _normaltest(v):
    return normaltest(v).pvalue


# Uniformity: Chi-Sq test or KL test against uniform distribution?
univariate_tests = {
    'gini': gini,
    'entropy': entropy,  # Shannon entropy, base e
    'normality': _normaltest,  # Requires at least n >= 8
    'variance': np.var,
    'size': len,
    # 'median': np.median,
    # 'average': np.average,
    # 'std': np.std,
    # 'maximum': np.max,
    # 'minimum': np.min,
    # 'mode': _mode,
}


def _correlation(v1, v2):
    print pearsonr(v1, v2)
    return pearsonr(v1, v2)[0]

# Two quantitative variables
bivariate_tests = {
    'correlation': _correlation
    # 'linearRegression': linregress
}

# Statistical
def get_statistical_properties(data, gp, ts):
    stat_docs = []
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
                test_value = None
                try:
                    test_value = test_fn(v)
                    if np.isnan(test_value) or np.isinf(test_value):
                        test_value = None
                except:
                    logger.debug('Failed scoring for test %s, gp %s, ts %s', test_name, gp, ts)
                    continue
                stat_docs.append({
                    'type': test_name,
                    'score': test_value
                })
            for test_name, test_fn in bivariate_tests.iteritems():
                stat_docs.append({
                    'type': test_name,
                    'score': None
                })


    if ts in [TypeStructure.Q_Q.value]:
        stat_docs = []
        v1 = data.get('fieldA')
        v2 = data.get('fieldB')
        if v1 and v2:
            for test_name, test_fn in bivariate_tests.iteritems():
                test_value = None
                try:
                    test_value = test_fn(v1, v2)
                    if np.isnan(test_value) or np.isinf(test_value):
                        test_value = None
                except:
                    logger.debug('Failed scoring for test %s, gp %s, ts %s', test_name, gp, ts)
                    continue
                stat_docs.append({
                    'type': test_name,
                    'score': test_value
                })
            for test_name, test_fn in univariate_tests.iteritems():
                stat_docs.append({
                    'type': test_name,
                    'score': None
                })
    return stat_docs


def get_relevance_score(spec, visualization_field_ids, selected_fields):
    ''' Increase by number specified fields that are included '''
    score = 0
    print visualization_field_ids, selected_fields
    for field in selected_fields:
        if field['field_id'] in visualization_field_ids:
            score = score + 10
    return score


def score_spec(spec, selected_fields):
    '''
    Entry point for getting relevance and statistical scores

    Returns a list in the format [ {type, score}, ...]
    '''
    score_docs = []

    data = spec['data']['score']
    visualization_field_ids = spec['field_ids']
    gp = spec['generating_procedure']
    ts = spec['type_structure']

    relevance_score_doc = {
        'type': 'relevance',
        'score': get_relevance_score(spec, visualization_field_ids, selected_fields)
    }
    score_docs.append(relevance_score_doc)

    stat_score_docs = get_statistical_properties(data, gp, ts)
    score_docs.extend(stat_score_docs)

    logger.info(score_docs)
    return score_docs
