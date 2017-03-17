from __future__ import division
import numpy as np
from scipy.stats import entropy, normaltest, mode, pearsonr, linregress
from dive.base.constants import GeneratingProcedure, TypeStructure

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

def _z_score(v):
    return np.var(v) / np.max(v)

# Uniformity: Chi-Sq test or KL test against uniform distribution?
univariate_tests = {
    # 'gini': gini,
    'entropy': entropy,  # Shannon entropy, base e
    # 'normality': _normaltest,  # Requires at least n >= 8
    'variance': _z_score,
    'size': len,
    # 'median': np.median,
    # 'average': np.average,
    # 'std': np.std,
    # 'maximum': np.max,
    # 'minimum': np.min,
    # 'mode': _mode,
}


def _correlation(v1, v2):
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
    if ts in [TypeStructure.C_Q.value, TypeStructure.B_Q.value, TypeStructure.liC_Q.value]:
        if gp in [ GeneratingProcedure.VAL_AGG.value ]:
            v = data.get('agg_field')
        if gp in [ GeneratingProcedure.VAL_BOX.value ]:
            v = data.get('boxed_field')
        elif gp in [ GeneratingProcedure.IND_VAL.value ]:
            v = data.get('val')
        elif gp in [ GeneratingProcedure.BIN_AGG.value, GeneratingProcedure.MULTIGROUP_AGG.value, GeneratingProcedure.MULTIGROUP_COUNT.value, GeneratingProcedure.VAL_VAL_Q.value ]:
            v = data.get('agg')
        elif gp in [ GeneratingProcedure.VAL_COUNT.value ]:
            v = data.get('count')


        for test_name, test_fn in univariate_tests.iteritems():
            test_value = None
            try:
                test_value = test_fn(v)
                if np.isnan(test_value) or np.isinf(test_value):
                    test_value = None
            except Exception as e:
                logger.debug('Failed scoring for test %s, gp %s, ts %s', test_name, gp, ts)
                logger.debug(e)

            stat_docs.append({
                'type': test_name,
                'score': test_value
            })
        for test_name, test_fn in bivariate_tests.iteritems():
            stat_docs.append({
                'type': test_name,
                'score': None
            })


    elif ts in [TypeStructure.Q_Q.value]:
        stat_docs = []
        v1 = data.get('field_a')
        v2 = data.get('field_b')
        for test_name, test_fn in univariate_tests.iteritems():
            test_value = None
            try:
                test_value = test_fn(v2)
                if np.isnan(test_value) or np.isinf(test_value):
                    test_value = None
            except:
                logger.debug('Failed scoring for test %s, gp %s, ts %s', test_name, gp, ts)

        for test_name, test_fn in bivariate_tests.iteritems():
            test_value = None
            try:
                test_value = test_fn(v1, v2)
                if np.isnan(test_value) or np.isinf(test_value):
                    test_value = None
            except:
                logger.debug('Failed scoring for test %s, gp %s, ts %s', test_name, gp, ts)
            stat_docs.append({
                'type': test_name,
                'score': test_value
            })
    return stat_docs


def get_relevance_score(spec, visualization_field_ids, selected_fields):
    ''' Increase by number specified fields that are included '''
    score = 0
    for field in selected_fields:
        if field['field_id'] in visualization_field_ids:
            score = score + 1
    if len(selected_fields):
        score = score / len(selected_fields)
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

    return score_docs
