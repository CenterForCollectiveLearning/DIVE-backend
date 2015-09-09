from . import GeneratingProcedure, TypeStructure


# Expressiveness
def get_expressiveness(spec):
    result = {}
    return result

# Effectiveness
def get_effectiveness(spec):
    result = {}
    return result

# http://planspace.org/2013/06/21/how-to-calculate-gini-coefficient-from-raw-data-in-python/
def gini(list_of_values):
  sorted_list = sorted(list_of_values)
  height, area = 0, 0
  for value in sorted_list:
    height += value
    area += height - value / 2.
  fair_area = height * len(list_of_values) / 2
  return (fair_area - area) / fair_area


# Uniformity: Chi-Sq test or KL test against uniform distribution?
import numpy as np
from scipy.stats import entropy, normaltest, mode
univariate_tests = {
    'gini': gini,
    'entropy': entropy,  # Shannon entropy, base e
    'normality': normaltest,  # Requires at least n >= 8
    'median': np.median,
    'average': np.average,
    'std': np.std,
    'variance': np.var,
    'maximum': np.max,
    'minimum': np.min,
    'mode': mode
}


from scipy.stats import pearsonr, linregress
# Two quantitative variables
bivariate_tests = {
    'correlation': pearsonr,
    'linear_regression': linregress
}

# Statistical
def get_statistical_properties(data, gp, ts):
    stats = {}
    # Single quantitative field:
    if ts in [TypeStructure.C_Q, TypeStructure.B_Q, TypeStructure.Q_Q]:
        v = None
        if gp == GeneratingProcedure.VAL_AGG:
            v = data.get('agg_field')
        if gp == GeneratingProcedure.IND_VAL:
            v = data.get('val')
        if gp == GeneratingProcedure.BIN_AGG:
            v = data.get('agg')

        if v:
            for test_name, test_fn in univariate_tests.iteritems():
                try:
                    stats[test_name] = test_fn(v)
                except:
                    # TODO Need to ingest dates properly!!!
                    print data
                    print test_name, gp, ts
                    pass
    if ts in [TypeStructure.Q_Q]:
        v = None
        if gp == GeneratingProcedure.AGG_AGG:
            v1 = data.get('field_a')
            v2 = data.get('field_b')
        if gp == GeneratingProcedure.VAL_VAL:
            v1 = data.get('field_a')
            v2 = data.get('field_b')

        if v:
            for test_name, test_fn in bivariate_tests.iteritems():
                stats[test_name] = test_fn(v1, v2)

    return stats

def score_spec(spec):
    score_doc = {
        'stats': {}
    }
    data = spec['data']
    gp = spec['generating_procedure']
    ts = spec['type_structure']

    score_doc['stats'] = get_statistical_properties(data, gp, ts)

    return score_doc
