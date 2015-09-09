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
def get_statistical_properties(spec):
    stats = {}
    return stats

def score_spec(spec):
    score_doc = {
        'score': 1.0
    }
    data = spec['data']
    type_structure = spec['type_structure']

    # Single quantitative field:
    if type_structure in [TypeStructure.C_Q, TypeStructure.B_Q]:
        print "c:q, b:q"
    elif type_structure in [TypeStructure.Q_Q]:
        print "q:q"

    return score_doc
