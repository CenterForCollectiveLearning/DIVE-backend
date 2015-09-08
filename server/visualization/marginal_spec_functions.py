from itertools import combinations
import numpy as np
from enum import Enum

# TODO How to document defaults?
aggregation_functions = {
    'sum': np.sum,
    'min': np.min,
    'max': np.max,
    'mean': np.mean,
    'count': np.size
}

elementwise_functions = {
    'add': '+',
    'subtract': '-',
    'multiply': '*',
    'divide': '/'
}

# Value indicates whether function has been implemented
binning_procedures = {
    'freedman': True,
    'sturges': False,
    'scott': False,
    'shimazaki': False,
    'bayesian': False
}


class GeneratingProcedure(Enum):
    IND_VAL = 'ind:val'
    VAL_COUNT = 'val:count'
    BIN_AGG = 'bin:agg'
    VAL_VAL = 'val:val'
    VAL_AGG = 'val:agg'
    AGG_AGG = 'agg:agg'
    VAL_VAL_Q = 'val:val:q'


class TypeStructure(Enum):
    C_C = 'c:c'
    C_Q = 'c:q'
    liC_Q = '[c]:q'
    liQ_liQ = '[c]:[q]'
    Q_Q = 'q:q'
    Q_liQ = 'q:[q]'
    B_Q = 'b:q'

###
# Functions providing only the new specs for each case (subsumed cases are taken care of elsewhere)
###
def A(q_field):
    specs = []

    q_label = q_field['label']

    # { Index: value }
    index_spec = {
        'generating_procedure': GeneratingProcedure.IND_VAL,
        'type_structure': TypeStructure.Q_Q,
        'args': {
            'field_a': q_label
        },
        'meta': {
            'desc': 'Plot %s against its index' % (q_label)
        }
    }
    specs.append(index_spec)

    if not q_field['unique']:
        # { Value: count }
        count_spec = {
            'generating_procedure': GeneratingProcedure.VAL_COUNT,
            'type_structure': TypeStructure.Q_Q,
            'args': {
                'field_a': q_label  # TODO How to deal with dervied fields?
            },
            'meta': {
                'desc': 'Plot values of %s against count of occurrences' % q_label
            }
        }
        specs.append(count_spec)

    # TODO Implement binning algorithm
    # { Bins: Aggregate(binned values) }
    for agg_fn in aggregation_functions.keys():
        for binning_procedure, implemented in binning_procedures.iteritems():
            if implemented:
                bin_spec = {
                    'generating_procedure': GeneratingProcedure.VAL_COUNT,
                    'type_structure': TypeStructure.B_Q,
                    'args': {
                        'agg_fn': agg_fn,
                        'agg_field_a': q_label,
                        'binning_procedure': binning_procedure,
                        'binning_field': q_label
                    },
                    'meta': {
                        'desc': 'Bin %s, then aggregate binned values by %s' % (q_label, agg_fn)
                    }
                }
                specs.append(bin_spec)
    return specs

def B(q_fields):
    specs = []
    return specs

    # Function on pairs of columns
    for (field_a, field_b) in combinations(q_fields, 2):
        label_a = field_a['label']
        label_b = field_b['label']
        for ew_fn, ew_op in elementwise_functions.iteritems():
            derived_column_field = {
                'transform': '2:1',
                'label': "%s %s %s" % (label_a, ew_op, label_b),
                'unique': False  # TODO Run property detection again?
            }
            A_specs = A(derived_column_field)
            specs.extend(A_specs)
    return specs

def C(c_field):
    specs = []
    c_label = c_field['label']

    # TODO Only create if values are non-unique
    spec = {
        'generating_procedure': GeneratingProcedure.VAL_COUNT,
        'type_structure': TypeStructure.C_Q,
        'args': {
            'field_a': c_label
        },
        'meta': {
            'desc': 'Unique values of %s mapped to number of occurrences' % (c_label)
        }
    }
    specs.append(spec)
    return specs

def D(c_field, q_field):
    specs = []
    c_label = c_field['label']
    q_label = q_field['label']

    if c_field['unique']:
        spec = {
            'generating_procedure': GeneratingProcedure.VAL_VAL,
            'type_structure': TypeStructure.Q_Q,
            'args': {
                'field_a': c_label,
                'field_b': q_label,
            },
            'meta': {
                'desc': 'Plotting raw values of %s against corresponding values of %s' % (c_label, q_label)
            }
        }
        specs.append(spec)
    else:
        for agg_fn in aggregation_functions.keys():
            spec = {
                'generating_procedure': GeneratingProcedure.VAL_AGG,
                'type_structure': TypeStructure.Q_Q,
                'args': {
                    'agg_fn': agg_fn,
                    'grouped_field': c_label,
                    'agg_field': q_label,
                },
                'meta': {
                    'desc': 'Plotting raw values of %s against corresponding values of %s, aggregated by %s' % (c_label, q_label, agg_fn)
                }
            }
            specs.append(spec)
    return specs

def E(c_field, q_fields):
    specs = []

    # Two-field agg:agg
    if not c_field['unique']:
        c_label = c_field['label']
        for (q_field_a, q_field_b) in combinations(q_fields, 2):
            q_label_a, q_label_b = q_field_a['label'], q_field_b['label']
            for agg_fn in aggregation_functions.keys():
                spec = {
                    'generating_procedure': GeneratingProcedure.VAL_AGG,
                    'type_structure': TypeStructure.Q_Q,
                    'args': {
                        'agg_fn': agg_fn,
                        'agg_field_a': q_label_a,
                        'agg_field_b': q_label_b,
                        'grouped_field': c_label
                    },
                    'meta': {
                        'desc': 'Plotting aggregated values of %s against aggregated values of %s, grouped by %s' % (q_label_a, q_label_b, c_label)
                    }
                }
    return specs

def F(c_fields):
    specs = []

    # Two-field val:val
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['label'], c_field_b['label']
        spec = {
            'generating_procedure': GeneratingProcedure.VAL_VAL,
            'type_structure': TypeStructure.Q_Q,
            'args': {
                'field_a': c_label_a,
                'field_b': c_label_b
            },
            'meta': {
                'desc': 'Plotting values of %s against corresponding values of %s' % (c_label_a, c_label_b)
            }
        }
        specs.append(spec)
    return specs

def G(c_fields, q_field):
    specs = []
    # TODO How do you deal with this?
    # Two-field val:val:q with quantitative data
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['label'], c_field_b['label']
        q_label = q_field['label']
        spec = {
            'generating_procedure': GeneratingProcedure.VAL_VAL_Q,
            'type_structure': TypeStructure.liC_Q,
            'args': {
                'field_a': c_label_a,
                'field_b': c_label_b,
                'data_field_a': q_label
            },
            'meta': {
                'desc': 'Plotting values of %s against corresponding values of %s with attached data field %s' % (c_label_a, c_label_b, q_label)
            }
        }
        specs.append(spec)
    return specs

def H(c_fields, q_fields):
    specs = []
    # TODO How do you deal with this?
    # Two-field val:val:[q] with quantitative data
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['label'], c_field_b['label']
        q_labels = [ f['label'] for f in q_fields ]
        spec = {
            'generating_procedure': GeneratingProcedure.VAL_VAL_Q,
            'type_structure': TypeStructure.liC_Q,
            'args': {
                'field_a': c_label_a,
                'field_b': c_label_b,
                'data_fields': q_labels
            },
            'meta': {
                'desc': 'Plotting values of %s against corresponding values of %s with attached data fields %s' % (c_label_a, c_label_b, q_labels)
            }
        }
        specs.append(spec)
    return specs
