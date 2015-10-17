import numpy as np
from itertools import combinations

from dive.tasks.visualization import GeneratingProcedure, TypeStructure, TermType, aggregation_functions

# TODO How to document defaults?


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

###
# Functions providing only the new specs for each case (subsumed cases are taken care of elsewhere)
###
def A(q_field):
    specs = []

    q_label = q_field['name']

    # 0-D Aggregations
    for agg_fn_label, agg_fn in aggregation_functions.items():
        agg_spec = {
            'generating_procedure': GeneratingProcedure.AGG.value,
            'type_structure': TypeStructure.Q.value,
            'args': {
                'aggFn': agg_fn_label,
                'aggFieldA': q_field
            },
            'meta': {
                'desc': '%s of %s' % (agg_fn_label, q_label),
                'construction': [
                    { 'string': agg_fn_label, 'type': TermType.OPERATION.value },
                    { 'string': 'of', 'type': TermType.PLAIN.value },
                    { 'string': q_label, 'type': TermType.FIELD.value },
                ]
            }
        }
        specs.append(agg_spec)

    if not q_field['is_unique']:
        # { Value: count }
        count_spec = {
            'generating_procedure': GeneratingProcedure.VAL_COUNT.value,
            'type_structure': TypeStructure.C_Q.value,
            'args': {
                'fieldA': q_field  # TODO How to deal with dervied fields?
            },
            'meta': {
                'desc': 'Count of %s' % q_label,
                'construction': [
                    { 'string': 'count', 'type': TermType.OPERATION.value },
                    { 'string': 'of', 'type': TermType.PLAIN.value },
                    { 'string': q_label, 'type': TermType.FIELD.value },
                ]
            }
        }
        specs.append(count_spec)

    # TODO Implement binning algorithm
    # { Bins: Aggregate(binned values) }
    for binning_procedure, implemented in binning_procedures.iteritems():
        if implemented:
            bin_spec = {
                'generating_procedure': GeneratingProcedure.BIN_AGG.value,
                'type_structure': TypeStructure.B_Q.value,
                'args': {
                    'aggFn': 'count',
                    'aggFieldA': q_field,
                    'binningProcedure': binning_procedure,
                    'binningField': q_field
                },
                'meta': {
                    'description': '%s of %s by bin' % (agg_fn, q_label),
                    'construction': [
                        { 'string': 'count', 'type': TermType.OPERATION.value },
                        { 'string': 'of', 'type': TermType.PLAIN.value },
                        { 'string': q_label, 'type': TermType.FIELD.value },
                        { 'string': 'by bin', 'type': TermType.TRANSFORMATION.value },
                    ]
                }
            }
            specs.append(bin_spec)
    return specs

def B(q_fields):
    specs = []
    return specs

    # Function on pairs of columns
    # for (field_a, field_b) in combinations(q_fields, 2):
    #     label_a = field_a['name']
    #     label_b = field_b['name']
    #     for ew_fn, ew_op in elementwise_functions.iteritems():
    #         derived_column_field = {
    #             'transform': '2:1',
    #             'name': "%s %s %s" % (label_a, ew_op, label_b),
    #             'is_unique': False  # TODO Run property detection again?
    #         }
    #         A_specs = A(derived_column_field)
    #         specs.extend(A_specs)

def C(c_field):
    '''
    Single categorical field
    '''
    specs = []
    c_label = c_field['name']

    # 0D
    most_frequent_spec = {
        'generating_procedure': GeneratingProcedure.AGG.value,
        'type_structure': TypeStructure.C.value,
        'args': {
            'aggFn': 'mode',
            'aggFieldA': c_field
        },
        'meta': {
            'desc': 'Most frequent value of %s' % (c_field),
            'construction': [
                { 'string': 'Most frequent', 'type': TermType.OPERATION.value },
                { 'string': 'value of', 'type': TermType.PLAIN.value },
                { 'string': c_label, 'type': TermType.FIELD.value },
            ]
        }
    }

    # 2D
    val_count_spec = {
        'generating_procedure': GeneratingProcedure.VAL_COUNT.value,
        'type_structure': TypeStructure.C_Q.value,
        'args': {
            'fieldA': c_field
        },
        'meta': {
            'desc': 'Count of %s' % (c_label),
            'construction': [
                { 'string': 'count', 'type': TermType.OPERATION.value },
                { 'string': 'of', 'type': TermType.PLAIN.value },
                { 'string': c_label, 'type': TermType.FIELD.value },
            ]
        }
    }

    specs.append(most_frequent_spec)
    specs.append(val_count_spec)
    return specs

def D(c_field, q_field):
    specs = []
    c_label = c_field['name']
    q_label = q_field['name']

    if c_field['is_unique']:
        spec = {
            'generating_procedure': GeneratingProcedure.VAL_VAL.value,
            'type_structure': TypeStructure.C_Q.value,
            'args': {
                'fieldA': c_field,
                'fieldB': q_field,
            },
            'meta': {
                'desc': '%s vs. %s ' % (c_label, q_label),
                'construction': [
                    { 'string': c_label, 'type': TermType.FIELD.value },
                    { 'string': 'vs.', 'type': TermType.PLAIN.value },
                    { 'string': q_label, 'type': TermType.FIELD.value },
                ]
            }
        }
        specs.append(spec)
    else:
        for agg_fn in aggregation_functions.keys():
            spec = {
                'generating_procedure': GeneratingProcedure.VAL_AGG.value,
                'type_structure': TypeStructure.C_Q.value,
                'args': {
                    'aggFn': agg_fn,
                    'groupedField': c_field,
                    'aggField': q_field,
                },
                'meta': {
                    'desc': '%s of %s grouped by %s' % (agg_fn, q_label, c_label),
                    'construction': [
                        { 'string': agg_fn, 'type': TermType.OPERATION.value },
                        { 'string': 'of', 'type': TermType.PLAIN.value },
                        { 'string': q_label, 'type': TermType.FIELD.value },
                        { 'string': 'grouped by', 'type': TermType.OPERATION.value },
                        { 'string': c_label, 'type': TermType.FIELD.value },
                    ]
                }
            }
            specs.append(spec)
    return specs

def E(c_field, q_fields):
    specs = []

    # Two-field agg:agg
    if not c_field['is_unique']:
        c_label = c_field['name']
        for (q_field_a, q_field_b) in combinations(q_fields, 2):
            q_label_a, q_label_b = q_field_a['name'], q_field_b['name']
            for agg_fn in aggregation_functions.keys():
                spec = {
                    'generating_procedure': GeneratingProcedure.AGG_AGG.value,
                    'type_structure': TypeStructure.Q_Q.value,
                    'args': {
                        'aggFn': agg_fn,
                        'aggFieldA': q_field_a,
                        'aggFieldB': q_field_b,
                        'groupedField': c_label
                    },
                    'meta': {
                        'desc': 'Group by %s and aggregate %s and %s by %s' % (c_label, q_label_a, q_label_b, agg_fn),
                        'construction': [
                            { 'string': 'Group by', 'type': TermType.OPERATION.value },
                            { 'string': c_label, 'type': TermType.FIELD.value },
                            { 'string': 'and', 'type': TermType.PLAIN.value },
                            { 'string': 'aggregate', 'type': TermType.OPERATION.value },
                            { 'string': q_label_a, 'type': TermType.FIELD.value },
                            { 'string': 'and', 'type': TermType.PLAIN.value },
                            { 'string': q_label_b, 'type': TermType.FIELD.value },
                            { 'string': 'by', 'type': TermType.PLAIN.value },
                            { 'string': agg_fn, 'type': TermType.OPERATION.value },
                        ]
                    }
                }
    return specs

def F(c_fields):
    specs = []

    # Two-field val:val
    # Not useful...
    # for (c_field_a, c_field_b) in combinations(c_fields, 2):
    #     c_label_a, c_label_b = c_field_a['name'], c_field_b['name']
    #     spec = {
    #         'generating_procedure': GeneratingProcedure.VAL_VAL.value,
    #         'type_structure': TypeStructure.C_C.value,
    #         'args': {
    #             'fieldA': c_field_a,
    #             'fieldB': c_field_b
    #         },
    #         'meta': {
    #             'desc': '%s vs. %s' % (c_label_a, c_label_b),
    #             'construction': [
    #                 { 'string': c_label_a, 'type': TermType.FIELD.value },
    #                 { 'string': 'vs.', 'type': TermType.PLAIN.value },
    #                 { 'string': c_label_b, 'type': TermType.FIELD.value },
    #             ]
    #         }
    #     }
    #     specs.append(spec)
    return specs

def G(c_fields, q_field):
    specs = []
    # TODO How do you deal with this?
    # Two-field val:val:q with quantitative data
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['name'], c_field_b['name']
        q_label = q_field['name']
        spec = {
            'generating_procedure': GeneratingProcedure.VAL_VAL_Q.value,
            'type_structure': TypeStructure.liC_Q.value,
            'args': {
                'fieldA': c_field_a,
                'fieldB': c_field_b,
                'dataFieldA': q_label
            },
            'meta': {
                'desc': 'Connect %s and %s, with attribute %s' % (c_label_a, c_label_b, q_label),
                'construction': [
                    { 'string': 'connect', 'type': TermType.PLAIN.value },
                    { 'string': c_label_a, 'type': TermType.FIELD.value },
                    { 'string': 'and', 'type': TermType.PLAIN.value },
                    { 'string': c_label_b, 'type': TermType.FIELD.value },
                    { 'string': 'with attribute', 'type': TermType.PLAIN.value },
                    { 'string': q_label, 'type': TermType.FIELD.value },
                ]
            }
        }
        specs.append(spec)
    return specs

def H(c_fields, q_fields):
    specs = []
    # TODO How do you deal with this?
    # Two-field val:val:[q] with quantitative data
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['name'], c_field_b['name']
        q_labels = [ f['name'] for f in q_fields ]
        spec = {
            'generating_procedure': GeneratingProcedure.VAL_VAL_Q.value,
            'type_structure': TypeStructure.liC_Q.value,
            'args': {
                'fieldA': c_field_a,
                'fieldB': c_field_b,
                'dataFields': q_labels
            },
            'meta': {
                'desc': 'Connect %s with %s, with attributes %s' % (c_label_a, c_label_b, q_labels),
                'construction': [
                    { 'string': 'connect', 'type': TermType.PLAIN.value },
                    { 'string': c_label_a, 'type': TermType.FIELD.value },
                    { 'string': 'and', 'type': TermType.PLAIN.value },
                    { 'string': c_label_b, 'type': TermType.FIELD.value },
                    { 'string': 'with attributes', 'type': TermType.PLAIN.value },
                    { 'string': q_labels, 'type': TermType.FIELD.value },
                ]
            }
        }
        specs.append(spec)
    return specs
