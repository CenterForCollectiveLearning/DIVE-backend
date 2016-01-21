'''
Functions providing only the new specs for each case (subsumed cases are taken care of elsewhere)
'''
import numpy as np
from itertools import combinations

from dive.tasks.visualization import GeneratingProcedure as GP, TypeStructure as TS, \
    TermType, aggregation_functions, VizType as VT

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


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


def single_q(q_field):
    '''
    Single numeric field

    - For non-unique fields, aggregate on count
    - For all, count on bin
    '''
    specs = []
    logger.debug('A: Single Q - %s', q_field['name'])

    q_label = q_field['name']

    if not q_field['is_unique']:
        # { Value: count }
        count_spec = {
            'generating_procedure': GP.VAL_COUNT.value,
            'type_structure': TS.C_Q.value,
            'viz_types': [ VT.TREE.value, VT.PIE.value, VT.BAR.value ],
            'field_ids': [ q_field['id'] ],
            'args': {
                'field_a': q_field
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

    # { Bins: Aggregate(binned values) }
    for binning_procedure, implemented in binning_procedures.iteritems():
        if implemented:
            bin_spec = {
                'generating_procedure': GP.BIN_AGG.value,
                'type_structure': TS.B_Q.value,
                'viz_types': [ VT.HIST.value ],
                'field_ids': [ q_field['id'] ],
                'args': {
                    'agg_fn': 'count',
                    'agg_field_a': q_field,
                    'binning_procedure': binning_procedure,
                    'binning_field': q_field
                },
                'meta': {
                    'description': '%s of %s by bin' % ('count', q_label),
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


def single_t(t_field):
    logger.debug('Single T - %s', t_field['name'])
    specs = []

    return specs


def single_ctq(c_field, t_field, q_field):
    specs = []
    return specs


def single_ct(c_field, t_field):
    logger.debug('Single C Single T - %s, %s', c_field['name'], t_field['name'])
    specs = []

    return specs


def single_tq(t_field, q_field):
    t_label = t_field['name']
    q_label = q_field['name']

    logger.debug('Single T Single Q - %s, %s', t_label, q_label)
    specs = []
    raw_time_series_spec = {
        'generating_procedure': GP.VAL_VAL.value,
        'type_structure': TS.T_Q.value,
        'viz_types': [ VT.LINE.value, VT.SCATTER.value ],
        'args': {
            'field_a': t_field,
            'field_b': q_field
        },
        'meta': {
            'desc': '%s vs. %s' % (t_label, q_label),
            'construction': [
                { 'string': t_label, 'type': TermType.FIELD.value },
                { 'string': 'vs.', 'type': TermType.PLAIN.value },
                { 'string': q_label, 'type': TermType.FIELD.value },
            ]
        }
    }
    return specs


def single_ctq(c_field, t_field, q_field):
    specs = []
    logger.debug('Single C Single T Single Q')
    return specs

def multi_q(q_fields):
    logger.debug("B: Multi Q - %s", [f['name'] for f in q_fields])
    specs = []

    #Function on pairs of columns
    for (q_field_a, q_field_b) in combinations(q_fields, 2):
        q_label_a = q_field_a['name']
        q_label_b = q_field_b['name']

        # Raw comparison
        raw_comparison_spec = {
            'generating_procedure': GP.VAL_VAL.value,
            'type_structure': TS.Q_Q.value,
            'field_ids': [ q_field_a['id'], q_field_b['id'] ],
            'viz_types': [ VT.SCATTER.value ],
            'args': {
                'field_a': q_field_a,
                'field_b': q_field_b
            },
            'meta': {
                'desc': '%s vs. %s' % (q_label_a, q_label_b),
                'construction': [
                    { 'string': q_label_a, 'type': TermType.FIELD.value },
                    { 'string': 'vs.', 'type': TermType.PLAIN.value },
                    { 'string': q_label_b, 'type': TermType.FIELD.value },
                ]
            }
        }
        specs.append(raw_comparison_spec)

        # for ew_fn, ew_op in elementwise_functions.iteritems():
        #     derived_column_field = {
        #         'transform': '2:1',
        #         'name': "%s %s %s" % (label_a, ew_op, label_b),
        #         'is_unique': False  # TODO Run property detection again?
        #     }
        #     A_specs = A(derived_column_field)
        #     specs.extend(A_specs)
    return specs

def single_c(c_field):
    '''
    Single categorical field
    '''
    specs = []
    c_label = c_field['name']
    logger.debug('C: Single C')

    # 2D
    val_count_spec = {
        'generating_procedure': GP.VAL_COUNT.value,
        'type_structure': TS.C_Q.value,
        'viz_types': [ VT.TREE.value, VT.PIE.value ],
        'field_ids': [ c_field['id'] ],
        'args': {
            'field_a': c_field
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

    # specs.append(most_frequent_spec)
    specs.append(val_count_spec)
    return specs

def single_cq(c_field, q_field):
    specs = []
    c_label = c_field['name']
    q_label = q_field['name']
    logger.debug('D: Single C Single Q')

    if c_field['is_unique']:
        spec = {
            'generating_procedure': GP.VAL_VAL.value,
            'type_structure': TS.C_Q.value,
            'viz_types': [ VT.BAR.value, VT.TREE.value, VT.PIE.value ],
            'field_ids': [ c_field['id'], q_field['id'] ],
            'args': {
                'field_a': c_field,
                'field_b': q_field,
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
            if agg_fn == 'count':
                continue

            spec = {
                'generating_procedure': GP.VAL_AGG.value,
                'type_structure': TS.C_Q.value,
                'viz_types': [ VT.BAR.value ],
                'field_ids': [ c_field['id'], q_field['id'] ],
                'args': {
                    'agg_fn': agg_fn,
                    'grouped_field': c_field,
                    'agg_field': q_field,
                },
                'meta': {
                    'desc': '%s of %s by %s' % (agg_fn, q_label, c_label),
                    'construction': [
                        { 'string': agg_fn, 'type': TermType.OPERATION.value },
                        { 'string': 'of', 'type': TermType.PLAIN.value },
                        { 'string': q_label, 'type': TermType.FIELD.value },
                        { 'string': 'by', 'type': TermType.OPERATION.value },
                        { 'string': c_label, 'type': TermType.FIELD.value },
                    ]
                }
            }
            specs.append(spec)
    return specs

def single_c_multi_q(c_field, q_fields):
    logger.debug("E: Single C Multi Q")
    specs = []

    # Two-field agg:agg
    if not c_field['is_unique']:
        c_label = c_field['name']
        for (q_field_a, q_field_b) in combinations(q_fields, 2):
            q_label_a, q_label_b = q_field_a['name'], q_field_b['name']
            for agg_fn in aggregation_functions.keys():
                if agg_fn == 'count':
                    continue
                spec = {
                    'generating_procedure': GP.AGG_AGG.value,
                    'type_structure': TS.Q_Q.value,
                    'viz_types': [ VT.SCATTER.value ],
                    'field_ids': [ q_field_a['id'], q_field_b['id'], c_field['id'] ],
                    'args': {
                        'agg_fn': agg_fn,
                        'agg_field_a': q_field_a,
                        'agg_field_b': q_field_b,
                        'grouped_field': c_field
                    },
                    'meta': {
                        'desc': 'Group by %s and aggregate %s and %s by %s' % (c_label, q_label_a, q_label_b, agg_fn),
                        'construction': [
                            { 'string': 'Group', 'type': TermType.OPERATION.value },
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
                specs.append(spec)
    return specs

def multi_c(c_fields):
    logger.debug("F: Multi C")
    specs = []

    # Count of one field given another
    # E.g. count of position by gender
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['name'], c_field_b['name']
        if (c_field_a['is_unique'] or c_field_b['is_unique']):
            continue
        spec = {
            'generating_procedure': GP.MULTIGROUP_COUNT.value,
            'type_structure': TS.liC_Q.value,
            'viz_types': [ VT.STACKED_BAR.value ],
            'field_ids': [ c_field_a['id'], c_field_b['id'] ],
            'args': {
                'field_a': c_field_a,
                'field_b': c_field_b,
            },
            'meta': {
                'desc': 'Count by %s then %s' % (c_label_a, c_label_b),
                'construction': [
                    { 'string': 'Count', 'type': TermType.OPERATION.value },
                    { 'string': 'by', 'type': TermType.PLAIN.value },
                    { 'string': c_label_a, 'type': TermType.FIELD.value },
                    { 'string': 'then', 'type': TermType.PLAIN.value },
                    { 'string': c_label_b, 'type': TermType.FIELD.value },
                ]
            }
        }
        specs.append(spec)

    # Two-field val:val
    # Not useful...
    # for (c_field_a, c_field_b) in combinations(c_fields, 2):
    #     c_label_a, c_label_b = c_field_a['name'], c_field_b['name']
    #     spec = {
    #         'generating_procedure': GP.VAL_VAL.value,
    #         'type_structure': TS.C_C.value,
    #         'args': {
    #             'field_a': c_field_a,
    #             'field_b': c_field_b
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

def single_c_multi_q(c_fields, q_field):
    specs = []
    logger.debug('G: Single C Multi Q')
    # TODO How do you deal with this?
    # Two-field val:val:q with quantitative data
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['name'], c_field_b['name']
        q_label = q_field['name']
        spec = {
            'generating_procedure': GP.VAL_VAL_Q.value,
            'type_structure': TS.liC_Q.value,
            'viz_types': [ VT.NETWORK.value ],
            'field_ids': [ c_field_a['id'], c_field_b['id'], q_field['id'] ],
            'args': {
                'field_a': c_field_a,
                'field_b': c_field_b,
                'data_field_a': q_label
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

def multi_cq(c_fields, q_fields):
    specs = []
    logger.debug('H: Multi C Multi Q')
    # TODO How do you deal with this?
    # Two-field val:val:[q] with quantitative data
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['name'], c_field_b['name']
        q_labels = [ f['name'] for f in q_fields ]
        q_field_ids = [ f['id'] for f in q_fields ]
        spec = {
            'generating_procedure': GP.VAL_VAL_Q.value,
            'type_structure': TS.liC_Q.value,
            'viz_types': [ VT.NETWORK.value ],
            'field_ids': [ c_field_a['id'], c_field_b['id'] ] + q_field_ids,
            'args': {
                'field_a': c_field_a,
                'field_b': c_field_b,
                'dataFields': q_labels
            },
            'meta': {
                'desc': 'Connect %s with %s, with attributes %s' % (c_label_a, c_label_b, ', '.join(q_labels)),
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
