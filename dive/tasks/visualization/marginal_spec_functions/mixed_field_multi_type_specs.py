from itertools import combinations
from dive.tasks.visualization import GeneratingProcedure as GP, TypeStructure as TS, \
    TermType, aggregation_functions, VizType as VT
from dive.tasks.visualization.marginal_spec_functions import elementwise_functions, binning_procedures

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def single_c_multi_q(c_field, q_fields):
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
    logger.debug('Single C Multi Q: %s specs', len(specs))
    return specs


def single_q_multi_c(c_fields, q_field):
    specs = []
    logger.debug('Multi C Single Q')
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


def single_c_multi_t(c_field, t_fields):
    specs = []
    logger.debug('Single C Multi T: %s specs', len(specs))
    return specs


def single_t_multi_c(t_field, c_fields):
    specs = []
    logger.debug('Single T Multi C: %s specs', len(specs))
    return specs


def single_t_multi_q(t_field, q_fields):
    specs = []
    logger.debug('Single T Multi Q: %s specs', len(specs))
    return specs


def single_q_multi_t(q_field, t_fields):
    specs = []
    logger.debug('Single Q Multi T: %s specs', len(specs))
    return specs
