from itertools import combinations

from dive.tasks.visualization import GeneratingProcedure as GP, TypeStructure as TS, \
    VizType as VT, TermType, aggregation_functions
from dive.tasks.visualization.marginal_spec_functions import elementwise_functions, binning_procedures


from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


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
    bin_spec = {
        'generating_procedure': GP.BIN_AGG.value,
        'type_structure': TS.B_Q.value,
        'viz_types': [ VT.HIST.value ],
        'field_ids': [ q_field['id'] ],
        'args': {
            'agg_fn': 'count',
            'agg_field_a': q_field,
            'binning_field': q_field
        },
        'meta': {
            'desc': '%s of %s by bin' % ('count', q_label),
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
    ''' Return distribution if not uniform '''
    logger.debug('Single T - %s', t_field['name'])
    specs = []

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
