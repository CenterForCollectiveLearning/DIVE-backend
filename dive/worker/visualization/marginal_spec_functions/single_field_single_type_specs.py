from itertools import combinations

from dive.base.constants import GeneralDataType as GDT, DataType as DT, Scale
from dive.base.constants import GeneratingProcedure as GP, TypeStructure as TS, \
    VizType as VT, TermType, aggregation_functions
from dive.worker.visualization.marginal_spec_functions import elementwise_functions, binning_procedures


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
    scale = q_field['scale']

    if (scale == Scale.ORDINAL):
        # { Value: count }
        count_spec = {
            'generating_procedure': GP.VAL_COUNT.value,
            'type_structure': TS.C_Q.value,
            'viz_types': [ VT.BAR.value ],
            'field_ids': [ q_field['id'] ],
            'args': {
                'field_a': q_field
            },
            'meta': {
                'desc': 'Distribution of %s' % q_label,
                'construction': [
                    { 'string': 'Distribution', 'type': TermType.OPERATION.value },
                    { 'string': 'of', 'type': TermType.PLAIN.value },
                    { 'string': q_label, 'type': TermType.FIELD.value },
                ]
            }
        }
        specs.append(count_spec)

    # { Bins: Aggregate(binned values) }
    else:
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
                'desc': 'Distribution of %s' % (q_label),
                'construction': [
                    { 'string': 'Distribution', 'type': TermType.OPERATION.value },
                    { 'string': 'of', 'type': TermType.PLAIN.value },
                    { 'string': q_label, 'type': TermType.FIELD.value }
                ],
                'labels': {
                    'x': '%s by bin' % q_label,
                    'y': 'Frequency'
                },
            }
        }
        specs.append(bin_spec)
    return specs


def single_t(t_field):
    ''' Return distribution if not uniform '''
    logger.debug('Single T - %s', t_field['name'])
    specs = []

    t_label = t_field['name']
    t_scale = t_field['scale']


    if (t_scale == Scale.CONTINUOUS.value):
        raw_count_spec_types = [ VT.LINE.value, VT.SCATTER.value ]
    elif (t_scale == Scale.ORDINAL.value):
        raw_count_spec_types = [ VT.BAR.value, VT.TREE.value, VT.PIE.value ]

    raw_count_spec = {
        'case': 'single_tq',
        'generating_procedure': GP.VAL_COUNT.value,
        'type_structure': TS.T_Q.value,
        'viz_types': raw_count_spec_types,
        'field_ids': [ t_field['id'] ],
        'args': {
            'field_a': t_field
        },
        'meta': {
            'desc': 'Count of %s' % (t_label),
            'construction': [
                { 'string': 'count', 'type': TermType.OPERATION.value },
                { 'string': 'of', 'type': TermType.PLAIN.value },
                { 'string': t_label, 'type': TermType.FIELD.value },
            ],
            'labels': {
                'x': t_label,
                'y': 'Count'
            }
        }
    }
    specs.append(raw_count_spec)

    if (t_scale == Scale.CONTINUOUS.value):
        bin_spec = {
            'generating_procedure': GP.BIN_AGG.value,
            'type_structure': TS.B_Q.value,
            'viz_types': [ VT.HIST.value ],
            'field_ids': [ t_field['id'] ],
            'args': {
                'agg_fn': 'count',
                'agg_field_a': t_field,
                'binning_field': t_field
            },
            'meta': {
                'desc': '%s of %s by bin' % ('count', t_label),
                'construction': [
                    { 'string': 'count', 'type': TermType.OPERATION.value },
                    { 'string': 'of', 'type': TermType.PLAIN.value },
                    { 'string': t_label, 'type': TermType.FIELD.value },
                    { 'string': 'by bin', 'type': TermType.TRANSFORMATION.value },
                ],
                'labels': {
                    'x': '%s by bin' % t_label,
                    'y': 'Count by bin'
                },
            }
        }
        specs.append(bin_spec)
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
        'viz_types': [ VT.BAR.value, VT.TREE.value, VT.PIE.value ],
        'field_ids': [ c_field['id'] ],
        'args': {
            'field_a': c_field
        },
        'meta': {
            'desc': '%s distribution' % (c_label),
            'construction': [
                { 'string': c_label, 'type': TermType.FIELD.value },
                { 'string': 'distribution', 'type': TermType.OPERATION.value },                
            ],
            'labels': {
                'x': c_label,
                'y': 'Frequency'
            },
        }
    }

    # specs.append(most_frequent_spec)
    specs.append(val_count_spec)
    return specs
