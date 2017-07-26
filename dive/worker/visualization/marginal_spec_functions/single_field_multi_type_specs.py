from itertools import combinations
from dive.base.constants import GeneratingProcedure as GP, TypeStructure as TS, \
    TermType, aggregation_functions, VizType as VT
from dive.worker.visualization.marginal_spec_functions import elementwise_functions, binning_procedures


from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def single_ct(c_field, t_field):
    logger.debug('Single C Single T - %s, %s', c_field['name'], t_field['name'])
    specs = []

    return specs


def single_cq(c_field, q_field):
    specs = []
    c_label = c_field['name']
    q_label = q_field['name']

    if c_field['is_unique']:
        spec = {
            'case': 'single_cq',
            'generating_procedure': GP.VAL_VAL.value,
            'type_structure': TS.C_Q.value,
            'viz_types': [ VT.BAR.value, VT.TREE.value, VT.PIE.value ],
            'field_ids': [ c_field['id'], q_field['id'] ],
            'args': {
                'field_a': c_field,
                'field_b': q_field,
            },
            'meta': {
                'desc': '%s vs. %s' % (c_label, q_label),
                'construction': [
                    { 'string': c_label, 'type': TermType.FIELD.value },
                    { 'string': 'vs.', 'type': TermType.PLAIN.value },
                    { 'string': q_label, 'type': TermType.FIELD.value },
                ],
                'labels': {
                    'x': c_label,
                    'y': q_label
                }
            }
        }
        specs.append(spec)
    else:
    #     for agg_fn in aggregation_functions.keys():
    #         if agg_fn == 'count':
    #             continue

    #         spec = {
    #             'case': 'single_cq',
    #             'generating_procedure': GP.VAL_AGG.value,
    #             'type_structure': TS.C_Q.value,
    #             'viz_types': [ VT.BAR.value ],
    #             'field_ids': [ c_field['id'], q_field['id'] ],
    #             'args': {
    #                 'agg_fn': agg_fn,
    #                 'grouped_field': c_field,
    #                 'agg_field': q_field,
    #             },
    #             'meta': {
    #                 'desc': '%s of %s by %s' % (agg_fn, q_label, c_label),
    #                 'construction': [
    #                     { 'string': agg_fn, 'type': TermType.OPERATION.value },
    #                     { 'string': 'of', 'type': TermType.PLAIN.value },
    #                     { 'string': q_label, 'type': TermType.FIELD.value },
    #                     { 'string': 'by', 'type': TermType.OPERATION.value },
    #                     { 'string': c_label, 'type': TermType.FIELD.value },
    #                 ],
    #                 'labels': {
    #                     'x': c_label,
    #                     'y': '%s of %s' % (agg_fn, q_label),
    #                 }
    #             }
    #         }
    #         specs.append(spec)

        spec = {
            'case': 'single_cq',
            'generating_procedure': GP.VAL_BOX.value,
            'type_structure': TS.C_Q.value,
            'viz_types': [ VT.BOX.value ],
            'field_ids': [ c_field['id'], q_field['id'] ],
            'args': {
                'grouped_field': c_field,
                'boxed_field': q_field
            },
            'meta': {
                'desc': 'Distribution of %s grouped by %s' % (q_label, c_label),
                'construction': [
                    { 'string': 'Distribution', 'type': TermType.OPERATION.value },
                    { 'string': 'of', 'type': TermType.PLAIN.value },
                    { 'string': q_label, 'type': TermType.FIELD.value },
                    { 'string': 'by', 'type': TermType.OPERATION.value },
                    { 'string': c_label, 'type': TermType.FIELD.value },
                ],
                'labels': {
                    'x': c_label,
                    'y': 'Distribution of %s' % (q_label),
                }
            }
        }
        specs.append(spec)
    return specs


def single_tq(t_field, q_field):
    t_label = t_field['name']
    q_label = q_field['name']

    specs = []

    # Raw time vs. value
    if t_field['is_unique']:
        raw_time_series_spec = {
            'case': 'single_tq',
            'generating_procedure': GP.VAL_VAL.value,
            'type_structure': TS.T_Q.value,
            'viz_types': [ VT.LINE.value, VT.SCATTER.value ],
            'field_ids': [ t_field['id'], q_field['id'] ],
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
                ],
                'labels': {
                    'x': t_label,
                    'y': q_label
                }
            }
        }
        specs.append(raw_time_series_spec)

    for agg_fn in aggregation_functions.keys():
        if agg_fn in ['mean', 'count', 'sum']:
            aggregated_time_series_spec_on_value = {
                'case': 'single_tq',
                'generating_procedure': GP.VAL_AGG.value,
                'type_structure': TS.T_Q.value,
                'viz_types': [ VT.LINE.value, VT.SCATTER.value ],
                'field_ids': [ t_field['id'], q_field['id'] ],
                'args': {
                    'agg_fn': agg_fn,
                    'grouped_field': t_field,
                    'agg_field': q_field
                },
                'meta': {
                    'desc': '%s of %s by %s' % (agg_fn, t_label, q_label),
                    'construction': [
                        { 'string': agg_fn.capitalize(), 'type': TermType.OPERATION.value },
                        { 'string': 'of', 'type': TermType.PLAIN.value },
                        { 'string': q_label, 'type': TermType.FIELD.value },
                        { 'string': 'by', 'type': TermType.OPERATION.value },
                        { 'string': t_label, 'type': TermType.FIELD.value },
                    ],
                    'labels': {
                        'x': t_label,
                        'y': q_label,
                    }
                }
            }
            specs.append(aggregated_time_series_spec_on_value)

    logger.debug('Single TQ: %s specs', len(specs))
    return specs


def single_ctq(c_field, t_field, q_field):
    specs = []
    c_label = c_field['name']
    t_label = t_field['name']
    q_label = q_field['name']

    for agg_fn in aggregation_functions.keys():
        if agg_fn in ['mean', 'count', 'sum']:
            aggregated_time_series_spec_on_value = {
                'case': 'single_ctq',
                'generating_procedure': GP.MULTIGROUP_AGG.value,
                'type_structure': TS.liC_liQ.value,
                'viz_types': [ VT.LINE.value, VT.SCATTER.value ],
                'field_ids': [ t_field['id'], q_field['id'] ],
                'args': {
                    'agg_fn': agg_fn,
                    'grouped_field_a': t_field,
                    'grouped_field_b': c_field,
                    'agg_field': q_field
                },
                'meta': {
                    'desc': '%s of %s by %s and %s' % (agg_fn, q_label, c_label, t_label),
                    'construction': [
                        { 'string': agg_fn.capitalize(), 'type': TermType.OPERATION.value },
                        { 'string': 'of', 'type': TermType.PLAIN.value },
                        { 'string': q_label, 'type': TermType.FIELD.value },
                        { 'string': 'by', 'type': TermType.OPERATION.value },
                        { 'string': c_label, 'type': TermType.FIELD.value },
                        { 'string': 'and', 'type': TermType.OPERATION.value },
                        { 'string': t_label, 'type': TermType.FIELD.value },
                    ],
                    'labels': {
                        'x': t_label,
                        'y': q_label,
                    }
                }
            }
            specs.append(aggregated_time_series_spec_on_value)

    logger.debug('Single CTQ: %s specs', len(specs))
    return specs
