from itertools import combinations
from dive.base.constants import GeneratingProcedure as GP, TypeStructure as TS, \
    TermType, aggregation_functions, VizType as VT, Scale
from dive.worker.visualization.marginal_spec_functions import elementwise_functions, binning_procedures
from dive.worker.visualization.marginal_spec_functions.single_field_multi_type_specs import single_cq

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def multi_c(c_fields):
    logger.debug("F: Multi C")
    specs = []

    # Count of one field given another
    # E.g. count of position by gender
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['name'], c_field_b['name']
        if (c_field_a['is_unique'] or c_field_b['is_unique']):
            continue
        spec_1 = {
            'generating_procedure': GP.MULTIGROUP_COUNT.value,
            'type_structure': TS.liC_Q.value,
            'viz_types': [ VT.STACKED_BAR.value, VT.GRID.value ],
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
                ],
                'labels': {
                    'x': 'Grouping by %s then %s' % (c_label_a, c_label_b),
                    'y': 'Count'
                },
            }
        }
        spec_2 = {
            'generating_procedure': GP.MULTIGROUP_COUNT.value,
            'type_structure': TS.liC_Q.value,
            'viz_types': [ VT.STACKED_BAR.value, VT.GRID.value ],
            'field_ids': [ c_field_a['id'], c_field_b['id'] ],

            'args': {
                'field_a': c_field_b,
                'field_b': c_field_a,
            },
            'meta': {
                'desc': 'Count by %s then %s' % (c_label_b, c_label_a),
                'construction': [
                    { 'string': 'Count', 'type': TermType.OPERATION.value },
                    { 'string': 'by', 'type': TermType.PLAIN.value },
                    { 'string': c_label_b, 'type': TermType.FIELD.value },
                    { 'string': 'then', 'type': TermType.PLAIN.value },
                    { 'string': c_label_a, 'type': TermType.FIELD.value },
                ],
                'labels': {
                    'x': 'Grouping by %s then %s' % (c_label_b, c_label_a),
                    'y': 'Count'
                },
            }
        }
        specs.append(spec_1)
        specs.append(spec_2)
    return specs


def multi_q(q_fields):
    logger.debug("B: Multi Q - %s", [f['name'] for f in q_fields])
    specs = []

    #Function on pairs of columns
    for (q_field_a, q_field_b) in combinations(q_fields, 2):
        q_label_a = q_field_a['name']
        q_label_b = q_field_b['name']
        q_scale_a = q_field_a['scale']
        q_scale_b = q_field_b['scale']

        if (q_scale_a == Scale.CONTINUOUS.value and q_scale_b == Scale.CONTINUOUS.value):
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
                    ],
                    'labels': {
                        'x': q_label_a,
                        'y': q_label_b
                    },
                }
            }
            specs.append(raw_comparison_spec)

        if (q_scale_a in [Scale.ORDINAL.value, Scale.NOMINAL.value] and q_scale_b == Scale.CONTINUOUS.value):
            specs.extend(single_cq(q_field_a, q_field_b))
        if (q_scale_a == Scale.CONTINUOUS.value and q_scale_b in [Scale.ORDINAL.value, Scale.NOMINAL.value]):   
            specs.extend(single_cq(q_field_b, q_field_a))
        if (q_scale_a in [Scale.ORDINAL.value, Scale.NOMINAL.value] and q_scale_b in [Scale.ORDINAL.value, Scale.NOMINAL.value]): 
            specs.extend(multi_c([q_field_a, q_field_b]))

    return specs


def multi_t(t_fields):
    logger.debug('Multi T - %s', [f['name'] for f in t_fields])
    specs = []
    return specs
