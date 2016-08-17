from itertools import combinations
from dive.worker.visualization import GeneratingProcedure as GP, TypeStructure as TS, \
    TermType, aggregation_functions, VizType as VT
from dive.worker.visualization.marginal_spec_functions import elementwise_functions, binning_procedures


from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def multi_ct(c_fields, t_fields):
    specs = []
    logger.debug('Multi C Multi T: %s specs', len(specs))
    return specs


def multi_cq(c_fields, q_fields):
    specs = []

    # TODO How do you deal with this?
    # Two-field val:val:[q] with quantitative data
    for (c_field_a, c_field_b) in combinations(c_fields, 2):
        c_label_a, c_label_b = c_field_a['name'], c_field_b['name']
        q_labels = [ f['name'] for f in q_fields ]
        q_field_ids = [ f['id'] for f in q_fields ]
        spec = {
            'case': 'multi_cq',
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
    logger.debug('Multi C Multi Q: %s specs', len(specs))
    return specs


def multi_tq(t_fields, q_fields):
    specs = []
    logger.debug('Multi T Multi Q: %s specs', len(specs))
    return specs


def multi_ctq(c_fields, t_fields, q_fields):
    specs = []
    logger.debug('Multi C Multi T Multi Q: %s specs', len(specs))
    return specs
