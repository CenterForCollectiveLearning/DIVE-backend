from dive.worker.visualization.constants import TypeStructure as TS

import logging
logger = logging.getLogger(__name__)

viz_types = ['tree', 'bar', 'scatter', 'hist', 'pie', 'network']

# TODO Come up with a better name for this...
data_types_to_viz_types = {
    TS.C.value: ['string'],
    TS.Q.value: ['number'],
    TS.liC.value: ['table'],
    TS.liQ.value: ['table', 'line'],
    TS.C_C.value: ['network'],
    TS.C_Q.value: ['tree', 'pie', 'bar'],
    TS.Q_Q.value: ['scatter', 'line'],
    TS.B_Q.value: ['hist'],
    TS.liC_Q.value: ['network'],
    TS.liC_liQ: ['line']
}

def get_viz_types_from_spec(spec):
    viz_types = data_types_to_viz_types[spec['type_structure']]
    return viz_types
