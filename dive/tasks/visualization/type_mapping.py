from dive.tasks.visualization import GeneratingProcedure, TypeStructure

import logging
logger = logging.getLogger(__name__)

viz_types = ['tree', 'bar', 'scatter', 'hist', 'pie', 'network']

# TODO Come up with a better name for this...
data_types_to_viz_types = {
    'c:q': ['tree', 'pie', 'bar'],
    '[c]:q': ['tree', 'pie', 'bar'],
    'q:q': ['scatter', 'line'],
    'b:q': ['hist'],
    'c:c': ['network'],
    '[c]:q': ['network'],
    '[c]:[q]': ['network'],
    'c:[q]': ['line']
}

def get_viz_types_from_spec(spec):
    viz_types = data_types_to_viz_types[spec['type_structure']]
    return viz_types
