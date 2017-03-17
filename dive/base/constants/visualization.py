from enum import Enum
import numpy as np

class GeneratingProcedure(Enum):
    AGG = 'agg'
    IND_VAL = 'ind:val'
    VAL_COUNT = 'val:count'
    VAL_BOX = 'val:box'
    BIN_AGG = 'bin:agg'
    BIN_COUNT = 'bin:count'
    VAL_VAL = 'val:val'
    VAL_AGG = 'val:agg'
    AGG_AGG = 'agg:agg'
    VAL_VAL_Q = 'val:val:q'
    MULTIGROUP_AGG = 'multigroup:agg'
    MULTIGROUP_COUNT = 'multigroup:count'

# TODO Remove this? Doesn't really make sense anymore
class TypeStructure(Enum):
    C = 'c'
    Q = 'q'
    liC = '[c]'
    liQ = '[q]'
    T_Q = 't:q'
    C_C = 'c:c'
    C_Q = 'c:q'
    liC_Q = '[c]:q'
    liQ_liQ = '[q]:[q]'
    liC_liQ = '[c]:[q]'
    Q_Q = 'q:q'
    Q_liQ = 'q:[q]'
    B_Q = 'b:q'


class QuantitativeFunction(Enum):
    AUTO = 'auto'
    RAW = 'raw'
    SUM = 'sum'
    MEAN = 'mean'
    MEDIAN = 'median'
    MIN = 'min'
    MAX = 'min'
    BIN = 'bin'


class TemporalFunction(Enum):
    AUTO = 'auto'
    RAW = 'raw'
    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'
    DATE = 'date'
    HOURS = 'hours'
    MINUTES = 'minutes'
    SECONDS = 'seconds'
    MEAN = 'mean'
    MEDIAN = 'median'
    MIN = 'min'
    MAX = 'max'


class VizType(Enum):
    TREE = 'tree'
    GRID = 'grid'
    BOX = 'box'
    PIE = 'pie'
    SCATTER = 'scatter'
    LINE = 'line'
    NETWORK = 'network'
    HIST = 'hist'
    BAR = 'bar'
    STACKED_BAR = 'bar'


class TermType(Enum):
    '''
    List of terms in natural language visualization description

        Plain: and, by
        Field: binned_field, field_a
        Operation: aggregate, group by
        Transformation: binned
    '''
    PLAIN = 'plain'
    FIELD = 'field'
    TRANSFORMATION = 'transformation'
    OPERATION = 'operation'


aggregation_functions = {
    'sum': np.sum,
    'min': np.min,
    'max': np.max,
    'mean': np.mean,
    'count': np.size
}
