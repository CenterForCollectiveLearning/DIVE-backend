from enum import Enum
import numpy as np

class GeneratingProcedure(Enum):
    AGG = 'agg'
    IND_VAL = 'ind:val'
    VAL_COUNT = 'val:count'
    BIN_AGG = 'bin:agg'
    BIN_COUNT = 'bin:count'
    VAL_VAL = 'val:val'
    VAL_AGG = 'val:agg'
    AGG_AGG = 'agg:agg'
    VAL_VAL_Q = 'val:val:q'


class TypeStructure(Enum):
    C = 'c'
    Q = 'q'
    liC = '[c]'
    liQ = '[q]'
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

specific_to_general_type = {
    'float': 'q',
    'integer': 'q',
    'string': 'c',
    'continent': 'c',
    'countryName': 'c',
    'countryCode2': 'c',
    'countryCode3': 'c',
    'decimal': 'q',
    'datetime': 'q',
}

aggregation_functions = {
    'sum': np.sum,
    'min': np.min,
    'max': np.max,
    'mean': np.mean,
    'count': np.size
}
