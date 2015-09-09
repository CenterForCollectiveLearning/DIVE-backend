from enum import Enum


class GeneratingProcedure(Enum):
    IND_VAL = 'ind:val'
    VAL_COUNT = 'val:count'
    BIN_AGG = 'bin:agg'
    VAL_VAL = 'val:val'
    VAL_AGG = 'val:agg'
    AGG_AGG = 'agg:agg'
    VAL_VAL_Q = 'val:val:q'


class TypeStructure(Enum):
    C_C = 'c:c'
    C_Q = 'c:q'
    liC_Q = '[c]:q'
    liQ_liQ = '[c]:[q]'
    Q_Q = 'q:q'
    Q_liQ = 'q:[q]'
    B_Q = 'b:q'
