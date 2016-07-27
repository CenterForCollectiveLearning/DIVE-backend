from enum import Enum

class ModelSelectionType(Enum):
    ALL_BUT_ONE = 'ALL_BUT_ONE'
    FORWARD_R2 = 'stepwise'
    LASSO = 'lasso'
    RIDGE = 'ridge'
    LARS = 'lars'
