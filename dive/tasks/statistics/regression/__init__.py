from enum import Enum

class ModelSelectionType(Enum):
    ALL_BUT_ONE = 'ALL_BUT_ONE'
    LASSO = 'LASSO'
    FORWARD_R2 = 'FORWARD_R2'
