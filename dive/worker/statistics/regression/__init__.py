from enum import Enum

class ModelRecommendationType(Enum):
    LASSO = 'LASSO'
    FORWARD_R2 = 'FORWARD_R2'


class ModelCompletionType(Enum):
    ALL_BUT_ONE = 'ALL_BUT_ONE'
    ALL_VARIABLES = 'ALL_VARIABLES'
    ONE_AT_A_TIME_AND_ALL_BUT_ONE = 'ONE_AT_A_TIME_AND_ALL_BUT_ONE'
