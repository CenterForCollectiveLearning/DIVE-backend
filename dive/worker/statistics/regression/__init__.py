from enum import Enum

class ModelRecommendationType(Enum):
    LASSO = u'LASSO'
    FORWARD_R2 = u'FORWARD_R2'


class ModelCompletionType(Enum):
    LEAVE_ONE_OUT = u'leaveOneOut'
    ALL_VARIABLES = u'all'
    ONE_AT_A_TIME = u'oneAtATime'
