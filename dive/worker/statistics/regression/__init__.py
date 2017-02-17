from enum import Enum

class ModelRecommendationType(Enum):
    LASSO = u'lasso'
    FORWARD_R2 = u'forwardR2'
    RFE = u'rfe'
    FORWARD_F = u'forwardF'


class ModelCompletionType(Enum):
    LEAVE_ONE_OUT = u'leaveOneOut'
    ALL_VARIABLES = u'all'
    ONE_AT_A_TIME = u'oneAtATime'
