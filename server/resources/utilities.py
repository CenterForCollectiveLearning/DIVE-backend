import pandas as pd
import numpy as np

from core import logger

class RoundedFloat(float):
    def __repr__(self):
        return '%.3f' % self

def format_json(obj):
    if isinstance(obj, np.float32) or isinstance(obj, np.float64):
        return obj.item()
    elif isinstance(obj, float):
        return RoundedFloat(obj)
    elif isinstance(obj, dict):
        return dict((k, format_json(v)) for k, v in obj.items())
    elif isinstance(obj, (np.ndarray, list, tuple)):
        return map(format_json, obj)
    elif isinstance(obj,(pd.DataFrame,pd.Series)):
        return format_json(obj.to_dict())
    return obj
