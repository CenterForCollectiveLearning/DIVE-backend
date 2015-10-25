import pandas as pd
import numpy as np

import logging
logger = logging.getLogger(__name__)


class RoundedFloat(float):
    def __repr__(self):
        return '%.3f' % self


def to_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0] + "".join(x.title() for x in components[1:])


def replace_unserializable_numpy(obj):
    if isinstance(obj, dict):
        return dict((k, replace_unserializable_numpy(v)) for k, v in obj.items())
    elif isinstance(obj, np.float32) or isinstance(obj, np.float64):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj.item()
    elif isinstance(obj, (np.ndarray, list, tuple)):
        return map(replace_unserializable_numpy, obj)
    elif isinstance(obj,(pd.DataFrame, pd.Series)):
        return replace_unserializable_numpy(obj.to_dict())
    elif isinstance(obj, str):
        return obj.replace('nan', 'null').replace('NaN', 'null')
    return obj

def format_json(obj):
    if isinstance(obj, dict):
        return dict((to_camel_case(k), format_json(v)) for k, v in obj.items())
    if isinstance(obj, np.float32) or isinstance(obj, np.float64):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return RoundedFloat(obj.item())
    elif isinstance(obj, float):
        return RoundedFloat(obj)
    elif isinstance(obj, (np.ndarray, list, tuple)):
        return map(format_json, obj)
    elif isinstance(obj,(pd.DataFrame, pd.Series)):
        return format_json(obj.to_dict())
    elif isinstance(obj, str):
        return obj.replace('nan', 'null').replace('NaN', 'null')
    return obj
