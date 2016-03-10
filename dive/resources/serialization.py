import pandas as pd
import numpy as np

from time import time
from math import isnan, isinf
import json

from flask import current_app

import logging
logger = logging.getLogger(__name__)


def object_handler(obj):
    if hasattr(obj, 'isoformat'):
       return obj.isoformat()


def jsonify(args, status=200):
    args = pre_serialize(args)
    json_string = json.dumps(
        args,
        allow_nan=False,
        default=object_handler,
        check_circular=False
    )
    return current_app.response_class(json_string, mimetype='application/json', status=status)


def to_camel_case(snake_str):
    if '_' in snake_str:
        components = snake_str.split('_')
        return components[0] + "".join(x.title() for x in components[1:])
    else:
        return snake_str


def replace_unserializable_numpy(obj):
    if isinstance(obj, dict):
        return dict((k, replace_unserializable_numpy(v)) for k, v in obj.items())
    elif isinstance(obj, np.float32) or isinstance(obj, np.float64):
        if isnan(obj) or isinf(obj):
            return None
        return obj.item()
    elif isinstance(obj, float) or isinstance(obj, int):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (np.ndarray, list, tuple)):
        return map(replace_unserializable_numpy, obj)
    elif isinstance(obj,(pd.DataFrame, pd.Series)):
        return replace_unserializable_numpy(obj.to_dict())
    elif obj == None:
        return None
    elif isinstance(obj, str) or isinstance(obj, unicode) or isinstance(obj.keys()[0], unicode):
        return obj.replace('nan', 'null').replace('NaN', 'null')
    else:
        return obj


def pre_serialize(obj):
    if isinstance(obj, dict):
        if obj:
            first_key = obj.keys()[0]
            if isinstance(first_key, basestring):
                return dict((to_camel_case(k), pre_serialize(v)) for k, v in obj.iteritems())
        else:
            return obj
    elif isinstance(obj, list) or isinstance(obj, tuple):
        return map(pre_serialize, obj)
    elif isinstance(obj, float):
        # Faster than np.isnan
        if isnan(obj) or isinf(obj):
            return None
        else:
            return obj
    elif isinstance(obj, np.ndarray):
        return map(pre_serialize, obj)
    return obj
