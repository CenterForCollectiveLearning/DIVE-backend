import json
import pandas as pd
import numpy as np
from math import isnan, isinf
from flask import current_app


def to_camel_case(snake_str):
    if '_' in snake_str:
        components = snake_str.split('_')
        return components[0] + "".join(x.title() for x in components[1:])
    else:
        return snake_str


def object_handler(obj):
    if hasattr(obj, 'isoformat'):
       return obj.isoformat()


def replace_unserializable_numpy(obj):
    if isinstance(obj, dict):
        return dict((k, replace_unserializable_numpy(v)) for k, v in obj.items())
    elif isinstance(obj, (np.ndarray, list, tuple)):
        return map(replace_unserializable_numpy, obj)
    elif isinstance(obj, (pd.DataFrame, pd.Series)):
        return replace_unserializable_numpy(obj.to_dict())
    elif isinstance(obj, float) or isinstance(obj, int):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    elif isinstance(obj, np.float32) or isinstance(obj, np.float64):
        if isnan(obj) or isinf(obj):
            return None
        return obj.item()
    elif isinstance(obj, str) or isinstance(obj, unicode):
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
    elif (obj is pd.NaT):
        return None
    return obj


def jsonify(args, status=200):
    args = pre_serialize(args)
    json_string = json.dumps(
        args,
        allow_nan=False,
        default=object_handler,
        check_circular=False
    )
    return current_app.response_class(json_string, mimetype='application/json', status=status)


# Use everywhere, including in writing to Postgres?
# http://stackoverflow.com/questions/21631878/celery-is-there-a-way-to-write-custom-json-encoder-decoder
from datetime import datetime
class DiveEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                '__type__': '__datetime__',
                'epoch': int(mktime(obj.timetuple()))
            }
        else:
            return json.JSONEncoder.default(self, obj)

def dive_decoder(obj):
    if '__type__' in obj:
        if obj['__type__'] == '__datetime__':
            return datetime.fromtimestamp(obj['epoch'])
    return obj

# Encoder function
def dive_json_dumps(obj):
    return json.dumps(obj,
        cls=DiveEncoder,
        allow_nan=False,
        default=object_handler,
        check_circular=False
    )

# Decoder function
def dive_json_loads(obj):
    return json.loads(obj, object_hook=dive_decoder)
