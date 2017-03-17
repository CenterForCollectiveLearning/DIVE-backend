import json
import numpy as np
import pandas as pd
from flask import current_app
import pandas.json as pjson
from datetime import date, datetime

def string_to_camel_case(snake_str):
    if '_' in snake_str:
        components = snake_str.split('_')
        return components[0] + ''.join(x.title() for x in components[1:])
    else:
        return snake_str


noop = lambda x: x
def format_json(obj, camel_case=False):
    casing_function = string_to_camel_case if camel_case else noop
    if isinstance(obj, dict):
        return dict((casing_function(k), format_json(v, camel_case=camel_case)) for k, v in obj.iteritems())
    elif isinstance(obj, list) or isinstance(obj, tuple) or isinstance(obj, np.ndarray):
        return [ format_json(e, camel_case=camel_case) for e in obj ]
    elif isinstance(obj, date) or isinstance(obj, datetime):
        return obj.isoformat(' ')
    elif isinstance(obj, np.datetime64):
        return pd.to_datetime(str(obj)).isoformat(' ')
    else:
        return obj


def jsonify(obj, status=200):
    json_string = pjson.dumps(format_json(obj, camel_case=True))
    return current_app.response_class(json_string, mimetype='application/json', status=status)


# Custom AMQP json encoding
# http://stackoverflow.com/questions/21631878/celery-is-there-a-way-to-write-custom-json-encoder-decoder
# Encoder function
def pjson_dumps(obj):
    return pjson.dumps(format_json(obj, camel_case=False))


# Decoder function
def pjson_loads(s):
    return pjson.loads(str(s))
