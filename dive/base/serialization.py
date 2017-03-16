import json
import numpy as np
from flask import current_app
import pandas.json as pjson
from datetime import date, datetime


def string_to_camel_case(snake_str):
    if '_' in snake_str:
        components = snake_str.split('_')
        return components[0] + ''.join(x.title() for x in components[1:])
    else:
        return snake_str


def dict_to_camel_case(obj):
    if isinstance(obj, dict):
        return dict((string_to_camel_case(k), dict_to_camel_case(v)) for k, v in obj.iteritems())
    elif isinstance(obj, list) or isinstance(obj, tuple):
        return map(dict_to_camel_case, obj)
    elif isinstance(obj, date) or isinstance(obj, datetime):
        return obj.isoformat(' ')
    else:
        return obj


def dates_to_iso(obj):
    if isinstance(obj, dict):
        return dict((k, dates_to_iso(v)) for k, v in obj.iteritems())
    elif isinstance(obj, list) or isinstance(obj, tuple):
        return map(dates_to_iso, obj)
    elif isinstance(obj, date) or isinstance(obj, datetime):
        return obj.isoformat(' ')
    else:
        return obj


def jsonify(obj, status=200):
    json_string = pjson.dumps(dict_to_camel_case(obj))
    return current_app.response_class(json_string, mimetype='application/json', status=status)


# Custom AMQP json encoding
# http://stackoverflow.com/questions/21631878/celery-is-there-a-way-to-write-custom-json-encoder-decoder
# Encoder function
def pjson_dumps(obj):
    return pjson.dumps(dates_to_iso(obj))

# Decoder function
def pjson_loads(s):
    return pjson.loads(str(s))
