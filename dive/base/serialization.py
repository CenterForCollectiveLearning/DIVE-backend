import json
from flask import current_app
import pandas.json as pjson


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
    else:
        return obj


def jsonify(obj, status=200):
    json_string = pjson.dumps(dict_to_camel_case(obj))
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
