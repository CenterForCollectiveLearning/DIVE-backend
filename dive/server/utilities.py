import json
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

def jsonify(args, status=200):
    args = pre_serialize(args)
    json_string = json.dumps(
        args,
        allow_nan=False,
        default=object_handler,
        check_circular=False
    )
    return current_app.response_class(json_string, mimetype='application/json', status=status)
