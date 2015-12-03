'''
Endpoints for updating fields
'''
import os
import json
from flask import request, make_response, jsonify
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.utilities import format_json

import logging
logger = logging.getLogger(__name__)


def object_type(j):
    return j


fieldPostParser = reqparse.RequestParser()
fieldPostParser.add_argument('project_id', type=str, required=True, location='json')
fieldPostParser.add_argument('type', type=object_type, required=True, location='json')
class Field(Resource):
    def post(self, field_id):
        args = fieldPostParser.parse_args()
        project_id = args.get('project_id')
        type = args.get('type')

        field_property_document = db_access.update_field_properties_type_by_id(project_id, field_id, type)
        return make_response(jsonify(format_json(field_property_document)))
