from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.utilities import format_json
from dive.tasks.ingestion.field_properties import compute_field_properties

import logging
logger = logging.getLogger(__name__)

fieldPropertiesGetParser = reqparse.RequestParser()
fieldPropertiesGetParser.add_argument('project_id', type=str, required=True)
fieldPropertiesGetParser.add_argument('dataset_id', type=str, required=True)
class FieldProperties(Resource):
    '''
    Args: project_id, dataset_id
    Returns: properties corresponding to that dataset_id
    '''
    def get(self):
        args = fieldPropertiesGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')
        dataset_id = args.get('dataset_id')

        field_properties = db_access.get_field_properties(project_id, dataset_id)
        return make_response(jsonify(format_json({"properties": field_properties})))
